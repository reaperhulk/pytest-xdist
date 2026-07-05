from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from itertools import islice
import time
from typing import NamedTuple

import pytest

from xdist.remote import Producer
from xdist.report import report_collection_diff
from xdist.workermanage import parse_tx_spec_config
from xdist.workermanage import WorkerController


class NodePending(NamedTuple):
    node: WorkerController
    pending: deque[int]


# Every worker needs at least 2 tests in queue - the current and the next one.
MIN_PENDING = 2


class WorkStealingScheduling:
    """Implement work-stealing scheduling.

    Initially, tests are distributed evenly among all nodes.

    When some node completes most of its assigned tests (when only one pending
    test remains), an attempt is made to reassign ("steal") some tests from
    other nodes to this node.

    Attributes::

    :numnodes: The expected number of nodes taking part.  The actual
       number of nodes will vary during the scheduler's lifetime as
       nodes are added by the DSession as they are brought up and
       removed either because of a dead node or normal shutdown.  This
       number is primarily used to know when the initial collection is
       completed.

    :node2collection: Map of nodes and their test collection.  All
       collections should always be identical.

    :node2pending: Map of nodes and the indices of their pending
       tests.  The indices are an index into ``.pending`` (which is
       identical to their own collection stored in
       ``.node2collection``).

    :collection: The one collection once it is validated to be
       identical between all the nodes.  It is initialised to None
       until ``.schedule()`` is called.

    :pending: List of indices of globally pending tests.  These are
       tests which have not yet been allocated to a chunk for a node
       to process.

    :log: A py.log.Producer instance.

    :config: Config object, used for handling hooks.

    :steal_requested_from_node: The node to which the current "steal" request
       was sent. ``None`` if there is no request in progress. Only one request
       can be in progress at any time, the scheduler doesn't send multiple
       simultaneous requests.
    """

    def __init__(self, config: pytest.Config, log: Producer | None = None) -> None:
        self.numnodes = len(parse_tx_spec_config(config))
        self.node2collection: dict[WorkerController, list[str]] = {}
        self.node2pending: dict[WorkerController, deque[int]] = {}
        self.pending: list[int] = []
        self.collection: list[str] | None = None
        self._collection_index: dict[str, int] | None = None
        if log is None:
            self.log = Producer("workstealsched")
        else:
            self.log = log.workstealsched
        self.config = config
        self.steal_requested_from_node: WorkerController | None = None
        # steal statistics, reported in the verbose terminal summary
        self.steal_requests = 0
        self.steal_requests_failed = 0
        self.steal_deferred = 0
        self.tests_stolen = 0
        self.steal_in_flight_total = 0.0
        self._steal_sent_at = 0.0

    @property
    def nodes(self) -> list[WorkerController]:
        """A list of all nodes in the scheduler."""
        return list(self.node2pending.keys())

    @property
    def collection_is_completed(self) -> bool:
        """Boolean indication initial test collection is complete.

        This is a boolean indicating all initial participating nodes
        have finished collection.  The required number of initial
        nodes is defined by ``.numnodes``.
        """
        return len(self.node2collection) >= self.numnodes

    @property
    def tests_finished(self) -> bool:
        """Return True if all tests have been executed by the nodes."""
        if not self.collection_is_completed:
            return False
        if self.pending:
            return False
        if self.steal_requested_from_node is not None:
            return False
        for pending in self.node2pending.values():
            if len(pending) >= MIN_PENDING:
                return False
        return True

    @property
    def has_pending(self) -> bool:
        """Return True if there are pending test items.

        This indicates that collection has finished and nodes are
        still processing test items, so this can be thought of as
        "the scheduler is active".
        """
        if self.pending:
            return True
        for pending in self.node2pending.values():
            if pending:
                return True
        return False

    def add_node(self, node: WorkerController) -> None:
        """Add a new node to the scheduler.

        From now on the node will be allocated chunks of tests to
        execute.

        Called by the ``DSession.worker_workerready`` hook when it
        successfully bootstraps a new node.
        """
        assert node not in self.node2pending
        self.node2pending[node] = deque()

    def add_node_collection(
        self, node: WorkerController, collection: Sequence[str]
    ) -> None:
        """Add the collected test items from a node.

        The collection is stored in the ``.node2collection`` map.
        Called by the ``DSession.worker_collectionfinish`` hook.
        """
        assert node in self.node2pending
        if self.collection_is_completed:
            # A new node has been added later, perhaps an original one died.
            # .schedule() should have
            # been called by now
            assert self.collection
            if collection != self.collection:
                other_node = next(iter(self.node2collection.keys()))
                msg = report_collection_diff(
                    self.collection, collection, other_node.gateway.id, node.gateway.id
                )
                self.log(msg)
                return
            self.node2collection[node] = self.collection
            return
        # All collections are expected to be identical, so share the first
        # received one instead of retaining one copy per node. Mismatching
        # collections keep their own copy so that ``.schedule()`` can report
        # the difference.
        if self.node2collection:
            canonical = next(iter(self.node2collection.values()))
            if collection == canonical:
                self.node2collection[node] = canonical
                return
        self.node2collection[node] = list(collection)

    def mark_test_complete(
        self, node: WorkerController, item_index: int, duration: float | None = None
    ) -> None:
        """Mark test item as completed by node.

        This is called by the ``DSession.worker_testreport`` hook.
        """
        pending = self.node2pending[node]
        if pending and pending[0] == item_index:
            pending.popleft()
        else:
            pending.remove(item_index)
        self.check_schedule()

    def mark_test_pending(self, item: str) -> None:
        assert self.collection is not None
        if self._collection_index is None:
            self._collection_index = {
                nodeid: index for index, nodeid in enumerate(self.collection)
            }
        self.pending.insert(
            0,
            self._collection_index[item],
        )
        self.check_schedule()

    def remove_pending_tests_from_node(
        self,
        node: WorkerController,
        indices: Sequence[int],
    ) -> None:
        """Node returned some test indices back in response to 'steal' command.

        This is called by ``DSession.worker_unscheduled``.
        """
        assert node is self.steal_requested_from_node
        self.steal_requested_from_node = None
        self.steal_in_flight_total += time.perf_counter() - self._steal_sent_at
        if indices:
            self.tests_stolen += len(indices)
        else:
            self.steal_requests_failed += 1

        indices_set = set(indices)
        self.node2pending[node] = deque(
            i for i in self.node2pending[node] if i not in indices_set
        )
        self.pending.extend(indices)
        self.check_schedule()

    def check_schedule(self) -> None:
        """Reschedule tests/perform load balancing."""
        for node, pending in self.node2pending.items():
            if len(pending) < MIN_PENDING and not node.shutting_down:
                break
        else:
            return

        nodes_up = [
            NodePending(node, pending)
            for node, pending in self.node2pending.items()
            if not node.shutting_down
        ]

        def get_idle_nodes() -> list[WorkerController]:
            return [node for node, pending in nodes_up if len(pending) < MIN_PENDING]

        idle_nodes = get_idle_nodes()
        if not idle_nodes:
            return

        if self.pending:
            # Distribute pending tests evenly among idle nodes
            for i, node in enumerate(idle_nodes):
                nodes_remaining = len(idle_nodes) - i
                num_send = len(self.pending) // nodes_remaining
                self._send_tests(node, num_send)

            idle_nodes = get_idle_nodes()
            # No need to steal anything if all nodes have enough work to continue
            if not idle_nodes:
                return

        # Only one active stealing request is allowed
        if self.steal_requested_from_node is not None:
            # idle nodes exist but they have to wait for the current request
            self.steal_deferred += 1
            return

        # Find the node that has the longest test queue
        steal_from = max(
            nodes_up, key=lambda node_pending: len(node_pending.pending), default=None
        )

        if steal_from is None:
            num_steal = 0
        else:
            # Steal half of the test queue - but keep that node running too.
            # If the node has 2 or less tests queued, stealing will fail
            # anyway.
            max_steal = max(0, len(steal_from.pending) - MIN_PENDING)
            num_steal = min(len(steal_from.pending) // 2, max_steal)

        if num_steal == 0:
            # Can't get more work - shutdown idle nodes. This will force them
            # to run the last test now instead of waiting for more tests.
            for node in idle_nodes:
                node.shutdown()
            return

        assert steal_from is not None
        steal_tail = list(
            islice(steal_from.pending, len(steal_from.pending) - num_steal, None)
        )
        self.steal_requests += 1
        self._steal_sent_at = time.perf_counter()
        steal_from.node.send_steal(steal_tail)
        self.steal_requested_from_node = steal_from.node

    def remove_node(self, node: WorkerController) -> str | None:
        """Remove a node from the scheduler.

        This should be called either when the node crashed or at
        shutdown time.  In the former case any pending items assigned
        to the node will be re-scheduled.  Called by the
        ``DSession.worker_workerfinished`` and
        ``DSession.worker_errordown`` hooks.

        Return the item which was being executing while the node
        crashed or None if the node has no more pending items.
        """
        pending = self.node2pending.pop(node)

        # If node was removed without completing its assigned tests - it crashed
        if pending:
            assert self.collection is not None
            crashitem = self.collection[pending.popleft()]
        else:
            crashitem = None

        self.pending.extend(pending)

        # Dead node won't respond to "steal" request
        if self.steal_requested_from_node is node:
            self.steal_requested_from_node = None

        self.check_schedule()
        return crashitem

    def schedule(self) -> None:
        """Initiate distribution of the test collection.

        Initiate scheduling of the items across the nodes.  If this
        gets called again later it behaves the same as calling
        ``.check_schedule()`` on all nodes so that newly added nodes
        will start to be used.

        This is called by the ``DSession.worker_collectionfinish`` hook
        if ``.collection_is_completed`` is True.
        """
        assert self.collection_is_completed

        # Initial distribution already happened, reschedule on all nodes
        if self.collection is not None:
            self.check_schedule()
            return

        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        # Collections are identical, create the index of pending items.
        self.collection = next(iter(self.node2collection.values()))
        self.pending[:] = range(len(self.collection))
        if not self.collection:
            return

        self.check_schedule()

    def _send_tests(self, node: WorkerController, num: int) -> None:
        tests_per_node = self.pending[:num]
        if tests_per_node:
            del self.pending[:num]
            self.node2pending[node].extend(tests_per_node)
            node.send_runtest_some(tests_per_node)

    def _check_nodes_have_same_collection(self) -> bool:
        """Return True if all nodes have collected the same items.

        If collections differ, this method returns False while logging
        the collection differences and posting collection errors to
        pytest_collectreport hook.
        """
        node_collection_items = list(self.node2collection.items())
        first_node, col = node_collection_items[0]
        same_collection = True
        for node, collection in node_collection_items[1:]:
            msg = report_collection_diff(
                col, collection, first_node.gateway.id, node.gateway.id
            )
            if msg:
                same_collection = False
                self.log(msg)
                if self.config is not None:
                    rep = pytest.CollectReport(
                        nodeid=node.gateway.id,
                        outcome="failed",
                        longrepr=msg,
                        result=[],
                    )
                    self.config.hook.pytest_collectreport(report=rep)

        return same_collection
