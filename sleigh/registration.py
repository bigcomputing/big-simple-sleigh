#
# Copyright (c) 2008-2009, REvolution Computing, Inc.
#
# NetWorkSpaces is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
# USA
#
"""Worker registration tracker."""

import nwss
_DEBUG = nwss.config.is_debug_enabled('plugin:Sleigh:registration')

from twisted.python import log
import time

class WorkerRegistration(object):
    """Worker registration manager.  This class manages the semantics of worker
    registration and the join phase."""

    def __init__(self):
        self.__max_workers_frozen  = False
        self.__max_workers         = -1
        self.__next_rank           = 0
        self.__sentinels           = []
        self.__sentinels_by_rank   = []
        self.__workers             = []

    def __set_max_workers(self, num_workers):
        """Set the maximum expected number of workers."""
        self.__max_workers = num_workers

    def __get_max_workers(self):
        """Get the maximum expected number of workers."""
        return self.__max_workers
    max_workers = property(__get_max_workers, __set_max_workers)

    def get_num_expected_workers(self):
        """Get the number of currently expected workers.  If we've been given a
        "max" worker count, we optimistically expect them all to show up, but
        if we have not, we can only use the currently connected number of
        workers as an estimate."""
        if self.__max_workers != -1:
            return self.__max_workers
        else:
            return self.__next_rank
    num_expected_workers = property(get_num_expected_workers)

    def freeze_max_workers(self, may_reduce=False):
        """Freeze the current maximum expected number of workers, optionally
        reducing it to the number of currently registered workers."""
        if not self.__max_workers_frozen:
            self.__max_workers_frozen = True
            if may_reduce or self.__max_workers == -1:
                self.__max_workers = self.__next_rank

    def get_num_allocated_ranks(self):
        """Get the number of worker ids allocated by sentinels."""
        return self.__next_rank

    def get_num_sentinels(self):
        """Get the number of registered sentinels."""
        return len(self.__sentinels)

    def get_num_pending_workers(self):
        """Get the count of workers whose sentinels have checked in, but who
        have not themselves checked in."""
        return self.__workers.count(None)

    def get_num_dead_workers(self):
        """Get the count of workers which checked in, but have, for one reason
        or another, been declared dead."""
        return self.__workers.count('dead')

    def get_num_registered_workers(self):
        """Get the count of currently registered and allegedly alive
        workers.  That is, all 'registered' workers which are neither 'pending'
        nor 'dead'."""
        total_workers = len(self.__workers)
        alive_workers = total_workers
        alive_workers -= self.get_num_dead_workers()
        alive_workers -= self.get_num_pending_workers()
        return alive_workers
    num_registered_workers = property(get_num_registered_workers)

    def get_sentinel_for_worker_rank(self, rank):
        """Get the sentinel for a given worker rank."""
        return self.__sentinels_by_rank[rank]

    def get_sentinel_for_worker(self, worker):
        """Get the sentinel for a given worker."""
        return self.get_sentinel_for_worker_rank(worker.nwsWorkerRank)

    def get_workers_for_sentinel(self, sentinel):
        """Get the workers claimed by a given sentinel."""
        return [self.__workers[rank] for rank in sentinel.nwsWorkerRanks]

    def get_worker(self, rank):
        """Find a worker, given its rank."""
        if 0 <= rank < len(self.__workers):
            worker = self.__workers[rank]
            if isinstance(worker, str):
                return None
        else:
            worker = None
        return worker

    def is_worker_registered(self, rank):
        """Check if a given worker rank has been registered."""
        return self.get_worker(rank) is not None

    def is_worker_dead(self, rank):
        """Check if a given worker has registered and then died."""
        return self.get_worker(rank) == 'dead'

    def register_sentinel(self, sentinel):
        """Register a sentinel."""
        if not sentinel in self.__sentinels:
            sentinel.nwsSentinelRank = len(self.__sentinels)
            self.__sentinels.append(sentinel)

    def allocate_rank(self, sentinel):
        """Allocate a new worker rank, assigning it to the specified
        sentinel."""

        # If max workers is < 0 or > next_rank, we can allocate more worker
        # ranks; otherwise, we cannot.
        if 0 < self.__max_workers <= self.__next_rank:
            return -1

        rank = self.__next_rank
        self.__next_rank += 1
        self.__workers.append(None)
        self.__sentinels_by_rank.append(sentinel)
        if not hasattr(sentinel, 'nwsWorkerRanks'):
            if _DEBUG:
                log.msg('Sentinel %s creating rank list' % str(sentinel))
            sentinel.nwsWorkerRanks = []
        sentinel.nwsWorkerRanks.append(rank)
        if _DEBUG:
            log.msg('Sentinel %s adding rank %d => %s' % \
                        (str(sentinel), rank, str(sentinel.nwsWorkerRanks)))
        return rank

    def register_worker(self, worker, rank):
        """Register a worker for the given worker rank."""
        if _DEBUG:
            log.msg('Attempting registration - Worker %s for rank %d' % 
                     (worker, rank))
        if 0 <= rank < len(self.__workers) and self.__workers[rank] is None:
            if _DEBUG:
                log.msg('Worker %s is rank %d' % (worker, rank))
            worker.nwsWorkerRank = rank
            self.__workers[rank] = worker
            return True
        else:
            return False

    def deregister_worker(self, rank):
        """Deregister the worker having the given worker rank."""
        sentinel = self.__sentinels_by_rank[rank]
        if sentinel is not None:
            sentinel.nwsWorkerRanks.remove(rank)
            if _DEBUG:
                log.msg('Sentinel %s killing rank %d => %s' % \
                        (str(sentinel), rank, str(sentinel.nwsWorkerRanks)))
            if len(sentinel.nwsWorkerRanks) == 0:
                if _DEBUG:
                    log.msg('Sentinel %s deregistered' % str(sentinel))
                self.__sentinels.remove(sentinel)
        self.__sentinels_by_rank[rank]   = None
        self.__workers[rank] = 'dead'

class SentinelTracker(object):

    """Keeps track of when we last saw each sentinel, and allows queries of
    sentinels which have not checked in for more than a certain duration.
    """

    def __init__(self):
        self.__timestamps = {}

    def update(self, sentinel):
        """A sentinel has checked in."""
        if self.__timestamps.has_key(sentinel):
            self.__timestamps[sentinel] = time.time()
            return True
        else:
            return False

    def create(self, sentinel):
        """Mark the creation of a new sentinel."""
        self.__timestamps[sentinel] = time.time()

    def delete(self, sentinel):
        """Remove a sentinel from the tracker; it is no longer relevant."""
        try:
            del self.__timestamps[sentinel]
        except KeyError:
            pass

    def find_stale(self, period):
        """Find any sentinels which haven't checked in in the last 'period'
        seconds.
        """
        cutoff = time.time() - period
        return [sentinel
                for sentinel, last_checkin in self.__timestamps.items()
                if cutoff > last_checkin]
