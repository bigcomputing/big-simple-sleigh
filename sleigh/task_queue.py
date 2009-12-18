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
"""Simple sleigh task queue implementation."""

from nwss.pyutils import new_list, remove_first

class TaskQueue(object):

    """Task manager which maintains broadcast and unicast task queues."""
    def __init__(self):
        self.__broadcast_master = []
        self.__unicast = new_list()
        self.__broadcast_by_worker = {}

    def add_worker(self, worker):
        """Add a new worker, to whom will be distributed another copy of each
        broadcast task.  Will return False if the group is closed or if the
        worker has already been added.
        """
        if worker in self.__broadcast_by_worker:
            return False
        if self.__broadcast_master is None:
            return False

        # Add the worker
        newbcastlist = new_list(self.__broadcast_master)
        self.__broadcast_by_worker[worker] = newbcastlist
        return True

    def remove_worker(self, worker):
        """Signal that a worker has died and will no longer receive broadcast
        tasks.
        """
        try:
            del self.__broadcast_by_worker[worker]
        except KeyError:
            pass

    def close_group(self):
        """Signal that the worker group has closed, and no further workers
        shall be permitted.
        """
        self.__broadcast_master = None

    def add_unicast(self, task):
        """Add a unicast task.

          Parameters:
              task     - the task structure
        """
        self.__unicast.append(task)

    def add_broadcast(self, task):
        """Add a broadcast task.

          Parameters:
              task     - the task structure
        """
        if self.__broadcast_master is not None:
            self.__broadcast_master.append(task)
        for bcastlist in self.__broadcast_by_worker.values():
            bcastlist.append(task)

    def get_all_broadcast(self, worker):
        """Get all broadcast tasks for a given worker.  Returns None if the
        group has closed and the worker is not *known* to the task queue.  If
        the group has not yet closed, the broadcast master list may be
        retrieved using 'None' for the worker.

          Parameters:
              worker   - the worker whose tasks to get
        """
        if worker in self.__broadcast_by_worker:
            return self.__broadcast_by_worker[worker]
        return self.__broadcast_master

    def count_matching(self, matchfunc):
        """Count the number of tasks for which matchfunc returns True."""
        total = 0
        for task in self.__unicast:
            if matchfunc(task):
                total += 1
        for queue in self.__broadcast_by_worker.values():
            for task in queue:
                if matchfunc(task):
                    total += 1
        return total

    def get_total_count(self):
        """Get the total count of all tasks in the queues."""
        unicast = len(self.__unicast)
        broadcast = sum([len(queue)
                         for queue in self.__broadcast_by_worker.values()])
        return unicast + broadcast

    def peek_next_task(self, worker):
        """Get the next task for a given worker, but do not remove it from the
        queues."""
        try:
            bcastlist = self.__broadcast_by_worker[worker]
            if len(bcastlist) != 0:
                return bcastlist[0]
            if len(self.__unicast) != 0:
                return self.__unicast[0]
        except KeyError:
            # No such worker.
            pass

        return None

    def fetch_next_task(self, worker):
        """Get the next task for a given worker, removing it from the
        queues."""
        try:
            bcastlist = self.__broadcast_by_worker[worker]
            if len(bcastlist) != 0:
                return remove_first(bcastlist)
            if len(self.__unicast) != 0:
                return remove_first(self.__unicast)
        except KeyError:
            # No such worker.
            pass

        return None

class ResultQueue(object):

    """Result manager which maintains queues of results by job ("batch") id."""

    def __init__(self):
        self.__results = {}

    def add_result(self, jid, result):
        """Add a new result."""
        if not self.__results.has_key(jid):
            self.__results[jid] = new_list()
        self.__results[jid].append(result)

    def get_result(self, jid):
        """Get the next result for a given job id."""
        if self.__results.has_key(jid):
            queue = self.__results[jid]
            result = remove_first(queue)
            if len(queue) == 0:
                del self.__results[jid]
            return result
        return None

    def get_total_count(self):
        """Get the total count of results in this tracker."""
        return sum([len(task_list) for task_list in self.__results.values()])
