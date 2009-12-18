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
"""Simple sleigh monitoring implementation."""

import time

try:
    set()                           #pylint: disable-msg=W8302
except NameError:
    from sets import Set as set     #pylint: disable-msg=W0622

class AgentTracker(object):
    """Simple tracker which keeps details of the current state of each of the
    agents in the system (the workers, the sentinels, and the master) and can
    prepare a human-readable report."""

    def __init__(self):
        self.__states = {}
        self.__master_state = 'Idle'
        self.__sentinels = set()

    SENTINEL_BLOCKED    = 'Blocked sentinels'
    SENTINEL_NOTBLOCKED = 'Active sentinels'
    SENTINEL_DEAD       = 'Dead sentinels'
    WORKER_IDLE         = 'Idle workers'
    WORKER_BLOCKED      = 'Blocked workers'
    WORKER_ACTIVE       = 'Active workers'
    WORKER_DEAD         = 'Dead workers'

    CATEGORIES = [ WORKER_IDLE,
                   WORKER_BLOCKED,
                   WORKER_ACTIVE,
                   WORKER_DEAD,
                   SENTINEL_BLOCKED,
                   SENTINEL_NOTBLOCKED,
                   SENTINEL_DEAD]

    def __get_state_list(self, state):
        """Get the agent list for a given state."""
        if not self.__states.has_key(state):
            self.__states[state] = []
        return self.__states[state]

    def __transition(self, conn, old_state, new_state):
        """Transition an agent from one state to another state."""
        try:
            self.__get_state_list(old_state).remove(conn)
            self.__get_state_list(new_state).append(conn)
        except ValueError:
            pass

    def __add_to_state(self, state, client):
        """Add an agent to a particular state."""
        state_list = self.__get_state_list(state)
        state_list.append(client)

    def sentinel_register(self, client):
        """Mark the registration of a sentinel."""
        if not client in self.__sentinels:
            self.__add_to_state(self.SENTINEL_NOTBLOCKED, client)
            self.__sentinels.add(client)

    def sentinel_block(self, client):
        """Mark the blocking of a sentinel."""
        self.__transition(client, self.SENTINEL_NOTBLOCKED,
                                  self.SENTINEL_BLOCKED)

    def sentinel_unblock(self, client):
        """Mark the unblocking of a sentinel."""
        self.__transition(client, self.SENTINEL_BLOCKED,
                                  self.SENTINEL_NOTBLOCKED)

    def sentinel_dead(self, client):
        """Mark the death of a sentinel."""
        self.__transition(client, self.SENTINEL_BLOCKED,    self.SENTINEL_DEAD)
        self.__transition(client, self.SENTINEL_NOTBLOCKED, self.SENTINEL_DEAD)

    def worker_register(self, client):
        """Mark the registration of a worker."""
        self.__add_to_state(self.WORKER_IDLE, client)

    def worker_block(self, client):
        """Mark the blocking of a worker."""
        self.__transition(client, self.WORKER_IDLE, self.WORKER_BLOCKED)

    def worker_gottask(self, client):
        """Mark the assignment of a task to a worker."""
        self.__transition(client, self.WORKER_BLOCKED, self.WORKER_ACTIVE)
        self.__transition(client, self.WORKER_IDLE,    self.WORKER_ACTIVE)

    def worker_returnedresult(self, client):
        """Mark the return of a result from a worker."""
        self.__transition(client, self.WORKER_ACTIVE, self.WORKER_IDLE)

    def worker_dead(self, client):
        """Mark the death of a worker."""
        self.__transition(client, self.WORKER_IDLE,    self.WORKER_DEAD)
        self.__transition(client, self.WORKER_BLOCKED, self.WORKER_DEAD)
        self.__transition(client, self.WORKER_ACTIVE,  self.WORKER_DEAD)

    def master_awaitresult(self, client):
        #pylint: disable-msg=W0613
        """Mark the blocking of the master for a result."""
        self.__master_state = 'Blocked for results'

    def master_gotresult(self, client):
        #pylint: disable-msg=W0613
        """Mark the presentation to the master of a result."""
        self.__master_state = 'Idle'

    def __report(self, categories):
        """Prepare a report of the agents in each state for the given set of
        state names."""
        report = ''
        for cat in categories:
            report += '\n' + cat + '\n'
            report += '\n'.join(['  - ' + v.nwsClientLabel
                                  for v in self.__get_state_list(cat)])
        return report

    def get_report(self):
        """Get a complete report of the states of all agents in the system."""
        return ('Master state: %s\n' % self.__master_state) + \
                 self.__report(self.CATEGORIES)

class Scoreboard(object):
    """Simple tracker keeping a "scoreboard" of the number of tasks taken and
    completed by each worker, and the aggregate amounts of time spent in each
    state.  The ultimate goal of this class is to prepare a human-readable
    report."""

    def __init__(self):
        self.__completed = {}
        self.__total_time_active = {}
        self.__total_time_idle = {}
        self.__time_last_active = {}
        self.__time_task_started = {}

    def worker_registered(self, conn):
        """Mark the registration of a new worker."""
        self.__completed[conn] = 0
        self.__total_time_active[conn] = 0
        self.__total_time_idle[conn] = 0
        self.__time_last_active[conn] = time.time()
        self.__time_task_started[conn] = 0

    def worker_tooktask(self, conn):
        """Mark the start of a new task for a worker."""
        now = time.time()
        idle_time = now - self.__time_last_active[conn]
        self.__time_task_started[conn] = now
        self.__total_time_idle[conn] += idle_time

    def worker_finishedtask(self, conn):
        """Mark the completion of a task by a worker."""
        now = time.time()
        active_time = now - self.__time_task_started[conn]
        self.__time_last_active[conn] = now
        self.__total_time_active[conn] += active_time
        self.__completed[conn] += 1

    def worker_dead(self, conn):
        """Mark the death of a worker."""
        self.__time_last_active[conn] = self.__time_task_started[conn] + 1

    def report(self):
        """Prepare a report."""
        report  = 'Worker\tNum Completed\tAverage Time (sec)\t'
        report += '% Active\tTime on Current Task (sec)\n'

        # Iterate over all workers
        for client in self.__completed:
            label = client.nwsClientLabel
            num_completed = self.__completed[client]
            active_time   = self.__total_time_active[client]
            idle_time     = self.__total_time_idle[client]
            time_started  = self.__time_task_started[client]
            last_active   = self.__time_last_active[client]

            # Compute average time per task
            if num_completed != 0:
                avg_time      = active_time / num_completed
            else:
                avg_time      = 0

            # Compute percentage of time spent "active"
            total_time = active_time + idle_time
            if total_time > 0.0:
                pct_active = 100.0 * active_time / total_time
            else:
                pct_active = 0

            # Compute length of time on current task
            if time_started > last_active:
                time_on_current_task = time.time() - time_started
            else:
                time_on_current_task = 0

            # Add a line for this worker to the report
            report += '%s\t%d\t%.4g\t%.3g\t%.4g\n' % (label,
                    num_completed,
                    avg_time,
                    pct_active,
                    time_on_current_task)
        return report

