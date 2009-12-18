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
"""Simple sleigh plugin implementation -- waiter queues.  The classes in this
module are used to manage the various clients that may be waiting on tasks or
results."""

from nwss.workspace import OperationFailure
from nwss.pyutils import new_list, remove_first

class TaskWaiters(object):
    """Manager for a list of task waiters.  Each task waiter consists of a
    client and a request which blocked while trying to get a task, as well as a
    callback function via which we may send a result (or a failure) back to the
    client."""

    def __init__(self, workspace):
        self.workspace = workspace
        self.__blocked_clients = new_list()

    def add_waiter(self, client, request, handler):
        """Add a waiter to our list of blocked clients.

          Parameters:
              client        - who's asking?
              request       - what are they asking for?
              handler       - how do we respond to the client?
        """
        self.__blocked_clients.append((client, request, handler))

    def wake_one(self):
        """Wake any task waiter.  This should be called any time a task is
        submitted, in case there are any blocked clients ready to service the
        request.
        """
        if len(self.__blocked_clients) != 0:
            client, request, handler = remove_first(self.__blocked_clients)
            try:
                val = self.workspace.task_queue_fetch(client, request)
            except OperationFailure, fail:
                handler(fail)
                return

            if val is not None:
                handler(val)
            else:
                self.__blocked_clients.append((client, request, handler))

    def wake_all(self):
        """Wake all task waiters.  This should be called any time a broadcast
        task is submitted, to wake any sleeping workers.
        """
        clients = self.__blocked_clients
        self.__blocked_clients = new_list()
        for client, request, handler in clients:
            try:
                val = self.workspace.task_queue_fetch(client, request)
            except OperationFailure, fail:
                handler(fail)
                continue

            if val is not None:
                handler(val)
            else:
                self.__blocked_clients.append((client, request, handler))

    def cancel_waiter(self, client):
        """Cancel a waiter's request, removing them from the waiter list.
        Returns True if the client was actually waiting, and False otherwise.

          Parameters:
              client     - the client whose request to cancel
        """
        for idx in range(len(self.__blocked_clients)):
            blocker = self.__blocked_clients[idx]
            if blocker[0] == client:
                blocker[2](OperationFailure('operation cancelled'))
                del self.__blocked_clients[idx]
                return True
        else:
            return False

class ResultWaiters(object):

    """Manager for a list of result waiters, very similar to the task waiter
    queue.
    """

    def __init__(self, workspace):
        self.__workspace = workspace
        self.__blocked_clients = {}

    def add_waiter(self, jid, client, request, handler):
        """Add a result queue waiter.

          Parameters:
              jid        - job id whose results we seek
              client     - who seeks the results?
              request    - original request object
              handler    - callback to receive results
        """
        if not self.__blocked_clients.has_key(jid):
            self.__blocked_clients[jid] = new_list()
        self.__blocked_clients[jid].append((client, request, handler))
        return None

    def kill_all(self):
        """Kill all result queue waiters."""
        reply = OperationFailure('sleigh is dead')
        for waiters in self.__blocked_clients.values():
            for waiter in waiters:
                waiter[2](reply)
        self.__blocked_clients = {}

    def wake_one(self, jid):
        """Wake one of the result waiters waiting for a given job id, if anyone
        is waiting for the job.
        """
        if self.__workspace.is_shutting_down:
            self.kill_all()
            return

        if self.__blocked_clients.has_key(jid):
            client = remove_first(self.__blocked_clients[jid])

            try:
                val = self.__workspace.result_queue_fetch(* client[0:2])
                if val is None:
                    self.__blocked_clients[jid].append(client)
                else:
                    client[2](val)
                    if len(self.__blocked_clients[jid]) == 0:
                        del self.__blocked_clients[jid]
            except Exception:
                self.__blocked_clients[jid].append(client)
                raise
