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
"""Simple sleigh plugin "join phase" implementation."""

from nwss.base      import Response
from nwss.workspace import call_after_delay
from twisted.python import log
import nwss

__all__ = ['JoinPhaseTracker']

_DEBUG = nwss.config.is_debug_enabled('plugin:Sleigh:join')

STATE_OPEN    = 0
STATE_JOINING = 1
STATE_CLOSED  = 2

STATE_LABELS = ["open", "joining", "closed"]

class JoinPhaseTracker(object):
    """Class for managing the sleigh worker join phase."""

    def __init__(self, registration):
        self.__waiters = []
        self.__state = STATE_OPEN
        self.__num_workers = None
        self.__registration = registration
        self.duration = 60.0
        self.__group_close_callbacks = []

    def __is_joining(self):
        """Find out if we've begun the join phase."""
        return self.__state == STATE_JOINING
    joining = property(__is_joining)

    def __is_closed(self):
        """Find out if the group has closed."""
        return self.__state == STATE_CLOSED
    closed = property(__is_closed)

    def __get_num_workers(self):
        """Get the number of workers as confirmed by the join phase, if it has
        completed.  Before the group has closed, returns None."""
        return self.__num_workers
    num_workers = property(__get_num_workers)

    def add_waiter(self, handler):
        """Add a 'waiter', who is waiting for the group to close."""
        self.__waiters.append(handler)

    def wake_waiter(self, handler, count):
        """Wake up a particular passive waiter if it has not been awakened
        already."""
        try:
            self.__waiters.remove(handler)
            handler(Response(value=count))
        except ValueError:
            # No message necessary -- already sent when group closed
            pass

    def request_group_close(self):
        """Request that we begin the join phase, if it has not already started.
        This is called, for instance, in response to an eackWorker task.
        """
        return self.__schedule_group_close(self.duration, False)

    def __schedule_group_close(self, delay, may_reduce):
        """Schedule the closure of the worker group after a specified delay."""
        if self.__state == STATE_OPEN:
            self.__state = STATE_JOINING
            call_after_delay(delay, self.close_group, may_reduce)
            return True
        return False

    def add_group_close_callback(self, callback):
        """Add a callback to be called when the group closes.  The callback
        must accept a single argument, which is the actual worker count after
        the group closes.
        """
        self.__group_close_callbacks.append(callback)

    def close_group(self, may_reduce=False):
        """Initiate the closure of the worker group, optionally reducing the
        maximum number of workers to the highest allocated worker rank."""
        if _DEBUG:
            log.msg('Attempting immediate group closure.')
        if not self.closed:
            if _DEBUG:
                log.msg('Performing group closure.')
            self.__registration.freeze_max_workers(may_reduce)
            n_workers = self.__registration.num_registered_workers
            self.__group_closed(n_workers)
            for callback in self.__group_close_callbacks:
                callback(n_workers)

        return self.num_workers

    def begin_join_phase(self, delay, handler):
        """Initiate the join phase on behalf of a client."""
        if self.closed:
            if _DEBUG:
                log.msg('Group already closed.')
            handler(self.num_workers)
            return True

        if _DEBUG:
            log.msg('Scheduling group closure.')
        if self.__schedule_group_close(delay, True):
            actual = self.__registration.num_registered_workers
            if actual == self.__registration.max_workers:
                handler(self.close_group())

        self.add_waiter(handler)
        return True

    def __group_closed(self, count):
        """Close the group, notifying all waiters."""
        self.__num_workers = count
        self.__state = STATE_CLOSED

        resp = Response(value=count)
        for i in self.__waiters:
            i(resp)

    def get_status(self):
        """Get the current join status -- either 'open', 'closed', or
        'joining'.
        """
        return STATE_LABELS[self.__state]
