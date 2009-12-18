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
"""Simple sleigh plugin implementation."""

from nwss.base      import WorkspaceFailure, Response
from nwss.stdvars   import Constant
from nwss.workspace import WorkSpace
from nwss.workspace import MetaVariable
from nwss.workspace import constant_return, singleton_iter_provider
from nwss.workspace import simple_metavariable, OperationFailure
from nwss.workspace import call_after_delay
import nwss
from twisted.python import log
import pkg_resources
import os
import itertools

from sleigh.registration import WorkerRegistration, SentinelTracker
from sleigh.monitoring import AgentTracker, Scoreboard
from sleigh.join_phase import JoinPhaseTracker
from sleigh.waiter_queues import TaskWaiters, ResultWaiters
from sleigh.task_queue import TaskQueue, ResultQueue

_DEBUG = nwss.config.is_debug_enabled('plugin:Sleigh')

def version_hash(fname, stream):
    """Return a hash of the contents of the plugin which can be used to
    establish the exact plugin version."""
    try:
        #pylint: disable-msg=W8419
        import hashlib
        vhash = hashlib.md5()
    except ImportError:
        import md5
        vhash = md5.new()
    vhash.update(stream.read())
    return "%s: %s" % (fname, vhash.hexdigest())

def get_file_version(name, path):
    """Compute a version hash for a given file."""
    #pylint: disable-msg=E1101
    dirname = os.path.dirname(path)
    fname   = os.path.basename(path)
    return version_hash(name,
               pkg_resources.resource_stream(dirname, fname))

def _assemble_task(val, metadata, job_id, is_broadcast):
    """Assemble and return a task tuple.

      Parameters:
          val           - the task data
          metadata      - the task metadata
          job_id        - the job id ("batch" id)
          is_broadcast  - True iff this is a "broadcast" task
    """
    return (job_id, val, metadata, is_broadcast)

##
## Interface:
##
##     'version'        always returns '1' for current plugin interface
##
##     'status'         'fetch' initiates join phase, returns the final number
##                              of joined workers
##                      'store' sets the join phase delay
##     'workerCount'    get the maximum number of workers
##     'rankCount'      get the currently joined number of workers
##     'join_status'    get the current status w.r.t joining ("closed",
##                              "joining", or "open")
##
##     'worker_ids'     sentinel fetches worker ids from here, worker stores
##                              them to register
##     'sentinel_alive' sentinel fetches this as a 'keepalive'
##
##     'task'           master stores tasks here, worker fetches them
##     'broadcast'      master stores 'eachWorker' tasks here
##

class SleighWorkSpace(WorkSpace):
    #pylint: disable-msg=R0902,R0904

    """Simple sleigh workspace plugin."""

    # Sleigh protocol version 1
    PROTO_VERSION = 1

    # Plugin priority 20
    PRIORITY = 20

    def __init__(self, name):
        WorkSpace.__init__(self, name)

        # Initialize shutdown machinery
        self.is_shutting_down = False
        self.create_standard_var('bye', 'fifo', hidden=True)

        # Monitoring
        self.agent_tracker = AgentTracker()
        self.scoreboard = Scoreboard()
        self.__init_monitoring()

        # Initialize logging
        self.__init_logging()

        # Job id
        self.__results = ResultQueue()

        # Prepare for sleigh plugin version identification
        self.__init_version_info()

        # Prepare for sentinel keep-alive messages
        self.__sentinels = SentinelTracker()
        self.__blocked_sentinels = []
        self.__init_sentinel_keepalive()

        # Prepare for worker/Sentinel registration
        self.__registration = WorkerRegistration()
        self.__init_worker_reg_variables()

        # Prepare the grace period
        self.__grace_period_ended = False

        # Prepare the join phase machinery
        self.__join_phase = JoinPhaseTracker(self.__registration)
        self.__init_join_phase()

        # Prepare the task queues
        self.__task_queue = TaskQueue()
        self.__task_by_worker = {}
        self.__task_waiters = TaskWaiters(self)
        self.__result_waiters = ResultWaiters(self)
        self.__init_task_queue()
        
        # Prepare to generate fixedarg IDs
        self.__init_fixedarg_id_gen()

    def __get_sentinels(self):
        """Get the sentinel tracker."""
        return self.__sentinels
    sentinels = property(__get_sentinels)

    def __get_registration(self):
        """Get the worker registration structure."""
        return self.__registration
    registration = property(__get_registration)

    def __get_task_queue(self):
        """Get the task queue structure."""
        return self.__task_queue
    task_queue = property(__get_task_queue)

    def __get_join_phase_tracker(self):
        """Get the join phase tracker structure."""
        return self.__join_phase
    join_phase_tracker = property(__get_join_phase_tracker)

    def get_plugin_version(self):
        #pylint: disable-msg=R0201,E1101
        """Get plugin version information."""
        return get_file_version(__name__, 'sleigh/plugin.py')

    def hook_created_ws(self, metadata):
        """Callback from the NWS machinery upon workspace creation.  Reads out
        relevant options from the metadata.

          Parameters:
              metadata     - metadata passed in at WS creation time
        """
        # Set the maximum worker count, if specified
        rank_count = metadata.get('rankCount')
        if rank_count is not None:
            self.registration.max_workers = int(rank_count)

        # Start the sentinel waker
        sentinel_refresh = float(metadata.get('sentinelRefreshPeriod', '60'))
        self.add_periodic_task(sentinel_refresh, self.__wake_sentinels)

        # Begin the worker grace period
        delay = float(metadata.get('workerGracePeriod', '300'))
        self.__schedule_grace_period_end(delay)

    def hook_store_pre(self, var, value, metadata):
        #pylint: disable-msg=W0613
        """Hook to detect store of 'Sleigh ride over' variable."""
        if var.name == 'Sleigh ride over':
            self.initiate_shutdown()

    #############################################################
    ## Version info

    def __init_version_info(self):
        """Create the version info variables for this workspace."""
        self.create_var(Constant('version', SleighWorkSpace.PROTO_VERSION),
                        hidden=True)
        self.create_var(Constant('plugin_version', self.get_plugin_version()))

    #############################################################

    #############################################################
    ## Monitoring

    def __init_monitoring(self):
        """Create the monitoring variables for this workspace."""
        get_report = self.agent_tracker.get_report
        self.create_var(simple_metavariable('agentStatus', get_report))

        get_score = self.scoreboard.report
        self.create_var(simple_metavariable('scoreboard', get_score))

    #############################################################

    #############################################################
    ## Logging        
    def __init_logging(self):
        """Create the logging FIFOs for this workspace."""
        self.create_standard_var('logDebug', 'fifo')
        self.create_standard_var('logError', 'fifo')

    #############################################################

    #############################################################
    ## Grace period
    def __schedule_grace_period_end(self, delay):
        """Schedule the end of the grace period for workers.  Once the grace
        period is over, if we have no workers, we will kill the job."""
        call_after_delay(delay, self.__close_grace_period)

    def __is_grace_period_over(self):
        """Check if the grace period is over."""
        return self.__grace_period_ended
    grace_period_over = property(__is_grace_period_over)

    def __close_grace_period(self):
        """Close the grace period, killing the job if we have no workers."""
        if self.registration.num_registered_workers == 0:
            self.initiate_shutdown()
        else:
            self.__grace_period_ended = True

    #############################################################

    #############################################################
    ## Join phase

    def __worker_count_fetch(self):
        """Query the number of expected workers, freezing the value so that
        future queries return the same value."""
        self.registration.freeze_max_workers()
        return self.registration.num_expected_workers

    def __worker_count_store(self, client, request):
        #pylint: disable-msg=W0613
        """Set the number of expected workers, as by a store to the
        'workerCount' variable."""
        self.registration.max_workers = int(request.value)

    def __get_num_actual_workers(self):
        """Get the number of workers that have actually joined the computation
        thus far."""
        return self.registration.num_registered_workers
    num_actual_workers = property(__get_num_actual_workers)

    def __disarm_status_var(self, num_workers):
        """Callback on group closure: Change the semantics of the status and
        waitForWorkers variables so they don't keep trying to initiate the join
        phase."""
        new_get  = constant_return(Response(value=num_workers))
        new_iter = constant_return(([num_workers], 1))

        waitforworkers_var = self.get_variable('waitForWorkers')
        waitforworkers_var.get_handler = new_get
        waitforworkers_var.iter_handler = new_iter

        status_var = self.get_variable('status')
        status_var.get_handler = new_get
        status_var.iter_handler = new_iter

    def __mark_deadbeat_workers(self, num_workers):
        #pylint: disable-msg=W0613
        """Callback on group closure: Mark as dead any workers which never
        reported for duty.
        """
        max_workers = self.registration.max_workers
        if _DEBUG:
            log.msg('Group closed: %d workers max' % max_workers)
        for rank in range(max_workers):
            if not self.registration.is_worker_registered(rank):
                if _DEBUG:
                    log.msg('Worker %d never reported' % rank)
                self.call_hook('deadbeat_worker', rank)
            else:
                if _DEBUG:
                    log.msg('Worker %d present' % rank)

    def __clear_saved_broadcast_tasks(self, num_workers):
        #pylint: disable-msg=W0613
        """Callback on group closure: Clear the list of unassigned broadcast
        tasks.  (This is a list of all broadcast tasks to date in case another
        worker shows up.
        """
        self.__task_queue.close_group()

    def __is_group_closed(self):
        """Check whether the worker group is closed."""
        return self.__join_phase.closed
    group_is_closed = property(__is_group_closed)

    def hook_dead_worker(self, wrank, worker):
        #pylint: disable-msg=W0613
        """Callback invoked when a worker, once active, dies."""
        self.initiate_shutdown()

    def __wait_for_workers_cb(self, client, request, handler):
        """Schedule a callback to a given client when the result from the
        waitForWorkers variable is available.
        """
        metadata = request.metadata
        delay = float(metadata.get('delay', self.__join_phase.duration))
        thunk = lambda: self.__join_phase.wake_waiter(handler,
                                                      self.num_actual_workers)
        call_after_delay(delay, thunk)
        self.__join_phase.add_waiter(handler)
        return True

    def __status_cb(self, client, request, handler):
        """Schedule a callback to a given client when the result from the
        status variable is available.
        """
        metadata = request.metadata
        delay = float(metadata.get('delay', self.__join_phase.duration))
        return self.__join_phase.begin_join_phase(delay, handler)

    def __status_store(self, client, request):
        #pylint: disable-msg=W0613
        """Handle a store request for either the 'status' or 'waitForWorkers'
        variables.
        """
        self.__join_phase.duration = float(request.value)

    def __init_join_phase(self):
        """Initialize all variables related to the join phase."""

        # Create 'waitForWorkers'
        var = MetaVariable('waitForWorkers',
                           constant_return(None),
                           cb_handler=self.__wait_for_workers_cb)
        var.set_handler = self.__status_store
        self.create_var(var, hidden=True)

        # Create 'status'
        var = MetaVariable('status',
                           constant_return(None),
                           cb_handler=self.__status_cb)
        var.set_handler = self.__status_store
        self.create_var(var, hidden=True)

        # Create 'join_status'
        var = simple_metavariable('join_status', self.__join_phase.get_status)
        self.create_var(var)

        # Create 'rankCount'
        var = simple_metavariable('rankCount', self.__get_num_actual_workers)
        self.create_var(var)

        # Create 'workerCount'
        get_worker_count = self.__worker_count_fetch
        var = simple_metavariable('workerCount', get_worker_count)
        var.set_handler = self.__worker_count_store
        self.create_var(var)

        # Attach callbacks for the end of the join phase
        cb0 = lambda n_workers: self.call_hook('group_close_pre', n_workers)
        cb1 = self.__disarm_status_var
        cb2 = self.__mark_deadbeat_workers
        cb3 = self.__clear_saved_broadcast_tasks
        cb4 = lambda n_workers: self.call_hook('group_close_post', n_workers)
        self.__join_phase.add_group_close_callback(cb0)
        self.__join_phase.add_group_close_callback(cb1)
        self.__join_phase.add_group_close_callback(cb2)
        self.__join_phase.add_group_close_callback(cb3)
        self.__join_phase.add_group_close_callback(cb4)

    #############################################################

    #############################################################
    ## Worker/Sentinel registration subsystem

    def __declare_worker(self, conn):
        """Callback activated when the sentinel requests an id for a worker."""
        if self.__join_phase.closed:
            log.msg('  -> failure (group closed)')
            return -1
        self.agent_tracker.sentinel_register(conn)
        self.registration.register_sentinel(conn)
        worker_id = self.registration.allocate_rank(conn)
        conn.nwsClientLabel = 'Sentinel %d: %s:%d' % (worker_id,
                conn.transport.getPeer().host,
                conn.transport.getPeer().port)
        self.sentinels.create(conn)
        if _DEBUG:
            log.msg('Declare worker (sentinel=%s, id=%d)' % \
                    (str(conn), worker_id))
        self.call_hook('declare_worker', conn, worker_id)
        return worker_id

    def __register_worker(self, conn, worker_id, metadata):
        """Callback activated when a worker reports for duty."""
        if _DEBUG:
            log.msg('Worker registered (conn=%s, id=%d, metadata=%s)' % \
                    (str(conn), worker_id, str(metadata)))
        if self.__join_phase.closed:
            log.msg('  -> failure (group closed)')
            raise WorkspaceFailure('Worker registration failed: ' +
                                   'group closed.')
        elif self.registration.is_worker_registered(worker_id):
            log.msg("  -> failure (worker id already registered)")
            raise WorkspaceFailure('Worker registration failed: ' +
                                   'duplicate worker id.')
        self.agent_tracker.worker_register(conn)
        self.scoreboard.worker_registered(conn)
        conn.nwsClientLabel = 'Worker %d: %s:%d' % \
                (worker_id,
                 conn.transport.getPeer().host,
                 conn.transport.getPeer().port)
        self.registration.register_worker(conn, worker_id)
        self.get_variable('worker registrations').store(None,
                                                        str(worker_id),
                                                        metadata)
        self.__task_queue.add_worker(conn)
        self.call_hook('register_worker', conn, worker_id)
        expected = self.registration.num_expected_workers
        actual = self.num_actual_workers
        if actual == expected:
            self.__join_phase.close_group()
        log.msg("  -> SUCCESS")

    def __worker_id_queue_fetch(self, client, request):
        """Implementation of fetch/find for the 'worker_ids' variable."""
        metadata = {}
        if request.remove:
            new_id = self.__declare_worker(client)
            metadata['allocated'] = '1'
        else:
            new_id = self.registration.get_num_allocated_ranks()
            metadata['allocated'] = '0'
        return Response(value=str(new_id), metadata=metadata)

    def __worker_id_queue_store(self, client, request):
        """Implementation of store for the 'worker_ids' variable."""
        worker_id = int(request.value)
        worker_metadata = request.metadata
        self.__register_worker(client, worker_id, worker_metadata)

    def __init_worker_reg_variables(self):
        """Initialze the 'worker_ids' and 'worker registrations' variables."""
        # Create 'worker_ids'
        iterator_value = self.registration.get_num_allocated_ranks
        var = MetaVariable('worker_ids',
                           self.__worker_id_queue_fetch,
                           singleton_iter_provider(iterator_value))
        var.set_handler = self.__worker_id_queue_store
        self.create_var(var, hidden=True)

        self.create_standard_var('worker registrations', 'fifo')

    def kill_worker(self, rank):
        """Mark a worker as dead and kill the worker.  When the worker next
        requests a task, it will fail."""
        conn = self.registration.get_worker(rank)
        if conn is not None:
            self.call_hook('dead_worker', rank, conn)
            self.agent_tracker.worker_dead(conn)
            self.scoreboard.worker_dead(conn)
            self.registration.deregister_worker(rank)
            self.__task_queue.remove_worker(conn)
            self.set_worker_task(conn, None)
            self.__task_waiters.cancel_waiter(conn)

    def kill_sentinel(self, client):
        """Kill a sentinel, failing all of its workers.  Generally only done if
        we already suspect the sentinel to be dead.  It's really the only
        merciful thing to do.
        """
        self.agent_tracker.sentinel_dead(client)
        self.sentinels.delete(client)
        wranks = client.nwsWorkerRanks
        for rank in wranks:
            self.kill_worker(rank)

    #############################################################

    #############################################################
    ## Sentinel keepalive system

    def __sentinel_alive(self, conn):
        """Callback activated when a sentinel checks in."""
        if conn is not None:
            if _DEBUG:
                log.msg('Sentinel checked in: %s' % str(conn))
            if self.sentinels.update(conn):
                return True
            else:
                log.msg('Sentinel, once declared dead, checked in: %s' %
                           str(conn))
                return False
        return False

    def __sentinel_alive_fetch(self, client, request):
        #pylint: disable-msg=W0613
        """Fetch/find handler for the 'sentinel_alive' variable."""
        if not self.is_shutting_down:
            if not self.__sentinel_alive(client):
                # Dead sentinel.  Send a shutdown response.
                return Response(value='1')
            self.agent_tracker.sentinel_block(client)
            return None
        else:
            # Time to shut down.  Send a "shutdown" response.
            return Response(value='1')

    def __sentinel_alive_iter(self):
        """Iterator handler for the 'sentinel_alive' variable."""
        if self.is_shutting_down:
            return '1'
        else:
            return None

    def __sentinel_alive_cb(self, client, request, handler):
        """Blocked fetch/find handler for the 'sentinel_alive' variable."""
        self.__blocked_sentinels.append(handler)
        return True

    def __wake_sentinels(self):
        """Wake all of the sentinels."""
        if self.is_shutting_down:
            reply = Response(value='1')
        else:
            reply = Response(value='0')
        for handler in self.__blocked_sentinels:
            handler(reply)
        self.__blocked_sentinels = []
        self.call_hook('woke_sentinels')

    def __init_sentinel_keepalive(self):
        """Initialize the variables required for the sentinel keepalive
        subsystem.
        """
        # Create 'sentinel_alive'
        var = MetaVariable('sentinel_alive',
                           self.__sentinel_alive_fetch,
                           self.__sentinel_alive_iter,
                           cb_handler=self.__sentinel_alive_cb)
        self.create_var(var, hidden=True)

    #############################################################

    #############################################################
    ## Normal task system

    def __register_result(self, jid, value, metadata):
        """Register a result.  This will not wake up any result waiters; it
        only stores the result in the relevant data structures.
        """
        result = (jid, value, dict(metadata))
        metadata['batchId'] = jid
        self.__results.add_result(jid, result)

    def set_worker_task(self, client, task):
        """Set the task assigned to a particular worker."""
        if task is None:
            if self.__task_by_worker.has_key(client):
                del self.__task_by_worker[client]
        else:
            self.__task_by_worker[client] = task

    def __get_job_status(self, jid):
        """Get the status of a particular batch.  The status is returned as a
        tuple of (num_pending, num_running).  Pending includes all tasks which
        have been submitted but for which a result has not been produced.
        (Thus, all running tasks are also pending tasks.)
        """
        pending_count = 0
        running_count = 0

        criterion = lambda task: task[0] == jid

        # Count the current running tasks
        for task in self.__task_by_worker.values():
            if criterion(task):
                pending_count += 1
                running_count += 1

        # Count the queued tasks
        pending_count += self.__task_queue.count_matching(criterion)

        return (pending_count, running_count)

    def __get_all_job_status(self):
        """Get the status of tasks in the system.  The status is returned as a
        tuple of (num_pending, num_running).  Pending includes all tasks which
        have been submitted but for which a result has not been produced.
        (Thus, all running tasks are also pending tasks.)
        """
        pending_count = len(self.__task_by_worker)
        running_count = pending_count
        pending_count += self.__task_queue.get_total_count()
        return (pending_count, running_count)

    def __job_status_fetch(self, client, request):
        #pylint: disable-msg=W0613
        """Fetch/find handler for the job_status variable."""
        fetch_all = request.metadata.get('all') == '1'
        if fetch_all:
            status = self.__get_all_job_status()
            pending_count, running_count = status
        else:
            jid = request.metadata.get('batchId')
            status = self.__get_job_status(jid)
            pending_count, running_count = status
        metadata = {'nwsRunning': str(running_count),
                    'nwsPending': str(pending_count)}
        return Response(value=pending_count, metadata=metadata)

    def submit_unicast(self, task):
        """Submit a unicast task, waking waiters."""
        self.__task_queue.add_unicast(task)
        self.__task_waiters.wake_one()

    def __task_queue_store(self, client, request):
        """Store handler for the task queue variable."""
        if self.is_shutting_down:
            raise WorkspaceFailure('Store failed: sleigh is shutting down.',
                                   5001)

        # Build the task
        data = request.value
        metadata = request.metadata
        jid = metadata.get('batchId')
        task = _assemble_task(data, metadata, jid, False)

        # Now submit it
        self.call_hook('new_unicast_pre', client, task)
        self.submit_unicast(task)
        self.call_hook('new_unicast_post', client, task)

    def task_queue_fetch(self, client, request):
        """Fetch/find handler for the task queue variable."""
        if self.is_shutting_down:
            raise OperationFailure('sleigh is dead')
        if not request.remove:
            raise OperationFailure('find is not supported on the task queue')
        task = self.__task_queue.fetch_next_task(client)
        if task is not None:
            self.agent_tracker.worker_gottask(client)
            self.scoreboard.worker_tooktask(client)

            jid, val, metadata, _ = task
            self.set_worker_task(client, task)
            self.call_hook('assign_task', client, task)
            metadata['batchId'] = jid
            return Response(value=val, metadata=metadata)
        else:
            return None

    def __task_queue_iter(self):
        #pylint: disable-msg=W0613
        """Iterator handler for the task queue variable."""
        length   = self.__task_queue.get_total_count()
        iterator = itertools.repeat('Opaque task data', length)
        return iterator, length

    def __task_queue_cb(self, client, request, handler):
        """Blocked fetch/find handler for the task queue variable."""
        self.agent_tracker.worker_block(client)
        self.__task_waiters.add_waiter(client, request, handler)
        return True

    def __broadcast_task_queue_store(self, client, request):
        """Store handler for the 'broadcast' variable."""
        data = request.value
        metadata = request.metadata
        jid = metadata.get('batchId')
        task = _assemble_task(data, metadata, jid, True)

        # Sending a broadcast task initiates a join phase
        if not self.__join_phase.closed:
            self.__join_phase.request_group_close()

        # Add to the task queues and wake the waiters
        self.call_hook('new_broadcast_pre', client, task)
        self.__task_queue.add_broadcast(task)
        self.__task_waiters.wake_all()
        self.call_hook('new_broadcast_post', client, task)

    def __broadcast_task_queue_fetch(self, client, request):
        #pylint: disable-msg=W0613,R0201
        """Fetch/find handler for the 'broadcast' variable."""
        return Response(value=0)

    def __result_queue_store(self, client, request):
        """Store handler for the results queue variable."""
        jid, _, task_metadata, _ = self.__task_by_worker[client]
        task_metadata.update(request.metadata)
        task_metadata['batchId'] = jid
        task_metadata['nwsWorkerRank'] = str(client.nwsWorkerRank)
        self.call_hook('finish_task_pre', client,
                       jid, request.value, task_metadata)
        self.set_worker_task(client, None)
        self.agent_tracker.worker_returnedresult(client)
        self.scoreboard.worker_finishedtask(client)
        self.__register_result(jid, request.value, task_metadata)
        self.__result_waiters.wake_one(jid)
        self.call_hook('finish_task_post', client,
                       jid, request.value, task_metadata)

    def result_queue_fetch(self, client, request):
        """Fetch/find handler for the results queue variable."""
        if self.is_shutting_down:
            raise OperationFailure('sleigh is dead')

        # Try to find the right result
        jid = request.metadata.get('batchId')
        result = self.__results.get_result(jid)
        if result is None:
            return None

        # Update stats
        self.agent_tracker.master_gotresult(client)
        _, data, metadata = result
        return Response(value=data, metadata=metadata)

    def __result_queue_iter(self):
        #pylint: disable-msg=W0613
        """Iterator handler for the results queue variable."""
        length   = self.__results.get_total_count()
        iterator = itertools.repeat('Opaque result data', length)
        return iterator, length

    def __result_queue_cb(self, client, request, handler):
        """Blocked fetch/find handler for the results queue variable."""
        jid = request.metadata.get('batchId')
        self.agent_tracker.master_awaitresult(client)
        self.__result_waiters.add_waiter(jid, client, request, handler)
        return True

    def put_result(self, jid, resp, wrank):
        """Synthesize a result to the task currently claimed by the worker
        whose rank is wrank.
        """
        if resp is None:
            resp = Response(value='', metadata={'nwsNull': '1'})
        resp.metadata['nwsWorkerRank'] = str(wrank)
        self.__register_result(jid, resp.value, resp.metadata)
        self.__result_waiters.wake_one(jid)

    def get_worker_task(self, worker):
        """Get the task which is currently assigned to the given worker."""
        return self.__task_by_worker.get(worker)

    def __init_task_queue(self):
        """Initialize all variables related to the task queue and result
        queue.
        """

        # Create 'task' variable
        var = MetaVariable('task',
                           self.task_queue_fetch,
                           self.__task_queue_iter,
                           self.__task_queue_cb)
        var.set_handler = self.__task_queue_store
        var.unwrap_stores = False
        self.create_var(var)

        # Create 'result' variable
        var = MetaVariable('result',
                           self.result_queue_fetch,
                           self.__result_queue_iter,
                           self.__result_queue_cb)
        var.set_handler = self.__result_queue_store
        var.unwrap_stores = False
        self.create_var(var)

        # Create 'jobStatus' variable
        iterfunc = lambda: self.__get_all_job_status()[0]
        var = MetaVariable('jobStatus',
                           self.__job_status_fetch,
                           singleton_iter_provider(iterfunc))
        self.create_var(var, hidden=True)

        # Create 'broadcast' variable
        var = MetaVariable('broadcast',
                           self.__broadcast_task_queue_fetch)
        var.set_handler = self.__broadcast_task_queue_store
        var.unwrap_stores = False
        self.create_var(var, hidden=True)

    #############################################################

    #############################################################
    ## Fixedarg system

    def __init_fixedarg_id_gen(self):
        """Create fixedargID generator variable."""
        self.fixedArgVal=0
        var = MetaVariable('fixedargID',
                           self.__fixedargID_fetch,
                           singleton_iter_provider(self.__fixedargID_get_cur))
        self.create_var(var,hidden=True)
    
    def __fixedargID_get_cur(self):
        return self.fixedArgVal

    def __fixedargID_fetch(self,x,y):
        self.fixedArgVal+=1
        self.create_standard_var("#fixedArg"+str(self.fixedArgVal),'single',
                                 hidden=True)
        return Response(value="#fixedArg"+str(self.fixedArgVal))

    #############################################################

    #############################################################
    ## Shutdown system

    def initiate_shutdown(self):
        """Initiate shutdown mode."""
        self.is_shutting_down = True
        self.__wake_sentinels()
        self.__result_waiters.kill_all()

