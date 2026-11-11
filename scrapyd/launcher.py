import copy
import datetime
import multiprocessing
import os
import sys
from itertools import chain

from twisted.application.service import Service
from twisted.internet import defer, error, protocol, reactor
from twisted.logger import Logger

from scrapyd import __version__, sqlite
from scrapyd.interfaces import IEnvironment, IJobStorage, IPoller

log = Logger()


def get_crawl_args(message):
    """Return the command-line arguments to use for the scrapy crawl process
    that will be started for this message
    """
    copied = message.copy()
    del copied["_project"]

    return [
        copied.pop("_spider"),
        *chain.from_iterable(["-s", f"{key}={value}"] for key, value in copied.pop("settings", {}).items()),
        *chain.from_iterable(["-a", f"{key}={value}"] for key, value in copied.items()),  # spider arguments
    ]


class Launcher(Service):
    name = "launcher"

    def __init__(self, config, app):
        self.processes = {}
        self.finished = app.getComponent(IJobStorage)
        self.max_proc = self._get_max_proc(config)
        self.runner = config.get("runner", "scrapyd.runner")
        self.app = app
        # Jobs survive a restart: running jobs are recorded, and on start any left over are marked interrupted in
        # the finished jobs and, optionally, put back on their project's queue with the same job ID.
        self.running_jobs = sqlite.initialize(sqlite.SqliteRunningJobs, config, "jobs", "running_jobs")
        self.requeue_interrupted = config.getboolean("requeue_interrupted", True)
        self.shutdown_timeout = config.getfloat("shutdown_timeout", 240)
        # A file that exists only while Scrapyd is stopping. Its path is passed to each crawl as
        # SCRAPYD_SHUTDOWN_MARKER, so a crawl that gets SIGTERM can tell a Scrapyd stop (it will be re-queued, so keep
        # its JOBDIR) from a cancel.json, which sends the same signal.
        self.shutdown_marker = config.get("shutdown_marker", "")
        self.stopping = False

    def startService(self):
        self._remove_shutdown_marker()
        self._recover_interrupted_jobs()
        for slot in range(self.max_proc):
            self._get_message(slot)
        log.info(
            "Scrapyd {version} started: max_proc={max_proc!r}, runner={runner!r}",
            version=__version__,
            max_proc=self.max_proc,
            runner=self.runner,
            log_system="Launcher",
        )

    def stopService(self):
        """
        Ask every running Scrapy process to stop gracefully (SIGTERM: Scrapy finishes in-flight requests, runs its
        close handlers and saves JOBDIR state), and wait for them to exit, for at most ``shutdown_timeout`` seconds.
        Processes still running after that are killed.

        Their running-job rows are kept, so the next start marks them interrupted and re-queues them.
        """
        self.stopping = True
        Service.stopService(self)
        self._write_shutdown_marker()

        processes = list(self.processes.values())
        if not processes:
            return defer.succeed(None)

        log.info(
            "Stopping {count} running job(s), waiting up to {timeout}s",
            count=len(processes),
            timeout=self.shutdown_timeout,
            log_system="Launcher",
        )
        for process in processes:
            _signal(process, "TERM")

        done = defer.DeferredList([process.deferred for process in processes], consumeErrors=True)
        timer = reactor.callLater(self.shutdown_timeout, self._kill_remaining)
        done.addBoth(lambda _: timer.cancel() if timer.active() else None)
        return done

    def _write_shutdown_marker(self):
        if not self.shutdown_marker:
            return
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.shutdown_marker)), exist_ok=True)
            with open(self.shutdown_marker, "w") as f:
                f.write(f"{os.getpid()} {datetime.datetime.now().isoformat()}\n")
        except OSError as e:
            log.error(
                "Could not write the shutdown marker {path!r}, crawls will treat this stop as a cancel: {error}",
                path=self.shutdown_marker,
                error=e,
                log_system="Launcher",
            )

    def _remove_shutdown_marker(self):
        if not self.shutdown_marker:
            return
        try:
            os.remove(self.shutdown_marker)
        except FileNotFoundError:
            pass

    def _kill_remaining(self):
        for process in list(self.processes.values()):
            log.warn(
                "Job did not stop within {timeout}s, killing: project={project!r} spider={spider!r} job={job!r}",
                timeout=self.shutdown_timeout,
                project=process.project,
                spider=process.spider,
                job=process.job,
                log_system="Launcher",
            )
            _signal(process, "KILL")

    def _recover_interrupted_jobs(self):
        queues = self.app.getComponent(IPoller).queues
        now = datetime.datetime.now()
        for message, start_time in self.running_jobs:
            project, spider, job = message["_project"], message["_spider"], message["_job"]

            interrupted = ScrapyProcessProtocol(project, spider, job, env={}, args=[])
            interrupted.start_time = start_time
            interrupted.end_time = now
            interrupted.interrupted = True
            self.finished.add(interrupted)

            requeued = False
            if self.requeue_interrupted:
                if project in queues:
                    spider_args = {k: v for k, v in message.items() if k not in ("_project", "_spider")}
                    queues[project].add(spider, **spider_args)
                    requeued = True
                else:
                    log.warn(
                        "Interrupted job's project no longer exists, not re-queued: project={project!r} job={job!r}",
                        project=project,
                        job=job,
                        log_system="Launcher",
                    )

            log.info(
                "Job interrupted by the previous shutdown{action}: project={project!r} spider={spider!r} job={job!r}",
                action=", re-queued" if requeued else "",
                project=project,
                spider=spider,
                job=job,
                log_system="Launcher",
            )
            self.running_jobs.remove(project, job)

    def _get_message(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)

    def _spawn_process(self, message, slot):
        project = message["_project"]
        if self.stopping:
            # The poller handed over a job after shutdown began: put it back rather than lose it.
            queue = self.app.getComponent(IPoller).queues.get(project)
            if queue is not None:
                queue.add(message["_spider"], **{k: v for k, v in message.items() if k not in ("_project", "_spider")})
            return

        # As received from the queue, before the environment adds per-run settings (log and feed paths).
        original = copy.deepcopy(message)

        environment = self.app.getComponent(IEnvironment)
        message.setdefault("settings", {})
        message["settings"].update(environment.get_settings(message))

        env = environment.get_environment(message, slot)
        if self.shutdown_marker:
            env["SCRAPYD_SHUTDOWN_MARKER"] = os.path.abspath(self.shutdown_marker)
        args = [sys.executable, "-m", self.runner, "crawl", *get_crawl_args(message)]

        process = ScrapyProcessProtocol(project, message["_spider"], message["_job"], env, args)
        process.deferred.addBoth(self._process_finished, slot)

        self.running_jobs.add(project, process.job, process.start_time, original)
        reactor.spawnProcess(process, sys.executable, args=args, env=env)
        self.processes[slot] = process

    def _process_finished(self, _, slot):
        process = self.processes.pop(slot)
        if self.stopping:
            # Interrupted by shutdown: keep the running-job row, so the next start records and re-queues it.
            return
        process.end_time = datetime.datetime.now()
        self.finished.add(process)
        self.running_jobs.remove(process.project, process.job)
        self._get_message(slot)

    def _get_max_proc(self, config):
        max_proc = config.getint("max_proc", 0)
        if max_proc:
            return max_proc

        try:
            cpus = multiprocessing.cpu_count()
        except NotImplementedError:  # Windows 17520a3
            cpus = 1
        return cpus * config.getint("max_proc_per_cpu", 4)


# https://docs.twisted.org/en/stable/api/twisted.internet.protocol.ProcessProtocol.html
class ScrapyProcessProtocol(protocol.ProcessProtocol):
    def __init__(self, project, spider, job, env, args):
        self.project = project
        self.spider = spider
        self.job = job
        self.pid = None
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.interrupted = False
        self.args = args
        self.env = env
        self.deferred = defer.Deferred()

    # For equality assertions in tests.
    def __eq__(self, other):
        return (
            self.project == other.project
            and self.spider == other.spider
            and self.job == other.job
            and self.pid == other.pid
            and self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.args == other.args
            and self.env == other.env
        )

    # For error messsages in tests.
    def __repr__(self):
        return (
            f"ScrapyProcessProtocol(project={self.project} spider={self.spider} job={self.job} pid={self.pid} "
            f"start_time={self.start_time} end_time={self.end_time} args={self.args} env={self.env})"
        )

    def outReceived(self, data):
        log.info(data.rstrip(), log_system=f"Launcher,{self.pid}/stdout")

    def errReceived(self, data):
        log.error(data.rstrip(), log_system=f"Launcher,{self.pid}/stderr")

    def connectionMade(self):
        self.pid = self.transport.pid
        self.log("info", "Process started:")

    # https://docs.twisted.org/en/stable/core/howto/process.html#things-that-can-happen-to-your-processprotocol
    def processEnded(self, status):
        if isinstance(status.value, error.ProcessDone):
            self.log("info", "Process finished:")
        else:
            self.log("error", f"Process died: exitstatus={status.value.exitCode!r}")
        self.deferred.callback(self)

    def log(self, level, action):
        getattr(log, level)(
            "{action} project={project!r} spider={spider!r} job={job!r} pid={pid!r} args={args!r}",
            action=action,
            project=self.project,
            spider=self.spider,
            job=self.job,
            pid=self.pid,
            args=self.args,
        )


def _signal(process, signal):
    try:
        process.transport.signalProcess(signal)
    except (AttributeError, error.ProcessExitedAlready):  # not started yet, or already exited
        pass
