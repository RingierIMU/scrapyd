import datetime
import sqlite3

import pytest
from twisted.internet import defer, error, task
from twisted.logger import LogLevel, capturedLogs, formatEvent
from twisted.python import failure

from scrapyd import launcher as launcher_module
from scrapyd import sqlite
from scrapyd.interfaces import IPoller
from scrapyd.launcher import Launcher
from scrapyd.spiderqueue import SqliteSpiderQueue

MESSAGE = {
    "_project": "p1",
    "_spider": "s1",
    "_job": "j1",
    "settings": {"JOBDIR": "/tmp/jobdir/j1"},
    "arg1": "val1",
}


def running_jobs(config):
    return sqlite.initialize(sqlite.SqliteRunningJobs, config, "jobs", "running_jobs")


@pytest.fixture()
def queues(app, config):
    queues = {"p1": SqliteSpiderQueue(config, "p1")}
    app.getComponent(IPoller).queues = queues
    return queues


class FakeTransport:
    def __init__(self):
        self.signals = []

    def signalProcess(self, signal):  # noqa: N802 Twisted API
        self.signals.append(signal)


class FakeProcess:
    """A ScrapyProcessProtocol stand-in that doesn't start a real process."""

    def __init__(self, project="p1", spider="s1", job="j1"):
        self.project = project
        self.spider = spider
        self.job = job
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.interrupted = False
        self.transport = FakeTransport()
        self.deferred = defer.Deferred()


def add_fake_process(launcher, slot, process):
    process.deferred.addBoth(launcher._process_finished, slot)  # noqa: SLF001
    launcher.processes[slot] = process
    launcher.running_jobs.add(process.project, process.job, process.start_time, MESSAGE)


def test_spawn_records_the_job_as_received(app, config):
    launcher = Launcher(config, app)
    launcher._spawn_process(dict(MESSAGE, settings=dict(MESSAGE["settings"])), 0)  # noqa: SLF001

    rows = list(running_jobs(config))
    assert len(rows) == 1
    message, start_time = rows[0]
    # The environment's per-run settings (LOG_FILE, FEEDS) are not recorded: a re-queued job gets fresh ones.
    assert message == MESSAGE
    assert start_time == launcher.processes[0].start_time


def test_normal_finish_removes_the_running_job(app, config):
    launcher = Launcher(config, app)
    launcher._spawn_process(dict(MESSAGE, settings=dict(MESSAGE["settings"])), 0)  # noqa: SLF001
    process = launcher.processes[0]

    process.processEnded(failure.Failure(error.ProcessDone(0)))

    assert list(running_jobs(config)) == []
    finished = list(launcher.finished)
    assert len(finished) == 1
    assert finished[0].job == "j1"
    assert finished[0].interrupted is False


def test_start_records_and_requeues_interrupted_jobs(app, config, queues):
    start_time = datetime.datetime(2026, 11, 10, 9, 0, 0, 1)
    running_jobs(config).add("p1", "j1", start_time, MESSAGE)

    launcher = Launcher(config, app)
    with capturedLogs() as captured:
        launcher.startService()

    finished = list(launcher.finished)
    assert len(finished) == 1
    assert (finished[0].project, finished[0].spider, finished[0].job) == ("p1", "s1", "j1")
    assert finished[0].interrupted is True
    assert finished[0].start_time == start_time

    # Same job ID, arguments and settings, so a JOBDIR keyed on the job ID resumes.
    assert queues["p1"].list() == [
        {"name": "s1", "_job": "j1", "settings": {"JOBDIR": "/tmp/jobdir/j1"}, "arg1": "val1"}
    ]
    assert list(running_jobs(config)) == []
    assert any("Job interrupted by the previous shutdown, re-queued" in formatEvent(e) for e in captured)


def test_requeue_can_be_turned_off(app, config, queues):
    config.cp.set(config.SECTION, "requeue_interrupted", "off")
    running_jobs(config).add("p1", "j1", datetime.datetime.now(), MESSAGE)

    launcher = Launcher(config, app)
    launcher.startService()

    assert [job.interrupted for job in launcher.finished] == [True]
    assert queues["p1"].list() == []
    assert list(running_jobs(config)) == []


def test_interrupted_job_of_a_deleted_project_is_recorded_not_requeued(app, config, queues):
    running_jobs(config).add("gone", "j9", datetime.datetime.now(), dict(MESSAGE, _project="gone", _job="j9"))

    launcher = Launcher(config, app)
    with capturedLogs() as captured:
        launcher.startService()

    assert [(job.project, job.interrupted) for job in launcher.finished] == [("gone", True)]
    assert queues["p1"].list() == []
    assert any(e["log_level"] == LogLevel.warn for e in captured)


def test_stop_sends_sigterm_and_keeps_rows_for_the_next_start(app, config):
    launcher = Launcher(config, app)
    process = FakeProcess()
    add_fake_process(launcher, 0, process)

    stopped = launcher.stopService()
    assert process.transport.signals == ["TERM"]
    assert not stopped.called

    process.deferred.callback(process)  # the process exits

    assert stopped.called
    assert launcher.processes == {}
    # Not added to the finished jobs now, and still recorded as running: the next start handles both.
    assert list(launcher.finished) == []
    assert len(list(running_jobs(config))) == 1


def test_stop_does_not_take_a_new_job_for_a_freed_slot(app, config, monkeypatch):
    launcher = Launcher(config, app)
    calls = []
    monkeypatch.setattr(launcher, "_get_message", calls.append)
    process = FakeProcess()
    add_fake_process(launcher, 0, process)

    launcher.stopService()
    process.deferred.callback(process)

    assert calls == []


def test_stop_kills_processes_still_running_after_the_timeout(app, config, monkeypatch):
    clock = task.Clock()
    monkeypatch.setattr(launcher_module, "reactor", clock)
    config.cp.set(config.SECTION, "shutdown_timeout", "10")
    launcher = Launcher(config, app)
    process = FakeProcess()
    add_fake_process(launcher, 0, process)

    stopped = launcher.stopService()
    clock.advance(9)
    assert process.transport.signals == ["TERM"]

    clock.advance(1)
    assert process.transport.signals == ["TERM", "KILL"]

    process.deferred.callback(process)
    assert stopped.called
    assert clock.getDelayedCalls() == []


def test_stop_with_nothing_running(app, config):
    assert Launcher(config, app).stopService().called


def test_job_handed_over_during_stop_goes_back_on_the_queue(app, config, queues):
    launcher = Launcher(config, app)
    launcher.stopService()

    launcher._spawn_process(dict(MESSAGE), 0)  # noqa: SLF001

    assert launcher.processes == {}
    assert queues["p1"].list() == [
        {"name": "s1", "_job": "j1", "settings": {"JOBDIR": "/tmp/jobdir/j1"}, "arg1": "val1"}
    ]
    assert list(running_jobs(config)) == []


def test_shutdown_marker_is_off_by_default(app, config, chdir):
    launcher = Launcher(config, app)
    launcher._spawn_process(dict(MESSAGE, settings=dict(MESSAGE["settings"])), 0)  # noqa: SLF001
    assert "SCRAPYD_SHUTDOWN_MARKER" not in launcher.processes[0].env

    add_fake_process(launcher, 1, FakeProcess(job="j2"))
    launcher.stopService()
    assert not any(p.name.startswith(".shutting") for p in chdir.rglob("*"))


def test_shutdown_marker_path_is_passed_to_each_crawl(app, config, tmp_path):
    marker = tmp_path / "state" / ".shutting-down"
    config.cp.set(config.SECTION, "shutdown_marker", str(marker))
    launcher = Launcher(config, app)

    launcher._spawn_process(dict(MESSAGE, settings=dict(MESSAGE["settings"])), 0)  # noqa: SLF001

    assert launcher.processes[0].env["SCRAPYD_SHUTDOWN_MARKER"] == str(marker)
    assert not marker.exists()


def test_stop_writes_the_shutdown_marker_before_signalling(app, config, tmp_path):
    marker = tmp_path / "state" / ".shutting-down"
    config.cp.set(config.SECTION, "shutdown_marker", str(marker))
    launcher = Launcher(config, app)
    seen = []

    class MarkerCheckingTransport(FakeTransport):
        def signalProcess(self, signal):  # noqa: N802 Twisted API
            seen.append((signal, marker.exists()))

    process = FakeProcess()
    process.transport = MarkerCheckingTransport()
    add_fake_process(launcher, 0, process)

    launcher.stopService()

    # The crawl reads the marker when it closes, which is after the signal, so it must already be there.
    assert seen == [("TERM", True)]
    process.deferred.callback(process)
    assert marker.exists()  # stays until the next start


def test_start_removes_a_leftover_shutdown_marker(app, config, queues, tmp_path):
    marker = tmp_path / ".shutting-down"
    marker.write_text("1 2026-11-11T00:00:00\n")
    config.cp.set(config.SECTION, "shutdown_marker", str(marker))

    Launcher(config, app).startService()

    assert not marker.exists()


def test_start_without_a_shutdown_marker_file(app, config, queues, tmp_path):
    config.cp.set(config.SECTION, "shutdown_marker", str(tmp_path / ".shutting-down"))
    Launcher(config, app).startService()


def test_unwritable_shutdown_marker_is_logged_and_the_stop_goes_on(app, config, tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("")
    config.cp.set(config.SECTION, "shutdown_marker", str(blocker / ".shutting-down"))
    launcher = Launcher(config, app)
    process = FakeProcess()
    add_fake_process(launcher, 0, process)

    with capturedLogs() as captured:
        launcher.stopService()

    assert process.transport.signals == ["TERM"]
    assert any(e["log_level"] == LogLevel.error and "shutdown marker" in formatEvent(e) for e in captured)


def test_finished_jobs_table_from_before_the_interrupted_column_is_migrated(tmp_path):
    database = str(tmp_path / "jobs.db")
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE finished_jobs "
        "(id integer PRIMARY KEY, project text, spider text, job text, start_time datetime, end_time datetime)"
    )
    conn.execute(
        "INSERT INTO finished_jobs (project, spider, job, start_time, end_time) VALUES (?, ?, ?, ?, ?)",
        ("p1", "s1", "j1", "2026-11-10 09:00:00.000001", "2026-11-10 10:00:00.000001"),
    )
    conn.commit()
    conn.close()

    jobs = sqlite.SqliteFinishedJobs(database, "finished_jobs")

    assert [row[-1] for row in jobs] == [False]
    # Opening it again doesn't try to add the column twice.
    assert len(sqlite.SqliteFinishedJobs(database, "finished_jobs")) == 1
