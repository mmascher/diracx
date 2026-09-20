# ruff: noqa: E402

"""Tests for the Condor job executor tasks."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tarfile
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import ValidationError

sys.modules.setdefault("htcondor2", types.ModuleType("htcondor2"))

from diracx.core.models import JobStatus
from diracx.tasks.jobs import CondorJobExecutorMonitorTask, CondorJobExecutorTask
from diracx.tasks.jobs import condor_job_executor as condor_job_executor_module
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.locks import MutexLock

FEATURE_ENABLED_ENV = "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_ENABLED"
FEATURE_INTERVAL_ENV = "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_INTERVAL_SECONDS"

SCHEDULER_STATE_SCRIPT = """
import json
import sys
import types
from datetime import UTC, datetime
from unittest.mock import MagicMock

sys.modules.setdefault("htcondor2", types.ModuleType("htcondor2"))

from diracx.tasks.plumbing.factory import load_task_registry
from diracx.tasks.plumbing.scheduler.scheduler import TaskScheduler

task_name = "jobs:CondorJobExecutorMonitorTask"
registry = load_task_registry()
task_cls = registry[task_name]
scheduler = TaskScheduler(
    broker=MagicMock(),
    redis_url="redis://unused",
    task_registry=registry,
)
before = datetime.now(tz=UTC)
scheduler._compute_initial_schedules()
next_run = scheduler._next_runs.get((task_name, ""))
print(
    json.dumps(
        {
            "enabled": task_cls._enabled,
            "tracked": next_run is not None,
            "delay_seconds": (
                (next_run - before).total_seconds() if next_run is not None else None
            ),
        }
    )
)
"""


def make_dependencies():
    return {
        "config": MagicMock(name="config"),
        "job_db": AsyncMock(name="job_db"),
        "job_logging_db": AsyncMock(name="job_logging_db"),
        "task_queue_db": MagicMock(name="task_queue_db"),
        "job_parameters_db": MagicMock(name="job_parameters_db"),
    }


@pytest.fixture
def sandbox_settings():
    return types.SimpleNamespace(
        bucket_name="sandboxes",
        url_validity_seconds=300,
        s3_client=types.SimpleNamespace(
            generate_presigned_url=AsyncMock(
                return_value="https://sandbox.invalid/archive"
            )
        ),
    )


@pytest.fixture
def fake_htcondor(monkeypatch):
    fake = types.SimpleNamespace(
        __file__="mock-htcondor",
        set_subsystem=MagicMock(),
        param={},
        enable_log=MagicMock(),
        SecurityContext=MagicMock(),
        Collector=MagicMock(),
        DaemonTypes=types.SimpleNamespace(Schedd="Schedd"),
        Schedd=MagicMock(),
        Submit=MagicMock(side_effect=lambda text: text),
    )
    fake.Collector.return_value.locate.return_value = {"Name": "test-schedd"}
    fake.Schedd.return_value.submit.return_value.cluster.return_value = 12345
    monkeypatch.setattr(condor_job_executor_module, "htcondor2", fake)
    monkeypatch.setenv("CONDOR_TOKEN", "test-token")
    monkeypatch.setattr(
        condor_job_executor_module,
        "_executor_settings",
        types.SimpleNamespace(
            **(
                condor_job_executor_module._executor_settings.model_dump()
                | {"proxy_path": None}
            )
        ),
    )
    return fake


def archive_bytes(members: list[tuple[tarfile.TarInfo, bytes]]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:bz2") as archive:
        for member, contents in members:
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
    return stream.getvalue()


def mock_sandbox_download(monkeypatch, content: bytes, status_code: int = 200):
    client_cls = httpx.AsyncClient
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status_code, content=content)
    )
    monkeypatch.setattr(
        condor_job_executor_module.httpx,
        "AsyncClient",
        lambda: client_cls(transport=transport),
    )


def get_scheduler_state(feature_env: dict[str, str]) -> dict:
    env = os.environ.copy()
    env.pop(FEATURE_ENABLED_ENV, None)
    env.pop(FEATURE_INTERVAL_ENV, None)
    env.update(feature_env)
    result = subprocess.run(
        [sys.executable, "-c", SCHEDULER_STATE_SCRIPT],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


def test_monitor_schedule_activation_is_environment_controlled():
    default_state = get_scheduler_state({})
    assert default_state == {
        "enabled": False,
        "tracked": False,
        "delay_seconds": None,
    }

    local_state = get_scheduler_state(
        {
            FEATURE_ENABLED_ENV: "true",
            FEATURE_INTERVAL_ENV: "10",
        }
    )
    assert local_state["enabled"] is True
    assert local_state["tracked"] is True
    assert 9 <= local_state["delay_seconds"] <= 11


def test_monitor_interval_must_be_positive():
    with pytest.raises(ValidationError):
        condor_job_executor_module.CondorJobExecutorSettings(interval_seconds=0)


def test_executor_takes_a_per_job_mutex():
    locks = CondorJobExecutorTask(job_id=42).execution_locks

    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert locks[0].redis_key == "lock:mutex:job:42"


async def test_executor_submits_job_and_marks_it_matched(monkeypatch, sandbox_settings):
    submission = condor_job_executor_module.CondorSubmitResult(
        cluster_id=1234,
        proc_id=7,
        schedd_name="analysis-schedd",
    )
    submit_to_condor = AsyncMock(return_value=submission)
    monkeypatch.setattr(CondorJobExecutorTask, "submit_to_condor", submit_to_condor)
    deps = make_dependencies()

    result = await CondorJobExecutorTask(job_id=42).execute(
        **deps, sandbox_settings=sandbox_settings
    )

    assert result == 42
    submit_to_condor.assert_awaited_once_with(
        config=deps["config"], job_db=deps["job_db"], sandbox_settings=sandbox_settings
    )
    deps["job_db"].set_job_attributes.assert_awaited_once()
    matched_updates = deps["job_db"].set_job_attributes.await_args.args[0]
    assert matched_updates[42]["Status"] == JobStatus.MATCHED
    assert matched_updates[42]["MinorStatus"] == "CondorExecutor"
    # The status collector requires ClusterId.ProcId, not a human-readable message.
    assert matched_updates[42]["ApplicationStatus"] == "1234.7"
    deps["job_logging_db"].insert_records.assert_awaited_once()


def test_jdl_key_value_pairs_are_extracted_as_dict():
    jdl = '[ Executable = "/bin/echo"; Arguments = "hello"; Requirements = (TARGET.FileSystemDomain == "disk"); ]'

    assert condor_job_executor_module._jdl_to_key_value_pairs(jdl) == {
        "Executable": "/bin/echo",
        "Arguments": "hello",
        "Requirements": 'TARGET.FileSystemDomain == "disk"',
    }


async def test_submit_to_condor_uses_htcondor_bindings(
    monkeypatch, fake_htcondor, sandbox_settings
):
    monkeypatch.setattr(
        condor_job_executor_module,
        "extractJDL",
        lambda raw_jdl: '[ Executable = "/bin/echo"; Arguments = "hello"; ]',
    )

    deps = make_dependencies()
    deps["job_db"].search.return_value = (1, [{"JobID": 42}])
    deps["job_db"].get_job_jdls.return_value = {42: "eJyFakeCompressedPayload"}

    result = await CondorJobExecutorTask(job_id=42).submit_to_condor(
        config=deps["config"],
        job_db=deps["job_db"],
        sandbox_settings=sandbox_settings,
    )

    assert result.cluster_id == 12345
    assert result.proc_id == 0
    assert (
        result.schedd_name == condor_job_executor_module._executor_settings.schedd_name
    )
    fake_htcondor.SecurityContext.assert_called_once_with(token="test-token")
    fake_htcondor.Collector.assert_called_once_with(
        condor_job_executor_module._executor_settings.collector_host,
        security=fake_htcondor.SecurityContext.return_value,
    )
    submit_text = fake_htcondor.Submit.call_args.args[0]
    assert "Executable = /bin/echo" in submit_text
    assert "transfer_executable = false" in submit_text
    fake_htcondor.Schedd.return_value.submit.assert_called_once_with(
        submit_text, spool=True
    )
    fake_htcondor.Schedd.return_value.spool.assert_called_once_with(
        fake_htcondor.Schedd.return_value.submit.return_value
    )
    assert not Path(fake_htcondor.param["SEC_TOKEN_DIRECTORY"]).exists()


async def test_monitor_moves_received_jobs_and_schedules_executors(monkeypatch):
    scheduled = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self.job_id)
        return "task-id"

    monkeypatch.setattr(CondorJobExecutorTask, "schedule", fake_schedule)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (2, [{"JobID": 1}, {"JobID": 2}])

    result = await CondorJobExecutorMonitorTask().execute(**deps)

    assert result == 2
    deps["job_db"].search.assert_awaited_once()
    (search_spec,) = deps["job_db"].search.await_args.args[1]
    assert search_spec["parameter"] == "Status"
    assert search_spec["value"] == JobStatus.RECEIVED
    deps["job_db"].set_job_attributes.assert_awaited_once()
    waiting_updates = deps["job_db"].set_job_attributes.await_args.args[0]
    assert waiting_updates[1]["Status"] == JobStatus.WAITING
    assert waiting_updates[2]["Status"] == JobStatus.WAITING
    deps["job_logging_db"].insert_records.assert_awaited_once()
    assert scheduled == [1, 2]


async def test_monitor_does_nothing_without_received_jobs(monkeypatch):
    schedule_executor = AsyncMock()
    monkeypatch.setattr(CondorJobExecutorTask, "schedule", schedule_executor)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (0, [])

    result = await CondorJobExecutorMonitorTask().execute(**deps)

    assert result == 0
    deps["job_db"].set_job_attributes.assert_not_awaited()
    deps["job_logging_db"].insert_records.assert_not_awaited()
    schedule_executor.assert_not_awaited()


def test_proxy_path_is_environment_controlled(monkeypatch):
    monkeypatch.setenv(
        "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_PROXY_PATH", "/credentials/x509up"
    )
    assert (
        condor_job_executor_module.CondorJobExecutorSettings().proxy_path
        == "/credentials/x509up"
    )


def test_executor_injects_sandbox_settings():
    wrapped = wrap_task(CondorJobExecutorTask)
    assert "sandbox_settings" in {
        dependency.name for dependency in wrapped._dependant.dependencies
    }


async def test_status_collector_still_queries_cluster_proc_id(fake_htcondor):
    job_db = AsyncMock()
    job_db.search.return_value = (1, [{"JobID": 42, "ApplicationStatus": "12345.0"}])
    fake_htcondor.Schedd.return_value.query.return_value = [{"JobStatus": 2}]

    result = await condor_job_executor_module.CondorJobStatusCollectorTask(
        job_id=42
    ).query_from_condor(job_db=job_db)

    assert result == 2
    fake_htcondor.Schedd.return_value.query.assert_called_once_with(
        constraint="ClusterId == 12345 && ProcId == 0", projection=["JobStatus"]
    )
    fake_htcondor.SecurityContext.assert_called_once_with(token="test-token")


@pytest.mark.parametrize("prefix", ["", "SB:SandboxSE|"])
async def test_materialise_downloads_the_correct_s3_key(
    monkeypatch, sandbox_settings, tmp_path, prefix
):
    executable = tarfile.TarInfo("mc_prod.sh")
    executable.mode = 0o755
    contents = b"#!/bin/sh\necho hello\n"
    mock_sandbox_download(monkeypatch, archive_bytes([(executable, contents)]))
    pfn = f"{prefix}/S3/sandboxes/CMS/group/user/sha256:digest.tar.bz2"

    files = await condor_job_executor_module._materialise_input_sandbox(
        [pfn], sandbox_settings, str(tmp_path)
    )

    assert files == [str(tmp_path / "mc_prod.sh")]
    assert Path(files[0]).read_bytes() == contents
    assert os.access(files[0], os.X_OK)
    sandbox_settings.s3_client.generate_presigned_url.assert_awaited_once_with(
        ClientMethod="get_object",
        Params={"Bucket": "sandboxes", "Key": "CMS/group/user/sha256:digest.tar.bz2"},
        ExpiresIn=300,
    )


async def test_materialise_keeps_local_inputs(sandbox_settings, tmp_path):
    local_file = str(tmp_path / "local.py")
    files = await condor_job_executor_module._materialise_input_sandbox(
        [local_file], sandbox_settings, str(tmp_path)
    )
    assert files == [local_file]
    sandbox_settings.s3_client.generate_presigned_url.assert_not_awaited()


async def test_materialise_reports_missing_archive(
    monkeypatch, sandbox_settings, tmp_path
):
    mock_sandbox_download(monkeypatch, b"NoSuchKey", status_code=404)
    with pytest.raises(httpx.HTTPStatusError):
        await condor_job_executor_module._materialise_input_sandbox(
            ["/S3/sandboxes/missing.tar.bz2"], sandbox_settings, str(tmp_path)
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("symlink", [False, True])
async def test_materialise_rejects_paths_outside_workdir(
    monkeypatch, sandbox_settings, tmp_path, symlink
):
    member = tarfile.TarInfo("../outside.txt")
    if symlink:
        member = tarfile.TarInfo("link")
        member.type = tarfile.SYMTYPE
        member.linkname = "../outside.txt"
    mock_sandbox_download(monkeypatch, archive_bytes([(member, b"")]))
    workdir = tmp_path / "sandbox"
    workdir.mkdir()

    with pytest.raises(tarfile.FilterError):
        await condor_job_executor_module._materialise_input_sandbox(
            ["/S3/sandboxes/unsafe.tar.bz2"], sandbox_settings, str(workdir)
        )
    assert not (tmp_path / "outside.txt").exists()


def test_submit_description_uses_materialised_paths():
    description = condor_job_executor_module._jdl_dict_to_submit_description(
        {
            "Executable": "mc_prod.sh",
            "InputSandbox": '{"SB:SandboxSE|/S3/sandboxes/archive.tar.bz2"}',
        },
        input_files=["/worker/sandbox/mc_prod.sh", "/credentials/x509up"],
        executable="/worker/sandbox/mc_prod.sh",
    )
    assert "Executable = /worker/sandbox/mc_prod.sh" in description
    assert (
        "transfer_input_files = /worker/sandbox/mc_prod.sh, /credentials/x509up"
        in description
    )
    assert "SB:" not in description
    assert "transfer_executable = false" not in description


@pytest.mark.parametrize("failure", [None, "download", "submit", "spool"])
async def test_sandbox_lifetime_covers_spooling(
    monkeypatch, sandbox_settings, fake_htcondor, tmp_path, failure
):
    contents = b"#!/bin/sh\necho hello\n"
    executable = tarfile.TarInfo("mc_prod.sh")
    executable.mode = 0o755
    mock_sandbox_download(
        monkeypatch,
        archive_bytes(
            [(executable, contents), (tarfile.TarInfo("config.py"), b"# config")]
        ),
        status_code=404 if failure == "download" else 200,
    )
    proxy = tmp_path / "x509up"
    proxy.write_text("test proxy, not a credential")
    proxy.chmod(0o600)
    monkeypatch.setattr(
        condor_job_executor_module._executor_settings, "proxy_path", str(proxy)
    )
    monkeypatch.setattr(
        condor_job_executor_module,
        "extractJDL",
        lambda raw: (
            '[Executable = "mc_prod.sh"; InputSandbox = {"SB:SandboxSE|/S3/sandboxes/archive.tar.bz2"};]'
        ),
    )
    # Keep temporary directories under the test directory so cleanup can be checked.
    monkeypatch.setattr(condor_job_executor_module.tempfile, "tempdir", str(tmp_path))
    deps = make_dependencies()
    deps["job_db"].search.return_value = (1, [{"JobID": 42}])
    deps["job_db"].get_job_jdls.return_value = {42: "compressed-jdl"}
    schedd = fake_htcondor.Schedd.return_value
    transferred = []

    def check_files_exist():
        assert [path.name for path in transferred] == [
            "mc_prod.sh",
            "config.py",
            "x509up",
        ]
        assert all(path.is_file() for path in transferred)
        assert transferred[0].read_bytes() == contents
        token = Path(fake_htcondor.param["SEC_TOKEN_DIRECTORY"]) / "dirac.token"
        assert token.read_text() == "test-token"
        assert token.stat().st_mode & 0o777 == 0o600

    def submit(description, *, spool):
        assert spool is True
        transfer_line = next(
            line
            for line in description.splitlines()
            if line.startswith("transfer_input_files = ")
        )
        transferred.extend(
            Path(path) for path in transfer_line.split(" = ", 1)[1].split(", ")
        )
        assert f"Executable = {transferred[0]}" in description
        assert "SB:" not in description
        check_files_exist()
        if failure == "submit":
            raise RuntimeError("test submit failure")
        return schedd.submit.return_value

    def spool(result):
        check_files_exist()
        if failure == "spool":
            raise RuntimeError("test spool failure")

    schedd.submit.side_effect = submit
    schedd.spool.side_effect = spool
    task = CondorJobExecutorTask(job_id=42)
    kwargs = dict(
        config=deps["config"], job_db=deps["job_db"], sandbox_settings=sandbox_settings
    )
    if failure == "download":
        with pytest.raises(httpx.HTTPStatusError):
            await task.submit_to_condor(**kwargs)
        schedd.submit.assert_not_called()
    elif failure:
        with pytest.raises(RuntimeError, match=f"test {failure} failure"):
            await task.submit_to_condor(**kwargs)
    else:
        result = await task.submit_to_condor(**kwargs)
        assert result.cluster_id == 12345
        schedd.spool.assert_called_once()

    assert not list(tmp_path.glob("diracx-condor-*"))
    assert not list(tmp_path.glob("condor-tokens-*"))
    assert proxy.is_file()  # Never delete the original configured credential.


async def test_missing_proxy_is_rejected_before_submission(
    monkeypatch, sandbox_settings, fake_htcondor, tmp_path
):
    monkeypatch.setattr(
        condor_job_executor_module._executor_settings,
        "proxy_path",
        str(tmp_path / "missing"),
    )
    monkeypatch.setattr(
        condor_job_executor_module,
        "extractJDL",
        lambda raw: '[Executable = "/bin/echo";]',
    )
    deps = make_dependencies()
    deps["job_db"].search.return_value = (1, [{"JobID": 42}])
    deps["job_db"].get_job_jdls.return_value = {42: "compressed-jdl"}

    with pytest.raises(FileNotFoundError, match="Condor input file does not exist"):
        await CondorJobExecutorTask(job_id=42).submit_to_condor(
            config=deps["config"],
            job_db=deps["job_db"],
            sandbox_settings=sandbox_settings,
        )
    fake_htcondor.Schedd.return_value.submit.assert_not_called()
