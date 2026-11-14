"""Qualification backends, started and stopped by the tests themselves.

Running ``pytest tests/qualification`` is enough: this module starts the local
emulators it needs, runs every test against each one, and tears them down
afterwards. Nothing has to be launched by hand.

An emulator speaks the real wire protocol through the real cloud SDK, so it
catches a class of defect a ``memory://`` filesystem cannot — anything
involving authentication, most obviously. It is still not the cloud service:
consistency, latency, throttling, and IAM are not reproduced.

Escape hatches:

- ``CARAVEL_QUALIFICATION_ARTIFACTS_ROOT`` pins one target and disables
  auto-start. This is how a real credentialed backend is supplied.
- ``CARAVEL_QUALIFICATION_NO_AUTOSTART=1`` disables auto-start and uses only
  already-running emulators on their conventional ports.
"""

from __future__ import annotations

import os as _os

# gcsfs 2026.10+ defaults to an extended implementation with gRPC features that
# cannot talk to a plain-HTTP emulator. The documented switch selects the
# standard ``gcsfs.core`` implementation, and it must be set before gcsfs is
# imported anywhere in the process -- hence the placement above every import.
_os.environ.setdefault("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")

import atexit
import contextlib
import json
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

# Published fixed values for local emulators. Not secrets: they only ever
# address a loopback emulator.
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:latest"
GCS_IMAGE = "fsouza/fake-gcs-server:latest"
GCS_PROJECT = "test"
MOTO_KEY = "testing"

READY_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class QualificationBackend:
    """One target the suite qualifies against."""

    name: str
    tier: str
    root: str
    storage_options: dict[str, Any]

    def __str__(self) -> str:
        return f"{self.name}[{self.tier}]"


@dataclass
class _Emulators:
    """Everything started for this session, and how to stop it."""

    backends: list[QualificationBackend] = field(default_factory=list)
    teardown: list[Callable[[], None]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    started: bool = False

    def stop(self) -> None:
        for finish in reversed(self.teardown):
            with contextlib.suppress(Exception):
                finish()
        self.teardown.clear()


_STATE = _Emulators()


# -- helpers ---------------------------------------------------------------


def _free_port() -> int:
    """Claim a free port, avoiding collisions with whatever else is listening.

    macOS in particular runs AirPlay on :5000, the conventional moto port.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _await_ready(check: Callable[[], bool], budget: float = READY_TIMEOUT_SECONDS) -> bool:
    """Poll until a backend answers, so startup latency is not a flaky test."""
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        with contextlib.suppress(Exception):
            if check():
                return True
        time.sleep(0.25)
    return False


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=15).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _autostart_enabled() -> bool:
    return os.getenv("CARAVEL_QUALIFICATION_NO_AUTOSTART", "").strip() not in (
        "1",
        "true",
        "yes",
    )


# -- backends --------------------------------------------------------------


def _azure_connection_string(host: str, port: int) -> str:
    return (
        f"DefaultEndpointsProtocol=http;AccountName={AZURITE_ACCOUNT};"
        f"AccountKey={AZURITE_KEY};"
        f"BlobEndpoint=http://{host}:{port}/{AZURITE_ACCOUNT};"
    )


def _azure_backend(host: str, port: int) -> QualificationBackend | None:
    """Provision a container on a reachable Azurite, or give up quietly."""
    import fsspec

    connection_string = _azure_connection_string(host, port)
    container = f"caravel-qual-{uuid.uuid4().hex[:12]}"

    def provision() -> bool:
        fsspec.filesystem("az", connection_string=connection_string).mkdir(container)
        return True

    if not _await_ready(provision, budget=20.0):
        return None
    return QualificationBackend(
        name="azure-blob",
        tier="emulator",
        root=f"az://{container}/run",
        storage_options={"connection_string": connection_string},
    )


def _s3_backend(endpoint: str) -> QualificationBackend | None:
    """Provision a bucket on a reachable S3 endpoint, or give up quietly."""
    import s3fs

    options: dict[str, Any] = {
        "key": MOTO_KEY,
        "secret": MOTO_KEY,
        "client_kwargs": {"endpoint_url": endpoint},
    }
    bucket = f"caravel-qual-{uuid.uuid4().hex[:12]}"

    def provision() -> bool:
        s3fs.S3FileSystem(
            key=MOTO_KEY, secret=MOTO_KEY, client_kwargs={"endpoint_url": endpoint}
        ).mkdir(bucket)
        return True

    if not _await_ready(provision, budget=20.0):
        return None
    return QualificationBackend(
        name="s3", tier="emulator", root=f"s3://{bucket}/run", storage_options=options
    )


def _gcs_backend(host: str, port: int) -> QualificationBackend | None:
    """Provision a bucket on a reachable fake-gcs-server, or give up quietly."""
    import fsspec
    import requests

    endpoint = f"http://{host}:{port}"
    bucket = f"caravel-qual-{uuid.uuid4().hex[:12]}"
    options: dict[str, Any] = {
        "token": "anon",
        "project": GCS_PROJECT,
        "endpoint_url": endpoint,
    }

    def provision() -> bool:
        requests.post(
            f"{endpoint}/storage/v1/b?project={GCS_PROJECT}",
            json={"name": bucket},
            timeout=5,
        )
        fsspec.filesystem("gs", **options).ls(bucket)
        return True

    if not _await_ready(provision, budget=20.0):
        return None
    return QualificationBackend(
        name="gcs", tier="emulator", root=f"gs://{bucket}/run", storage_options=options
    )


# -- starting them ---------------------------------------------------------


def _start_azurite(state: _Emulators) -> None:
    """Azurite needs a container runtime; without one, Azure is simply skipped."""
    host = os.getenv("CARAVEL_AZURITE_HOST", "127.0.0.1")
    configured_port = os.getenv("CARAVEL_AZURITE_PORT")

    if configured_port and _reachable(host, int(configured_port)):
        backend = _azure_backend(host, int(configured_port))
        if backend is not None:
            state.backends.append(backend)
        return
    if _reachable(host, 10000):
        backend = _azure_backend(host, 10000)
        if backend is not None:
            state.backends.append(backend)
            return

    if not _autostart_enabled():
        state.notes.append("azure-blob: no emulator running and auto-start disabled")
        return
    if not _docker_available():
        state.notes.append("azure-blob: skipped, no container runtime available")
        return

    port = _free_port()
    name = f"caravel-azurite-{uuid.uuid4().hex[:8]}"
    started = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            f"{port}:10000",
            AZURITE_IMAGE,
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--inMemoryPersistence",
            # The SDK negotiates a service API version that an Azurite release
            # may predate, which fails every request with InvalidHeaderValue.
            # Both ends float here, so the emulator is told to accept the
            # version rather than pinning two moving dependencies to each other.
            "--skipApiVersionCheck",
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if started.returncode != 0:
        state.notes.append(
            f"azure-blob: container failed to start ({started.stderr.strip()[:120]})"
        )
        return
    state.teardown.append(
        lambda: subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)
    )

    if not _await_ready(lambda: _reachable(host, port)):
        state.notes.append("azure-blob: container never became reachable")
        return
    backend = _azure_backend(host, port)
    if backend is None:
        state.notes.append("azure-blob: container reachable but would not provision")
        return
    state.backends.append(backend)


def _start_gcs(state: _Emulators) -> None:
    """fake-gcs-server needs a container runtime; without one, GCS is skipped."""
    host = os.getenv("CARAVEL_GCS_HOST", "127.0.0.1")
    configured_port = os.getenv("CARAVEL_GCS_PORT")

    for candidate in (configured_port, "4443"):
        if candidate and _reachable(host, int(candidate)):
            backend = _gcs_backend(host, int(candidate))
            if backend is not None:
                state.backends.append(backend)
            return

    if not _autostart_enabled():
        state.notes.append("gcs: no emulator running and auto-start disabled")
        return
    if not _docker_available():
        state.notes.append("gcs: skipped, no container runtime available")
        return

    port = _free_port()
    name = f"caravel-gcs-{uuid.uuid4().hex[:8]}"
    started = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            f"{port}:{port}",
            GCS_IMAGE,
            "-scheme",
            "http",
            "-port",
            str(port),
            "-public-host",
            f"{host}:{port}",
            "-backend",
            "memory",
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if started.returncode != 0:
        state.notes.append(f"gcs: container failed to start ({started.stderr.strip()[:120]})")
        return
    state.teardown.append(
        lambda: subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)
    )

    if not _await_ready(lambda: _reachable(host, port)):
        state.notes.append("gcs: container never became reachable")
        return
    backend = _gcs_backend(host, port)
    if backend is None:
        state.notes.append("gcs: container reachable but would not provision")
        return
    state.backends.append(backend)


def _start_moto(state: _Emulators) -> None:
    """moto runs in-process, so S3 needs no container runtime at all."""
    host = os.getenv("CARAVEL_MOTO_HOST", "127.0.0.1")
    configured_port = os.getenv("CARAVEL_MOTO_PORT")
    if configured_port and _reachable(host, int(configured_port)):
        backend = _s3_backend(f"http://{host}:{configured_port}")
        if backend is not None:
            state.backends.append(backend)
        return

    if not _autostart_enabled():
        state.notes.append("s3: no emulator running and auto-start disabled")
        return
    try:
        from moto.server import ThreadedMotoServer
    except ImportError:
        state.notes.append("s3: skipped, moto[server] is not installed")
        return

    port = _free_port()
    server = ThreadedMotoServer(ip_address=host, port=port, verbose=False)
    server.start()
    state.teardown.append(server.stop)

    if not _await_ready(lambda: _reachable(host, port)):
        state.notes.append("s3: moto never became reachable")
        return
    backend = _s3_backend(f"http://{host}:{port}")
    if backend is None:
        state.notes.append("s3: moto reachable but would not provision")
        return
    state.backends.append(backend)


def _configured_backend() -> QualificationBackend | None:
    root = os.getenv("CARAVEL_QUALIFICATION_ARTIFACTS_ROOT", "").strip()
    if not root:
        return None
    raw = os.getenv("CARAVEL_QUALIFICATION_STORAGE_OPTIONS", "").strip()
    return QualificationBackend(
        name=root.split("://", 1)[0] or "configured",
        tier="configured",
        root=root.rstrip("/"),
        storage_options=json.loads(raw) if raw else {},
    )


# -- pytest wiring ---------------------------------------------------------

SKIP_REASON = (
    "No qualification backend could be started. Install the test extra "
    "(moto[server], s3fs, adlfs) for in-process S3, and/or start a container "
    "runtime for Azurite. Alternatively set CARAVEL_QUALIFICATION_ARTIFACTS_ROOT "
    "to a dedicated prefix on real object storage. Until one runs, remote "
    "artifact storage is LOCALLY QUALIFIED only."
)


def _ensure_started() -> None:
    """Start emulators once, on first need.

    This deliberately does not use ``pytest_sessionstart``: a conftest in a
    subdirectory is only loaded during collection, so that hook never fires for
    a full-suite run and the qualification tests would silently skip.
    Collection-time hooks are the earliest point this module is guaranteed to
    be active.
    """
    if _STATE.started:
        return
    _STATE.started = True
    atexit.register(_STATE.stop)

    configured = _configured_backend()
    if configured is not None:
        _STATE.backends.append(configured)
        return
    _start_moto(_STATE)
    _start_azurite(_STATE)
    _start_gcs(_STATE)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    del session, exitstatus
    _STATE.stop()


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "backend" not in metafunc.fixturenames:
        return
    _ensure_started()
    if not _STATE.backends:
        return
    metafunc.parametrize(
        "backend",
        _STATE.backends,
        ids=[str(backend) for backend in _STATE.backends],
    )


@pytest.fixture()
def backend() -> QualificationBackend:
    """Only reached when nothing could be started; parametrization replaces it."""
    detail = "; ".join(_STATE.notes) if _STATE.notes else "no backends discovered"
    pytest.skip(f"{SKIP_REASON} ({detail})")
    raise AssertionError("unreachable")  # pragma: no cover


def pytest_report_collectionfinish(config: pytest.Config) -> list[str]:
    """Report after collection: that is when the emulators have been started."""
    del config
    if not _STATE.started:
        return []
    if not _STATE.backends:
        return [f"qualification backends: none ({'; '.join(_STATE.notes) or 'n/a'})"]
    listed = ", ".join(f"{backend} -> {backend.root}" for backend in _STATE.backends)
    skipped = f" | unavailable: {'; '.join(_STATE.notes)}" if _STATE.notes else ""
    return [f"qualification backends: {listed}{skipped}"]
