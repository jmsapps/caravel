"""Readable published output, derived from committed execution state.

Execution keeps immutable attempt artifacts under internal storage: unique,
uuid-named, never rewritten. That is right for recovery and wrong for people.
This module projects committed results into a readable tree named after the
declaration:

    <output>/<pipeline>/<job>/<NNN-stage>/<NNN-step>/<partition>.json

Three properties keep the projection honest:

- **It is derived, never authoritative.** Committed metadata is the only source
  of truth. Nothing reads the published tree back as state, so it cannot
  silently disagree with execution.
- **Progress is tracked in the store**, as ``work_units.published_generation``,
  not in a sidecar beside the files. There is no second mutable copy to drift.
- **It copies committed bytes** rather than re-serializing, so published
  content cannot differ from what was committed.

Publication is asynchronous with respect to the commit: a unit is committed,
then materialized, then marked published. A crash between those steps leaves
the unit sealed-but-unpublished, and the next invocation republishes it.
Republishing is idempotent because it rewrites the same committed bytes.

Materialization uses isolated generation directories. A delayed publisher writes
only its own generation, while conditional progress updates establish the current
publication and its file references. Retention cleanup preserves newer generations.

Promotion is a rename on a local filesystem. On an object store there is no
atomic directory swap, so a reader can observe a partially promoted partition.

**The store is the completion authority, not the marker.** ``_job.json`` is a
convenience snapshot, written last and withdrawn when the writer notices the
job moved underneath it. That check is best effort: a writer that dies between
writing the file and re-reading the job leaves a stale marker behind, and no
file-only protocol can prevent it. So **marker presence alone never means a
job completed.** Confirm it with :func:`validate_job_manifest`, or ask the
store directly. A reader with no store access gets a consistent snapshot of
some past moment, which is a weaker thing and is documented as such.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable

from ..storage import (
    StoragePath,
    ensure_parent_dir,
    join_path,
    resolve_fs,
    to_storage_string,
)
from .commits import BatchPublisher
from .output import AttemptOutputStore, OutputReference
from .sqlite import SQLiteDurableStore
from .types import WorkUnitKey

PUBLICATION_SCHEMA_VERSION = 1
JOB_MANIFEST_NAME = "_job.json"
_STAGING_SUFFIX = ".__staging__"
_TRASH_SUFFIX = ".__trash__"

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")
_MAX_SEGMENT = 80


def readable_segment(value: str) -> str:
    """Return a readable, safe, collision-free path segment.

    Unlike the internal attempt encoding, this optimises for a human reading a
    directory listing. When sanitising changes the value -- or truncates it --
    a short digest of the original is appended so two different identities can
    never collapse onto one published path.
    """
    if not value:
        return "_empty"
    cleaned = _UNSAFE.sub("-", value).strip("-") or "_"
    if cleaned in (".", ".."):
        cleaned = f"_{cleaned.replace('.', 'dot')}"
    lossy = cleaned != value
    if len(cleaned) > _MAX_SEGMENT:
        cleaned = cleaned[:_MAX_SEGMENT]
        lossy = True
    if lossy:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
        return f"{cleaned}~{digest}"
    return cleaned


@dataclass(frozen=True)
class PublishedFile:
    """One committed artifact and where it is published.

    ``relative`` is relative to the job directory, so a completion manifest
    entry resolves directly. ``inner`` is the path within the partition's
    staging directory, which is what promotion moves.
    """

    source: str
    relative: str
    inner: str


@dataclass(frozen=True)
class PublishedLayout:
    """Where a job's readable output lives."""

    root: StoragePath
    pipeline_id: str
    job_id: str

    def job_dir(self) -> str:
        """Nest a hierarchical pipeline id rather than flattening it.

        A pipeline id is hierarchical by design -- ``customer-42/documents`` --
        so each component becomes a directory. Every component is sanitised
        individually, so a separator inside one component still cannot escape.
        """
        parts = [readable_segment(part) for part in self.pipeline_id.split("/") if part]
        return join_path(self.root, *parts, readable_segment(self.job_id))

    def manifest_path(self) -> str:
        return join_path(self.job_dir(), JOB_MANIFEST_NAME)

    def step_dir(self, node: Any) -> str:
        """Named after the declaration, ordered by its indexes."""
        stage = f"{node.stage_index:03d}-{readable_segment(node.stage_name)}"
        step = f"{node.entry_index:03d}-{readable_segment(node.step.name or node.node_id)}"
        return join_path(self.job_dir(), stage, step)

    def partition_base(self, node: Any, partition_id: str, generation: int) -> str:
        """Where one generation of a partition is published.

        Each generation gets its own path, so publishing is write-once. A
        publisher that stalled across a recomputation writes its own
        generation's files and can never overwrite a newer generation's. The
        current generation is named by the store and by the completion
        manifest, never inferred from the directory listing.
        """
        leaf = readable_segment(partition_id)
        if generation:
            leaf = f"{leaf}@g{generation}"
        return join_path(self.step_dir(node), leaf)

    def partition_prefix(self, node: Any, partition_id: str) -> str:
        """The name every generation of this partition starts with."""
        return readable_segment(partition_id)


class OutputPublisher:
    """Materializes committed results into the published tree."""

    def __init__(
        self,
        store: SQLiteDurableStore,
        attempts: AttemptOutputStore,
        *,
        root: StoragePath,
        pipeline_id: str,
        storage_options: dict[str, Any] | None = None,
        keep_superseded: int | None = None,
    ) -> None:
        if keep_superseded is not None and keep_superseded < 0:
            raise ValueError("keep_superseded must be None or non-negative.")
        self._store = store
        self._attempts = attempts
        self._publisher = BatchPublisher(store, attempts)
        self.root = root
        self.pipeline_id = pipeline_id
        self._storage_options = storage_options
        self.keep_superseded = keep_superseded

    def layout(self, job_id: str) -> PublishedLayout:
        return PublishedLayout(root=self.root, pipeline_id=self.pipeline_id, job_id=job_id)

    # -- filesystem helpers ------------------------------------------------

    def _remove(self, path: str) -> None:
        fs, resolved = resolve_fs(path, self._storage_options)
        if fs.exists(resolved):
            fs.rm(resolved, recursive=True)

    def _copy(self, source: str, destination: str) -> None:
        source_fs, source_path = resolve_fs(source, self._attempts.storage_options)
        payload = source_fs.cat_file(source_path)
        target_fs, target_path = resolve_fs(destination, self._storage_options)
        ensure_parent_dir(target_fs, target_path)
        with target_fs.open(target_path, "wb") as handle:
            handle.write(payload)

    def _move(self, source: str, destination: str) -> None:
        fs, resolved_source = resolve_fs(source, self._storage_options)
        _, resolved_target = resolve_fs(destination, self._storage_options)
        ensure_parent_dir(fs, resolved_target)
        fs.mv(resolved_source, resolved_target, recursive=True)

    # -- deciding what goes where -----------------------------------------

    def planned_files(self, node: Any, key: WorkUnitKey, generation: int) -> list[PublishedFile]:
        """Map a unit's committed artifacts onto published paths.

        A pure function of committed metadata, so the completion manifest can
        record exactly what publication writes without a second source.

        Paths *inside* a committed artifact are preserved: a partitioned
        dataset keyed ``en/record_001`` publishes to ``en/record_001.json``,
        not to a flattened ``record_001.json`` that would collide with the
        French one.
        """
        references = self._publisher.committed_references(key)
        batches = self._store.work_unit_batches(key)
        single_batch = len(references) == 1
        layout = self.layout(key.job_id)
        directory = posixpath.basename(layout.partition_base(node, key.partition_id, generation))
        readable = readable_segment(key.partition_id)
        step_prefix = posixpath.relpath(
            to_storage_string(layout.step_dir(node)), to_storage_string(layout.job_dir())
        )
        planned: list[PublishedFile] = []

        for index, reference in enumerate(references):
            files = self._committed_files(reference)
            single_file = single_batch and len(files) == 1 and reference.partition_keys is None
            batch_prefix = "" if single_batch else readable_segment(batches[index].batch_id)
            for source in files:
                artifact_inner = self._relative_to_artifact(reference.path, source)
                parts = []
                if batch_prefix:
                    parts.append(batch_prefix)
                if single_file:
                    # A single-file dataset keeps the partition's own name
                    # rather than the attempt's opaque token.
                    parts.append(f"{readable}{posixpath.splitext(artifact_inner)[1]}")
                else:
                    parts.extend(
                        readable_segment(part)
                        for part in posixpath.dirname(artifact_inner).split("/")
                        if part
                    )
                    leaf_stem, extension = posixpath.splitext(posixpath.basename(artifact_inner))
                    parts.append(f"{readable_segment(leaf_stem)}{extension}")
                inner = "/".join(parts)
                planned.append(
                    PublishedFile(
                        source=source,
                        relative=f"{step_prefix}/{directory}/{inner}",
                        inner=inner,
                    )
                )
        return planned

    @staticmethod
    def _relative_to_artifact(artifact_root: str, source: str) -> str:
        root = to_storage_string(artifact_root).rstrip("/")
        text = to_storage_string(source)
        prefix = f"{root}/"
        if text.startswith(prefix):
            return text[len(prefix) :]
        return posixpath.basename(text)

    def _committed_files(self, reference: OutputReference) -> list[str]:
        fs, resolved = resolve_fs(reference.path, self._attempts.storage_options)
        if not fs.exists(resolved):
            return []
        return sorted(str(path) for path in fs.find(resolved, withdirs=False, detail=False))

    # -- writing -----------------------------------------------------------

    def _generation_dirs(self, node: Any, key: WorkUnitKey, *, upto: int) -> list[tuple[int, str]]:
        """Every published generation of *this* partition, by exact name.

        Ownership is enumerated from generations the store knows about, never
        inferred from a filename. Stripping extensions to guess ownership
        cannot distinguish partition ``doc`` from partition ``doc.other``, and
        would delete one while retiring the other.
        """
        layout = self.layout(key.job_id)
        found: list[tuple[int, str]] = []
        for generation in range(0, max(upto, 0) + 1):
            path = layout.partition_base(node, key.partition_id, generation)
            if self._exists(path):
                found.append((generation, path))
        return found

    def _current_generation(self, key: WorkUnitKey) -> int | None:
        unit = self._store.load_work_unit(key)
        return None if unit is None else unit.output_generation

    def _sweep_superseded(self, node: Any, key: WorkUnitKey) -> None:
        """Prune generations older than the authoritative current one.

        The current generation is re-read from the store here rather than
        trusted from the caller: a publisher that paused before sweeping must
        not delete a generation that became current while it waited. Only
        strictly older generations are ever removed.

        ``keep_superseded`` is the retention policy. ``None`` -- the default --
        retains every superseded generation, so a manifest a reader already
        holds keeps resolving. Pruning is opt-in.
        """
        if self.keep_superseded is None:
            return
        current = self._current_generation(key)
        if current is None:
            return
        older = sorted(
            (item for item in self._generation_dirs(node, key, upto=current) if item[0] < current),
            key=lambda item: item[0],
            reverse=True,
        )
        for _, path in older[self.keep_superseded :]:
            self._remove(path)

    def publish_unit(
        self, node: Any, key: WorkUnitKey, *, attempts: int = 3, repair: bool = False
    ) -> list[str]:
        """Materialize a sealed unit's current generation.

        Correct at every interruption point, because a generation's published
        location is write-once: staged bytes are promoted into a path that
        belongs to that generation alone. A publisher that stalls across a
        recomputation therefore cannot overwrite newer output no matter when it
        resumes, and a crash before any repair leaves at most an unreferenced
        older generation rather than mixed current output.
        """
        for _ in range(max(1, attempts)):
            unit = self._store.load_work_unit(key)
            if unit is None or unit.status != "sealed":
                return []
            generation = unit.output_generation
            if unit.published_generation == generation and not repair:
                return []

            base = self.layout(key.job_id).partition_base(node, key.partition_id, generation)
            planned = self.planned_files(node, key, generation)
            # Unique per attempt: two publishers must never share a staging
            # directory, and an abandoned one must not be adopted.
            staging = f"{base}{_STAGING_SUFFIX}{uuid.uuid4().hex[:8]}"
            self._remove(staging)
            for item in planned:
                self._copy(item.source, join_path(staging, item.inner))

            self._promote(staging, base, planned, repair=repair)
            if self._store.mark_published(
                key, generation=generation, files=[item.relative for item in planned]
            ):
                self._sweep_superseded(node, key)
                return [item.relative for item in planned]
            # A newer generation won. Loop to publish it; this generation's
            # files stay where they are and are swept when the winner lands.
        return []

    def _exists(self, path: str) -> bool:
        fs, resolved = resolve_fs(path, self._storage_options)
        return bool(fs.exists(resolved))

    def _promote(
        self,
        staging: str,
        base: str,
        planned: list[PublishedFile],
        *,
        repair: bool,
    ) -> None:
        """Move staged bytes into this generation's directory without truncating.

        A generation's published content is immutable, so a second publisher of
        the same generation is a no-op. ``repair`` is for an explicit rebuild,
        which must overwrite output changed outside Caravel; even then the
        previous directory is only discarded once its replacement is in place.
        """
        del planned
        if self._exists(base) and not repair:
            self._remove(staging)
            return

        if not self._exists(base):
            self._recover_interrupted_swap(base)

        if not self._exists(base):
            self._move(staging, base)
            return

        # A directory cannot be atomically replaced, so swap through a trash
        # name. On failure the previous publication is put back: losing it
        # because the replacement failed would be worse than not repairing.
        trash = f"{base}{_TRASH_SUFFIX}{uuid.uuid4().hex[:8]}"
        self._move(base, trash)
        try:
            self._move(staging, base)
        except BaseException:
            if not self._exists(base):
                self._move(trash, base)
            raise
        self._remove(trash)

    def _recover_interrupted_swap(self, base: str) -> None:
        """Restore a directory left in trash by an interrupted swap.

        A process killed between the two renames leaves the previous
        publication under a trash name and nothing at ``base``. Put it back
        rather than treating the partition as unpublished.
        """
        parent = posixpath.dirname(base)
        fs, resolved = resolve_fs(parent, self._storage_options)
        if not fs.exists(resolved):
            return
        leaf = posixpath.basename(base)
        for entry in fs.ls(resolved, detail=False):
            name = posixpath.basename(str(entry).rstrip("/"))
            if name.startswith(f"{leaf}{_TRASH_SUFFIX}"):
                self._move(join_path(parent, name), base)
                return

    def unpublish_unit(
        self, node: Any, key: WorkUnitKey, *, through_generation: int | None = None
    ) -> None:
        """Retire a partition's published output under the retention policy.

        Invalidation makes the previously current generation superseded, so the
        same policy applies: ``keep_superseded`` of the most recent generations
        stay resolvable for readers holding a manifest, and the rest go.
        """
        if self.keep_superseded is None:
            return
        current = self._current_generation(key)
        if current is None:
            return
        # The invalidation transaction supplies its old generation. Never expand
        # that bound to include output produced while cleanup was delayed.
        bound = current - 1 if through_generation is None else min(through_generation, current - 1)
        ordering = (
            sorted(
                self._generation_dirs(node, key, upto=bound),
                key=lambda item: item[0],
                reverse=True,
            )
            if bound >= 0
            else []
        )
        for _, path in ordering[self.keep_superseded :]:
            self._remove(path)

    # -- draining ----------------------------------------------------------

    def reconcile(self, job_id: str, nodes: Iterable[Any], *, page: int = 200) -> int:
        """Publish every sealed unit whose current generation is not yet written.

        Drains with bounded pagination rather than a single capped fetch, so a
        job with more pending units than one page still reaches completion.
        """
        by_id = {node.node_id: node for node in nodes}
        published = 0
        seen_without_progress = 0
        while True:
            pending = self._store.unpublished_sealed_units(job_id, limit=page)
            if not pending:
                return published
            progressed = False
            for unit in pending:
                node = by_id.get(unit.key.node_id)
                if node is None:
                    continue
                if self.publish_unit(node, unit.key):
                    published += 1
                    progressed = True
                elif self._store.load_work_unit(unit.key) is not None and (
                    (state := self._store.load_work_unit(unit.key)) is not None
                    and state.published_generation == state.output_generation
                ):
                    progressed = True
            if not progressed:
                seen_without_progress += 1
                if seen_without_progress > 1:
                    return published
            else:
                seen_without_progress = 0

    def rebuild(self, job_id: str, nodes: Iterable[Any]) -> int:
        """Rebuild the published tree from committed metadata, unconditionally.

        ``reconcile`` only republishes units the store believes are pending, so
        it cannot repair output deleted behind its back. This is the supported
        operation for that: it republishes every sealed unit regardless of
        recorded publication progress, then rewrites the completion manifest if
        the job is complete.
        """
        rebuilt = 0
        for node in nodes:
            for partition_id in self._store.work_unit_partitions(job_id, node.node_id):
                key = WorkUnitKey(job_id, node.node_id, partition_id)
                unit = self._store.load_work_unit(key)
                if unit is None or unit.status != "sealed":
                    continue
                self._store.mark_unpublished(key)
                if self.publish_unit(node, key, repair=True):
                    rebuilt += 1
        job = self._store.load_job(job_id)
        if job is not None and job.status == "completed":
            self.write_job_manifest(job_id, nodes)
        return rebuilt

    # -- completion snapshot (advisory) ------------------------------------

    def write_job_manifest(self, job_id: str, nodes: Iterable[Any]) -> str | None:
        """Publish the completion snapshot, last.

        Refuses while any sealed unit is unpublished, and withdraws the file if
        the job generation moved while it was being written. Both are best
        effort: a writer that dies between the write and the re-read leaves a
        stale marker, so presence is advisory and
        :func:`validate_job_manifest` is the check that makes it safe.

        Returns ``None`` when no marker was published.
        """
        nodes = list(nodes)
        if self._store.unpublished_sealed_units(job_id, limit=1):
            return None
        recorded_files = self._store.publication_snapshot(job_id)["files"]

        entries: list[dict[str, Any]] = []
        for node in nodes:
            for partition_id in self._store.work_unit_partitions(job_id, node.node_id):
                key = WorkUnitKey(job_id, node.node_id, partition_id)
                unit = self._store.load_work_unit(key)
                if unit is None or unit.status != "sealed":
                    continue
                identity = (node.node_id, partition_id)
                if identity not in recorded_files:
                    return None
                entries.append(
                    {
                        "stage": node.stage_name,
                        "step": node.step.name or node.node_id,
                        "node_id": node.node_id,
                        "partition_id": partition_id,
                        "generation": unit.output_generation,
                        "batches": unit.batch_count,
                        "files": recorded_files[identity],
                    }
                )

        job = self._store.load_job(job_id)
        if job is None or job.status != "completed":
            return None
        observed_generation = job.generation
        manifest = {
            "schema_version": PUBLICATION_SCHEMA_VERSION,
            "pipeline_id": self.pipeline_id,
            "job_id": job_id,
            "job_generation": observed_generation,
            "status": job.status,
            "outputs": sorted(entries, key=lambda item: (item["node_id"], item["partition_id"])),
        }
        path = self.layout(job_id).manifest_path()
        fs, resolved = resolve_fs(path, self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, "wt", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)

        # The job may have been invalidated while this marker was being
        # written. Re-read and withdraw it rather than advertise completion
        # over superseded state.
        latest = self._store.load_job(job_id)
        if (
            latest is None
            or latest.status != "completed"
            or latest.generation != observed_generation
        ):
            self._remove(path)
            return None
        return path

    def remove_job_manifest(self, job_id: str) -> None:
        self._remove(self.layout(job_id).manifest_path())


def read_job_manifest(
    root: StoragePath,
    pipeline_id: str,
    job_id: str,
    *,
    storage_options: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Read a completion manifest, or None when none is published.

    A manifest that exists is **not** proof the job is complete now: a
    publisher that died at the wrong moment can leave one behind. Pass the
    result to :func:`validate_job_manifest` when the store is reachable.
    """
    layout = PublishedLayout(root=root, pipeline_id=pipeline_id, job_id=job_id)
    fs, resolved = resolve_fs(layout.manifest_path(), storage_options)
    if not fs.exists(resolved):
        return None
    with fs.open(resolved, "rt", encoding="utf-8") as handle:
        loaded: dict[str, Any] = json.load(handle)
    return loaded


def validate_job_manifest(manifest: dict[str, Any], store: SQLiteDurableStore) -> list[str]:
    """Check a completion manifest against one consistent store snapshot.

    Returns the disagreements, empty only when the manifest describes the job
    as it is now: same pipeline identity, completed, same job generation, and
    exactly the sealed units, generations, and recorded publication file references.
    This validates metadata at the snapshot's instant, not subsequent file
    availability or integrity. Use explicit rebuilding to repair external damage.

    This is the only way to turn a marker into a completion guarantee. The
    writer removes stale markers on a best-effort basis, but a publisher that
    dies between writing the file and re-reading the job leaves one behind.
    """
    problems: list[str] = []
    job_id = str(manifest.get("job_id", ""))
    if not job_id:
        return ["manifest names no job"]

    version = manifest.get("schema_version")
    if version != PUBLICATION_SCHEMA_VERSION:
        problems.append(f"manifest schema version {version!r} is not {PUBLICATION_SCHEMA_VERSION}")

    snapshot = store.publication_snapshot(job_id)
    job = snapshot["job"]
    if job is None:
        return [f"job '{job_id}' does not exist"]

    if manifest.get("pipeline_id") != job.pipeline_id:
        problems.append(
            f"manifest pipeline_id {manifest.get('pipeline_id')!r} is not {job.pipeline_id!r}"
        )
    if job.status != "completed":
        problems.append(f"job '{job_id}' is '{job.status}', not completed")
    if manifest.get("job_generation") != job.generation:
        problems.append(
            f"manifest job_generation {manifest.get('job_generation')!r} is not {job.generation}"
        )

    sealed: dict[tuple[str, str], int] = snapshot["sealed"]
    published_files: dict[tuple[str, str], list[str]] = snapshot["files"]
    listed: dict[tuple[str, str], Any] = {}
    for entry in manifest.get("outputs", []):
        try:
            unit = (str(entry["node_id"]), str(entry["partition_id"]))
        except (KeyError, TypeError):
            problems.append(f"manifest entry is malformed: {entry!r}")
            continue
        if unit in listed:
            problems.append(f"{unit[0]}/{unit[1]} is listed more than once")
        listed[unit] = entry.get("generation")
        files = entry.get("files")
        if unit not in published_files:
            problems.append(f"{unit[0]}/{unit[1]} has no recorded publication; rebuild output")
        elif (
            not isinstance(files, list)
            or any(not isinstance(path, str) for path in files)
            or sorted(files) != published_files[unit]
        ):
            problems.append(f"{unit[0]}/{unit[1]} files disagree with committed publication")

    for unit in sorted(sealed.keys() - listed.keys()):
        problems.append(f"{unit[0]}/{unit[1]} is sealed but missing from the manifest")
    for unit in sorted(listed.keys() - sealed.keys()):
        problems.append(f"{unit[0]}/{unit[1]} is listed but not sealed")
    for unit in sorted(sealed.keys() & listed.keys()):
        if listed[unit] != sealed[unit]:
            problems.append(
                f"{unit[0]}/{unit[1]} is generation {sealed[unit]}, manifest says {listed[unit]!r}"
            )
    return problems


__all__ = [
    "JOB_MANIFEST_NAME",
    "PUBLICATION_SCHEMA_VERSION",
    "OutputPublisher",
    "PublishedFile",
    "PublishedLayout",
    "read_job_manifest",
    "readable_segment",
    "validate_job_manifest",
]
