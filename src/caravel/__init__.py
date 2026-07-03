from importlib.metadata import PackageNotFoundError, version

from .branch import Branch
from .cli import make_cli
from .datasets import (
    BytesDataset,
    JSONDataset,
    PartitionedBytesDataset,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    TextDataset,
)
from .loaders import CallableLoader, MultiSourceLoader, dataset_as_loader
from .paths import (
    format_stage_dir,
    format_step_dir,
    partition_key_to_relpath,
    resolve_run_root,
    resolve_step_output_dir,
    validate_partition_key,
)
from .pipeline import Pipeline, Stage, Step, step
from .runner import run
from .types import (
    SOURCE_FIELD,
    BranchPredicate,
    CheckpointCommitUncertainError,
    CheckpointError,
    CheckpointIntegrityError,
    Dataset,
    EmptyOutputError,
    KeyCollisionError,
    Loader,
    MissingPriorOutputError,
    MissingSourceTagError,
    PartitionId,
    Partitions,
    Record,
    StepContext,
    StepFn,
    UnsupportedCheckpointVersionError,
)
from .viz import to_mermaid

try:
    __version__ = version("caravel")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    "PartitionId",
    "Record",
    "Partitions",
    "SOURCE_FIELD",
    "StepContext",
    "StepFn",
    "Dataset",
    "EmptyOutputError",
    "CheckpointError",
    "CheckpointIntegrityError",
    "CheckpointCommitUncertainError",
    "UnsupportedCheckpointVersionError",
    "Loader",
    "BranchPredicate",
    "Branch",
    "make_cli",
    "KeyCollisionError",
    "MissingSourceTagError",
    "MissingPriorOutputError",
    "JSONDataset",
    "PartitionedJSONDataset",
    "TextDataset",
    "PartitionedTextDataset",
    "BytesDataset",
    "PartitionedBytesDataset",
    "CallableLoader",
    "MultiSourceLoader",
    "dataset_as_loader",
    "Step",
    "Stage",
    "Pipeline",
    "step",
    "run",
    "format_stage_dir",
    "format_step_dir",
    "resolve_step_output_dir",
    "resolve_run_root",
    "validate_partition_key",
    "partition_key_to_relpath",
    "to_mermaid",
]
