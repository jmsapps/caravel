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
from .durability import (
    CodecError,
    CommitScope,
    DurabilityError,
    DurabilityOptions,
    ExecutorBusyError,
    InMemorySource,
    InvalidTransitionError,
    OutputIntegrityError,
    RecoveryStore,
    RunResult,
    SnapshotCodec,
    SourceError,
    StoreUnavailableError,
    StructureMismatchError,
    UnsupportedStoreVersionError,
)
from .loaders import CallableLoader, MultiSourceLoader, dataset_as_loader
from .paths import partition_key_to_relpath, validate_partition_key
from .pipeline import Pipeline, Stage, Step
from .retry import (
    RetryBudgetExhaustedError,
    RetryCancelledError,
    RetryError,
    RetryOptions,
    RetryPolicyError,
    RetryResultExhaustedError,
    compute_retry_delay,
    retry_operation,
)
from .runner import run
from .types import (
    SOURCE_FIELD,
    BranchPredicate,
    Dataset,
    EmptyOutputError,
    KeyCollisionError,
    Loader,
    MissingSourceTagError,
    PartitionId,
    Partitions,
    Record,
    StepContext,
    StepFn,
)
from .viz import to_mermaid

try:
    __version__ = version("caravel")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    # Declarations
    "Pipeline",
    "Stage",
    "Step",
    "Branch",
    # Execution
    "run",
    "RunResult",
    # Durability
    "DurabilityOptions",
    "CommitScope",
    "RecoveryStore",
    "SnapshotCodec",
    # Input sources
    "InMemorySource",
    "CallableLoader",
    "MultiSourceLoader",
    "dataset_as_loader",
    # Retries
    "RetryOptions",
    "retry_operation",
    "compute_retry_delay",
    "RetryError",
    "RetryPolicyError",
    "RetryResultExhaustedError",
    "RetryCancelledError",
    "RetryBudgetExhaustedError",
    # Datasets
    "JSONDataset",
    "PartitionedJSONDataset",
    "TextDataset",
    "PartitionedTextDataset",
    "BytesDataset",
    "PartitionedBytesDataset",
    # Types
    "PartitionId",
    "Record",
    "Partitions",
    "SOURCE_FIELD",
    "StepContext",
    "StepFn",
    "Dataset",
    "Loader",
    "BranchPredicate",
    # Errors
    "DurabilityError",
    "CodecError",
    "ExecutorBusyError",
    "InvalidTransitionError",
    "OutputIntegrityError",
    "SourceError",
    "StoreUnavailableError",
    "StructureMismatchError",
    "UnsupportedStoreVersionError",
    "EmptyOutputError",
    "KeyCollisionError",
    "MissingSourceTagError",
    # Utilities
    "make_cli",
    "to_mermaid",
    "validate_partition_key",
    "partition_key_to_relpath",
]
