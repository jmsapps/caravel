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
    AllocatedRange,
    AttemptOutputStore,
    BatchFeed,
    ChangeEvent,
    CommitUncertainError,
    DurabilityError,
    DurableRunResult,
    DurableStorage,
    ExecutionBudget,
    ExecutionPolicy,
    FenceViolationError,
    GenerationConflictError,
    InMemorySource,
    IncrementalInput,
    InvalidTransitionError,
    JobResults,
    LeasePolicy,
    ListChangeAdapter,
    ListSnapshotAdapter,
    LoaderSource,
    OutputIntegrityError,
    RetentionExpiredError,
    RetentionGapError,
    SnapshotInput,
    SourceError,
    SourceVersionError,
    SQLiteDurableStore,
    StorageCapabilityError,
    UnsupportedDurableDeclarationError,
)
from .inputs import InputPartition, StepInputs
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
    InputScope,
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
    # Execution: one entry point, one model
    "run",
    "DurableRunResult",
    "DurableStorage",
    "JobResults",
    "ExecutionBudget",
    "ExecutionPolicy",
    "LeasePolicy",
    # Input sources
    "InMemorySource",
    "SnapshotInput",
    "IncrementalInput",
    "LoaderSource",
    "ListSnapshotAdapter",
    "ListChangeAdapter",
    "ChangeEvent",
    "AllocatedRange",
    "CallableLoader",
    "MultiSourceLoader",
    "dataset_as_loader",
    # Runtime inputs
    "StepInputs",
    "InputPartition",
    # Retries
    "RetryOptions",
    "retry_operation",
    "compute_retry_delay",
    "RetryError",
    "RetryPolicyError",
    "RetryResultExhaustedError",
    "RetryCancelledError",
    "RetryBudgetExhaustedError",
    # Consumers and storage internals
    "BatchFeed",
    "AttemptOutputStore",
    "SQLiteDurableStore",
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
    "InputScope",
    "Dataset",
    "Loader",
    "BranchPredicate",
    # Errors
    "DurabilityError",
    "CommitUncertainError",
    "FenceViolationError",
    "GenerationConflictError",
    "InvalidTransitionError",
    "OutputIntegrityError",
    "RetentionExpiredError",
    "RetentionGapError",
    "SourceError",
    "SourceVersionError",
    "StorageCapabilityError",
    "UnsupportedDurableDeclarationError",
    "EmptyOutputError",
    "KeyCollisionError",
    "MissingSourceTagError",
    # Utilities
    "make_cli",
    "to_mermaid",
    "validate_partition_key",
    "partition_key_to_relpath",
]
