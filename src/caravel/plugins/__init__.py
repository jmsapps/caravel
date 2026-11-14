"""Plugin capabilities for one durable execution invocation.

Two capabilities remain, because only these extend behavior the engine does not
already own: observers receive lifecycle events after committed transitions,
and run guards wrap one invocation in an external precondition.

Checkpoint, ownership, and lease capabilities were retired when Caravel
consolidated onto one durable execution model. The authoritative progress store
owns committed-output evidence, cross-run inventory, and worker leases.
"""

from .run_evidence import RunEvidencePlugin
from .api import (
    EventKind,
    NodeFacts,
    ObserverCriticality,
    PluginFailureError,
    PluginSet,
    RunEvent,
    RunFacts,
    RunGuard,
    RunObserver,
    RunOutcome,
    validate_plugins,
)

__all__ = [
    "RunEvidencePlugin",
    "EventKind",
    "NodeFacts",
    "ObserverCriticality",
    "PluginFailureError",
    "PluginSet",
    "RunEvent",
    "RunFacts",
    "RunGuard",
    "RunObserver",
    "RunOutcome",
    "validate_plugins",
]
