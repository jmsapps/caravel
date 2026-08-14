# Conceptual RAG ingestion saga

This local-only example models an existing RAG ingestion design without ABFS,
Azure, embeddings, or Azure AI Search. Local directories stand in for external
resources:

- `staging/` and `current/` represent chunk storage;
- `version.json` represents committed version metadata;
- `index/` represents an index that is deleted and recreated in place;
- backup directories make the compensating actions visible.

The steps stage chunks, replace the simulated index, promote staging to current,
and commit version metadata. By default the final step fails and the run guard
restores the prior `current/`, index, and version:

```bash
python -m examples.rag_ingestion_saga
```

Run the commit path with:

```bash
python -m examples.rag_ingestion_saga --succeed
```

Inspect `data/output/rag_ingestion_saga/external_workspace`. Pipeline step
outputs remain under the adjacent `pipeline/` directory as execution history.

This is a saga, not an ACID transaction. The local index backup demonstrates a
possible compensation; a deleted remote index might instead require a complete
rebuild. A production implementation also needs idempotent compensations and
recovery of an `active` journal after process termination.
