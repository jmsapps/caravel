# Step indexes and incremental ingestion

Status: rough notes for future work.

- Each synchronized Step output has a persistent logical index. Its definition
  specifies record IDs/keys, output schema (potentially Pydantic), and a processing
  revision. These indexes enable record-level inserts, updates, and deletes across
  jobs. Steps may produce different keys and cardinalities, such as documents
  becoming chunks.
- Index identity persists across job IDs; job IDs identify execution attempts.
  Index definitions/generations provide the local compatibility boundary rather
  than requiring every change to bump a pipeline-wide version. Database schema
  version remains a separate concern.
- Diff the declared index definition against the stored definition. An unchanged
  definition allows incremental sync; an incompatible change to keys, schema, or
  processing revision triggers replacement ingestion for that Step. Exact diff
  compatibility rules remain to be defined. Provide an explicit full-refresh option.
- Incremental sync compares keyed records/content or consumes source changes.
  Missing records imply deletion only after a complete authoritative input scan;
  partial or incremental input needs explicit deletion information.
- Indexes are independent. Rebuilding B does not automatically rebuild C: B may
  add a field that C ignores. How to identify relevant input changes remains open;
  do not infer blanket downstream invalidation from an upstream definition diff.
- Replacement uses blue-green generations: build the new index while retaining
  the active one, switch the active generation atomically after success, then
  clean up the old generation. A failed rebuild leaves the active index available.
- Processing/prompt changes need an explicit revision; schema diff detection alone
  cannot reliably identify semantic changes. Source cursors, record lineage,
  downstream change propagation, and generation cleanup need further design.
