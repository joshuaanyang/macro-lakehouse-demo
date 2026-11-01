
# Macro lakehouse demo (Databricks, Delta Lake)
Bronze/silver/gold pipeline for US rates, GBPUSD and US CPI (FRED).
- Bronze: raw CSVs landed as-is with source file and ingest time
- Silver: typed, deduplicated, idempotent MERGE; series metadata table
- Quality gate: duplicates, metadata coverage, ranges, freshness; results
logged to dq_
results
- Gold: 2s10s spread, GBPUSD returns and 20d vol, point-in-time CPI (no look-
ahead)
- Orchestrated as a Databricks job
Next: Auto Loader for incremental files, Lakeflow declarative pipelines with
expectations,
real release-date vintages (ALFRED), news ingestion with embeddings for agent
search.
