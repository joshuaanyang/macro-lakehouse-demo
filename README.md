
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

<img width="1218" height="567" alt="image" src="https://github.com/user-attachments/assets/2be58a1c-f0ec-4739-84fa-6f6500e37a57" />


<img width="1713" height="681" alt="image" src="https://github.com/user-attachments/assets/da79e21a-f5bc-4f30-b4d4-85627cd7f7a4" />


