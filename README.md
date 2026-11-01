
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

<img width="1224" height="519" alt="image" src="https://github.com/user-attachments/assets/ce64e4d5-f515-4b95-9364-bea74a89fb47" />
<img width="1698" height="381" alt="image" src="https://github.com/user-attachments/assets/10c60c98-5617-4941-9546-e55413ecc14b" />
<img width="1686" height="660" alt="image" src="https://github.com/user-attachments/assets/9eeba13f-974e-42f4-8581-dc1ebbb7e879" />





