
# Macro lakehouse demo (Databricks, Delta Lake)

A small medallion pipeline for US rates, GBP/USD and US CPI from FRED,
built to practise the data foundation a macro research team needs.

## Data
DGS2 and DGS10 (US 2y and 10y Treasury yields, daily), DEXUSUK (USD per GBP,
daily, published weekly), CPIAUCSL (US CPI, monthly). Downloaded from FRED
and uploaded to a Unity Catalog volume.

## Pipeline
| Notebook | Writes | What it does |
|---|---|---|
| 00_explore | - | Profiles the raw files: schema, ranges, missing values |
| 01_bronze | bronze_fred | Lands raw CSVs as text with source file and load time (append-only) |
| 02_silver | silver_fred, series_metadata | Types values, drops blanks, keeps latest load per key, idempotent MERGE; metadata per series |
| 03_quality | data_quality_results | Duplicates, completeness, metadata coverage, ranges, per-series freshness; stops the run on failure |
| 04_gold | gold_rates_fx_daily | 2s10s spread, GBP/USD returns and 20-day volatility, point-in-time US CPI |
| 05_explore_gold_fx | - | Sanity checks, Delta history and time travel, table comments |

## Lessons from building it
- A loop bug loaded only 1 of 4 files; the first quality gate missed it, so I added a completeness check.
- GBP/USD failed a fixed freshness limit because it's published weekly; limits now live per series in the metadata table.

## Next steps
Auto Loader for incremental files, Lakeflow declarative pipelines with expectations,
real release-date vintages (ALFRED), news ingestion with embeddings for agent search.

<img width="1224" height="519" alt="image" src="https://github.com/user-attachments/assets/ce64e4d5-f515-4b95-9364-bea74a89fb47" />
<img width="1698" height="381" alt="image" src="https://github.com/user-attachments/assets/10c60c98-5617-4941-9546-e55413ecc14b" />
<img width="1686" height="660" alt="image" src="https://github.com/user-attachments/assets/9eeba13f-974e-42f4-8581-dc1ebbb7e879" />





