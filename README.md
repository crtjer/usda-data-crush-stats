# USDA Grape Crush Statistics Pipeline

Repeatable data pipeline that ingests USDA NASS California Grape Crush Report TB08 data (2016–2024) into bronze/silver/gold analytical layers.

## Data Source

[USDA NASS California Grape Crush Reports](https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush/Reports/index.php)

TB08 is the machine-readable flat file containing crush statistics by district, grape variety, price, and Brix level.

## Quick Start

```bash
pip install -r requirements.txt
python pipeline/run.py                    # full pipeline, 2016-2024
python pipeline/run.py --years 2022-2024  # specific years
python pipeline/run.py --skip-download    # reprocess without re-downloading
python pipeline/validate.py               # run validation checks
```

## Pipeline Layers

### Bronze (`data/bronze/`) — Raw Downloads
- Downloaded CSV/ZIP files from USDA NASS, organized by year
- Download log at `data/bronze/download_log.jsonl`
- **Not committed to repo** (gitignored)

### Silver (`data/silver/`) — Parsed + Normalized
- Per-year parquet files with typed columns and parsed brix values
- Dimension table parquets (district, grape type, variety, crop year)
- **Not committed to repo** (gitignored)

### Gold (`data/gold/`) — Analytical CSVs
Committed to repo for direct consumption:

| File | Description |
|------|-------------|
| `fact_crush_stats.csv` | All data rows — year/district/variety/brix grain |
| `fact_crush_summary.csv` | Summary rows — district/variety totals |
| `dim_district.csv` | 17 CA grape pricing districts + state total |
| `dim_grape_variety.csv` | All grape varieties with type and category |
| `dim_grape_type.csv` | Grape type codes (raisin/table/wine white/wine red) |
| `dim_crop_year.csv` | Crop years with report metadata |
| `analysis_top_varieties.csv` | Top 20 wine grape varieties by total tons |
| `analysis_price_trends.csv` | Avg price/ton per year per grape type |
| `analysis_district_totals.csv` | Total tons by district by year |

## Schema

### TB08 Columns
| Column | Type | Description |
|--------|------|-------------|
| district | int | 1–17 = pricing districts, 0/18 = state total |
| grape_type_code | int | 5=Raisin, 6=Table, 7=Wine White, 8=Wine Red |
| variety_code | int | Unique variety identifier |
| variety_name | str | e.g. "Chardonnay", "Cabernet Sauvignon" |
| wt_price | decimal | Weighted average price per ton (USD) |
| brix_code | str | Raw Brix code ("024500" = 24.5, "000100" = no contract) |
| brix_value | float | Parsed Brix value (null if no contract) |
| tons | decimal | Tons crushed |
| row_type_code | int | 2 = data row, 3 = summary row |
| crop_year | int | Harvest year |
| is_state_total | bool | True if district 0 or 18 |

## Example Usage

```python
import pandas as pd

# Load fact table and show top 10 varieties by 2024 crush volume
facts = pd.read_csv("data/gold/fact_crush_stats.csv")
recent = facts[facts["crop_year"] == 2024]
top = (recent.groupby("variety_name")["tons_crushed"]
       .sum().sort_values(ascending=False).head(10))
print(top)
```
