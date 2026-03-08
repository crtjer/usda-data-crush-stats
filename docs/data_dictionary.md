# Data Dictionary — USDA Grape Crush Statistics

## Gold Layer Tables (`data/final/`)

### dim_district.csv

California Grape Pricing Districts as defined by USDA NASS.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| district_id | int | USDA district number (0, 1–17, 18) | 4 |
| district_name | str | District name | Napa |
| region | str | Geographic region | North Coast |
| description | str | Counties included | Napa County |

**Notes:**
- District 0 and 18 both represent the State Total
- 17 geographic districts (1–17) + 2 state total rows

### dim_grape_variety.csv

Grape varieties found in crush reports, derived from TB08 data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| variety_code | int | USDA variety code | 7004 |
| variety_name | str | Variety name | Chardonnay |
| grape_type_code | int | Grape type (4–8) | 7 |
| grape_type_name | str | Grape type name | Wine Grapes (White) |
| grape_category | str | Category: wine/table/raisin | wine |

**Grape Type Codes:**
- 4 = Raisin Grapes
- 5 = Table Grapes
- 6 = Wine Grapes (Red/Black)
- 7 = Wine Grapes (White)
- 8 = Wine Grapes (Red/Black) — alternate code in some years

### dim_crop_year.csv

Crop years with metadata about the source report.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| crop_year | int | Crop year | 2024 |
| report_type | str | Final, Prelim, or Errata | Final |
| source_url | str | NASS download URL | https://... |
| first_ingested_at | str | Date first ingested | 2026-03-08 |

### fact_crush_stats.csv

Grain-level crush statistics: one row per year/district/variety/brix bucket.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| id | int | Row ID | 1 |
| crop_year | int | Crop year | 2024 |
| district_id | int | FK → dim_district | 4 |
| variety_code | int | FK → dim_grape_variety | 7004 |
| grape_type_code | int | Grape type code | 7 |
| brix_code | str | Contract/Brix range code | 024500 |
| brix_value | float | Parsed Brix (brix_code/1000) | 24.5 |
| wt_price_per_ton | float | Weighted avg price (USD/ton) | 2450.00 |
| tons_crushed | float | Tons in this price bucket | 125.5 |
| report_type | str | Report type used | Final |

**Notes:**
- `brix_code` "000100" = no Brix contract specified → `brix_value` is NULL
- `wt_price_per_ton` may be NULL on some rows
- Only includes data rows (row_type_code=2), not summary rows

### fact_crush_summary.csv

Summary-level crush statistics: one row per year/district/variety.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| id | int | Row ID | 1 |
| crop_year | int | Crop year | 2024 |
| district_id | int | FK → dim_district | 4 |
| variety_code | int | FK → dim_grape_variety | 7004 |
| grape_type_code | int | Grape type code | 7 |
| total_tons | float | Total tons crushed | 5432.1 |
| avg_price_per_ton | float | Weighted avg price | 2100.00 |

### fact_acreage.csv

Grape acreage by variety (placeholder — PDF parsing not yet implemented).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| id | int | Row ID | 1 |
| crop_year | int | Crop year | 2024 |
| variety_code | int | FK → dim_grape_variety | 7004 |
| bearing_acres | float | Bearing acreage | 45000 |
| non_bearing_acres | float | Non-bearing acreage | 2000 |
| total_acres | float | Total acreage | 47000 |

### fact_ttb_wine.csv

TTB wine production statistics (placeholder — TTB download not yet automated).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| id | int | Row ID | 1 |
| year | int | Calendar year | 2024 |
| state | str | State | CA |
| wine_type | str | Wine category | table |
| gallons_produced | float | Gallons produced | 500000000 |
| gallons_removed | float | Gallons removed | 450000000 |
| gallons_on_hand | float | Gallons on hand | 100000000 |

### bridge_crush_to_wine.csv

Cross-reference linking grape crush tonnage to TTB wine production.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| crop_year | int | Crop year | 2024 |
| ca_wine_tons_crushed | float | CA wine grape tons | 3500000 |
| ca_gallons_wine_produced | float | CA wine gallons (TTB) | NULL |
| tons_per_gallon_ratio | float | Conversion ratio | NULL |
| source_crush | str | Crush data source | USDA NASS TB08 |
| source_ttb | str | TTB data source | TTB not available |

## Source Mapping

| Gold Table | Source | Transform |
|------------|--------|-----------|
| fact_crush_stats | TB08 CSV (row_type=2) | parse_crush_tb08.py |
| fact_crush_summary | TB08 CSV (row_type=3) | parse_crush_tb08.py |
| fact_acreage | Acreage PDFs | parse_acreage_pdf.py |
| fact_ttb_wine | TTB Wine CSVs | parse_ttb.py |
| dim_district | Hardcoded (USDA spec) | build_dimensions.py |
| dim_grape_variety | TB08 all years | build_dimensions.py |
| dim_crop_year | TB08 all years | build_dimensions.py |
| bridge_crush_to_wine | fact_crush + TTB | build_bridge.py |

## Known Data Quirks

1. **wt_price is NULL on summary rows** — Summary rows (row_type_code=3) aggregate across price buckets
2. **brix_code "000100" = no Brix contract** — Not all grapes are sold on Brix contracts
3. **District 0 vs 18** — Both represent state totals; 18 is used in newer data, 0 in older
4. **XLS variety codes are synthetic** — Pre-2022 XLS files don't include USDA variety codes; codes are generated via hash
5. **Errata supersedes Final** — When errata exists for a year, it contains corrections and should be used instead of Final
