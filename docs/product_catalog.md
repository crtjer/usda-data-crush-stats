## Data Products Catalog

This catalog summarizes the analytical data products built by the pipeline, with plain‑language descriptions and links back to the official government sources they are derived from.

### 1. USDA NASS California Grape Crush – Gold Layer

- **Product name**: `fact_crush_stats.csv`  
  - **Type**: Fact table — grain \(crop_year × district × variety × Brix/price bucket\)  
  - **Description**: Long‑form table of California grape crush activity from the USDA NASS California Grape Crush reports TB08 flat file. Each row represents grapes of a specific variety crushed in a given pricing district and Brix/contract bucket, with the tons crushed and weighted average price per ton.  
  - **Key fields**: `crop_year`, `district_id`, `variety_code`, `brix_code`/`brix_value`, `tons_crushed`, `wt_price_per_ton`, `grape_type_code`.  
  - **Government source**: USDA NASS *California Grape Crush Report* table TB08, “Grapes Crushed and Weighted Average Degrees Brix, by Variety and District” — see the official reports index at `[USDA NASS California Grape Crush Reports](https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Crush/Reports/index.php)`.  
  - **Coverage**: California only, wine/table/raisin grapes; years and report types as listed in `dim_crop_year.csv`.

- **Product name**: `fact_crush_summary.csv`  
  - **Type**: Fact table — grain \(crop_year × district × variety\)  
  - **Description**: Summary‑level crush statistics, aggregated from `fact_crush_stats.csv`. Provides total tons crushed, average price per ton, and related metrics for each variety × district × year. Conceptually corresponds to the state and district totals shown in TB01/TB02/TB06 in the USDA reports.  
  - **Key fields**: `crop_year`, `district_id`, `variety_code`, `total_tons`, `avg_price_per_ton`.  
  - **Government source**: Derived from USDA NASS TB08 data which itself is consistent with the published tables in the *California Grape Crush Report* series. The official report PDFs at the link above can be used to validate totals and prices by district and variety.

- **Product name**: `dim_district.csv`  
  - **Type**: Dimension table — California grape pricing districts  
  - **Description**: Lookup table for the 17 California grape pricing districts plus state total, with human‑readable names, regions, and short descriptions. District definitions match those used in the USDA NASS California Grape Crush publications.  
  - **Key fields**: `district_id`, `district_name`, `region`, `description`.  
  - **Government source**: USDA NASS *California Grape Crush Report* documentation and maps (district numbers 1–17 plus state totals 0/18) as published alongside the annual reports at the USDA link above.

- **Product name**: `dim_grape_variety.csv`  
  - **Type**: Dimension table — grape varieties and types  
  - **Description**: Master list of grape varieties observed in the crush data, including USDA variety codes, names, grape type \(wine/table/raisin\), and category. Variety codes and names align with those listed in the USDA TB08 files and report appendices.  
  - **Key fields**: `variety_code`, `variety_name`, `grape_type_code`, `grape_type_name`, `grape_category`.  
  - **Government source**: USDA NASS TB08 CSV “Variety” and “Grape Type” columns from the California Grape Crush reports; variety lists can be cross‑checked against the variety tables in the PDF reports at the USDA link above.

- **Product name**: `dim_crop_year.csv`  
  - **Type**: Dimension table — crop years and report metadata  
  - **Description**: One row per crop year summarizing which version of the USDA Grape Crush report was used \(Preliminary, Final, Errata\), the source URL, and first ingestion date. Acts as a linkage between gold‑layer data and the underlying USDA report files.  
  - **Key fields**: `crop_year`, `report_type`, `source_url`, `first_ingested_at`.  
  - **Government source**: USDA NASS California Grape Crush report index and per‑year report URLs at the USDA link above.

### 2. USDA NASS California Grape Acreage (Planned)

- **Product name**: `fact_acreage.csv`  
  - **Type**: Fact table — grain \(crop_year × variety\) \(district breakdown planned\)  
  - **Description**: Intended to hold bearing and non‑bearing acreage by grape variety for California, sourced from the USDA NASS *California Grape Acreage* annual reports. Parsing from PDF is not yet fully implemented, so current contents should be treated as a placeholder.  
  - **Key fields**: `crop_year`, `variety_code`, `bearing_acres`, `non_bearing_acres`, `total_acres`.  
  - **Government source**: USDA NASS *California Grape Acreage* reports, available at `[USDA NASS California Grape Acreage](https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Acreage/index.php)`. These PDFs list acreage by variety and geography and can be used to validate values once the parser is complete.

### 3. TTB Wine Statistics (Planned)

- **Product name**: `fact_ttb_wine.csv`  
  - **Type**: Fact table — grain \(year × state × wine_type\)  
  - **Description**: Placeholder table for annual wine production statistics by state, derived from Alcohol and Tobacco Tax and Trade Bureau \(TTB\) wine statistics. Intended to capture gallons produced, removed, and on hand by wine type so that California crush volumes can be compared with finished wine production.  
  - **Key fields**: `year`, `state`, `wine_type`, `gallons_produced`, `gallons_removed`, `gallons_on_hand`.  
  - **Government source**: TTB *Wine Statistics* publications under the Statistics section of the TTB website (`https://www.ttb.gov/statistics`, Wine area). TTB provides annual tables of wine volume by state and category, which this product is designed to mirror; current file is marked as a placeholder until automated download/parse is wired up.

### 4. Crush ↔ Wine Production Bridge (Planned)

- **Product name**: `bridge_crush_to_wine.csv`  
  - **Type**: Analytical bridge — grain \(crop_year\)  
  - **Description**: Time‑series bridge table designed to relate California wine‑grape tons crushed to TTB‑reported wine gallons produced. For each crop year, the table will record total wine‑grape tons in California, TTB‑reported California wine gallons, and an implied tons‑per‑gallon conversion ratio.  
  - **Key fields**: `crop_year`, `ca_wine_tons_crushed`, `ca_gallons_wine_produced`, `tons_per_gallon_ratio`, `source_crush`, `source_ttb`.  
  - **Government sources**:  
    - **Crush side**: USDA NASS California Grape Crush TB08 data \(wine‑grape types only\) from the USDA reports link above.  
    - **Wine side**: TTB *Wine Statistics* tables for California volumes under the Statistics → Wine section of `https://www.ttb.gov`. The bridge explicitly documents which USDA and TTB files were used for each year via the `source_crush` and `source_ttb` columns.

### 5. How to Use This Catalog

- **For analysts**: Use this catalog to choose the right table for your question—for example, price‑sensitive variety analysis should start from `fact_crush_stats.csv`, while high‑level trend analysis can usually rely on `fact_crush_summary.csv` plus the dimension tables.  
- **For data stewards**: Use the listed government source links to verify definitions, confirm that variety and district labels match the official USDA texts, and reconcile state totals and production numbers against the USDA and TTB publications.  
- **For future development**: Tables marked “Planned” rely on government PDF or CSV sources that are already identified here; as those parsers mature, this catalog remains the high‑level description of what each product should contain and how it maps back to the original USDA/TTB statistics.

