# Forgent vF - Procurement Data Pipeline

A data pipeline that consolidates purchase order and receipt data from multiple ERP systems (NetSuite and Epicor) into a unified, analytics-ready fact table for Power BI dashboards.

## Features

- **Multi-source ingestion**: CSV-driven normalizer maps NetSuite and Epicor exports to a golden schema
- **AI enrichment**: Automated item categorization (14-category, 3-level hierarchy), vendor classification (name standardization, supplier type, location), and electrical/raw material spec extraction via OpenRouter API (Gemini 3 Flash Preview)
- **Semantic item consolidation**: 3-pass deduplication — exact match, spec fingerprint, AI fuzzy match — assigns canonical names and group IDs
- **Copper price tracking**: Scrapes COMEX monthly averages from CNWire for commodity analysis
- **Data quality gates**: Row-count validation between phases, merge-rate thresholds, regression tests on final output
- **3-category OTD**: Early / On Time / Late delivery classification with configurable grace period
- **Payment terms standardization**: Cross-system terms normalization via terms_map.csv (~30 raw values → 9 buckets)
- **Power BI ready**: Outputs CSV and Parquet with explicit PyArrow schema (v2.9.0, 100+ typed columns)

## Pipeline Architecture

The pipeline runs 4 phases. Steps 5 and 6 are consumed downstream, not executed by the runner.

```
Phase 1: Normalization (Critical)
  └─ Map raw ERP exports to golden schema → all_transactions_normalized.csv

Phase 2: Enrichment (Critical)
  └─ Apply derivations, boolean cleanup, calculated fields → pre_cleaned_staging.csv

Phase 3: Maintenance (Best-effort, continues on failure)
  ├─ update_copper_prices  → Scrape COMEX monthly copper prices
  ├─ refresh_mappings      → Add new vendors/items from staging to mapping tables
  ├─ enrich_vendors        → AI: standardize names, classify type & location
  ├─ enrich_items          → Description-match then AI: categorize, extract specs
  ├─ clean_item_map        → Deduplicate, remove placeholders, quality-score conflicts
  └─ consolidate_items     → 3-pass semantic grouping (exact → fingerprint → AI)

Phase 4: Assembly (Critical)
  ├─ Join staging with vendor_map + item_map + terms_map
  ├─ Filter to reporting period (≥ 2024-01-01)
  ├─ Standardize payment terms, calculate spend buckets, OTD, short names
  ├─ Run data quality checks
  └─ Export po_fact_table.csv + .parquet

Post-pipeline:
  ├─ Coverage summary (source systems, date ranges, top vendors)
  ├─ Completeness check (row counts raw → normalized → staging → final)
  └─ Regression tests (row minimums, null rates, match rates, dollar ranges)
```

## Directory Structure

```
forgent-vF/
├── pipeline_runner.py              # Main orchestrator (4 phases)
├── requirements.txt                # Python dependencies
├── models.json                     # AI model registry (pricing, context limits)
├── .env                            # OPENROUTER_API_KEY (not committed)
│
├── step_1_ingestion/
│   ├── a.raw_data/                 # Input: Netsuite/ and Epicor/ exports
│   │   ├── Netsuite/               #   Netsuite Receipts*.csv
│   │   │                           #   Netsuite Open POs*.csv
│   │   │                           #   Netsuite PO Line Dates*.csv  ← required for OTD promise date
│   │   └── Epicor/                 #   Epicor Receipts*.xlsx
│   │                               #   Epicor Open POs*.xlsx
│   ├── b.config/                   # source_mappings.yaml, transaction_field_mapping.csv
│   ├── c.schemas/                  # transactions.yaml (golden schema)
│   ├── d_scripts/                  # normalizer.py, config_loader.py
│   └── e.normalized/               # Output: all_transactions_normalized.csv, pipeline_metadata.json
│
├── step_2_enrichment/
│   ├── a_scripts/                  # pre_cleaner.py (derivations, boolean cleanup)
│   └── b.staging/                  # Output: pre_cleaned_staging.csv
│
├── step_3_maintenance/
│   ├── a.mappings/                 # vendor_map.csv, item_map.csv, terms_map.csv,
│   │                               #   business_unit_map.csv, copper_price_index.csv
│   ├── b_scripts/                  # Pipeline-callable orchestrators:
│   │   ├── update_copper_prices.py #   COMEX price scraper
│   │   ├── refresh_mappings.py     #   Add new vendors/items to maps
│   │   ├── enrich_vendors.py       #   AI vendor classification
│   │   ├── enrich_items.py         #   Description-match + AI categorization
│   │   ├── clean_item_map.py       #   Dedup, quality scoring, placeholder removal
│   │   └── consolidate_items.py    #   Semantic item grouping (3-pass)
│   └── c_ai_enrichment/            # AI modules:
│       ├── taxonomy.py             #   Central config, 14 L1 categories, spec fields
│       ├── ai_categorizer.py       #   Batch item categorization (L1/L2/L3)
│       ├── spec_extractor.py       #   Electrical + raw material spec extraction
│       ├── run_ai_enrichment.py    #   Item enrichment orchestrator
│       ├── vendor_taxonomy.py      #   Vendor config, supplier types, name cleaning
│       ├── vendor_classifier.py    #   Batch vendor enrichment
│       ├── run_vendor_enrichment.py#   Vendor enrichment orchestrator
│       └── item_consolidator.py    #   Semantic dedup + canonical naming
│
├── step_4_assembly/
│   ├── a_scripts/
│   │   ├── final_builder.py        # Join, filter, calculate metrics, export
│   │   ├── schema_contract.py      # PyArrow schema (v2.9.0, 100+ columns)
│   │   ├── grain_definition.py     # Hybrid grain: PO line-release + receipt line
│   │   └── data_quality.py         # DataQualityChecker (critical/warning severity)
│   └── b.final/                    # Output: po_fact_table.csv/.parquet, quality report
│
├── step_5_powerbi/                 # Power BI project (.pbip), theme, dashboard guide
│                                   # 16 pages: Cover, Executive Spend Summary,
│                                   # Category Overview, Supplier Overview/Detail/Risk,
│                                   # Open Order Pipeline, Overdue POs, OTD,
│                                   # Buyer Performance, Savings, Price & Cost,
│                                   # Raw Material vs Index, Explorers (3)
│
└── step_6_analysis/
    ├── validate_output.py          # Regression tests (row counts, nulls, match rates)
    ├── saved/                      # Pipeline flowchart, field mappings
    └── temp/                       # Working analyses
```

## Setup

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd forgent-vF
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # or: source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure API key** (required for AI enrichment)
   Create a `.env` file in the project root:
   ```
   OPENROUTER_API_KEY=your_key_here
   ```

5. **Add raw data files**
   Place ERP export files in the folders below. File names must match the patterns shown — the pipeline uses glob matching, so the date suffix can be anything.

   | Folder | File | Required? | Purpose |
   |--------|------|-----------|---------|
   | `step_1_ingestion/a.raw_data/Netsuite/` | `Netsuite Receipts*.csv` | Yes | NS receipt transactions |
   | `step_1_ingestion/a.raw_data/Netsuite/` | `Netsuite Open POs*.csv` | Yes | NS open purchase orders |
   | `step_1_ingestion/a.raw_data/Netsuite/` | `Netsuite PO Line Dates*.csv` | Yes | Promise/due dates per PO — **required for OTD % calculations on NS receipts** |
   | `step_1_ingestion/a.raw_data/Epicor/` | `Epicor Receipts*.xlsx` | Yes | Epicor receipt transactions |
   | `step_1_ingestion/a.raw_data/Epicor/` | `Epicor Open POs*.xlsx` | Yes | Epicor open purchase orders |

   > **Note on PO Line Dates**: This is a separate NetSuite saved search (`Maximum of PO New Promise Date`, `Maximum of PO Promise Date`, `Maximum of PO Due Date` grouped by PO Number). Without it, `promise_date` and `due_date` will be null for all NS receipts and the OTD % (Promise Date) metric will be blank in Power BI.

## Running the Pipeline

```bash
python pipeline_runner.py
```

The pipeline will:
1. Normalize all source files to a common schema
2. Apply enrichment derivations (open order flags, buyer IDs, boolean cleanup)
3. Scrape COMEX copper prices
4. Refresh vendor/item mapping tables with new entries from staging
5. AI-classify new vendors (name standardization, supplier type, location)
6. AI-categorize new items (3-level hierarchy, electrical specs, raw material specs)
7. Clean and deduplicate the item map (quality scoring, placeholder removal)
8. Consolidate items semantically (canonical names and group IDs)
9. Build the final fact table with all mappings, payment terms standardization, metrics, and date filtering
10. Run data quality checks and regression tests

Maintenance steps (3-8) are best-effort — failures are logged but the pipeline continues with existing mapping data.

## Output Files

| File | Location | Description |
|------|----------|-------------|
| `all_transactions_normalized.csv` | step_1_ingestion/e.normalized/ | Combined normalized transactions |
| `pipeline_metadata.json` | step_1_ingestion/e.normalized/ | Source file hashes and row counts |
| `pre_cleaned_staging.csv` | step_2_enrichment/b.staging/ | Enriched staging data |
| `vendor_map.csv` | step_3_maintenance/a.mappings/ | Vendor standardization and classification |
| `item_map.csv` | step_3_maintenance/a.mappings/ | Item categorization and specs |
| `terms_map.csv` | step_3_maintenance/a.mappings/ | Payment terms normalization (~30 → 9 buckets) |
| `business_unit_map.csv` | step_3_maintenance/a.mappings/ | Business unit lookups |
| `copper_price_index.csv` | step_3_maintenance/a.mappings/ | COMEX monthly copper prices |
| `po_fact_table.csv` | step_4_assembly/b.final/ | Final fact table (CSV) |
| `po_fact_table.parquet` | step_4_assembly/b.final/ | Final fact table (Parquet, schema v2.9.0) |
| `data_quality_report.txt` | step_4_assembly/b.final/ | Validation results |

## Configuration

### Field Mappings
Edit `step_1_ingestion/b.config/transaction_field_mapping.csv` to customize how source fields map to the normalized schema. This is the authoritative mapping definition.

### Source Definitions
Edit `step_1_ingestion/b.config/source_mappings.yaml` to add new data sources or modify file patterns.

### Reference Tables
- **vendor_map.csv** — Vendor standardization (Raw_Name, Standardized_Name, Intercompany, Supplier_Type, Location)
- **item_map.csv** — Item categorization (14 L1 categories, 3-level hierarchy, 10 electrical spec fields, 6 raw material spec fields, canonical name/group)
- **terms_map.csv** — Payment terms normalization (maps ~30 raw ERP terms to 9 standardized buckets)
- **business_unit_map.csv** — Business unit lookups
- **copper_price_index.csv** — COMEX monthly average prices

### Pipeline Thresholds (in taxonomy.py)
- `REPORTING_START_DATE`: 2024-01-01
- `MIN_VENDOR_MATCH_RATE`: 70%
- `MIN_ITEM_MATCH_RATE`: 30%
- `BATCH_SIZE`: 20 items / 30 vendors per API call
- `RATE_LIMIT_DELAY`: 3 seconds between batches

## Dependencies

```
pandas >= 2.0.0
numpy >= 1.24.0
pyarrow >= 14.0.0
pyyaml >= 6.0
beautifulsoup4 >= 4.12.0
requests >= 2.31.0
openpyxl >= 3.1.0
```
