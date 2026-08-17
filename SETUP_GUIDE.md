# Forgent Pipeline - Setup Guide

## What You Received

A zip file containing a Python data pipeline that processes your NetSuite and Epicor purchase order data into a unified fact table for Power BI.

## First-Time Setup

### 1. Install Python

Download Python 3.10 or newer from [python.org](https://www.python.org/downloads/). During installation, check **"Add Python to PATH"**.

### 2. Unzip and Install Dependencies

Unzip the folder wherever you'd like to keep it. Then open a terminal (Command Prompt or PowerShell), navigate to the folder, and run:

```
cd "path\to\forgent-vF"
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

You'll need to run `.venv\Scripts\activate` each time you open a new terminal before running the pipeline.

### 3. Run the Pipeline

```
python pipeline_runner.py
```

That's it. The pipeline will process whatever raw data files are already in `step_1_ingestion\a.raw_data\` and produce output in `step_4_assembly\b.final\`.

## Updating with New Data

Whenever you have new ERP exports:

1. Replace the files in the folders below with your latest exports:

   | Folder | File | Notes |
   |--------|------|-------|
   | `step_1_ingestion\a.raw_data\Netsuite\` | `Netsuite Receipts*.csv` | |
   | `step_1_ingestion\a.raw_data\Netsuite\` | `Netsuite Open POs*.csv` | |
   | `step_1_ingestion\a.raw_data\Netsuite\` | `Netsuite PO Line Dates*.csv` | Required for OTD. Saved search: PO number + promise/due dates. |
   | `step_1_ingestion\a.raw_data\Epicor\` | `Epicor Receipts*.xlsx` | |
   | `step_1_ingestion\a.raw_data\Epicor\` | `Epicor Open POs*.xlsx` | |

2. Run `python pipeline_runner.py`
3. Updated output will appear in `step_4_assembly\b.final\` as `po_fact_table.csv` and `po_fact_table.parquet`
4. Refresh your Power BI report to pick up the new data

## AI Classification (Optional)

The pipeline can automatically categorize new items and vendors using AI. This is **optional** — without it, everything still works, but new items and vendors will appear in the output with blank classification columns (Category, Supplier Type, etc.). You can fill those in manually in the mapping files if you prefer, or just leave them blank.

To enable AI classification:

1. Create a free account at [openrouter.ai](https://openrouter.ai)
2. Generate an API key from your dashboard
3. Create a file called `.env` in the project root folder with this single line:
   ```
   OPENROUTER_API_KEY=your_key_here
   ```
4. Run the pipeline as normal — new items and vendors will be classified automatically

API costs are minimal (typically under $1 per full pipeline run using the default Gemini Flash model).

## Output Files

| File | Location | Description |
|------|----------|-------------|
| `po_fact_table.csv` | `step_4_assembly\b.final\` | Final fact table (CSV) |
| `po_fact_table.parquet` | `step_4_assembly\b.final\` | Final fact table (Parquet) |
| `data_quality_report.txt` | `step_4_assembly\b.final\` | Data quality check results |

## Key Files You May Want to Edit

| File | What It Does |
|------|-------------|
| `step_3_maintenance\a.mappings\vendor_map.csv` | Vendor names, types, and locations. Edit in Excel to manually classify vendors. |
| `step_3_maintenance\a.mappings\item_map.csv` | Item categories and specs. Edit in Excel to manually classify items. |
| `step_3_maintenance\a.mappings\terms_map.csv` | Payment terms normalization. Add rows if new payment terms appear. |
| `step_1_ingestion\b.config\source_mappings.yaml` | Source file patterns. Update if your export filenames change. |
| `step_1_ingestion\b.config\transaction_field_mapping.csv` | Column name mappings from your ERP exports to the pipeline schema. |

## Troubleshooting

- **"python is not recognized"** — Python isn't on your PATH. Reinstall and check "Add to PATH", or use the full path to python.exe.
- **"No module named pandas"** — You need to activate the virtual environment first: `.venv\Scripts\activate`
- **AI steps show warnings** — If you haven't set up an OpenRouter key, this is expected. The pipeline continues without AI.
- **New vendors/items have blank categories** — Either set up AI classification (see above) or fill in the mapping CSVs manually.
