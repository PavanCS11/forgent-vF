# Forgent Procurement Data Pipeline - Detailed Flowchart

> Auto-generated pipeline documentation. See `pipeline_runner.py` for orchestration.
> Schema Version: 2.6.0 | ~120 columns | ~59K rows

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'primaryColor': '#008B8B',
    'primaryTextColor': '#000405',
    'primaryBorderColor': '#2F4F4F',
    'lineColor': '#008B8B',
    'secondaryColor': '#73F3FF',
    'tertiaryColor': '#AFEEEE',
    'background': '#FAFAFA',
    'mainBkg': '#FAFAFA',
    'textColor': '#000405',
    'fontSize': '13px'
  }
}}%%

flowchart TD
    %% =================================================================
    %% FORGENT PROCUREMENT DATA PIPELINE - DETAILED ARCHITECTURE
    %% Orchestrated by pipeline_runner.py (4 execution phases, 6 steps)
    %% =================================================================

    %% ─────────────────────────────────────────────────────────────────
    %% SOURCE SYSTEMS
    %% ─────────────────────────────────────────────────────────────────
    subgraph SOURCE["SOURCE SYSTEMS"]
        EPICOR["EPICOR ERP<br/>States Business Unit"]
        NETSUITE["NETSUITE ERP<br/>PwrQ Business Unit"]
    end

    subgraph RAW_FILES["RAW DATA FILES"]
        E_PO["Epicor Open POs<br/>.csv / .xlsx"]
        E_REC["Epicor Receipts<br/>.csv / .xlsx"]
        N_PO["NetSuite Open POs<br/>.csv"]
        N_REC["NetSuite Receipts<br/>.csv"]
    end

    EPICOR --> E_PO
    EPICOR --> E_REC
    NETSUITE --> N_PO
    NETSUITE --> N_REC

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 1: INGESTION
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP1["STEP 1: INGESTION - Normalization"]
        subgraph S1_CONFIG["Configuration"]
            S1_YAML["source_mappings.yaml<br/>6 source definitions"]
            S1_FMAP["transaction_field_mapping.csv<br/>Field mappings per source"]
            S1_SCHEMA["transactions.yaml<br/>56-field schema + types"]
        end

        S1_LOADER["config_loader.py<br/>Load YAML config"]
        S1_NORM["normalizer.py<br/>Normalizer class"]
        S1_GLOB["Find files by<br/>glob pattern"]
        S1_READ["Read CSV / Excel<br/>per source"]
        S1_MAP["Apply CSV-driven<br/>field mapping"]
        S1_DATE["Extract file dates<br/>MMDDYY format"]
        S1_UNION["Union all sources<br/>pd.concat"]
        S1_META["Write pipeline_metadata.json<br/>max_file_date + file dates"]
    end

    S1_YAML --> S1_LOADER
    S1_LOADER --> S1_NORM
    S1_FMAP --> S1_MAP
    S1_SCHEMA --> S1_MAP

    E_PO --> S1_GLOB
    E_REC --> S1_GLOB
    N_PO --> S1_GLOB
    N_REC --> S1_GLOB

    S1_GLOB --> S1_READ
    S1_READ --> S1_MAP
    S1_READ --> S1_DATE
    S1_MAP --> S1_UNION
    S1_DATE --> S1_META

    S1_UNION --> OUT1["all_transactions_normalized.csv"]
    S1_META --> OUT1_META["pipeline_metadata.json"]

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 2: ENRICHMENT
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP2["STEP 2: ENRICHMENT - Pre-Cleaning"]
        S2_LOAD["Load normalized CSV"]
        S2_CALC["Recalculate open_quantity<br/>total_qty - received_qty"]
        S2_DERIVE["Derive is_open_order<br/>qty-based, not status-based"]
        S2_CLEAN["Clean whitespace + NBSP<br/>Standardize to uppercase"]
        S2_NAN["Replace 'nan' strings<br/>with actual None"]
    end

    OUT1 --> S2_LOAD
    S2_LOAD --> S2_CALC
    S2_CALC --> S2_DERIVE
    S2_DERIVE --> S2_CLEAN
    S2_CLEAN --> S2_NAN

    S2_NAN --> OUT2["pre_cleaned_staging.csv"]

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 3: MAINTENANCE
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP3["STEP 3: MAINTENANCE - 4 Sequential Sub-Steps"]

        subgraph S3A["3a. Copper Prices"]
            S3A_FETCH["Fetch COMEX prices<br/>from cnwire.com"]
            S3A_PARSE["Parse HTML table<br/>BeautifulSoup"]
            S3A_FIND["Find new months<br/>not in existing CSV"]
            S3A_APPEND["Append + sort<br/>by year_month"]
        end

        S3A_FETCH --> S3A_PARSE
        S3A_PARSE --> S3A_FIND
        S3A_FIND --> S3A_APPEND
        S3A_APPEND --> OUT3A["copper_price_index.csv"]

        subgraph S3B["3b. Refresh Mappings"]
            S3B_VEND["Extract unique vendors<br/>from staging"]
            S3B_VNEW["Identify new vendors<br/>not in vendor_map"]
            S3B_VADD["Add new vendors<br/>to top of CSV"]
            S3B_ITEM["Extract unique items<br/>with best description"]
            S3B_DESC["Description priority:<br/>item_desc > display_name<br/>> mpn > line_desc"]
            S3B_INEW["Identify new items<br/>not in item_map"]
            S3B_IADD["Add new items<br/>to top of CSV"]
        end

        S3B_VEND --> S3B_VNEW
        S3B_VNEW --> S3B_VADD
        S3B_ITEM --> S3B_DESC
        S3B_DESC --> S3B_INEW
        S3B_INEW --> S3B_IADD

        subgraph S3C["3c. Enrich Vendors - AI Classification"]
            S3C_DEFAULT["Default Intercompany='N'<br/>for empty values"]
            S3C_REGEX["clean_vendor_name_regex<br/>Strip legal suffixes<br/>Apply title case"]
            S3C_API["OpenRouter API<br/>gemini-3-flash-preview<br/>Batch: 30 vendors<br/>Rate: 3s delay"]
            S3C_PARSE["Parse JSON response<br/>Validate Supplier_Type"]
            S3C_APPLY["Apply results:<br/>Standardized_Name<br/>Supplier_Type<br/>Location"]
        end

        S3C_DEFAULT --> S3C_REGEX
        S3C_REGEX --> S3C_API
        S3C_API --> S3C_PARSE
        S3C_PARSE --> S3C_APPLY

        subgraph S3D["3d. Enrich Items - Waterfall"]
            S3D_MATCH["Step 1: Description Match<br/>Inherit categories from<br/>similar existing items"]
            S3D_CAT["Step 2: AI Categorization<br/>14 L1 categories<br/>Batch: 20, Rate: 3s"]
            S3D_ELEC["Step 3: Electrical Specs"]
            S3D_RAW["Step 4: Raw Material Specs"]
        end

        subgraph S3D_ELEC_DETAIL["Electrical Spec Extraction"]
            S3D_E_REGEX["Regex extraction first<br/>amps, voltage, phase<br/>poles, kaic, frame"]
            S3D_E_CACHE["Check spec_cache.json<br/>SHA256 hash lookup"]
            S3D_E_AI["AI fallback for<br/>complex descriptions"]
            S3D_E_OUT["Outputs: spec_type<br/>spec_amps, spec_voltage<br/>spec_phase, spec_poles<br/>spec_kaic, spec_frame<br/>spec_kva, spec_gauge<br/>spec_nema"]
        end

        subgraph S3D_RAW_DETAIL["Raw Material Spec Extraction"]
            S3D_R_REGEX["Regex: T x W x L<br/>Gauge-to-thickness<br/>Weight calculation"]
            S3D_R_CACHE["Check spec_cache.json<br/>rm_ prefix keys"]
            S3D_R_AI["AI fallback<br/>for dimensions"]
            S3D_R_OUT["Outputs: RawMat_Finish<br/>Thickness, Width, Length<br/>UoM, Weight_Lbs"]
        end

        S3D_MATCH --> S3D_CAT
        S3D_CAT --> S3D_ELEC
        S3D_ELEC --> S3D_RAW

        S3D_E_REGEX --> S3D_E_CACHE
        S3D_E_CACHE --> S3D_E_AI
        S3D_E_AI --> S3D_E_OUT

        S3D_R_REGEX --> S3D_R_CACHE
        S3D_R_CACHE --> S3D_R_AI
        S3D_R_AI --> S3D_R_OUT
    end

    OUT2 --> S3B_VEND
    OUT2 --> S3B_ITEM
    S3B_VADD --> OUT3B_V["vendor_map.csv"]
    S3B_IADD --> OUT3B_I["item_map.csv"]
    OUT3B_V --> S3C_DEFAULT
    OUT3B_I --> S3D_MATCH
    S3C_APPLY --> OUT3C["vendor_map.csv<br/>+ AI enrichment"]
    S3D_E_OUT --> OUT3D_CACHE["spec_cache.json"]
    S3D_R_OUT --> OUT3D_CACHE
    S3D_RAW --> OUT3D["item_map.csv<br/>+ AI enrichment"]

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 4: ASSEMBLY
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP4["STEP 4: ASSEMBLY - FinalBuilder"]

        subgraph S4P1["Phase 1: Base Processing"]
            S4P1_TYPES["Standardize data types<br/>dates, numerics, currencies"]
            S4P1_VJOIN["Join vendor_map<br/>on vendor_name = Raw_Name"]
            S4P1_IJOIN["Join item_map<br/>Pass 1: item_id<br/>Pass 2: mpn fallback"]
            S4P1_SHORT["Create short_name<br/>25-char smart truncation"]
            S4P1_DESC["Description fallback<br/>display > raw_desc<br/>> line_desc > mpn"]
        end

        subgraph S4P2["Phase 2: ETL Enhancements"]
            S4P2_CAL["Calendar attributes<br/>year, month, quarter<br/>year_month"]
            S4P2_BU["Business unit mapping<br/>EPICOR = States<br/>NETSUITE = PwrQ"]
            S4P2_ADATE["Compute analysis_date<br/>Open: due > promise > expected > order<br/>Closed: receipt > due > order"]
        end

        subgraph S4P3["Phase 3: Metrics"]
            S4P3_RAWM["Raw material metrics<br/>total_weight, price_per_lb"]
            S4P3_CU["Copper price index join<br/>on order_year_month"]
            S4P3_TIME["Time metrics: lead_time<br/>days_until_due<br/>days_since_order"]
            S4P3_FLAGS["Status flags:<br/>is_overdue<br/>is_fully_received"]
            S4P3_OTD["OTD metrics (3-cat):<br/>Early / On Time / Late<br/>vs due + vs promise"]
            S4P3_QTY["6-col qty/amount:<br/>open, received<br/>open+received"]
            S4P3_SPEND["Spend bucketing:<br/>Micro to Strategic"]
        end

        subgraph S4P4["Phase 4: Validation"]
            S4P4_SCHEMA["Enforce schema types<br/>text-to-numeric conversion"]
            S4P4_DQ["Data quality checks<br/>Critical: required keys<br/>Warning: neg qty, dates<br/>grain uniqueness, costs"]
        end

        subgraph S4P5["Phase 5: Export"]
            S4P5_CSV["Save po_fact_table.csv<br/>backwards compatibility"]
            S4P5_PQ["Save po_fact_table.parquet<br/>PyArrow schema v2.6.0<br/>Snappy compression"]
            S4P5_DQR["Save data_quality_report.txt"]
        end

        S4P1_TYPES --> S4P1_VJOIN
        S4P1_VJOIN --> S4P1_IJOIN
        S4P1_IJOIN --> S4P1_SHORT
        S4P1_SHORT --> S4P1_DESC

        S4P1_DESC --> S4P2_CAL
        S4P2_CAL --> S4P2_BU
        S4P2_BU --> S4P2_ADATE

        S4P2_ADATE --> S4P3_RAWM
        S4P3_RAWM --> S4P3_CU
        S4P3_CU --> S4P3_TIME
        S4P3_TIME --> S4P3_FLAGS
        S4P3_FLAGS --> S4P3_OTD
        S4P3_OTD --> S4P3_QTY
        S4P3_QTY --> S4P3_SPEND

        S4P3_SPEND --> S4P4_SCHEMA
        S4P4_SCHEMA --> S4P4_DQ

        S4P4_DQ --> S4P5_CSV
        S4P4_DQ --> S4P5_PQ
        S4P4_DQ --> S4P5_DQR
    end

    OUT2 --> S4P1_TYPES
    OUT3C --> S4P1_VJOIN
    OUT3D --> S4P1_IJOIN
    OUT3A --> S4P3_CU
    OUT1_META --> S4P2_ADATE

    S4P5_CSV --> OUT4_CSV["po_fact_table.csv"]
    S4P5_PQ --> OUT4_PQ["po_fact_table.parquet<br/>~120 columns, ~59K rows"]
    S4P5_DQR --> OUT4_DQ["data_quality_report.txt"]

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 5: POWER BI
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP5["STEP 5: POWER BI DASHBOARD"]
        S5_THEME["forgent_theme.json<br/>Teal #008B8B + Cyan #73F3FF"]
        S5_REL["Relationships:<br/>analysis_date > Time<br/>vendor > DimSupplier<br/>category > DimCategory<br/>item > DimItem"]
        subgraph S5_PAGES["6 Dashboard Pages"]
            S5_P1["Overview<br/>KPIs, Spend Trend<br/>Top Vendors"]
            S5_P2["Vendor Analysis<br/>Vendor Slicer<br/>Category Mix"]
            S5_P3["Category Analysis<br/>Decomposition Tree<br/>Spend by L1/L2/L3"]
            S5_P4["Parts Analysis<br/>Item Search<br/>Price Trend"]
            S5_P5["Data Explorer<br/>Drill-down Matrices<br/>PO/Vendor/Category/Time"]
            S5_P6["Supplier Overview<br/>Concentration<br/>Top Supplier %"]
        end
    end

    OUT4_PQ --> STEP5

    %% ─────────────────────────────────────────────────────────────────
    %% STEP 6: ANALYSIS
    %% ─────────────────────────────────────────────────────────────────
    subgraph STEP6["STEP 6: ANALYSIS"]
        S6_SAVED["Saved reports<br/>step_6_analysis/saved/"]
        S6_TEMP["Working analyses<br/>step_6_analysis/temp/"]
    end

    OUT4_CSV --> STEP6
    OUT4_PQ --> STEP6

    %% ─────────────────────────────────────────────────────────────────
    %% LEGEND
    %% ─────────────────────────────────────────────────────────────────
    subgraph LEGEND["LEGEND"]
        L_RED["Red = Human-Updated Files"]
        L_GREEN["Green = Auto-Updated Files"]
        L_TEAL["Teal = Pipeline Process Steps"]
        L_PURPLE["Purple = AI / API Calls"]
        L_BLUE["Blue = Intermediate Outputs"]
    end

    %% =================================================================
    %% STYLING
    %% =================================================================

    %% Source Systems
    style SOURCE fill:#008B8B,stroke:#2F4F4F,stroke-width:3px,color:#FAFAFA
    style EPICOR fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style NETSUITE fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405

    %% Raw Files (Human Update = Red)
    style RAW_FILES fill:#FFEBEE,stroke:#C62828,stroke-width:4px,color:#000405
    style E_PO fill:#FFFFFF,stroke:#C62828,stroke-width:2px,color:#000405
    style E_REC fill:#FFFFFF,stroke:#C62828,stroke-width:2px,color:#000405
    style N_PO fill:#FFFFFF,stroke:#C62828,stroke-width:2px,color:#000405
    style N_REC fill:#FFFFFF,stroke:#C62828,stroke-width:2px,color:#000405

    %% Step 1: Ingestion
    style STEP1 fill:#73F3FF,stroke:#008B8B,stroke-width:3px,color:#000405
    style S1_CONFIG fill:#AFEEEE,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_YAML fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S1_FMAP fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S1_SCHEMA fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S1_LOADER fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_NORM fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_GLOB fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_READ fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_MAP fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_DATE fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_UNION fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S1_META fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style OUT1 fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#000405
    style OUT1_META fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#000405

    %% Step 2: Enrichment
    style STEP2 fill:#40E0D0,stroke:#008B8B,stroke-width:3px,color:#000405
    style S2_LOAD fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S2_CALC fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S2_DERIVE fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S2_CLEAN fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S2_NAN fill:#FFFFFF,stroke:#008B8B,stroke-width:2px,color:#000405
    style OUT2 fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#000405

    %% Step 3: Maintenance
    style STEP3 fill:#AFEEEE,stroke:#008B8B,stroke-width:3px,color:#000405

    %% 3a: Copper (Auto-updated = Green)
    style S3A fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405
    style S3A_FETCH fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style S3A_PARSE fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style S3A_FIND fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style S3A_APPEND fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style OUT3A fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405

    %% 3b: Refresh Mappings
    style S3B fill:#AFEEEE,stroke:#008B8B,stroke-width:2px,color:#000405
    style S3B_VEND fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_VNEW fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_VADD fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_ITEM fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_DESC fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_INEW fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3B_IADD fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style OUT3B_V fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#000405
    style OUT3B_I fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#000405

    %% 3c: Vendor AI (Purple for AI)
    style S3C fill:#E8D5E8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3C_DEFAULT fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3C_REGEX fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3C_API fill:#CE93D8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3C_PARSE fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3C_APPLY fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style OUT3C fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405

    %% 3d: Item AI (Purple for AI)
    style S3D fill:#E8D5E8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3D_MATCH fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3D_CAT fill:#CE93D8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3D_ELEC fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3D_RAW fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3D_ELEC_DETAIL fill:#F3E5F5,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3D_E_REGEX fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3D_E_CACHE fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style S3D_E_AI fill:#CE93D8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3D_E_OUT fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3D_RAW_DETAIL fill:#F3E5F5,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style S3D_R_REGEX fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S3D_R_CACHE fill:#FFFFFF,stroke:#2E7D32,stroke-width:1px,color:#000405
    style S3D_R_AI fill:#CE93D8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style S3D_R_OUT fill:#FFFFFF,stroke:#6A1B9A,stroke-width:1px,color:#000405
    style OUT3D fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405
    style OUT3D_CACHE fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405

    %% Step 4: Assembly
    style STEP4 fill:#008B8B,stroke:#2F4F4F,stroke-width:3px,color:#FAFAFA
    style S4P1 fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style S4P2 fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style S4P3 fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style S4P4 fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style S4P5 fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405

    style S4P1_TYPES fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P1_VJOIN fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P1_IJOIN fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P1_SHORT fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P1_DESC fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    style S4P2_CAL fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P2_BU fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P2_ADATE fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    style S4P3_RAWM fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_CU fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_TIME fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_FLAGS fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_OTD fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_QTY fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P3_SPEND fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    style S4P4_SCHEMA fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P4_DQ fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    style S4P5_CSV fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P5_PQ fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S4P5_DQR fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    style OUT4_CSV fill:#00CED1,stroke:#008B8B,stroke-width:3px,color:#000405
    style OUT4_PQ fill:#00CED1,stroke:#008B8B,stroke-width:3px,color:#000405
    style OUT4_DQ fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#000405

    %% Step 5: Power BI
    style STEP5 fill:#48D1CC,stroke:#008B8B,stroke-width:3px,color:#000405
    style S5_THEME fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_REL fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_PAGES fill:#73F3FF,stroke:#008B8B,stroke-width:2px,color:#000405
    style S5_P1 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_P2 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_P3 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_P4 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_P5 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S5_P6 fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    %% Step 6: Analysis
    style STEP6 fill:#AFEEEE,stroke:#008B8B,stroke-width:3px,color:#000405
    style S6_SAVED fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405
    style S6_TEMP fill:#FFFFFF,stroke:#008B8B,stroke-width:1px,color:#000405

    %% Legend
    style LEGEND fill:#FAFAFA,stroke:#2F4F4F,stroke-width:2px,color:#000405
    style L_RED fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#000405
    style L_GREEN fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#000405
    style L_TEAL fill:#40E0D0,stroke:#008B8B,stroke-width:2px,color:#000405
    style L_PURPLE fill:#CE93D8,stroke:#6A1B9A,stroke-width:2px,color:#000405
    style L_BLUE fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#000405
```

---

## Pipeline Summary

| Step | Phase | Script(s) | Input | Output |
|------|-------|-----------|-------|--------|
| 1 | Normalization | `normalizer.py`, `config_loader.py` | Raw CSVs/Excel (4+ sources) | `all_transactions_normalized.csv` |
| 2 | Enrichment | `pre_cleaner.py` | Normalized CSV | `pre_cleaned_staging.csv` |
| 3a | Copper Prices | `update_copper_prices.py` | cnwire.com HTML | `copper_price_index.csv` |
| 3b | Refresh Mappings | `refresh_mappings.py` | Staging CSV | `vendor_map.csv`, `item_map.csv` |
| 3c | Vendor AI | `enrich_vendors.py` > `vendor_classifier.py` | `vendor_map.csv` | `vendor_map.csv` (enriched) |
| 3d | Item AI | `enrich_items.py` > `ai_categorizer.py` + `spec_extractor.py` | `item_map.csv` | `item_map.csv` (enriched) |
| 4 | Assembly | `final_builder.py`, `schema_contract.py`, `data_quality.py` | Staging + all maps | `po_fact_table.parquet` + `.csv` |
| 5 | Power BI | 6 dashboard pages | Parquet | Interactive dashboards |
| 6 | Analysis | Saved reports | CSV/Parquet | Reports and analyses |

## AI Model Configuration

| Setting | Value |
|---------|-------|
| **Model** | `google/gemini-3-flash-preview` via OpenRouter |
| **Item Batch Size** | 20 items per API call |
| **Vendor Batch Size** | 30 vendors per API call |
| **Rate Limit** | 3 seconds between batches |
| **Max Retries** | 5 with exponential backoff |
| **Spec Cache** | `spec_cache.json` (SHA256 hash of description) |

## Key File Locations

| File | Path |
|------|------|
| Pipeline Orchestrator | `pipeline_runner.py` |
| Normalizer | `step_1_ingestion/d_scripts/normalizer.py` |
| Source Config | `step_1_ingestion/b.config/source_mappings.yaml` |
| Field Mapping | `step_1_ingestion/b.config/transaction_field_mapping.csv` |
| Schema | `step_1_ingestion/c.schemas/transactions.yaml` |
| PreCleaner | `step_2_enrichment/a_scripts/pre_cleaner.py` |
| Vendor Map | `step_3_maintenance/a.mappings/vendor_map.csv` |
| Item Map | `step_3_maintenance/a.mappings/item_map.csv` |
| Copper Index | `step_3_maintenance/a.mappings/copper_price_index.csv` |
| AI Categorizer | `step_3_maintenance/c_ai_enrichment/ai_categorizer.py` |
| Vendor Classifier | `step_3_maintenance/c_ai_enrichment/vendor_classifier.py` |
| Spec Extractor | `step_3_maintenance/c_ai_enrichment/spec_extractor.py` |
| Spec Cache | `step_3_maintenance/c_ai_enrichment/spec_cache.json` |
| Final Builder | `step_4_assembly/a_scripts/final_builder.py` |
| Schema Contract | `step_4_assembly/a_scripts/schema_contract.py` |
| Data Quality | `step_4_assembly/a_scripts/data_quality.py` |
| Parquet Output | `step_4_assembly/b.final/po_fact_table.parquet` |
| PowerBI Theme | `step_5_powerbi/forgent_theme.json` |
| Dashboard Guide | `step_5_powerbi/dashboard_guide.md` |
