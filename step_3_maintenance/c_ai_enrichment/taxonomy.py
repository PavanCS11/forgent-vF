"""
Taxonomy Definitions for Item Categorization

Contains valid category values and hierarchical relationships
extracted from existing item_map.csv data.
"""

# =============================================================================
# API & MODEL CONFIGURATION
# =============================================================================

# OpenRouter API settings
API_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-3-flash-preview"  # Unified model for all tasks
BATCH_SIZE = 20  # Items per API call
RATE_LIMIT_DELAY = 3.0  # Seconds between batches (increased to avoid 429 errors)
MAX_RETRIES = 5  # Increased from 3 for better resilience

# =============================================================================
# CONFIDENCE SCORE STANDARDIZATION
# =============================================================================

# Map text labels to numeric values (for data migration)
CONFIDENCE_TEXT_TO_NUMERIC = {
    "high": 0.9,
    "medium": 0.7,
    "low": 0.3,
    "": 0.0,
}

# Confidence thresholds for extraction methods
CONFIDENCE_REGEX_HIGH = 0.9      # 3+ fields extracted via regex
CONFIDENCE_REGEX_MEDIUM = 0.7   # 1-2 fields extracted via regex
CONFIDENCE_AI_EXTRACTION = 0.8   # AI extraction
CONFIDENCE_AI_CATEGORIZATION = 0.85  # AI categorization
CONFIDENCE_FALLBACK = 0.0        # Failed extraction

# Consolidation confidence thresholds
CONFIDENCE_CONSOLIDATION_EXACT = 1.0       # Pass 1: exact normalized match
CONFIDENCE_CONSOLIDATION_FINGERPRINT = 0.9  # Pass 2: spec fingerprint match
CONFIDENCE_CONSOLIDATION_AI = 0.8          # Pass 3: AI-confirmed fuzzy match

# Consolidation settings
CONSOLIDATION_CLUSTER_SIZE = 10     # Max items per AI consolidation cluster
CONSOLIDATION_SIMILARITY_THRESHOLD = 0.75  # Min Jaccard similarity for AI candidates

# =============================================================================
# VALID ANALYSIS TYPES
# =============================================================================

VALID_ANALYSIS_TYPES = [
    "regex_extraction",
    "ai_extraction",
    "ai_categorization",
    "ai_categorization_failed",
]

# Valid Category Level 1 values (top-level categories)
CATEGORY_LEVEL_1 = [
    "Circuit Protection",
    "Electrical Components",
    "MRO & Indirect",
    "Enclosures & Hardware",
    "Switchgear & Distribution",
    "Raw Materials",
    "Transformers",
    "Professional Services",
    "Wire & Cable",
    "Logistics & Freight",
    "Industrial Equipment",
    "Office & Administrative",
    "Engineering Services",
    "Miscellaneous/Other",
]

# Hierarchical category structure: L1 -> L2 -> [L3 options]
CATEGORY_HIERARCHY = {
    "Circuit Protection": {
        "Low Voltage Breakers": ["MCCB", "MCB", "ACB", "Safety Switches"],
        "Breaker Accessories": ["Mounting Kits", "Trip Units", "Auxiliary Contacts", "Rating Plugs", "Shunt Trips"],
        "Fuses & Disconnects": ["Power Fuses", "Non-Fusible Disconnect Switch", "Fuse Holders", "Fuse Blocks"],
        "Protection Devices": ["Surge Protective Devices", "Ground Fault Protection"],
    },
    "Electrical Components": {
        "Terminal & Wiring": ["Terminal Blocks", "Connectors", "Wire Markers", "Lugs", "Splices"],
        "Power Supplies": ["Batteries", "DC Power Supplies", "UPS", "Chargers"],
        "Switching & Control": ["Relays", "Control Switches", "Contactors", "Motor Starters", "Pushbuttons"],
        "Metering & Sensing": ["Power Meters", "Current Sensors", "Energy Meters", "Current Transformers"],
    },
    "Raw Materials": {
        "Steel Products": ["Steel Plate", "Galvanized Sheet", "Galvanneal Sheet", "C-Channel", "Angle Iron"],
        "Aluminum Products": ["Aluminum Bus Bar", "Aluminum Sheet", "Aluminum Extrusion"],
        "Insulation & Coatings": ["Insulating Materials", "Paint", "Powder Coating", "Sealants"],
        "Copper Products": ["Copper Bus Bar", "Copper Wire", "Copper Sheet", "Copper Tubing"],
    },
    "Enclosures & Hardware": {
        "Structural Components": ["DIN Rails", "Mounting Hardware", "Miscellaneous Hardware", "Brackets"],
        "Fasteners & Fittings": ["Bolts & Screws", "Nuts & Washers", "Rivets", "Anchors"],
        "Doors & Covers": ["Enclosure Doors", "Cover Plates", "Panels", "Gaskets"],
        "Electrical Enclosures": ["NEMA Enclosures", "Junction Boxes", "Pull Boxes", "Cabinets"],
    },
    "Transformers": {
        "Low Voltage Transformers": ["Control Transformer", "Isolation Transformer", "Buck-Boost"],
        "Medium Voltage Transformers": ["Distribution Transformer", "Pad-Mount Transformer"],
        "Specialty Transformers": ["Current Transformer", "Potential Transformer", "Autotransformer"],
    },
    "Wire & Cable": {
        "Building Wire": ["THHN/THWN", "NM-B Cable", "UF-B Cable", "SE Cable"],
        "Control Cable": ["Multi-Conductor", "Shielded Cable", "Instrumentation Cable"],
        "Power Cable": ["Medium Voltage Cable", "Tray Cable", "Armored Cable"],
        "Cable Assemblies": ["Plug Assemblies", "Pre-Terminated Assemblies", "Whips"],
    },
    "Switchgear & Distribution": {
        "Low Voltage Switchgear": ["Panelboards", "Switchboards", "Motor Control Centers"],
        "Medium Voltage Switchgear": ["Metal-Clad Switchgear", "Metal-Enclosed Switchgear"],
        "Busway": ["Bus Duct", "Plug-In Busway", "Feeder Busway"],
    },
    "MRO & Indirect": {
        "Office & Administrative": ["Office Supplies", "IT Equipment", "Consumables", "Cleaning Supplies"],
        "Industrial Equipment": ["Packaging Materials", "Material Handling", "Manufacturing Equipment"],
        "Tools & Equipment": ["Hand Tools", "Power Tools", "Safety Equipment", "PPE"],
        "Facilities": ["Factory Maintenance", "Building Supplies", "HVAC", "Plumbing"],
    },
    "Professional Services": {
        "Engineering Services": ["Testing Services", "Design Services", "Consulting"],
        "Field Services": ["Site Visits", "Installation", "Commissioning"],
    },
    "Logistics & Freight": {
        "Transportation": ["Freight Shipping", "Expedited Shipping", "LTL Freight"],
    },
    "Miscellaneous/Other": {
        "Inactive Items": ["No Transaction History", "Master Data Only"],
        "Uncategorized": ["Pending Review", "Insufficient Information", "Requires Manual Review"],
    },
}

# Categories that require electrical spec extraction
ELECTRICAL_CATEGORIES = [
    "Circuit Protection",
    "Electrical Components",
    "Switchgear & Distribution",
    "Transformers",
    "Wire & Cable",
]

# Category that requires raw material spec extraction
RAW_MATERIAL_CATEGORY = "Raw Materials"

# Electrical spec fields to extract
ELECTRICAL_SPEC_FIELDS = [
    "spec_type",
    "spec_amps",
    "spec_voltage",
    "spec_phase",
    "spec_poles",
    "spec_kaic",
    "spec_frame",
    "spec_kva",
    "spec_gauge",
    "spec_nema",
]

# Raw material spec fields to extract
RAW_MATERIAL_SPEC_FIELDS = [
    "RawMat_Finish",
    "RawMat_Thickness",
    "RawMat_Width",
    "RawMat_Length",
    "RawMat_UoM",
    "RawMat_Weight_Lbs",
]


# =============================================================================
# PIPELINE-WIDE CONFIGURATION
# =============================================================================

# Date filtering — assembly excludes transactions before this date
REPORTING_START_DATE = '2024-01-01'

# Merge validation thresholds — assembly fails if match rates drop below these
MIN_VENDOR_MATCH_RATE = 0.70   # 70% of rows must match vendor map
MIN_ITEM_MATCH_RATE = 0.30     # 30% of rows must have non-Uncategorized category

# Row count validation — pipeline fails if too many rows lost between steps
MAX_ROW_LOSS_PCT = 5.0         # Max acceptable row loss between normalization and enrichment

# Data quality thresholds
MAX_OVERLAP_THRESHOLD = 100    # Transaction type overlap > this becomes CRITICAL


def get_taxonomy_prompt() -> str:
    """
    Generate a formatted taxonomy string for LLM prompts.
    Excludes Miscellaneous/Other since those are catch-all categories.
    """
    lines = []
    for l1, l2_dict in CATEGORY_HIERARCHY.items():
        if l1 == "Miscellaneous/Other":
            continue  # Skip catch-all category for categorization prompts
        lines.append(f"\n{l1}:")
        for l2, l3_list in l2_dict.items():
            l3_str = ", ".join(l3_list)
            lines.append(f"  - {l2}: {l3_str}")
    return "\n".join(lines)
