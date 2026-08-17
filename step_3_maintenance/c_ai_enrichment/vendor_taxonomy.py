"""
Vendor Taxonomy Definitions for Vendor Classification

Contains valid vendor enrichment values, API configuration,
and prompt generation utilities for AI-powered vendor enrichment.
"""

import re

# =============================================================================
# API & MODEL CONFIGURATION (re-exported from taxonomy.py for single source of truth)
# =============================================================================

try:
    from .taxonomy import (
        API_BASE_URL,
        DEFAULT_MODEL,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
    )
except ImportError:
    from taxonomy import (
        API_BASE_URL,
        DEFAULT_MODEL,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
    )

# Vendor-specific batch size: vendor names are shorter than item descriptions
# so we can fit more per batch while staying within token limits
VENDOR_BATCH_SIZE = 30

# =============================================================================
# VALID SUPPLIER TYPES
# =============================================================================

VALID_SUPPLIER_TYPES = [
    "Manufacturer",
    "Distributor",
    "Service Provider",
    "Individual",
]

# =============================================================================
# CONFIDENCE SCORES
# =============================================================================

CONFIDENCE_VENDOR_AI = 0.85
CONFIDENCE_VENDOR_DEFAULT = 1.0       # For Intercompany="N" default
CONFIDENCE_VENDOR_FALLBACK = 0.0

# =============================================================================
# VALID ANALYSIS TYPES (for vendor enrichment)
# =============================================================================

VALID_VENDOR_ANALYSIS_TYPES = [
    "ai_vendor_classification",
    "ai_vendor_classification_failed",
    "default_value",
]

# =============================================================================
# VENDOR NAME CLEANUP PATTERNS
# =============================================================================

# Common legal suffixes to strip (applied in order)
VENDOR_NAME_SUFFIX_PATTERNS = [
    re.compile(r",?\s*\b(LLC|L\.L\.C\.|Inc\.?|Incorporated|Corp\.?|Corporation|Co\.?|Company|Ltd\.?|Limited|LP|L\.P\.)\s*$", re.IGNORECASE),
    re.compile(r",?\s*\b(DBA|D/B/A|D\.B\.A\.)\s+.*$", re.IGNORECASE),
]

# Known acronyms that should stay uppercase
PRESERVE_UPPERCASE = {
    "ABB", "GE", "3M", "UPS", "HVAC", "MRO", "PPE", "CNC", "LED",
    "USA", "DHL", "FPL", "IBM", "HP", "AMD", "LLC", "INC",
}


def clean_vendor_name_regex(raw_name: str) -> str:
    """
    Pre-process vendor name with regex before AI call.

    1. Strip common legal suffixes (LLC, Inc, Corp, etc.)
    2. Apply title case while preserving known acronyms
    3. Normalize whitespace

    Args:
        raw_name: Original vendor name from ERP system.

    Returns:
        Cleaned vendor name string.
    """
    if not raw_name or not isinstance(raw_name, str):
        return raw_name or ""

    name = raw_name.strip()

    # Strip legal suffixes
    for pattern in VENDOR_NAME_SUFFIX_PATTERNS:
        name = pattern.sub("", name).strip()

    # Remove trailing commas/periods left after suffix removal
    name = name.rstrip(",. ")

    # Title case, but preserve known acronyms
    words = name.split()
    result_words = []
    for word in words:
        upper_word = word.upper().strip(".,;:")
        if upper_word in PRESERVE_UPPERCASE:
            result_words.append(word.upper())
        else:
            result_words.append(word.title())
    name = " ".join(result_words)

    # Normalize whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


def get_vendor_classification_prompt() -> str:
    """Generate the vendor classification prompt template."""
    return """You are a procurement and supply chain expert specializing in
industrial electrical manufacturing vendors.

For each vendor, determine:
1. **Standardized_Name**: Clean, professional vendor name
2. **Supplier_Type**: One of: Manufacturer, Distributor, Service Provider, Individual
3. **Location**: Primary geographic location (country level)

## Name Standardization Rules:
- Remove legal suffixes: LLC, Inc., Corp., Ltd., L.P., Co.
- Use proper title case (except known acronyms: ABB, GE, 3M, etc.)
- Use the well-known/trade name when identifiable
  (e.g., "AUTOMATION DIRECT.COM INC" -> "AutomationDirect")
- For "DBA" patterns, use the primary business name
- Keep ampersands as "&" (e.g., "S & C Electric")
- The regex_clean field shows a pre-cleaned version; use it as a starting
  point but apply your knowledge of the actual company

## Supplier Type Definitions:
- **Manufacturer**: Makes/assembles products (e.g., Schneider Electric, Eaton, ABB, Siemens)
- **Distributor**: Resells products from manufacturers (e.g., Anixter, Graybar, WESCO, Grainger)
- **Service Provider**: Provides services (engineering, testing, consulting, freight, staffing, construction)
- **Individual**: Person's name, sole proprietor, or individual contractor

## Location Rules:
- For well-known companies, use their headquarters country
- For US companies, use "USA"
- For multinational companies with US operations, use "USA" if that is their primary market presence
- Use "Unknown" if you cannot determine the location
- Be specific when possible: "Germany", "Japan", "Canada", "France", etc.

## Response Format (JSON array):
[
  {{"id": "VENDOR RAW NAME", "name": "Clean Name", "type": "Manufacturer", "location": "USA", "confidence": 0.95}},
  {{"id": "ANOTHER VENDOR", "name": "Another Vendor", "type": "Distributor", "location": "Germany", "confidence": 0.80}}
]

Rules:
- "id" must match the original vendor id exactly
- "type" must be exactly one of: Manufacturer, Distributor, Service Provider, Individual
- "confidence" is 0.0-1.0 indicating your overall confidence
- If uncertain, set confidence < 0.5

Respond ONLY with the JSON array, no other text."""
