"""
AI Enrichment Module for Item Categorization and Spec Extraction

This module provides AI-powered categorization and specification extraction
for procurement items using OpenRouter's Gemini Flash 3.0 API.

Usage:
    from step_3_maintenance.c_ai_enrichment import AICategorizer, SpecExtractor

    categorizer = AICategorizer()
    results = categorizer.categorize_batch(items)
"""

try:
    from .taxonomy import (
        CATEGORY_LEVEL_1,
        CATEGORY_HIERARCHY,
        ELECTRICAL_CATEGORIES,
        RAW_MATERIAL_CATEGORY,
    )
    from .ai_categorizer import AICategorizer
    from .spec_extractor import SpecExtractor
    from .vendor_classifier import VendorClassifier
except ImportError:
    from taxonomy import (
        CATEGORY_LEVEL_1,
        CATEGORY_HIERARCHY,
        ELECTRICAL_CATEGORIES,
        RAW_MATERIAL_CATEGORY,
    )
    from ai_categorizer import AICategorizer
    from spec_extractor import SpecExtractor
    from vendor_classifier import VendorClassifier

__all__ = [
    "AICategorizer",
    "SpecExtractor",
    "VendorClassifier",
    "CATEGORY_LEVEL_1",
    "CATEGORY_HIERARCHY",
    "ELECTRICAL_CATEGORIES",
    "RAW_MATERIAL_CATEGORY",
]
