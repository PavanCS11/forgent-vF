"""
Specification Extractor

Extracts electrical and raw material specifications from item descriptions.
Uses regex patterns first, then AI fallback for complex cases.
"""

import re
import json
import time
import random
import hashlib
import requests
from fractions import Fraction
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os

try:
    from .taxonomy import (
        ELECTRICAL_SPEC_FIELDS,
        RAW_MATERIAL_SPEC_FIELDS,
        API_BASE_URL,
        DEFAULT_MODEL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_REGEX_HIGH,
        CONFIDENCE_REGEX_MEDIUM,
        CONFIDENCE_AI_EXTRACTION,
        CONFIDENCE_FALLBACK,
    )
except ImportError:
    from taxonomy import (
        ELECTRICAL_SPEC_FIELDS,
        RAW_MATERIAL_SPEC_FIELDS,
        API_BASE_URL,
        DEFAULT_MODEL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_REGEX_HIGH,
        CONFIDENCE_REGEX_MEDIUM,
        CONFIDENCE_AI_EXTRACTION,
        CONFIDENCE_FALLBACK,
    )

# Load .env from project root
ENV_PATH = Path(__file__).parent.parent.parent / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)


class SpecExtractor:
    """
    Extracts electrical and raw material specifications from item descriptions.
    """

    # Material densities (lbs per cubic inch)
    DENSITIES = {
        "copper": 0.323,
        "aluminum": 0.098,
        "steel": 0.284,
        "galvanized": 0.284,
    }

    # Extended gauge-to-thickness mapping (USS/MSG standard)
    GAUGE_TO_THICKNESS = {
        "7": 0.1793, "8": 0.1644, "9": 0.1495, "10": 0.1345,
        "11": 0.1196, "12": 0.1046, "13": 0.0897, "14": 0.0747,
        "15": 0.0673, "16": 0.0598, "17": 0.0538, "18": 0.0478,
        "19": 0.0418, "20": 0.0359, "21": 0.0329, "22": 0.0299,
        "23": 0.0269, "24": 0.0239, "25": 0.0209, "26": 0.0179,
    }

    def __init__(self, api_key: Optional[str] = None, cache: Optional[Dict] = None):
        """Initialize the extractor.

        Args:
            api_key: OpenRouter API key.
            cache: Optional dict for caching AI results by description hash.
                   Pass a shared dict to persist across calls; results are
                   written in-place so the caller can save to disk.
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            print("WARNING: OPENROUTER_API_KEY not found. AI extraction will be disabled.")
        self._cache = cache if cache is not None else {}
        self._cache_hits = 0

    # =========================================================================
    # CACHE HELPERS
    # =========================================================================

    def _check_cache(self, items: List[Dict], prefix: str = "") -> Tuple[List[Dict], List[Dict]]:
        """
        Check cache for previously processed items.

        Args:
            items: Items that need AI processing (post-regex).
            prefix: Cache key prefix (e.g. "rm_" for raw materials).

        Returns:
            (cached_results, cache_misses) — cached_results to append to results,
            cache_misses to send to AI.
        """
        if not items or not self._cache:
            return [], items

        cached_results = []
        cache_misses = []
        cache_skip_count = 0

        for item in items:
            desc = str(item.get("Raw_Description", "")).strip().lower()
            cache_key = prefix + hashlib.sha256(desc.encode()).hexdigest()[:16]
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                if entry.get("_failed"):
                    cached_results.append({"Item_ID": str(item.get("Item_ID", ""))})
                    cache_skip_count += 1
                else:
                    cached_results.append({**entry, "Item_ID": str(item.get("Item_ID", ""))})
                self._cache_hits += 1
            else:
                cache_misses.append(item)

        if self._cache_hits > 0:
            msg = f"  Cache hits: {self._cache_hits:,} items"
            if cache_skip_count > 0:
                msg += f" ({cache_skip_count:,} previously failed, skipped)"
            print(msg)

        return cached_results, cache_misses

    def _store_cache(self, items: List[Dict], results: List[Dict], prefix: str = ""):
        """
        Store AI results in cache for future lookups.

        Args:
            items: The items that were sent to AI.
            results: The AI results (same order as items).
            prefix: Cache key prefix (e.g. "rm_" for raw materials).
        """
        for item, result in zip(items, results):
            desc = str(item.get("Raw_Description", "")).strip().lower()
            cache_key = prefix + hashlib.sha256(desc.encode()).hexdigest()[:16]
            cache_entry = {k: v for k, v in result.items() if k != "Item_ID"}
            self._cache[cache_key] = cache_entry

    # =========================================================================
    # ELECTRICAL SPEC EXTRACTION
    # =========================================================================

    def _deduplicate_items(self, items: List[Dict]) -> tuple:
        """
        Deduplicate items by description to reduce API calls.

        Returns:
            tuple: (unique_items, desc_to_item_ids) - unique items and mapping
        """
        desc_to_item_ids = {}  # description -> list of Item_IDs
        unique_items = []

        for item in items:
            desc = str(item.get("Raw_Description", "")).strip()
            item_id = str(item.get("Item_ID", ""))

            if desc not in desc_to_item_ids:
                desc_to_item_ids[desc] = []
                unique_items.append(item)
            desc_to_item_ids[desc].append(item_id)

        return unique_items, desc_to_item_ids

    def _expand_deduplicated_results(
        self, results: List[Dict], desc_to_item_ids: dict, items: List[Dict]
    ) -> List[Dict]:
        """Expand deduplicated results back to all original items."""
        # Build description -> result mapping
        desc_to_result = {}
        for result in results:
            item_id = result.get("Item_ID")
            # Find the description for this item
            for item in items:
                if str(item.get("Item_ID", "")) == item_id:
                    desc = str(item.get("Raw_Description", "")).strip()
                    desc_to_result[desc] = result
                    break

        # Expand to all items with same description
        expanded = []
        for desc, item_ids in desc_to_item_ids.items():
            if desc in desc_to_result:
                base_result = desc_to_result[desc]
                for item_id in item_ids:
                    expanded.append({**base_result, "Item_ID": item_id})
            else:
                # No result found, add empty results
                for item_id in item_ids:
                    expanded.append({"Item_ID": item_id})

        return expanded

    def extract_electrical_specs(self, items: List[Dict]) -> List[Dict]:
        """
        Extract electrical specifications from items.

        Args:
            items: List of dicts with 'Item_ID' and 'Raw_Description'.

        Returns:
            List of dicts with spec fields.
        """
        # Deduplicate by description to reduce processing
        unique_items, desc_to_item_ids = self._deduplicate_items(items)
        if len(unique_items) < len(items):
            print(f"  Deduplicated: {len(items):,} -> {len(unique_items):,} unique descriptions")

        results = []
        regex_needed_ai = []

        for item in unique_items:
            regex_result = self._extract_electrical_regex(item)
            if regex_result and regex_result.get("spec_type"):
                results.append(regex_result)
            else:
                regex_needed_ai.append(item)

        # Check cache before calling AI
        cached_results, cache_needed_ai = self._check_cache(regex_needed_ai)
        results.extend(cached_results)

        # Use AI for items that couldn't be parsed by regex or cache
        if cache_needed_ai and self.api_key:
            ai_results = self._extract_electrical_ai(cache_needed_ai)
            self._store_cache(cache_needed_ai, ai_results)
            results.extend(ai_results)
        elif cache_needed_ai:
            # No API key, return empty specs
            for item in cache_needed_ai:
                results.append({"Item_ID": str(item.get("Item_ID", ""))})

        # Expand results back to all original items
        return self._expand_deduplicated_results(results, desc_to_item_ids, unique_items)

    def _normalize_description(self, desc: str) -> str:
        """Normalize description for better regex matching."""
        # Replace newlines with spaces for multiline descriptions
        desc = desc.replace("\n", " ").replace("\r", " ")
        # Normalize multiple spaces
        desc = re.sub(r"\s+", " ", desc)
        return desc.upper().strip()

    def _extract_electrical_regex(self, item: Dict) -> Dict:
        """Extract electrical specs using regex patterns."""
        desc = self._normalize_description(str(item.get("Raw_Description", "")))
        item_id = str(item.get("Item_ID", ""))

        result = {"Item_ID": item_id, "analysis_type": "regex_extraction"}

        # Spec Type detection (order matters - more specific first)
        spec_type_patterns = {
            "MCCB": r"\bMCCB\b|MOLDED\s*CASE|MOLDED-CASE",
            "MCB": r"\bMCB\b|MINIATURE\s*CIRCUIT\s*BREAKER",
            "ACB": r"\bACB\b|AIR\s*CIRCUIT\s*BREAKER",
            "Contactor": r"\bCONTACTOR\b",
            "Relay": r"\bRELAY\b",
            "Motor Starter": r"\bMOTOR\s*STARTER\b",
            "Fuse": r"\bFUSE\b",
            "Transformer": r"\bTRANSFORMER\b|\bXFMR\b|\bCPT\b",
            "Disconnect": r"\bDISCONNECT\b|\bDISC\s*SWITCH\b",
            "Panelboard": r"\bPANELBOARD\b|\bLOAD\s*CENTER\b",
            "Breaker": r"\bBREAKER\b|\bCIRCUIT\s*BKR\b|\bCB\b",
        }

        for spec_type, pattern in spec_type_patterns.items():
            if re.search(pattern, desc):
                result["spec_type"] = spec_type
                break

        # Amperage: Handle dual ratings like "600A/200A", "600/200A"
        amp_patterns = [
            r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*/\s*\d+\s*(?:A|AMP)",  # Dual: 600/200A (capture first)
            r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:A|AMP|AMPS)\b",
            r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*AMPERE",
            r"\b(\d+)A\b",
        ]
        for pattern in amp_patterns:
            match = re.search(pattern, desc)
            if match:
                amp_str = match.group(1).replace(",", "")  # Remove thousands separator
                result["spec_amps"] = float(amp_str)
                break

        # Voltage: Handle compound like "480/277VAC", "208/120V"
        volt_patterns = [
            r"(\d+)\s*/\s*(\d+)\s*(?:V|VAC|VDC)",  # Compound voltage
            r"(\d+(?:\.\d+)?)\s*(?:V|VAC|VDC|VOLT|VOLTS)\b",
            r"\b(\d+)V\b",
        ]
        for pattern in volt_patterns:
            match = re.search(pattern, desc)
            if match:
                if match.lastindex == 2:  # Compound voltage
                    v1, v2 = match.group(1), match.group(2)
                    unit = "VDC" if ("DC" in desc or "VDC" in desc) else "VAC"
                    result["spec_voltage"] = f"{v1}/{v2}{unit}"
                else:
                    voltage = float(match.group(1))
                    unit = "VDC" if ("DC" in desc or "VDC" in desc) else "VAC"
                    result["spec_voltage"] = f"{int(voltage)}{unit}"
                break

        # Phase: Look for "3PH", "3 PHASE", "1PH", "SINGLE PHASE"
        if re.search(r"\b3\s*(?:PH|PHASE)\b|THREE\s*PHASE", desc):
            result["spec_phase"] = 3.0
        elif re.search(r"\b1\s*(?:PH|PHASE)\b|SINGLE\s*PHASE", desc):
            result["spec_phase"] = 1.0

        # Poles: Look for "3P", "3 POLE", "3-POLE"
        pole_match = re.search(r"\b(\d)\s*[-]?\s*(?:P|POLE|POLES)\b", desc)
        if pole_match:
            result["spec_poles"] = int(pole_match.group(1))

        # KAIC: Interrupting capacity in kA (more flexible patterns)
        kaic_patterns = [
            r"(\d+(?:\.\d+)?)\s*(?:KAIC|KAIC\b)",
            r"(\d+(?:\.\d+)?)\s*KA\s*(?:@|AT|\b)",  # 65KA @ or 65KA at
            r"(\d+(?:\.\d+)?)\s*K\s*(?:@|AIC)",  # 65K@ or 65KAIC
            r"@\s*\d+V?\s*=?\s*(\d+(?:\.\d+)?)\s*(?:KA|K)\b",
            r"AIC\s*[:=]?\s*(\d+(?:\.\d+)?)\s*(?:KA|K)?",
        ]
        for pattern in kaic_patterns:
            match = re.search(pattern, desc)
            if match:
                result["spec_kaic"] = float(match.group(1))
                break

        # Frame size: "L FRAME", "H FRAME", "400A FRAME", "POWERPACT L"
        frame_patterns = [
            r"([A-Z])\s*FRAME",
            r"(\d+)(?:A|AMP)?\s*FRAME",
            r"POWERPACT\s+([A-Z])\b",
        ]
        for pattern in frame_patterns:
            match = re.search(pattern, desc)
            if match:
                result["spec_frame"] = match.group(1)
                break

        # KVA for transformers (handle thousands separator)
        kva_patterns = [
            r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*KVA\b",
            r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*VA\b",
        ]
        for pattern in kva_patterns:
            match = re.search(pattern, desc)
            if match:
                value_str = match.group(1).replace(",", "")
                value = float(value_str)
                if "KVA" in pattern:
                    result["spec_kva"] = value
                else:
                    result["spec_kva"] = value / 1000  # Convert VA to KVA
                break

        # Wire gauge: "12 AWG", "#12", "12GA", "25mm²" (metric)
        gauge_patterns = [
            r"(\d+(?:/\d+)?)\s*(?:AWG|GA|GAUGE)\b",
            r"#(\d+)\b",
            r"(\d+(?:\.\d+)?)\s*MM[²2]?\b",  # Metric: 25mm²
        ]
        for pattern in gauge_patterns:
            match = re.search(pattern, desc)
            if match:
                gauge = match.group(1)
                if "MM" in pattern:
                    gauge = f"{gauge}mm²"
                result["spec_gauge"] = gauge
                break

        # NEMA rating: "NEMA 4X", "NEMA 3R", "TYPE 4X"
        nema_patterns = [
            r"NEMA\s*(\d+[A-Z]*)",
            r"TYPE\s+(\d+[A-Z]*)\s+(?:ENCL|RATED)",
        ]
        for pattern in nema_patterns:
            match = re.search(pattern, desc)
            if match:
                result["spec_nema"] = f"NEMA {match.group(1)}"
                break

        # Set confidence based on fields extracted
        fields_found = sum(1 for k, v in result.items() if v and k not in ["Item_ID", "analysis_type"])
        if fields_found >= 3:
            result["extraction_confidence"] = CONFIDENCE_REGEX_HIGH
        elif fields_found >= 1:
            result["extraction_confidence"] = CONFIDENCE_REGEX_MEDIUM
        else:
            result["extraction_confidence"] = CONFIDENCE_FALLBACK

        return result

    def _extract_electrical_ai(self, items: List[Dict]) -> List[Dict]:
        """Extract electrical specs using AI for complex cases."""
        if not self.api_key:
            return [{"Item_ID": str(item.get("Item_ID", ""))} for item in items]

        results = []
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1

            batch_results = self._call_ai_with_retry(
                self._call_electrical_ai, batch, batch_num, total_batches
            )
            results.extend(batch_results)

            # Rate limit between batches
            if batch_num < total_batches:
                time.sleep(RATE_LIMIT_DELAY)
        return results

    def _call_ai_with_retry(self, api_func, batch, batch_num, total_batches):
        """Call an AI extraction function with retry logic for rate limits."""
        for attempt in range(MAX_RETRIES):
            try:
                return api_func(batch)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limit
                    wait_time = (2**attempt) * 10 + random.uniform(0, 5)
                    print(f"  Rate limited (batch {batch_num}/{total_batches}), waiting {wait_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                elif e.response.status_code >= 500:  # Server error
                    wait_time = 2**attempt + 1
                    print(f"  Server error {e.response.status_code} (batch {batch_num}/{total_batches}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  AI extraction failed (batch {batch_num}/{total_batches}): {e}")
                    break
            except requests.exceptions.Timeout:
                wait_time = 2**attempt + 1
                print(f"  Timeout (batch {batch_num}/{total_batches}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            except json.JSONDecodeError as e:
                print(f"  JSON parse error (batch {batch_num}/{total_batches}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    break
            except Exception as e:
                print(f"  AI extraction failed (batch {batch_num}/{total_batches}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    break

        # All retries exhausted - return empty results with failure marker
        return [{"Item_ID": str(item.get("Item_ID", "")), "_failed": True} for item in batch]

    def _call_electrical_ai(self, items: List[Dict]) -> List[Dict]:
        """Call AI API for electrical spec extraction."""
        items_json = json.dumps(
            [
                {"id": str(item.get("Item_ID", "")), "desc": str(item.get("Raw_Description", ""))[:800]}
                for item in items
            ],
            indent=2,
        )

        prompt = f"""Extract electrical specifications from these item descriptions.

## Fields to Extract:
- spec_type: Product type (MCCB, MCB, ACB, Contactor, Relay, Fuse, Transformer, Breaker, etc.)
- spec_amps: Current rating as a number (e.g., 600 for "600A")
- spec_voltage: Voltage as string with unit (e.g., "480VAC", "24VDC")
- spec_phase: Phase as number (1 or 3)
- spec_poles: Number of poles (1, 2, 3, 4)
- spec_kaic: Interrupting rating in kA (e.g., 65 for "65kA")
- spec_frame: Frame size (e.g., "L", "H", "400")
- spec_kva: KVA rating for transformers
- spec_gauge: Wire gauge (e.g., "12 AWG")
- spec_nema: NEMA rating (e.g., "NEMA 4X")

## Items:
{items_json}

## Response Format (JSON array):
[
  {{"id": "item_id", "spec_type": "MCCB", "spec_amps": 600, "spec_voltage": "480VAC", ...}}
]

Use null for fields that cannot be determined. Respond ONLY with the JSON array."""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
        }

        response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()

        content = self._parse_json_response(response.json())
        results = json.loads(content)

        formatted = []
        for r in results:
            item = {"Item_ID": r.get("id", ""), "analysis_type": "ai_extraction"}
            for field in ELECTRICAL_SPEC_FIELDS:
                if r.get(field) is not None:
                    item[field] = r[field]
            item["extraction_confidence"] = CONFIDENCE_AI_EXTRACTION
            formatted.append(item)

        return formatted

    def _parse_json_response(self, response: dict) -> str:
        """Parse and clean JSON from API response."""
        content = response["choices"][0]["message"]["content"]
        content = content.strip()
        # Remove markdown code blocks if present
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
        return content.strip()

    # =========================================================================
    # RAW MATERIAL SPEC EXTRACTION
    # =========================================================================

    def extract_raw_material_specs(self, items: List[Dict]) -> List[Dict]:
        """
        Extract raw material specifications from items.

        Args:
            items: List of dicts with 'Item_ID', 'Raw_Description', and optionally 'Category_Level_2'.

        Returns:
            List of dicts with RawMat_* fields.
        """
        # Deduplicate by description to reduce processing
        unique_items, desc_to_item_ids = self._deduplicate_items(items)
        if len(unique_items) < len(items):
            print(f"  Deduplicated: {len(items):,} -> {len(unique_items):,} unique descriptions")

        results = []
        regex_needed_ai = []

        for item in unique_items:
            regex_result = self._extract_dimensions_regex(item)
            if regex_result and regex_result.get("RawMat_Thickness"):
                results.append(regex_result)
            else:
                regex_needed_ai.append(item)

        # Check cache before calling AI
        cached_results, cache_needed_ai = self._check_cache(regex_needed_ai, prefix="rm_")
        results.extend(cached_results)

        # Use AI for items that couldn't be parsed by regex or cache
        if cache_needed_ai and self.api_key:
            ai_results = self._extract_raw_material_ai(cache_needed_ai)
            self._store_cache(cache_needed_ai, ai_results, prefix="rm_")
            results.extend(ai_results)
        elif cache_needed_ai:
            for item in cache_needed_ai:
                results.append({"Item_ID": str(item.get("Item_ID", ""))})

        # Expand results back to all original items
        return self._expand_deduplicated_results(results, desc_to_item_ids, unique_items)

    def _parse_fraction(self, value: str) -> Optional[float]:
        """Convert fraction string to float."""
        value = value.strip()
        if "/" in value:
            try:
                return float(Fraction(value))
            except (ValueError, ZeroDivisionError):
                return None
        try:
            return float(value)
        except ValueError:
            return None

    def _extract_dimensions_regex(self, item: Dict) -> Dict:
        """
        Extract dimensions from raw material descriptions.
        Based on patterns from parse_copper_dimensions.py.
        """
        desc = self._normalize_description(str(item.get("Raw_Description", "")))
        item_id = str(item.get("Item_ID", ""))
        category_l2 = str(item.get("Category_Level_2", "")).lower()

        result = {
            "Item_ID": item_id,
            "RawMat_Thickness": None,
            "RawMat_Width": None,
            "RawMat_Length": None,
            "RawMat_UoM": None,
            "RawMat_Weight_Lbs": None,
            "RawMat_Finish": None,
            "analysis_type": "regex_extraction",
        }

        # Detect finish
        finish_patterns = {
            "Galvanized": r"\bGALV(?:ANIZED)?\b",
            "Galvanneal": r"\bGALVANNEAL\b",
            "Silver-Flashed": r"\bSILVER[\s-]*FLASH\b",
            "Bare": r"\bBARE\b",
            "Painted": r"\bPAINTED\b",
            "Powder Coated": r"\bPOWDER\s*COAT\b",
        }

        for finish, pattern in finish_patterns.items():
            if re.search(pattern, desc):
                result["RawMat_Finish"] = finish
                break

        # Pattern 1: T x W x L with feet notation for length
        pattern_3dim_feet = r"(\d+(?:/\d+)?(?:\.\d+)?)[\"']?\s*[xX×]\s*(\d+(?:/\d+)?(?:\.\d+)?)[\"']?\s*[xX×]\s*(\d+(?:/\d+)?(?:\.\d+)?)\s*['\']"
        match_feet = re.search(pattern_3dim_feet, desc)
        if match_feet:
            result["RawMat_Thickness"] = self._parse_fraction(match_feet.group(1))
            result["RawMat_Width"] = self._parse_fraction(match_feet.group(2))
            length_feet = self._parse_fraction(match_feet.group(3))
            if length_feet:
                result["RawMat_Length"] = length_feet * 12  # Convert feet to inches
            result["RawMat_UoM"] = "IN"

        # Pattern 2: T x W x L (all inches or no unit)
        if result["RawMat_Thickness"] is None:
            pattern_3dim = r"(\d+(?:/\d+)?(?:\.\d+)?)[\"']?\s*[xX×]\s*(\d+(?:/\d+)?(?:\.\d+)?)[\"']?\s*[xX×]\s*(\d+(?:/\d+)?(?:\.\d+)?)[\"']?"
            match = re.search(pattern_3dim, desc)
            if match:
                result["RawMat_Thickness"] = self._parse_fraction(match.group(1))
                result["RawMat_Width"] = self._parse_fraction(match.group(2))
                length_val = self._parse_fraction(match.group(3))
                if length_val:
                    # Heuristic: if <= 20, likely feet
                    if length_val <= 20:
                        result["RawMat_Length"] = length_val * 12
                    else:
                        result["RawMat_Length"] = length_val
                result["RawMat_UoM"] = "IN"

        # Pattern 3: T x W format (assume standard 12' length)
        if result["RawMat_Thickness"] is None:
            pattern_2dim = r"(\d+(?:/\d+)?(?:\.\d+)?)[\"']?\s*[xX×]\s*(\d+(?:/\d+)?(?:\.\d+)?)[\"']?"
            match = re.search(pattern_2dim, desc)
            if match:
                result["RawMat_Thickness"] = self._parse_fraction(match.group(1))
                result["RawMat_Width"] = self._parse_fraction(match.group(2))
                result["RawMat_Length"] = 144.0  # Standard 12'
                result["RawMat_UoM"] = "IN"

        # Pattern 4: Sheet dimensions (e.g., "8'X4' SHEET" or "48 X 96")
        sheet_pattern = r"(\d+)['\"]?\s*[xX×]\s*(\d+)['\"]?\s*(?:SHEET|SHT)"
        sheet_match = re.search(sheet_pattern, desc)
        if sheet_match and result["RawMat_Width"] is None:
            dim1 = float(sheet_match.group(1))
            dim2 = float(sheet_match.group(2))
            # Larger dimension is length
            result["RawMat_Width"] = min(dim1, dim2)
            result["RawMat_Length"] = max(dim1, dim2)
            # If values are small (like 4x8), they're feet
            if result["RawMat_Width"] <= 8 and result["RawMat_Length"] <= 16:
                result["RawMat_Width"] *= 12
                result["RawMat_Length"] *= 12
            result["RawMat_UoM"] = "IN"

        # Gauge for sheet metal (e.g., "12 GA", "GAUGE 12")
        gauge_match = re.search(r"(?:GA(?:UGE)?|GAGE)\s*(\d+)|(\d+)\s*(?:GA(?:UGE)?|GAGE)", desc)
        if gauge_match:
            gauge = gauge_match.group(1) or gauge_match.group(2)
            # Convert gauge to thickness using extended mapping
            if gauge in self.GAUGE_TO_THICKNESS:
                result["RawMat_Thickness"] = self.GAUGE_TO_THICKNESS[gauge]
                result["RawMat_UoM"] = "IN"

        # Calculate weight if we have all dimensions
        if all([result["RawMat_Thickness"], result["RawMat_Width"], result["RawMat_Length"]]):
            volume = result["RawMat_Thickness"] * result["RawMat_Width"] * result["RawMat_Length"]

            # Determine density based on category or description
            density = self.DENSITIES.get("steel")  # Default to steel
            if "copper" in category_l2 or "COPPER" in desc:
                density = self.DENSITIES["copper"]
            elif "aluminum" in category_l2 or "ALUMINUM" in desc or "ALUM" in desc:
                density = self.DENSITIES["aluminum"]

            result["RawMat_Weight_Lbs"] = round(volume * density, 2)

        # Set confidence
        if result["RawMat_Thickness"] and result["RawMat_Width"]:
            result["extraction_confidence"] = 0.85
        else:
            result["extraction_confidence"] = 0.0

        return result

    def _extract_raw_material_ai(self, items: List[Dict]) -> List[Dict]:
        """Extract raw material specs using AI."""
        if not self.api_key:
            return [{"Item_ID": str(item.get("Item_ID", ""))} for item in items]

        results = []
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1

            batch_results = self._call_ai_with_retry(
                self._call_raw_material_ai, batch, batch_num, total_batches
            )
            results.extend(batch_results)

            # Rate limit between batches
            if batch_num < total_batches:
                time.sleep(RATE_LIMIT_DELAY)
        return results

    def _call_raw_material_ai(self, items: List[Dict]) -> List[Dict]:
        """Call AI API for raw material spec extraction."""
        items_json = json.dumps(
            [
                {"id": str(item.get("Item_ID", "")), "desc": str(item.get("Raw_Description", ""))[:800]}
                for item in items
            ],
            indent=2,
        )

        prompt = f"""Extract raw material dimensions and properties from these item descriptions.

## Fields to Extract:
- RawMat_Finish: Surface finish (Galvanized, Bare, Silver-Flashed, Painted, etc.)
- RawMat_Thickness: Thickness in inches as a number
- RawMat_Width: Width in inches as a number
- RawMat_Length: Length in inches as a number (convert feet to inches: 12' = 144")
- RawMat_UoM: Unit of measure (always "IN" for inches)

## Items:
{items_json}

## Response Format (JSON array):
[
  {{"id": "item_id", "RawMat_Finish": "Galvanized", "RawMat_Thickness": 0.25, "RawMat_Width": 6, "RawMat_Length": 144}}
]

Use null for fields that cannot be determined. Respond ONLY with the JSON array."""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
        }

        response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()

        content = self._parse_json_response(response.json())
        results = json.loads(content)

        formatted = []
        for r in results:
            item = {"Item_ID": r.get("id", ""), "analysis_type": "ai_extraction", "RawMat_UoM": "IN"}
            for field in RAW_MATERIAL_SPEC_FIELDS:
                if r.get(field) is not None:
                    item[field] = r[field]
            item["extraction_confidence"] = CONFIDENCE_AI_EXTRACTION - 0.05  # Slightly lower for raw materials
            formatted.append(item)

        return formatted
