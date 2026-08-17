"""
Vendor Classifier - OpenRouter Gemini Flash Integration

Provides AI-powered vendor name standardization, type classification,
and location inference using batch processing with rate limiting.
"""

import os
import json
import time
import random
import requests
from typing import List, Dict, Optional
from pathlib import Path

# Load .env from project root
ENV_PATH = Path(__file__).parent.parent.parent / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)

try:
    from .vendor_taxonomy import (
        get_vendor_classification_prompt,
        clean_vendor_name_regex,
        VALID_SUPPLIER_TYPES,
        API_BASE_URL,
        DEFAULT_MODEL,
        VENDOR_BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_VENDOR_AI,
        CONFIDENCE_VENDOR_FALLBACK,
    )
except ImportError:
    from vendor_taxonomy import (
        get_vendor_classification_prompt,
        clean_vendor_name_regex,
        VALID_SUPPLIER_TYPES,
        API_BASE_URL,
        DEFAULT_MODEL,
        VENDOR_BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_VENDOR_AI,
        CONFIDENCE_VENDOR_FALLBACK,
    )


class VendorClassifier:
    """
    AI-powered vendor enrichment: standardization, classification, and location.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the classifier.

        Args:
            api_key: OpenRouter API key. If not provided, reads from OPENROUTER_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OPENROUTER_API_KEY not found. Set it in .env or pass api_key parameter."
            )

    def classify_batch(self, vendors: List[Dict]) -> List[Dict]:
        """
        Classify a batch of vendors.

        Args:
            vendors: List of dicts with 'Raw_Name' key.

        Returns:
            List of dicts with classification results:
            {Raw_Name, Standardized_Name, Supplier_Type, Location,
             vendor_confidence, vendor_analysis_type}
        """
        if not vendors:
            return []

        prompt = self._build_classification_prompt(vendors)

        for attempt in range(MAX_RETRIES):
            try:
                response = self._call_api(prompt)
                return self._parse_response(response, vendors)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limit
                    wait_time = (2**attempt) * 10 + random.uniform(0, 5)
                    print(f"  Rate limited, waiting {wait_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                elif e.response.status_code >= 500:  # Server error
                    wait_time = 2**attempt + 1
                    print(f"  Server error {e.response.status_code}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  HTTP error {e.response.status_code}: {e}")
                    raise
            except requests.exceptions.Timeout:
                wait_time = 2**attempt + 1
                print(f"  Timeout, retrying in {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(wait_time)
            except json.JSONDecodeError as e:
                print(f"  JSON parse error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    return self._get_fallback_results(vendors)
            except Exception as e:
                print(f"  Unexpected error: {type(e).__name__}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    return self._get_fallback_results(vendors)

        return self._get_fallback_results(vendors)

    def _build_classification_prompt(self, vendors: List[Dict]) -> str:
        """Build the vendor classification prompt."""
        vendors_json = json.dumps(
            [
                {
                    "id": str(v.get("Raw_Name", "")),
                    "name": str(v.get("Raw_Name", ""))[:200],
                    "regex_clean": clean_vendor_name_regex(str(v.get("Raw_Name", "")))[:200],
                }
                for v in vendors
            ],
            indent=2,
        )

        base_prompt = get_vendor_classification_prompt()

        return f"""{base_prompt}

## Vendors to Classify:
{vendors_json}"""

    def _call_api(self, prompt: str) -> dict:
        """Call OpenRouter API."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://forgent-pipeline.local",
            "X-Title": "Forgent Vendor Classifier",
        }

        payload = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,  # Low temperature for consistency
        }

        response = requests.post(
            API_BASE_URL, headers=headers, json=payload, timeout=60
        )
        response.raise_for_status()

        return response.json()

    def _parse_response(self, response: dict, original_vendors: List[Dict]) -> List[Dict]:
        """Parse LLM response and validate vendor classifications."""
        content = response["choices"][0]["message"]["content"]

        # Clean up response - remove markdown code blocks if present
        content = content.strip()
        if content.startswith("```"):
            first_line_end = content.find("\n")
            if first_line_end != -1:
                content = content[first_line_end + 1:]
            else:
                content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        try:
            results = json.loads(content)
        except json.JSONDecodeError:
            import re
            match = re.search(r'\[\s*\{[\s\S]*?\}\s*(?:,\s*\{[\s\S]*?\}\s*)*\]', content)
            if match:
                try:
                    results = json.loads(match.group())
                except json.JSONDecodeError:
                    raise ValueError(f"Could not parse JSON from response: {content[:200]}...")
            else:
                raise ValueError(f"No valid JSON array found in response: {content[:200]}...")

        if not isinstance(results, list):
            raise ValueError(f"Expected JSON array, got {type(results).__name__}")

        # Create lookup for original vendors
        original_lookup = {str(v.get("Raw_Name", "")): v for v in original_vendors}

        # Validate and format results
        validated = []
        for result in results:
            if not isinstance(result, dict):
                continue

            raw_name = str(result.get("id", ""))
            standardized_name = str(result.get("name", "")).strip()
            supplier_type = str(result.get("type", "")).strip()
            location = str(result.get("location", "")).strip()
            confidence = result.get("confidence", 0.0)

            # Ensure confidence is numeric
            try:
                confidence = float(confidence)
            except (TypeError, ValueError):
                confidence = 0.5

            # Validate Supplier_Type
            if supplier_type not in VALID_SUPPLIER_TYPES:
                # Try case-insensitive match
                matched = False
                for valid_type in VALID_SUPPLIER_TYPES:
                    if supplier_type.lower() == valid_type.lower():
                        supplier_type = valid_type
                        matched = True
                        break
                if not matched:
                    print(f"  Warning: Invalid Supplier_Type '{supplier_type}' for vendor '{raw_name}'")
                    supplier_type = "Service Provider"  # Safe fallback
                    confidence = min(confidence, 0.3)

            # Validate Standardized_Name is not empty
            if not standardized_name:
                standardized_name = clean_vendor_name_regex(raw_name)

            # Validate Location
            if not location or location.lower() in ("none", "null", "n/a"):
                location = "Unknown"

            validated.append(
                {
                    "Raw_Name": raw_name,
                    "Standardized_Name": standardized_name,
                    "Supplier_Type": supplier_type,
                    "Location": location,
                    "vendor_confidence": confidence,
                    "vendor_analysis_type": "ai_vendor_classification",
                }
            )

        return validated

    def _get_fallback_results(self, vendors: List[Dict]) -> List[Dict]:
        """Return fallback results for failed batch."""
        return [
            {
                "Raw_Name": str(v.get("Raw_Name", "")),
                "Standardized_Name": "",
                "Supplier_Type": "",
                "Location": "Unknown",
                "vendor_confidence": CONFIDENCE_VENDOR_FALLBACK,
                "vendor_analysis_type": "ai_vendor_classification_failed",
            }
            for v in vendors
        ]

    def classify_all(
        self, vendors: List[Dict], progress_callback=None
    ) -> List[Dict]:
        """
        Classify all vendors in batches.

        Args:
            vendors: List of vendors to classify.
            progress_callback: Optional callback(processed, total) for progress updates.

        Returns:
            List of all classification results.
        """
        all_results = []
        total = len(vendors)
        total_batches = (total + VENDOR_BATCH_SIZE - 1) // VENDOR_BATCH_SIZE

        for i in range(0, total, VENDOR_BATCH_SIZE):
            batch = vendors[i : i + VENDOR_BATCH_SIZE]
            batch_num = i // VENDOR_BATCH_SIZE + 1

            print(f"  Processing batch {batch_num}/{total_batches}...")

            try:
                results = self.classify_batch(batch)
                all_results.extend(results)
            except Exception as e:
                print(f"  Batch {batch_num} failed: {e}")
                all_results.extend(self._get_fallback_results(batch))

            if progress_callback:
                progress_callback(min(i + VENDOR_BATCH_SIZE, total), total)

            # Rate limit between batches
            if batch_num < total_batches:
                time.sleep(RATE_LIMIT_DELAY)

        return all_results
