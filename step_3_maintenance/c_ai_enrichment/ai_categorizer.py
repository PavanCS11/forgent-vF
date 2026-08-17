"""
AI Categorizer - OpenRouter Gemini Flash 3.0 Integration

Provides AI-powered categorization for uncategorized procurement items.
Uses batch processing with rate limiting and retry logic.
"""

import os
import json
import time
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
    from .taxonomy import (
        get_taxonomy_prompt,
        CATEGORY_HIERARCHY,
        API_BASE_URL,
        DEFAULT_MODEL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_AI_CATEGORIZATION,
    )
except ImportError:
    from taxonomy import (
        get_taxonomy_prompt,
        CATEGORY_HIERARCHY,
        API_BASE_URL,
        DEFAULT_MODEL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_AI_CATEGORIZATION,
    )


class AICategorizer:
    """
    AI-powered item categorization using OpenRouter API.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the categorizer.

        Args:
            api_key: OpenRouter API key. If not provided, reads from OPENROUTER_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OPENROUTER_API_KEY not found. Set it in .env or pass api_key parameter."
            )

    def categorize_batch(self, items: List[Dict]) -> List[Dict]:
        """
        Categorize a batch of items.

        Args:
            items: List of dicts with 'Item_ID' and 'Raw_Description' keys.

        Returns:
            List of dicts with categorization results:
            {Item_ID, Category_Level_1, Category_Level_2, Category_Level_3,
             extraction_confidence, analysis_type}
        """
        if not items:
            return []

        prompt = self._build_categorization_prompt(items)

        for attempt in range(MAX_RETRIES):
            try:
                response = self._call_api(prompt)
                return self._parse_response(response, items)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limit
                    # Add jitter to backoff to avoid thundering herd
                    import random
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
                    # Return uncategorized for failed batch
                    return self._get_fallback_results(items)
            except Exception as e:
                print(f"  Unexpected error: {type(e).__name__}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    return self._get_fallback_results(items)

        return self._get_fallback_results(items)

    def _build_categorization_prompt(self, items: List[Dict]) -> str:
        """Build the categorization prompt with taxonomy context."""
        items_json = json.dumps(
            [
                {
                    "id": str(item.get("Item_ID", "")),
                    "desc": str(item.get("Raw_Description", ""))[:800],
                }
                for item in items
            ],
            indent=2,
        )

        taxonomy = get_taxonomy_prompt()

        return f"""You are an industrial procurement categorization expert specializing in electrical equipment, raw materials, and MRO supplies.
Categorize each item into the correct 3-level hierarchy based on the item description.

## Valid Categories (Category_Level_1 > Category_Level_2 > Category_Level_3):
{taxonomy}

## Decision Rules for Overlapping Categories:

1. **Current Transformers:**
   - If description mentions "metering", "panel", "sensing" → Electrical Components > Metering & Sensing
   - If standalone power transformer → Transformers > Specialty Transformers > Current Transformer

2. **Control Devices (Relays, Contactors, Motor Starters):**
   - Default → Electrical Components > Switching & Control
   - Only use Circuit Protection if explicitly part of breaker/protection system

3. **Copper/Metal Products:**
   - Finished products (bus bars with specs, finished wire/cable) → Appropriate electrical category
   - Raw stock (sheet, plate, bar, tubing in stock dimensions) → Raw Materials > Copper Products

4. **Tools & Equipment:**
   - Hand tools, power tools, safety equipment → MRO & Indirect > Tools & Equipment
   - Large manufacturing equipment → MRO & Indirect > Industrial Equipment

## When to Use "Miscellaneous/Other":
Use "Miscellaneous/Other > Uncategorized > Insufficient Information" when:
- Description is too vague (e.g., "MISC PARTS", "SUPPLIES", "VARIOUS")
- Single word or extremely short descriptions without context
- Confidence level would be below 0.3
- Multiple equally valid categories and no way to choose

Use "Miscellaneous/Other > Inactive Items > Master Data Only" when:
- Item has no meaningful description (placeholder text, codes only)

Otherwise, choose the best-fit category from the taxonomy above.

## Confidence Scoring Guidelines:
- **0.9-1.0:** Clear, unambiguous categorization with confirming keywords
- **0.7-0.89:** Likely correct but some ambiguity
- **0.5-0.69:** Educated guess based on partial information
- **0.3-0.49:** Uncertain, multiple valid options
- **0.0-0.29:** Insufficient information (should be "Uncategorized")

## Items to Categorize:
{items_json}

## Response Format:
Respond with a JSON array. For each item, provide:
- "id": The item ID (string)
- "cat1": Category_Level_1 (must match exactly from the lists above)
- "cat2": Category_Level_2 (must match exactly from the lists above)
- "cat3": Category_Level_3 (must match exactly from the lists above)
- "confidence": A number 0.0-1.0 based on the guidelines above

Example response:
[
  {{"id": "12345", "cat1": "Circuit Protection", "cat2": "Low Voltage Breakers", "cat3": "MCCB", "confidence": 0.95}},
  {{"id": "67890", "cat1": "Electrical Components", "cat2": "Switching & Control", "cat3": "Contactors", "confidence": 0.85}},
  {{"id": "11111", "cat1": "Miscellaneous/Other", "cat2": "Uncategorized", "cat3": "Insufficient Information", "confidence": 0.15}}
]

Respond ONLY with the JSON array, no other text."""

    def _call_api(self, prompt: str) -> dict:
        """Call OpenRouter API."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://forgent-pipeline.local",
            "X-Title": "Forgent Item Categorizer",
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

    def _parse_response(self, response: dict, original_items: List[Dict]) -> List[Dict]:
        """Parse LLM response and validate categories."""
        content = response["choices"][0]["message"]["content"]

        # Clean up response - remove markdown code blocks if present
        content = content.strip()
        if content.startswith("```"):
            # Remove opening code fence (could be ```json or just ```)
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
            # Try to find JSON array in response with stricter pattern
            import re
            # Look for well-formed JSON array (starts with [, contains objects)
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

        # Create lookup for original items
        original_lookup = {str(item.get("Item_ID", "")): item for item in original_items}

        # Validate and format results
        validated = []
        for result in results:
            if not isinstance(result, dict):
                continue  # Skip malformed entries

            item_id = str(result.get("id", ""))
            cat1 = result.get("cat1", "Uncategorized")
            cat2 = result.get("cat2", "")
            cat3 = result.get("cat3", "")
            confidence = result.get("confidence", 0.0)

            # Ensure confidence is numeric
            try:
                confidence = float(confidence)
            except (TypeError, ValueError):
                confidence = 0.5

            # Validate category hierarchy
            if cat1 in CATEGORY_HIERARCHY:
                l2_dict = CATEGORY_HIERARCHY[cat1]
                if not l2_dict:
                    # Empty L2 dict is a taxonomy issue - log and use fallback
                    print(f"  Warning: Empty L2 dict for category '{cat1}'")
                    cat2 = ""
                    cat3 = ""
                    confidence = min(confidence, 0.3)
                elif cat2 not in l2_dict:
                    # Invalid L2 - pick first available
                    cat2 = list(l2_dict.keys())[0]
                    cat3 = ""
                    confidence = min(confidence, 0.5)
                elif cat3 not in l2_dict.get(cat2, []):
                    # Invalid L3 - pick first available or empty
                    l3_list = l2_dict.get(cat2, [])
                    cat3 = l3_list[0] if l3_list else ""
                    confidence = min(confidence, 0.7)
            elif cat1 != "Uncategorized":
                # Invalid L1 category
                print(f"  Warning: Invalid L1 category '{cat1}' for item {item_id}")
                cat1 = "Uncategorized"
                cat2 = "Pending Review"
                cat3 = "AI Returned Invalid Category"
                confidence = 0.0

            validated.append(
                {
                    "Item_ID": item_id,
                    "Category_Level_1": cat1,
                    "Category_Level_2": cat2,
                    "Category_Level_3": cat3,
                    "extraction_confidence": confidence,
                    "analysis_type": "ai_categorization",
                }
            )

        return validated

    def _get_fallback_results(self, items: List[Dict]) -> List[Dict]:
        """Return uncategorized results for failed batch."""
        return [
            {
                "Item_ID": str(item.get("Item_ID", "")),
                "Category_Level_1": "Uncategorized",
                "Category_Level_2": "Pending Review",
                "Category_Level_3": "AI Processing Failed",
                "extraction_confidence": 0.0,
                "analysis_type": "ai_categorization_failed",
            }
            for item in items
        ]

    def categorize_all(
        self, items: List[Dict], progress_callback=None
    ) -> List[Dict]:
        """
        Categorize all items in batches.

        Args:
            items: List of items to categorize.
            progress_callback: Optional callback(processed, total) for progress updates.

        Returns:
            List of all categorization results.
        """
        all_results = []
        total = len(items)
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        for i in range(0, total, BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1

            print(f"  Processing batch {batch_num}/{total_batches}...")

            try:
                results = self.categorize_batch(batch)
                all_results.extend(results)
            except Exception as e:
                print(f"  Batch {batch_num} failed: {e}")
                all_results.extend(self._get_fallback_results(batch))

            if progress_callback:
                progress_callback(min(i + BATCH_SIZE, total), total)

            # Rate limit between batches, not after each API call
            if batch_num < total_batches:
                time.sleep(RATE_LIMIT_DELAY)

        return all_results
