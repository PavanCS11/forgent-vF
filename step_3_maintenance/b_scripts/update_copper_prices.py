"""
Copper Price Updater
====================
Fetches monthly copper prices from CNWire and updates copper_price_index.csv.

Source: https://www.cnwire.com/CopperPrice.php (COMEX Monthly Averages)

Usage:
    python update_copper_prices.py [OPTIONS]

Options:
    --dry-run       Preview changes without saving
    --check-status  Run diagnostics and health check without updating
    --validate      Compare all website prices with CSV and report discrepancies

Examples:
    python update_copper_prices.py                 # Normal update
    python update_copper_prices.py --dry-run       # Preview new data
    python update_copper_prices.py --check-status  # Check current status
    python update_copper_prices.py --validate      # Validate price accuracy
"""

import logging
import re
import sys
from collections import deque
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Path configuration
SCRIPT_DIR = Path(__file__).parent
MAPPINGS_DIR = SCRIPT_DIR.parent / "a.mappings"
COPPER_PRICE_PATH = MAPPINGS_DIR / "copper_price_index.csv"
LOG_FILE_PATH = SCRIPT_DIR / "copper_price_updates.log"
SOURCE_URL = "https://www.cnwire.com/CopperPrice.php"

# Logging configuration
MAX_LOG_LINES = 500

# Month name to number mapping
MONTH_MAP = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12'
}


class DualOutputHandler(logging.Handler):
    """Custom logging handler that outputs to both stdout and rolling log file."""

    def __init__(self, log_file_path: Path, max_lines: int = 500):
        super().__init__()
        self.log_file_path = log_file_path
        self.max_lines = max_lines

    def emit(self, record):
        msg = self.format(record)

        # Always print to stdout
        print(msg)

        # Append to log file with rolling behavior
        try:
            # Read existing lines if file exists
            if self.log_file_path.exists():
                with open(self.log_file_path, 'r', encoding='utf-8') as f:
                    lines = deque(f.readlines(), maxlen=self.max_lines - 1)
            else:
                lines = deque(maxlen=self.max_lines - 1)

            # Add new line
            lines.append(msg + '\n')

            # Write back
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
        except Exception as e:
            print(f"Warning: Could not write to log file: {e}")


def setup_logging() -> logging.Logger:
    """Configure dual output: stdout and rolling log file."""
    logger = logging.getLogger('copper_price_updater')
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    # Add dual output handler
    handler = DualOutputHandler(LOG_FILE_PATH, MAX_LOG_LINES)
    formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def log_fetch_details(web_prices: list, existing_df: pd.DataFrame, logger: logging.Logger):
    """Log detailed comparison of website vs CSV months."""
    if not web_prices:
        logger.info("No prices found on website")
        return

    web_months = {p['year_month'] for p in web_prices}
    csv_months = set(existing_df['year_month'].astype(str)) if len(existing_df) > 0 else set()

    # Calculate ranges
    web_min = min(web_months) if web_months else None
    web_max = max(web_months) if web_months else None
    csv_min = min(csv_months) if csv_months else None
    csv_max = max(csv_months) if csv_months else None

    logger.info("")
    logger.info(f"Website months: {web_min} to {web_max}" if web_min else "Website months: None")
    logger.info(f"CSV months:     {csv_min} to {csv_max}" if csv_min else "CSV months:     None")
    logger.info("")

    # Compare
    matching = web_months & csv_months
    new_on_web = web_months - csv_months
    only_in_csv = csv_months - web_months

    logger.info("Comparison:")
    logger.info(f"  [OK] {len(matching)} months match between website and CSV")
    if only_in_csv:
        logger.info(f"  [!] {len(only_in_csv)} month(s) in CSV but not on website - website may have dropped old data")
        if len(only_in_csv) <= 5:  # Only show details if not too many
            for month in sorted(only_in_csv):
                logger.info(f"      - {month}")
    if new_on_web:
        logger.info(f"  [NEW] {len(new_on_web)} new month(s) to add")
    else:
        logger.info(f"  [NEW] 0 new months to add")


def check_current_month_status(latest_month: str, logger: logging.Logger) -> str:
    """Check if latest month is current and explain delays if needed."""
    if not latest_month:
        return None

    try:
        # Parse latest month
        latest_year = int(latest_month.split('-')[0])
        latest_month_num = int(latest_month.split('-')[1])

        # Get current month
        now = datetime.now()
        current_year = now.year
        current_month_num = now.month

        # Calculate month difference
        month_diff = (current_year - latest_year) * 12 + (current_month_num - latest_month_num)

        if month_diff == 1:
            # Latest is last month - this is expected
            current_month_name = now.strftime("%B %Y")
            msg = (f"\n[INFO] Note: {current_month_name} data not yet available. Monthly averages are\n"
                   f"       typically published in the first week after the month ends.")
            logger.info(msg)
            return msg
        elif month_diff > 1:
            # Data is more than 1 month behind
            msg = f"\n[WARNING] Latest data is {month_diff} months behind current month. Check website status."
            logger.info(msg)
            return msg

    except Exception:
        pass

    return None


def detect_data_loss(web_count: int, csv_count: int, logger: logging.Logger) -> str:
    """Detect if website has fewer months than CSV."""
    if web_count < csv_count:
        msg = (f"\n[WARNING] Website has {web_count} months but CSV has {csv_count}. This is expected if the\n"
               f"          website uses a rolling window, but verify data integrity.")
        logger.info(msg)
        return msg
    return None


def fetch_webpage(url: str, logger: logging.Logger) -> str:
    """Fetch webpage content from URL."""
    logger.info(f"Fetching data from: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"ERROR: Failed to fetch webpage: {e}")
        return None


def parse_month_string(month_str: str) -> tuple:
    """
    Convert month string to year_month format and price_date.

    Args:
        month_str: e.g., "December 2025"

    Returns:
        tuple: (year_month, price_date) e.g., ("2025-12", "2025-12-01")
    """
    month_str = month_str.strip().lower()

    # Extract month name and year
    match = re.match(r'(\w+)\s+(\d{4})', month_str)
    if not match:
        return None, None

    month_name = match.group(1)
    year = match.group(2)

    if month_name not in MONTH_MAP:
        return None, None

    month_num = MONTH_MAP[month_name]
    year_month = f"{year}-{month_num}"
    price_date = f"{year}-{month_num}-01"

    return year_month, price_date


def parse_price_table(html: str, logger: logging.Logger) -> list:
    """
    Parse HTML to extract copper price data from the table.

    Returns:
        list of dicts: [{year_month, copper_price_per_lb, price_date}, ...]
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Find all tables and look for the copper price table
    tables = soup.find_all('table')

    prices = []

    for table in tables:
        rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                month_text = cells[0].get_text(strip=True)
                price_text = cells[1].get_text(strip=True)

                # Skip header rows
                if 'month' in month_text.lower() or 'price' in price_text.lower():
                    continue

                # Try to parse the month
                year_month, price_date = parse_month_string(month_text)
                if not year_month:
                    continue

                # Parse price (remove $ and any whitespace)
                price_clean = price_text.replace('$', '').replace(',', '').strip()
                try:
                    price = float(price_clean)
                except ValueError:
                    continue

                prices.append({
                    'year_month': year_month,
                    'copper_price_per_lb': price,
                    'price_date': price_date
                })

    logger.info(f"Found {len(prices)} months of price data on website")
    return prices


def load_existing_prices(logger: logging.Logger) -> pd.DataFrame:
    """Load existing copper price index CSV."""
    if COPPER_PRICE_PATH.exists():
        logger.info(f"Loading existing prices from: {COPPER_PRICE_PATH}")
        df = pd.read_csv(COPPER_PRICE_PATH)
        logger.info(f"Existing CSV has {len(df)} months")
        return df
    else:
        logger.info("No existing price file found. Will create new.")
        return pd.DataFrame(columns=['year_month', 'copper_price_per_lb', 'price_date'])


def find_new_months(web_prices: list, existing_df: pd.DataFrame) -> list:
    """Find months in web data that are missing from CSV."""
    existing_months = set(existing_df['year_month'].astype(str))

    new_prices = []
    for price_data in web_prices:
        if price_data['year_month'] not in existing_months:
            new_prices.append(price_data)

    return new_prices


def validate_prices(web_prices: list, existing_df: pd.DataFrame, logger: logging.Logger):
    """Compare website prices with CSV and report discrepancies."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("Price Validation Report")
    logger.info("=" * 60)

    # Create dict for web prices
    web_dict = {p['year_month']: p['copper_price_per_lb'] for p in web_prices}

    # Compare
    discrepancies = []
    for _, row in existing_df.iterrows():
        month = str(row['year_month'])
        csv_price = float(row['copper_price_per_lb'])

        if month in web_dict:
            web_price = web_dict[month]
            if abs(web_price - csv_price) > 0.0001:  # Allow for floating point precision
                discrepancies.append({
                    'month': month,
                    'csv_price': csv_price,
                    'web_price': web_price,
                    'diff': web_price - csv_price
                })

    if discrepancies:
        logger.info(f"\nFound {len(discrepancies)} price discrepancy(ies):")
        for d in discrepancies:
            logger.info(f"  {d['month']}: CSV=${d['csv_price']:.4f}, Web=${d['web_price']:.4f}, Diff=${d['diff']:.4f}")
    else:
        logger.info("\n[OK] All matching months have identical prices")


def update_copper_prices(dry_run: bool = False, check_status: bool = False, validate: bool = False) -> bool:
    """
    Main function to update copper prices from web source.

    Args:
        dry_run: If True, don't save changes
        check_status: If True, only run diagnostics without updating
        validate: If True, compare all prices and report discrepancies

    Returns:
        bool: True if successful (even if no updates needed)
    """
    # Setup logging
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("Copper Price Updater")
    logger.info("=" * 60)

    # Fetch webpage
    html = fetch_webpage(SOURCE_URL, logger)
    if html is None:
        return False

    # Parse prices from HTML
    web_prices = parse_price_table(html, logger)
    if not web_prices:
        logger.error("ERROR: Could not parse any prices from webpage.")
        return False

    # Load existing CSV
    existing_df = load_existing_prices(logger)

    # Log detailed fetch comparison
    log_fetch_details(web_prices, existing_df, logger)

    # Run validation if requested
    if validate:
        validate_prices(web_prices, existing_df, logger)
        logger.info("")
        logger.info(f"Log saved to: {LOG_FILE_PATH}")
        return True

    # Check status mode - just diagnostics, no updates
    if check_status:
        # Get latest month from web data
        latest_web_month = max(p['year_month'] for p in web_prices) if web_prices else None
        latest_price = next((p['copper_price_per_lb'] for p in web_prices if p['year_month'] == latest_web_month), None)

        if latest_web_month:
            logger.info(f"\nLatest available: {latest_web_month} (${latest_price:.4f}/lb)")

        # Check current month status
        check_current_month_status(latest_web_month, logger)

        # Detect data loss
        detect_data_loss(len(web_prices), len(existing_df), logger)

        logger.info("")
        logger.info(f"Log saved to: {LOG_FILE_PATH}")
        return True

    # Find new months
    new_prices = find_new_months(web_prices, existing_df)

    if not new_prices:
        logger.info("")
        logger.info("No new months to add. CSV is up to date.")

        # Get latest month from web data
        latest_web_month = max(p['year_month'] for p in web_prices) if web_prices else None
        latest_price = next((p['copper_price_per_lb'] for p in web_prices if p['year_month'] == latest_web_month), None)

        if latest_web_month:
            logger.info(f"Latest available: {latest_web_month} (${latest_price:.4f}/lb)")

        # Check current month status
        check_current_month_status(latest_web_month, logger)

        # Detect data loss
        detect_data_loss(len(web_prices), len(existing_df), logger)

        logger.info("")
        logger.info(f"Log saved to: {LOG_FILE_PATH}")
        logger.info("=" * 60)
        return True

    # Report new months found
    logger.info(f"\nFound {len(new_prices)} NEW month(s):")
    for p in new_prices:
        logger.info(f"  {p['year_month']}: ${p['copper_price_per_lb']:.4f}/lb")

    if dry_run:
        logger.info("\n[DRY RUN] No changes saved.")
        logger.info(f"Log saved to: {LOG_FILE_PATH}")
        return True

    # Create DataFrame for new prices
    new_df = pd.DataFrame(new_prices)

    # Combine and sort
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.sort_values('year_month').reset_index(drop=True)

    # Save
    logger.info(f"\nSaving updated prices to: {COPPER_PRICE_PATH}")
    combined_df.to_csv(COPPER_PRICE_PATH, index=False)
    logger.info(f"Done! CSV now has {len(combined_df)} months.")
    logger.info("")
    logger.info(f"Log saved to: {LOG_FILE_PATH}")
    logger.info("=" * 60)

    return True


def main():
    """Main entry point."""
    dry_run = '--dry-run' in sys.argv
    check_status = '--check-status' in sys.argv
    validate = '--validate' in sys.argv

    success = update_copper_prices(dry_run=dry_run, check_status=check_status, validate=validate)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
