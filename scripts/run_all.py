"""
scripts/run_all.py
───────────────────
Run MCC scraper, DFC scraper, and consolidation in sequence.

Usage (from project root):
  python -m scripts.run_all

Or run individually:
  python -m scripts.mcc_scraper
  python -m scripts.dfc_scraper
  python -m scripts.consolidate
"""

from scripts.logger import get_logger
from scripts import mcc_scraper, dfc_scraper, consolidate

log = get_logger()


def main() -> None:
    log.info("=" * 55)
    log.info("  MCC + DFC Africa — Full Data Collection Run")
    log.info("=" * 55)

    log.info("\n>>> Starting MCC scraper...")
    mcc_scraper.main()

    log.info("\n>>> Starting DFC scraper...")
    dfc_scraper.main()

    log.info("\n>>> Building master workbook...")
    consolidate.main()

    log.info("\n" + "=" * 55)
    log.info("  All done. Check output/ folder.")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
