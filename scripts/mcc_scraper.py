"""
scripts/mcc_scraper.py
───────────────────────
MCC Africa contract & compact data scraper.

Sources:
  1. USASpending.gov API  — MCC award records + top recipients
  2. SAM.gov API          — MCC procurement opportunities (needs free API key)
  3. MCC.gov              — country compact pages (HTML scrape)

Run from project root:
  python -m scripts.mcc_scraper
"""

import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scripts.config import (
    AFRICA_KEYWORDS,
    AFRICA_MCC_COUNTRIES,
    API_DELAY,
    AWARD_TYPE_CODES,
    CRAWL_DELAY,
    FILES,
    MAX_PAGES,
    MCC_AGENCY_NAME,
    REQUEST_TIMEOUT,
    SAM_GOV_API_KEY,
    SAM_GOV_URL,
    SAM_SEARCH_KEYWORDS,
    SCRAPER_HEADERS,
    USASPENDING_URL,
)
from scripts.logger import get_logger

log = get_logger()


# ─────────────────────────────────────────────────────────────
# 1. USASpending — all MCC awards + Africa filter
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_awards() -> pd.DataFrame:
    """
    Pull all contract awards where awarding agency = MCC.
    Saves:
      raw_data/usaspending_mcc_all.xlsx      — full unfiltered pull
      output/usaspending_mcc_africa.xlsx     — Africa-filtered subset
    """
    log.info("── USASpending.gov: MCC awards ──────────────────────")
    url = f"{USASPENDING_URL}/search/spending_by_award/"

    payload = {
        "filters": {
            "award_type_codes": AWARD_TYPE_CODES,
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": MCC_AGENCY_NAME,
                }
            ],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Start Date",
            "End Date",
            "Award Type",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Description",
            "Place of Performance State Code",
            "Place of Performance Country Code",
            "Place of Performance Country Name",
        ],
        "sort": "Award Amount",
        "order": "desc",
        "limit": 100,
        "page": 1,
    }

    all_records = []
    for page in tqdm(range(1, MAX_PAGES + 1), desc="  Fetching pages"):
        payload["page"] = page
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"  Page {page} failed: {e}")
            break

        results = resp.json().get("results", [])
        if not results:
            log.info(f"  No more results after page {page - 1}")
            break

        all_records.extend(results)
        time.sleep(API_DELAY)

    if not all_records:
        log.warning("  USASpending returned no records.")
        return pd.DataFrame()

    df_all = pd.DataFrame(all_records)

    # Save full raw pull
    df_all.to_excel(FILES["usaspending_all"], index=False)
    log.info(f"  Raw awards saved → {FILES['usaspending_all']} ({len(df_all)} rows)")

    # Filter for Africa
    mask = df_all.apply(
        lambda row: any(
            kw.lower() in str(row).lower() for kw in AFRICA_KEYWORDS
        ),
        axis=1,
    )
    df_africa = df_all[mask].copy()
    df_africa.to_excel(FILES["usaspending_africa"], index=False)
    log.info(f"  Africa awards saved → {FILES['usaspending_africa']} ({len(df_africa)} rows)")

    return df_africa


# ─────────────────────────────────────────────────────────────
# 2. USASpending — top recipients (firms) of MCC money
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_recipients() -> pd.DataFrame:
    """
    Pull top firms by total MCC award value.
    Saves: output/usaspending_mcc_top_recipients.xlsx
    """
    log.info("── USASpending.gov: top recipients ──────────────────")
    url = f"{USASPENDING_URL}/search/spending_by_category/recipient/"

    payload = {
        "filters": {
            "award_type_codes": AWARD_TYPE_CODES,
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": MCC_AGENCY_NAME,
                }
            ],
        },
        "limit": 100,
        "page": 1,
    }

    try:
        resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"  Recipients request failed: {e}")
        return pd.DataFrame()

    results = resp.json().get("results", [])
    df = pd.DataFrame(results)

    if df.empty:
        log.warning("  No recipient data returned.")
        return df

    df.to_excel(FILES["usaspending_recipients"], index=False)
    log.info(f"  Recipients saved → {FILES['usaspending_recipients']} ({len(df)} rows)")

    # Preview top 10
    preview_cols = [c for c in ["name", "amount", "recipient_id"] if c in df.columns]
    log.info("\n  Top 10 recipients:\n" + df[preview_cols].head(10).to_string(index=False))

    return df


# ─────────────────────────────────────────────────────────────
# 3. SAM.gov — contract opportunities
# ─────────────────────────────────────────────────────────────

def scrape_samgov() -> pd.DataFrame:
    """
    Search SAM.gov for MCC Africa procurement opportunities.
    Requires a free API key — set SAM_GOV_API_KEY in config.py.
    Get your key at: https://sam.gov/profile/details
    Saves: output/samgov_mcc_africa.xlsx
    """
    log.info("── SAM.gov: procurement opportunities ───────────────")

    if not SAM_GOV_API_KEY or SAM_GOV_API_KEY == "YOUR_SAM_GOV_API_KEY":
        log.warning("  SAM.gov skipped — no API key set in config.py")
        log.warning("  Get a free key at: https://sam.gov/profile/details")
        return pd.DataFrame()

    all_records = []
    for keyword in tqdm(SAM_SEARCH_KEYWORDS, desc="  SAM keywords"):
        params = {
            "api_key": SAM_GOV_API_KEY,
            "q": keyword,
            "organizationName": MCC_AGENCY_NAME,
            "limit": 100,
            "offset": 0,
        }
        try:
            resp = requests.get(SAM_GOV_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            opps = resp.json().get("opportunitiesData", [])
            all_records.extend(opps)
            log.debug(f"  '{keyword}': {len(opps)} results")
        except requests.RequestException as e:
            log.error(f"  SAM.gov error for '{keyword}': {e}")
        time.sleep(API_DELAY)

    if not all_records:
        log.warning("  SAM.gov returned no records.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    id_col = "noticeId" if "noticeId" in df.columns else None
    df = df.drop_duplicates(subset=[id_col]) if id_col else df.drop_duplicates()

    df.to_excel(FILES["samgov"], index=False)
    log.info(f"  SAM.gov results saved → {FILES['samgov']} ({len(df)} rows)")

    return df


# ─────────────────────────────────────────────────────────────
# 4. MCC.gov — country compact pages
# ─────────────────────────────────────────────────────────────

def scrape_mcc_country_pages() -> pd.DataFrame:
    """
    Scrape each MCC Africa country page for compact descriptions,
    dollar amounts, and links to PDF documents.
    Saves: output/mcc_country_pages.xlsx
    """
    log.info("── MCC.gov: country compact pages ───────────────────")
    records = []

    for country, url in tqdm(AFRICA_MCC_COUNTRIES, desc="  Scraping countries"):
        try:
            resp = requests.get(url, headers=SCRAPER_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                log.warning(f"  {country}: HTTP {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Page title
            h1 = soup.find("h1")
            title_text = h1.get_text(strip=True) if h1 else country.title()

            # Paragraphs
            paragraphs = [
                p.get_text(strip=True)
                for p in soup.find_all("p")
                if len(p.get_text(strip=True)) > 40
            ]
            text_blob = " ".join(paragraphs)

            # Dollar amounts
            amounts = re.findall(
                r"\$[\d,\.]+\s*(?:million|billion)?", text_blob, re.IGNORECASE
            )

            # PDF document links
            pdf_links = [
                a["href"]
                for a in soup.find_all("a", href=True)
                if a["href"].lower().endswith(".pdf")
            ]

            # Compact status keywords
            status = "Unknown"
            for kw in ["completed", "active", "closed", "terminated", "in development"]:
                if kw in text_blob.lower():
                    status = kw.title()
                    break

            records.append(
                {
                    "country":            country.replace("-", " ").title(),
                    "url":                url,
                    "page_title":         title_text,
                    "compact_status":     status,
                    "amounts_mentioned":  "; ".join(amounts[:10]),
                    "pdf_documents":      "; ".join(pdf_links[:10]),
                    "description":        text_blob[:800],
                }
            )
            log.debug(f"  {country}: OK — {len(pdf_links)} PDFs, amounts: {amounts[:3]}")
            time.sleep(CRAWL_DELAY)

        except Exception as e:
            log.error(f"  {country}: Exception — {e}")

    df = pd.DataFrame(records)
    df.to_excel(FILES["mcc_countries"], index=False)
    log.info(f"  Country pages saved → {FILES['mcc_countries']} ({len(df)} rows)")

    return df


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 55)
    log.info("  MCC Africa Data Collection")
    log.info("=" * 55)

    df_awards     = scrape_usaspending_awards()
    df_recipients = scrape_usaspending_recipients()
    df_sam        = scrape_samgov()
    df_countries  = scrape_mcc_country_pages()

    log.info("\n" + "=" * 55)
    log.info("  SUMMARY")
    log.info("=" * 55)
    log.info(f"  USASpending Africa awards : {len(df_awards)} rows")
    log.info(f"  Top MCC recipients        : {len(df_recipients)} firms")
    log.info(f"  SAM.gov opportunities     : {len(df_sam)} rows")
    log.info(f"  MCC country pages         : {len(df_countries)} countries")
    log.info("\n  Output files:")
    for key, path in FILES.items():
        if path.exists():
            log.info(f"    {path.relative_to(path.parent.parent)}")


if __name__ == "__main__":
    main()
