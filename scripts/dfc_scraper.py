"""
scripts/dfc_scraper.py
───────────────────────
DFC Africa project & investment data scraper.

Sources:
  1. DFC transaction data      — full project database
  2. DFC investment stories    — named US firm success stories
  3. DFC press releases        — deal announcements with company names
  4. Federal Register API      — legally required project notices
  5. USASpending.gov API       — DFC operational contracts
  6. DFC sector pages          — Africa projects by sector

Run from project root:
  python -m scripts.dfc_scraper
"""

import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scripts.config import (
    AFRICA_DFC_COUNTRIES,
    AFRICA_CODE_TO_NAME,
    AFRICA_ISO_CODES,
    DFC_AFRICA_COUNTRY_NAMES,
    DFC_AFRICA_KEYWORDS,
    FEDERAL_REGISTER_PROJECT_KEYWORDS,
    API_DELAY,
    AWARD_TYPE_CODES,
    CRAWL_DELAY,
    MAX_PAGES,
    MAX_PRESS_RELEASES,
    DFC_ACTIVE_PROJECTS,
    DFC_AGENCY_NAME,
    DFC_BASE_URL,
    DFC_NEWSROOM,
    DFC_PRESS_RELEASES,
    DFC_REPORTS,
    DFC_SECTORS,
    DFC_TRANSACTION_DATA,
    DFC_TRANSPARENCY,
    DFC_WHERE_WE_WORK,
    DFC_IMPACT_PAGE,
    FEDERAL_REGISTER_API,
    FEDERAL_REGISTER_TERMS,
    FILES,
    REQUEST_TIMEOUT,
    SCRAPER_HEADERS,
    USASPENDING_URL,
)
from scripts.logger import get_logger

log = get_logger()

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _normalize_country_name(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_country_mentions(text: str) -> list[str]:
    """Extract Africa country mentions, longest-first to avoid 'Guinea'
    matching inside 'Equatorial Guinea' or 'Guinea-Bissau'."""
    t = str(text or "")
    tl = t.lower()
    found = []
    # Sort longest-first so "Equatorial Guinea" is matched before "Guinea"
    sorted_countries = sorted(AFRICA_DFC_COUNTRIES, key=len, reverse=True)
    matched_spans = []
    for c in sorted_countries:
        pattern = r'\b' + re.escape(c.lower()) + r'\b'
        for m in re.finditer(pattern, tl):
            # Skip if this span overlaps with an already-matched longer name
            if any(m.start() >= s and m.end() <= e for s, e in matched_spans):
                continue
            matched_spans.append((m.start(), m.end()))
            if c not in found:
                found.append(c)
    return found


def _is_africa(text: str) -> bool:
    t = str(text or "").lower()
    if any(re.search(r'\b' + re.escape(c.lower()) + r'\b', t) for c in AFRICA_DFC_COUNTRIES):
        return True
    return any(re.search(r'\b' + re.escape(kw.lower()) + r'\b', t) for kw in DFC_AFRICA_KEYWORDS)


def _is_africa_place_of_performance(row: pd.Series) -> bool:
    code = str(row.get("Place of Performance Country Code") or "").strip().upper()
    if code in AFRICA_ISO_CODES:
        return True
    name = _normalize_country_name(row.get("Place of Performance Country Name"))
    return name in DFC_AFRICA_COUNTRY_NAMES


def _standardize_country_series(df: pd.DataFrame) -> pd.Series:
    name_series = df.get("Place of Performance Country Name", pd.Series(index=df.index, dtype="object"))
    code_series = df.get("Place of Performance Country Code", pd.Series(index=df.index, dtype="object"))
    country = name_series.astype("object").where(name_series.notna(), code_series.map(AFRICA_CODE_TO_NAME))
    country = country.where(country.notna(), code_series)
    return country


def _is_project_like_federal_register_notice(text: str) -> bool:
    t = str(text or "").lower()
    has_agency = any(term in t for term in [
        "development finance corporation",
        "international development finance corporation",
        "overseas private investment corporation",
        "opic",
    ])
    has_project_hint = any(kw in t for kw in FEDERAL_REGISTER_PROJECT_KEYWORDS)
    has_africa = _is_africa(t)
    return has_agency and has_project_hint and has_africa


def _extract_amounts(text: str) -> list:
    return re.findall(
        r"\$[\d,\.]+\s*(?:million|billion|M|B)?", text, re.IGNORECASE
    )


def _safe_get(url: str, params: dict = None) -> requests.Response:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (DFC Research Bot - contact: your@email.com)"},
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        log.error(f"  GET failed [{url}]: {e}")
        return None


def _full_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return f"{DFC_BASE_URL}{href}"


# ─────────────────────────────────────────────────────────────
# 1. DFC transaction data
# ─────────────────────────────────────────────────────────────

def scrape_dfc_transaction_data() -> pd.DataFrame:
    """
    Scrape DFC transaction data page and download any available
    Excel/CSV files. Saves: output/dfc_active_projects_africa.xlsx
    """
    log.info("── DFC.gov: transaction data ────────────────────────")
    records = []

    urls_to_try = [
        DFC_TRANSACTION_DATA,
        DFC_ACTIVE_PROJECTS,
        DFC_WHERE_WE_WORK,
        DFC_TRANSPARENCY,
    ]

    for url in urls_to_try:
        log.info(f"  Trying: {url}")
        resp = _safe_get(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for downloadable data files
        data_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if any(ext in href.lower() for ext in [".csv", ".xlsx", ".json", ".xls"]):
                data_links.append({"text": text, "url": _full_url(href)})
                log.info(f"  Found data download: {text} -> {href}")

        # Download Excel files
        for dl in data_links:
            if ".xlsx" in dl["url"].lower() or ".xls" in dl["url"].lower():
                log.info(f"  Downloading: {dl['url']}")
                r = _safe_get(dl["url"])
                if r:
                    try:
                        import io
                        xl = pd.ExcelFile(io.BytesIO(r.content))
                        log.info(f"  Sheets found: {xl.sheet_names}")

                        # Find the data sheet — skip ToC and ReadMe
                        data_sheet = next(
                            (s for s in xl.sheet_names
                             if s.lower() not in ["table of contents", "read me", "readme", "toc"]),
                            xl.sheet_names[-1]
                        )
                        log.info(f"  Reading sheet: '{data_sheet}'")

                        # Row 0 is title, row 1 is headers, row 2+ is data
                        df_xl = pd.read_excel(
                            xl,
                            sheet_name=data_sheet,
                            header=1,        # row index 1 = second row = real headers
                        )

                        # Drop the title row if it snuck in
                        df_xl = df_xl[df_xl.iloc[:, 0] != data_sheet].copy()
                        df_xl.columns = df_xl.columns.str.strip()

                        log.info(f"  Columns: {list(df_xl.columns)}")
                        log.info(f"  Total rows: {len(df_xl)}")

                        # Save full raw file
                        df_xl.to_excel(FILES["dfc_raw_download"], index=False)
                        log.info(f"  Raw data saved -> {FILES['dfc_raw_download']}")

                        # Filter for Africa — use Region column if available
                        if "Region" in df_xl.columns:
                            df_africa = df_xl[
                                df_xl["Region"].str.lower().str.contains("africa", na=False)
                            ].copy()
                            log.info(f"  Africa rows (Region filter): {len(df_africa)}")
                        else:
                            # Fallback — search all columns
                            mask = df_xl.apply(
                                lambda row: any(
                                    c.lower() in str(row).lower()
                                    for c in AFRICA_DFC_COUNTRIES
                                ), axis=1
                            )
                            df_africa = df_xl[mask].copy()
                            log.info(f"  Africa rows (text filter): {len(df_africa)}")

                        df_africa.to_excel(FILES["dfc_active_projects"], index=False)
                        return df_africa

                    except Exception as e:
                        log.error(f"  Excel parse error: {e}")
                        import traceback
                        log.error(traceback.format_exc())

        # Download CSV files
        for dl in data_links:
            if ".csv" in dl["url"].lower():
                log.info(f"  Downloading CSV: {dl['url']}")
                r = _safe_get(dl["url"])
                if r:
                    try:
                        import io
                        df_csv = pd.read_csv(io.StringIO(r.text))
                        raw_path = FILES["usaspending_all"].parent / "dfc_raw_download.csv"
                        df_csv.to_csv(raw_path, index=False)
                        log.info(f"  CSV downloaded: {len(df_csv)} rows")
                        mask = df_csv.apply(
                            lambda row: any(
                                c.lower() in str(row).lower()
                                for c in AFRICA_DFC_COUNTRIES
                            ), axis=1
                        )
                        df_africa = df_csv[mask].copy()
                        df_africa.to_excel(FILES["dfc_active_projects"], index=False)
                        log.info(f"  Africa rows: {len(df_africa)}")
                        return df_africa
                    except Exception as e:
                        log.error(f"  CSV parse error: {e}")

        # Fallback — scrape tables
        tables = soup.find_all("table")
        if tables:
            for table in tables:
                rows = table.find_all("tr")
                headers = (
                    [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
                    if rows else []
                )
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if not cells:
                        continue
                    row_text = " ".join(cells)
                    country = "; ".join(
                        c for c in AFRICA_DFC_COUNTRIES
                        if re.search(r'\b' + re.escape(c.lower()) + r'\b', row_text.lower())
                    )
                    amounts = _extract_amounts(row_text)
                    record = {
                        "source_url": url,
                        "country":    country,
                        "amounts":    "; ".join(amounts[:5]),
                        "is_africa":  bool(country),
                        "scraped_at": TODAY,
                    }
                    for i, h in enumerate(headers):
                        if i < len(cells) and h:
                            record[h] = cells[i]
                    records.append(record)

        if records:
            break
        time.sleep(CRAWL_DELAY)

    df = pd.DataFrame(records)
    if not df.empty and "is_africa" in df.columns:
        df = df[df["is_africa"]].copy()

    df.to_excel(FILES["dfc_active_projects"], index=False)
    log.info(f"  Transaction data saved -> {FILES['dfc_active_projects']} ({len(df)} rows)")
    return df


# ─────────────────────────────────────────────────────────────
# 2. DFC investment stories
# ─────────────────────────────────────────────────────────────

def scrape_dfc_investment_stories() -> pd.DataFrame:
    """
    Scrape DFC investment stories for Africa entries.
    Saves: output/dfc_impact_stories.xlsx
    """
    log.info("── DFC.gov: investment stories ──────────────────────")

    resp = _safe_get(DFC_IMPACT_PAGE)
    if not resp:
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")
    records = []

    story_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if "/investment-story/" in href or "/what-we-do/investment" in href:
            story_links.append({"title": text, "url": _full_url(href)})

    for block in soup.find_all(["article", "div", "li"]):
        text = block.get_text(separator=" ", strip=True)
        if _is_africa(text) and len(text) > 50:
            link = block.find("a", href=True)
            story_links.append({
                "title": text[:100],
                "url": _full_url(link["href"]) if link else DFC_IMPACT_PAGE,
            })

    seen = set()
    unique_links = []
    for s in story_links:
        if s["url"] not in seen:
            seen.add(s["url"])
            unique_links.append(s)

    log.info(f"  Found {len(unique_links)} story links — following each...")

    for story in tqdm(unique_links, desc="  Stories"):
        r = _safe_get(story["url"])
        if not r:
            continue

        detail_soup = BeautifulSoup(r.text, "lxml")
        detail_text = detail_soup.get_text(separator=" ", strip=True)

        if not _is_africa(detail_text) and not _is_africa(story["title"]):
            time.sleep(API_DELAY)
            continue

        country = "; ".join(
            c for c in AFRICA_DFC_COUNTRIES
            if re.search(r'\b' + re.escape(c.lower()) + r'\b', detail_text.lower())
        )
        amounts = _extract_amounts(detail_text)
        jobs    = re.findall(r"[\d,]+\s*jobs?", detail_text, re.I)
        people  = re.findall(r"[\d,\.]+\s*(?:million\s*)?people", detail_text, re.I)

        records.append({
            "title":          story["title"],
            "country":        country,
            "amounts":        "; ".join(amounts[:5]),
            "jobs_created":   "; ".join(jobs[:3]),
            "people_reached": "; ".join(people[:3]),
            "url":            story["url"],
            "snippet":        detail_text[:500],
            "scraped_at":     TODAY,
        })
        time.sleep(CRAWL_DELAY)

    df = pd.DataFrame(records)
    df.to_excel(FILES["dfc_impact_stories"], index=False)
    log.info(f"  Investment stories saved -> {FILES['dfc_impact_stories']} ({len(df)} rows)")
    return df


# ─────────────────────────────────────────────────────────────
# 3. DFC press releases
# ─────────────────────────────────────────────────────────────

def scrape_dfc_press_releases() -> pd.DataFrame:
    """
    Scrape DFC press releases for Africa deal announcements.
    Saves: output/dfc_press_releases_africa.xlsx
    """
    log.info("── DFC.gov: press releases ──────────────────────────")

    resp = _safe_get(DFC_PRESS_RELEASES)
    if not resp:
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")

    pr_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/media/press-releases?/", href):
            pr_links.append({
                "title": a.get_text(strip=True),
                "url":   _full_url(href),
            })

    seen = set()
    unique_prs = []
    for p in pr_links:
        if p["url"] not in seen and len(p["title"]) > 5:
            seen.add(p["url"])
            unique_prs.append(p)

    log.info(f"  Found {len(unique_prs)} press release links")
    records = []

    for pr in tqdm(unique_prs[:MAX_PRESS_RELEASES], desc="  Press releases"):
        r = _safe_get(pr["url"])
        if not r:
            continue

        detail_soup = BeautifulSoup(r.text, "lxml")
        detail_text = detail_soup.get_text(separator=" ", strip=True)

        if not _is_africa(detail_text):
            time.sleep(API_DELAY)
            continue

        country = "; ".join(
            c for c in AFRICA_DFC_COUNTRIES
            if re.search(r'\b' + re.escape(c.lower()) + r'\b', detail_text.lower())
        )
        amounts  = _extract_amounts(detail_text)
        date_tag = detail_soup.find(
            ["time", "span", "div"],
            class_=re.compile(r"date|time|publish", re.I)
        )
        date_str = date_tag.get_text(strip=True) if date_tag else ""

        records.append({
            "title":      pr["title"],
            "country":    country,
            "date":       date_str,
            "amounts":    "; ".join(amounts[:5]),
            "url":        pr["url"],
            "snippet":    detail_text[:500],
            "scraped_at": TODAY,
        })
        time.sleep(CRAWL_DELAY)

    df = pd.DataFrame(records)
    df.to_excel(FILES["dfc_press_releases"], index=False)
    log.info(f"  Press releases saved -> {FILES['dfc_press_releases']} ({len(df)} rows)")
    return df


# ─────────────────────────────────────────────────────────────
# 4. Federal Register API
# ─────────────────────────────────────────────────────────────

def scrape_federal_register() -> pd.DataFrame:
    """
    Query the Federal Register API for DFC/OPIC Africa notices.
    Saves: output/dfc_federal_register_africa.xlsx
    """
    log.info("── Federal Register: DFC Africa notices ─────────────")

    all_records = []

    # Cap pages per query. The broader DFC full-text search can return thousands
    # of false positives that mention "DFC" but aren't agency notices.
    MAX_FR_PAGES = 15  # 15 * 100 = 1500 notices max per search

    searches = [
        {
            "conditions[agencies][]": "overseas-private-investment-corporation",
            "per_page": 100,
            "order": "newest",
            "fields[]": ["title", "abstract", "publication_date", "html_url", "pdf_url"],
        },
        {
            "conditions[agencies][]": "u-s-international-development-finance-corporation",
            "per_page": 100,
            "order": "newest",
            "fields[]": ["title", "abstract", "publication_date", "html_url", "pdf_url"],
        },
    ]

    for params in tqdm(searches, desc="  FR queries"):
        page = 1
        while page <= MAX_FR_PAGES:
            params["page"] = page
            resp = _safe_get(FEDERAL_REGISTER_API, params=params)
            if not resp:
                break

            data = resp.json()
            results = data.get("results", [])
            total_count = data.get("count", 0)
            if page == 1:
                pages_to_fetch = min(MAX_FR_PAGES, (total_count + 99) // 100)
                log.info(f"  FR query: {total_count} total notices, fetching up to {pages_to_fetch} pages...")

            for item in results:
                title    = item.get("title") or ""
                abstract = item.get("abstract") or ""
                text     = f"{title} {abstract}"
                countries = _extract_country_mentions(text)
                amounts  = _extract_amounts(text)
                keep_notice = _is_project_like_federal_register_notice(text) and bool(countries)
                all_records.append({
                    "title":            title,
                    "abstract":         abstract[:500],
                    "publication_date": item.get("publication_date", ""),
                    "country":          "; ".join(countries),
                    "amounts":          "; ".join(amounts[:5]),
                    "html_url":         item.get("html_url", ""),
                    "pdf_url":          item.get("pdf_url", ""),
                    "is_africa":        bool(countries),
                    "is_project_like":  keep_notice,
                })

            if len(results) < params.get("per_page", 100):
                break
            page += 1
            time.sleep(API_DELAY)

    if not all_records:
        log.warning("  Federal Register returned no records.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["title", "publication_date", "html_url"])

    df.to_excel(FILES["dfc_board_notices"], index=False)
    df_africa = df[df["is_project_like"]].copy()
    df_africa.to_excel(FILES["dfc_federal_register"], index=False)

    log.info(f"  FR all notices    -> {FILES['dfc_board_notices']} ({len(df)} rows)")
    log.info(f"  FR Africa notices -> {FILES['dfc_federal_register']} ({len(df_africa)} rows)")
    return df_africa


# ─────────────────────────────────────────────────────────────
# 5. USASpending — DFC operational contracts
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_dfc() -> pd.DataFrame:
    """
    Pull DFC contracts from USASpending.gov.
    Saves:
      raw_data/usaspending_dfc_all.xlsx
      output/usaspending_dfc_africa.xlsx
    """
    log.info("── USASpending.gov: DFC contracts ───────────────────")
    url = f"{USASPENDING_URL}/search/spending_by_award/"

    agency_names = [
        "US International Development Finance Corporation",
        "U.S. International Development Finance Corporation",
        "Development Finance Corporation",
    ]

    all_records = []
    for agency in agency_names:
        payload = {
            "filters": {
                "award_type_codes": AWARD_TYPE_CODES,
                "agencies": [
                    {"type": "awarding", "tier": "toptier", "name": agency}
                ],
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Start Date", "End Date", "Award Type",
                "Awarding Agency", "Description",
                "Place of Performance Country Code",
                "Place of Performance Country Name",
            ],
            "sort": "Award Amount",
            "order": "desc",
            "limit": 100,
            "page": 1,
        }

        for page in range(1, MAX_PAGES + 1):
            payload["page"] = page
            try:
                resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
            except requests.RequestException as e:
                log.error(f"  Page {page} failed: {e}")
                break

            results = resp.json().get("results", [])
            if not results:
                break
            all_records.extend(results)
            time.sleep(API_DELAY)

        if all_records:
            log.info(f"  Found results with agency name: '{agency}'")
            break

    if not all_records:
        log.warning("  USASpending returned no DFC records.")
        return pd.DataFrame()

    df_all = pd.DataFrame(all_records).drop_duplicates(subset=["Award ID"])
    df_all["Country"] = _standardize_country_series(df_all)
    df_all.to_excel(FILES["dfc_usaspending_all"], index=False)

    df_africa = df_all[df_all.apply(_is_africa_place_of_performance, axis=1)].copy()
    df_africa["Africa tagging basis"] = "Place of performance country code/name"
    df_africa.to_excel(FILES["dfc_usaspending"], index=False)
    log.info(f"  DFC all contracts    -> {FILES['dfc_usaspending_all']} ({len(df_all)} rows)")
    log.info(f"  DFC Africa contracts -> {FILES['dfc_usaspending']} ({len(df_africa)} rows)")
    return df_africa


# ─────────────────────────────────────────────────────────────
# 6. DFC sector pages
# ─────────────────────────────────────────────────────────────

def scrape_dfc_sectors() -> pd.DataFrame:
    """
    Directly scrape known DFC investment story URLs per sector.
    Sector pages are JS-rendered so we bypass them and hit
    the static story pages directly.
    Saves: output/dfc_sectors_africa.xlsx
    """
    log.info("── DFC.gov: sector pages ────────────────────────────")

    SECTOR_STORIES = {
        "Energy": [
            "https://www.dfc.gov/investment-story/financing-sierra-leones-first-major-utility-scale-power-plant",
            "https://www.dfc.gov/investment-story/tripling-electricity-generating-capacity-togo",
            "https://www.dfc.gov/what-we-do/investment-stories",
        ],
        "Food Security & Agribusiness": [
            "https://www.dfc.gov/investment-story/investing-africas-first-potash-mine-reduce-dependence-russian-imports",
            "https://www.dfc.gov/investment-story/investing-food-security-niger",
            "https://www.dfc.gov/what-we-do/investment-stories",
        ],
        "Health": [
            "https://www.dfc.gov/investment-story/expanding-access-medical-oxygen-kenya",
            "https://www.dfc.gov/what-we-do/investment-stories",
        ],
        "Infrastructure & Critical Minerals": [
            "https://www.dfc.gov/investment-story/strengthening-critical-mineral-supply-chains-countering-chinas-dominance",
            "https://www.dfc.gov/investment-story/helping-gabon-grow-exports-and-diversify-its-economy",
            "https://www.dfc.gov/investment-story/strengthening-strategic-competition-through-investments-africas-digital-economy",
            "https://www.dfc.gov/what-we-do/investment-stories",
        ],
        "Small Business & Financial Services": [
            "https://www.dfc.gov/investment-story/de-risking-small-business-and-agriculture-loans-malawi",
            "https://www.dfc.gov/what-we-do/investment-stories",
        ],
    }

    known_firms = [
        "GE", "Bechtel", "Chevron", "ExxonMobil", "Cargill",
        "Mastercard", "Google", "Microsoft", "Visa", "Caterpillar",
        "John Deere", "First Solar", "Baker Hughes", "Schlumberger",
        "Kosmos", "Freeport", "Albemarle", "Johnson & Johnson",
        "Merck", "3M", "Andela", "Occidental", "ADM", "Mars",
        "Hershey", "Gap", "PVH", "AECOM", "Chemonics", "DAI",
        "Abt", "RTI", "Tetra Tech", "KBR", "Jacobs",
    ]

    all_records = []

    for sector_name, story_urls in tqdm(SECTOR_STORIES.items(), desc="  Sectors"):
        log.info(f"  Scraping sector: {sector_name}")
        seen_urls = set()
        urls_to_visit = list(story_urls)

        for url in urls_to_visit:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            r = _safe_get(url)
            if not r:
                continue

            soup = BeautifulSoup(r.text, "lxml")

            # If listing page — collect more story links
            if "investment-stories" in url:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/investment-story/" in href:
                        full = _full_url(href)
                        if full not in seen_urls:
                            urls_to_visit.append(full)
                time.sleep(API_DELAY)
                continue

            detail_text = soup.get_text(separator=" ", strip=True)

            if not _is_africa(detail_text):
                time.sleep(API_DELAY)
                continue

            title_tag   = soup.find("h1") or soup.find("h2")
            title       = title_tag.get_text(strip=True) if title_tag else url
            paras       = [
                p.get_text(separator=" ", strip=True)
                for p in soup.find_all("p")
                if len(p.get_text(strip=True)) > 40
            ]
            description = " ".join(paras[:4])[:600]
            country     = "; ".join(
                c for c in AFRICA_DFC_COUNTRIES
                if re.search(r'\b' + re.escape(c.lower()) + r'\b', detail_text.lower())
            )
            amounts     = _extract_amounts(detail_text)
            jobs        = re.findall(r"[\d,]+\s*jobs?", detail_text, re.I)
            people      = re.findall(r"[\d,\.]+\s*(?:million\s*)?people", detail_text, re.I)
            firms       = [f for f in known_firms if f.lower() in detail_text.lower()]

            all_records.append({
                "sector":      sector_name,
                "country":     country,
                "project":     title,
                "description": description,
                "amounts":     "; ".join(amounts[:5]),
                "us_firm":     "; ".join(firms[:5]),
                "jobs":        "; ".join(jobs[:3]),
                "people":      "; ".join(people[:3]),
                "detail_url":  url,
                "scraped_at":  TODAY,
            })
            log.info(f"    + {title[:70]} [{country}]")
            time.sleep(CRAWL_DELAY)

    if not all_records:
        log.warning("  No sector data found.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["sector", "detail_url"]).reset_index(drop=True)

    summary = (
        df[df["country"] != ""]
        .groupby("sector")
        .agg(
            africa_projects=("country", "count"),
            countries=("country", lambda x: "; ".join(sorted(set(x)))),
            firms=("us_firm", lambda x: "; ".join(
                sorted(set(f for v in x if pd.notna(v) for f in v.split("; ") if f))
            )),
        )
        .reset_index()
    )

    with pd.ExcelWriter(FILES["dfc_sectors"], engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Africa detail", index=False)
        summary.to_excel(writer, sheet_name="Sector summary", index=False)

    log.info(f"  Sector data saved -> {FILES['dfc_sectors']}")
    log.info(f"    Detail rows : {len(df)}")
    log.info(f"    Sectors     : {len(summary)}")
    if not summary.empty:
        log.info("\n  Sector summary:")
        log.info(summary[["sector", "africa_projects", "countries"]].to_string(index=False))

    return df


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 55)
    log.info("  DFC Africa Data Collection")
    log.info("=" * 55)

    df_transactions = scrape_dfc_transaction_data()
    df_stories      = scrape_dfc_investment_stories()
    df_press        = scrape_dfc_press_releases()
    df_fr           = scrape_federal_register()
    df_spending     = scrape_usaspending_dfc()
    df_sectors      = scrape_dfc_sectors()

    log.info("\n" + "=" * 55)
    log.info("  SUMMARY")
    log.info("=" * 55)
    log.info(f"  DFC transaction data (Africa) : {len(df_transactions)} rows")
    log.info(f"  Investment stories (Africa)   : {len(df_stories)} rows")
    log.info(f"  Press releases (Africa)       : {len(df_press)} rows")
    log.info(f"  Federal Register notices      : {len(df_fr)} rows")
    log.info(f"  USASpending DFC (Africa)      : {len(df_spending)} rows")
    log.info(f"  Sector pages (Africa)         : {len(df_sectors)} rows")
    log.info("\n  Output files:")
    dfc_keys = [
        "dfc_active_projects", "dfc_impact_stories", "dfc_press_releases",
        "dfc_federal_register", "dfc_usaspending", "dfc_sectors",
    ]
    for key in dfc_keys:
        path = FILES[key]
        if path.exists():
            log.info(f"    {path.relative_to(path.parent.parent)}")


if __name__ == "__main__":
    main()
