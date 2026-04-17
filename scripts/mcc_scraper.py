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
    AFRICA_CODE_TO_NAME,
    AFRICA_ISO_CODES,
    MCC_AFRICA_COUNTRY_NAMES,
    AFRICA_MCC_COUNTRIES,
    API_DELAY,
    AWARD_TYPE_CODES,
    CRAWL_DELAY,
    FILES,
    GRANT_TYPE_CODES,
    MAX_PAGES,
    MCC_AGENCY_NAME,
    MCC_BASE_URL,
    MCC_OPEN_DATA_URL,
    REQUEST_TIMEOUT,
    SAM_GOV_API_KEY,
    SAM_GOV_URL,
    SAM_SEARCH_KEYWORDS,
    SCRAPER_HEADERS,
    USASPENDING_URL,
)
from scripts.logger import get_logger

log = get_logger()


AWARD_PAGE_SIZE = 100


def _normalize_country_name(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _is_africa_place_of_performance(row: pd.Series) -> bool:
    code = str(row.get("Place of Performance Country Code") or "").strip().upper()
    if code in AFRICA_ISO_CODES:
        return True

    name = _normalize_country_name(row.get("Place of Performance Country Name"))
    return name in MCC_AFRICA_COUNTRY_NAMES


def _us_fiscal_year_from_series(values: pd.Series) -> pd.Series:
    dt = pd.to_datetime(values, errors="coerce")
    fy = dt.dt.year + (dt.dt.month >= 10).astype("Int64")
    return fy


def _standardize_country_series(df: pd.DataFrame) -> pd.Series:
    name_series = df.get("Place of Performance Country Name", pd.Series(index=df.index, dtype="object"))
    code_series = df.get("Place of Performance Country Code", pd.Series(index=df.index, dtype="object"))
    country = name_series.astype("object").where(name_series.notna(), code_series.map(AFRICA_CODE_TO_NAME))
    country = country.where(country.notna(), code_series)
    return country


def _add_fiscal_year_with_fallback(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Fiscal Year, cascading through available date columns.
    Order: Start Date → End Date → Last Action Date (grants only populate the last)."""
    df = df.copy()
    fy = _us_fiscal_year_from_series(df.get("Start Date"))
    for col in ["End Date", "Last Action Date"]:
        if col in df.columns and df[col].notna().any():
            fallback = _us_fiscal_year_from_series(df[col])
            fy = fy.where(fy.notna(), fallback)
    df["Fiscal Year"] = fy
    return df


def _fetch_all_usaspending_rows(url: str, payload: dict, desc: str) -> list[dict]:
    all_records = []
    for page in tqdm(range(1, MAX_PAGES + 1), desc=desc):
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
        if len(results) < payload.get("limit", AWARD_PAGE_SIZE):
            break
        time.sleep(API_DELAY)

    return all_records


# ─────────────────────────────────────────────────────────────
# 1. USASpending — all MCC awards + Africa filter
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_awards() -> pd.DataFrame:
    """
    Pull all contract awards where awarding agency = MCC.
    Filters to Africa using place-of-performance country code/name rather
    than broad keyword matching across the full row.
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
            "naics_code",
            "naics_description",
            "recipient_location_country_name",
        ],
        "sort": "Award Amount",
        "order": "desc",
        "limit": AWARD_PAGE_SIZE,
        "page": 1,
    }

    all_records = _fetch_all_usaspending_rows(url, payload, "  Fetching award pages")
    if not all_records:
        log.warning("  USASpending returned no records.")
        return pd.DataFrame()

    df_all = pd.DataFrame(all_records).drop_duplicates(subset=["Award ID"])
    df_all["Country"] = _standardize_country_series(df_all)
    df_all = _add_fiscal_year_with_fallback(df_all)
    df_all.to_excel(FILES["usaspending_all"], index=False)
    log.info(f"  Raw awards saved → {FILES['usaspending_all']} ({len(df_all)} rows)")

    df_africa = df_all[df_all.apply(_is_africa_place_of_performance, axis=1)].copy()
    df_africa.to_excel(FILES["usaspending_africa"], index=False)
    log.info(f"  Africa awards saved → {FILES['usaspending_africa']} ({len(df_africa)} rows)")

    return df_africa


# ─────────────────────────────────────────────────────────────
# 2. USASpending — top recipients (firms) of MCC money
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_recipients() -> pd.DataFrame:
    """
    Pull recipient-category rows for MCC contract awards.
    Paginate through all available pages so the workbook is not limited
    to the first response page only.
    Saves: output/usaspending_mcc_top_recipients.xlsx
    """
    log.info("── USASpending.gov: top recipients ──────────────────")
    url = f"{USASPENDING_URL}/search/spending_by_category/recipient/"

    all_results = []
    for page in tqdm(range(1, MAX_PAGES + 1), desc="  Fetching recipient pages"):
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
            "limit": AWARD_PAGE_SIZE,
            "page": page,
        }

        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"  Recipient page {page} failed: {e}")
            break

        results = resp.json().get("results", [])
        if not results:
            log.info(f"  No more recipient rows after page {page - 1}")
            break

        all_results.extend(results)
        if len(results) < payload["limit"]:
            break
        time.sleep(API_DELAY)

    df = pd.DataFrame(all_results).drop_duplicates()

    if df.empty:
        log.warning("  No recipient data returned.")
        return df

    df.to_excel(FILES["usaspending_recipients"], index=False)
    log.info(f"  Recipients saved → {FILES['usaspending_recipients']} ({len(df)} rows)")

    preview_cols = [c for c in ["name", "amount", "recipient_id"] if c in df.columns]
    if preview_cols:
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

            # Status keywords captured as a summary, not a single definitive program status
            status_terms = [
                "closed", "completed", "active", "implementation", "signed",
                "developing", "development", "threshold", "compact", "terminated"
            ]
            found_statuses = [kw for kw in status_terms if kw in text_blob.lower()]
            status_summary = "; ".join(dict.fromkeys(found_statuses)) if found_statuses else "Unknown"

            records.append(
                {
                    "country":            country.replace("-", " ").title(),
                    "url":                url,
                    "page_title":         title_text,
                    "status_summary":     status_summary,
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
# 4. USASpending — MCC compact grants (financial assistance)
# ─────────────────────────────────────────────────────────────

def scrape_usaspending_mcc_grants() -> pd.DataFrame:
    """
    Pull MCC grant/compact awards (financial assistance) from USASpending,
    filtered to African countries using place-of-performance country code/name.
    Adds a derived US federal fiscal year for consistent rollups.
    Saves:
      raw_data/usaspending_mcc_grants_all.xlsx   — full unfiltered pull
      output/usaspending_mcc_grants_africa.xlsx  — Africa-filtered subset
    """
    log.info("── USASpending.gov: MCC grants / compacts ───────────")
    url = f"{USASPENDING_URL}/search/spending_by_award/"

    payload = {
        "filters": {
            "award_type_codes": GRANT_TYPE_CODES,
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
            "Last Action Date",   # grants rarely populate Start/End; this is reliably set
            "Award Type",
            "Awarding Agency",
            "Description",
            "Place of Performance Country Code",
            "Place of Performance Country Name",
        ],
        "sort": "Award Amount",
        "order": "desc",
        "limit": AWARD_PAGE_SIZE,
        "page": 1,
    }

    all_records = _fetch_all_usaspending_rows(url, payload, "  Fetching grant pages")
    if not all_records:
        log.warning("  USASpending returned no grant records.")
        return pd.DataFrame()

    df_all = pd.DataFrame(all_records).drop_duplicates(subset=["Award ID"])
    df_all["Country"] = _standardize_country_series(df_all)
    df_all = _add_fiscal_year_with_fallback(df_all)
    df_all.to_excel(FILES["mcc_grants_all"], index=False)
    log.info(f"  All grants saved → {FILES['mcc_grants_all']} ({len(df_all)} rows)")

    df_africa = df_all[df_all.apply(_is_africa_place_of_performance, axis=1)].copy()
    df_africa.to_excel(FILES["mcc_grants_africa"], index=False)
    log.info(f"  Africa grants saved → {FILES['mcc_grants_africa']} ({len(df_africa)} rows)")

    return df_africa


# ─────────────────────────────────────────────────────────────
# 5. MCC.gov — compact sector breakdowns
# ─────────────────────────────────────────────────────────────

# Keywords → high-level sector label (first match wins)
_SECTOR_MAP = [
    (["energy", "power", "electricity", "transmission", "generation",
      "grid", "solar", "renewable", "senelec"],             "Energy"),
    (["water", "sanitation", "drainage", "wash", "sewage"],  "Water & Sanitation"),
    (["agriculture", "agri", "farm", "food", "crop",
      "irrigation", "livestock"],                            "Agriculture"),
    (["transport", "road", "rail", "port", "mobility",
      "bridge", "highway", "connectivity", "rural transport"],"Transport & Infrastructure"),
    (["coastal", "climate", "resilience", "fisheries",
      "fishery", "marine", "ecosystem"],                     "Climate & Natural Resources"),
    (["land", "property", "cadastre", "tenure"],             "Land"),
    (["health", "medical", "hospital", "clinic"],            "Health"),
    (["education", "training", "tvet", "workforce",
      "employability", "skills", "school"],                  "Education & Workforce"),
    (["finance", "financial", "banking",
      "microfinance", "investment"],                         "Financial Services"),
    (["monitoring", "evaluation", "m&e"],                    "Monitoring & Evaluation"),
    (["administration", "admin", "program management"],      "Program Administration"),
]


def _classify_sector(component: str) -> str:
    lower = component.lower()
    for keywords, sector in _SECTOR_MAP:
        if any(kw in lower for kw in keywords):
            return sector
    return "Other"


def _parse_usd(text: str) -> float | None:
    """Extract a dollar value from strings like '$364,012,899' or '$1,234.50'."""
    cleaned = re.sub(r"[^\d.]", "", text)
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _extract_compact_total(text_blob: str) -> float | None:
    """Pull the first compact dollar total from prose (millions/billions aware)."""
    m = re.search(r'\$([\d,\.]+)\s*(million|billion)', text_blob, re.IGNORECASE)
    if not m:
        return None
    amount = float(m.group(1).replace(",", ""))
    if m.group(2).lower() == "billion":
        amount *= 1_000
    return round(amount * 1_000_000)


def _parse_budget_table(soup: BeautifulSoup) -> list[dict]:
    """
    Return component-level rows from a budget table, if one exists.
    Looks for 2-column tables where one column has dollar amounts.
    Skips 'Total' summary rows.
    """
    rows_out = []
    for table in soup.find_all("table"):
        # Collect all rows (including header-less tables)
        all_rows = table.find_all("tr")
        if len(all_rows) < 2:
            continue

        data_rows = []
        for tr in all_rows:
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) >= 2:
                data_rows.append(cells)

        if not data_rows:
            continue

        # Detect if this is a budget table: at least one cell must look like a dollar amount
        dollar_pattern = re.compile(r'\$[\d,]+')
        has_dollars = any(
            dollar_pattern.search(cell)
            for row in data_rows for cell in row[1:]
        )
        if not has_dollars:
            continue

        for cells in data_rows:
            component = cells[0].strip()

            # Skip header rows and total rows
            if not component or "total" in component.lower():
                continue

            # Find the first cell (after the label) containing a dollar amount
            amount_str = None
            for cell in cells[1:]:
                if dollar_pattern.search(cell):
                    amount_str = cell.strip()
                    break
            if not amount_str:
                continue

            amount = _parse_usd(amount_str)
            if amount is None or amount <= 0:
                continue

            rows_out.append({
                "Component": component,
                "Amount (USD)": amount,
            })

    return rows_out


def scrape_mcc_compact_sectors() -> pd.DataFrame:
    """
    For each MCC Africa country page, follow /where-we-work/program/ links
    and extract compact budget component breakdowns where available.
    Falls back to compact total + sector keywords from prose.
    Saves: output/mcc_compact_sectors.xlsx
    """
    log.info("── MCC.gov: compact sector breakdowns ───────────────")
    records = []

    for country_slug, country_url in tqdm(AFRICA_MCC_COUNTRIES, desc="  Scraping compact sectors"):
        country_name = country_slug.replace("-", " ").title()

        # Step 1: find program sub-page links on the country page
        try:
            resp = requests.get(country_url, headers=SCRAPER_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                log.warning(f"  {country_name}: HTTP {resp.status_code} on country page")
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            program_hrefs = list(dict.fromkeys(
                a["href"] for a in soup.find_all("a", href=True)
                if "/where-we-work/program/" in a["href"]
            ))
        except Exception as e:
            log.error(f"  {country_name}: country page error — {e}")
            continue

        if not program_hrefs:
            log.debug(f"  {country_name}: no program links found")
            continue

        # Step 2: scrape each program page
        for href in program_hrefs:
            prog_url   = (MCC_BASE_URL + href).rstrip("/") if href.startswith("/") else href
            compact_name = href.rstrip("/").split("/")[-1].replace("-", " ").title()
            time.sleep(CRAWL_DELAY)

            try:
                resp = requests.get(prog_url, headers=SCRAPER_HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code != 200:
                    log.warning(f"  {compact_name}: HTTP {resp.status_code}")
                    continue
                prog_soup  = BeautifulSoup(resp.text, "lxml")
                text_blob  = " ".join(p.get_text(strip=True) for p in prog_soup.find_all("p"))

                table_rows = _parse_budget_table(prog_soup)

                if table_rows:
                    # Use the detailed component breakdown
                    for row in table_rows:
                        records.append({
                            "Country":        country_name,
                            "Compact":        compact_name,
                            "Compact URL":    prog_url,
                            "Component":      row["Component"],
                            "Sector":         _classify_sector(row["Component"]),
                            "Amount (USD)":   row["Amount (USD)"],
                            "Source":         "Budget Table",
                        })
                    log.debug(f"  {compact_name}: {len(table_rows)} component rows from table")
                else:
                    # Fall back to compact total + sector keywords from prose
                    total = _extract_compact_total(text_blob)
                    if total:
                        sector_keywords = [
                            kw for kws, _ in _SECTOR_MAP
                            for kw in kws
                            if kw in text_blob.lower()
                            and kw not in ("administration", "admin", "monitoring",
                                           "evaluation", "m&e")
                        ]
                        # Resolve keywords back to sector labels (deduplicated)
                        seen, sectors = set(), []
                        for kw in sector_keywords:
                            label = _classify_sector(kw)
                            if label not in seen:
                                seen.add(label)
                                sectors.append(label)
                        records.append({
                            "Country":        country_name,
                            "Compact":        compact_name,
                            "Compact URL":    prog_url,
                            "Component":      "; ".join(sectors) if sectors else "Not Specified",
                            "Sector":         "; ".join(sectors) if sectors else "Not Specified",
                            "Amount (USD)":   total,
                            "Source":         "Compact Total (no component breakdown)",
                        })
                        log.debug(f"  {compact_name}: total only — ${total:,.0f}")
                    else:
                        log.debug(f"  {compact_name}: no amount found, skipping")

            except Exception as e:
                log.error(f"  {compact_name}: error — {e}")

    df = pd.DataFrame(records)
    if not df.empty:
        df["Amount (USD millions)"] = (df["Amount (USD)"] / 1_000_000).round(2)

    df.to_excel(FILES["mcc_compact_sectors"], index=False)
    log.info(f"  Compact sectors saved → {FILES['mcc_compact_sectors']} ({len(df)} rows)")
    return df


# ─────────────────────────────────────────────────────────────
# 6. MCC Open Data Catalog — obligations & disbursements by sector
# ─────────────────────────────────────────────────────────────

_AFRICA_NAMES_LOWER = {n.lower() for n in AFRICA_CODE_TO_NAME.values()}


def _is_africa_mcc_country(name: str) -> bool:
    """Match MCC country names like 'Morocco', 'Morocco II', 'Ghana Power'.
    Uses startswith to avoid 'Niger' matching 'Nigeria', etc."""
    name_lower = str(name).lower().strip()
    return any(
        name_lower == africa or name_lower.startswith(africa + " ")
        for africa in _AFRICA_NAMES_LOWER
    )


def _parse_dollar_column(series: pd.Series) -> pd.Series:
    """Convert accounting-formatted dollar strings to floats.
    Handles '$8,639.14', '($95,881.00)' (negative), '$0.00'.
    Unparseable values become NaN (not 0) so they don't silently disappear."""
    s = series.astype(str).str.strip()
    negative = s.str.startswith("(")
    s = s.str.replace(r"[\$,\s\(\)]", "", regex=True)
    values = pd.to_numeric(s, errors="coerce")
    values = values.where(~negative, -values)
    return values


def scrape_mcc_open_data() -> pd.DataFrame:
    """
    Download MCC Open Data Catalog — cumulative obligations and
    disbursements by program and sector. Filter to Africa, aggregate
    by country / project / sector.
    Source: data.mcc.gov (FY24Q2)
    Saves: output/mcc_open_data_sectors.xlsx
    """
    log.info("── MCC Open Data: obligations by sector ──────────────")

    # MCC's CSV is Windows-1252 encoded (has em-dashes, smart quotes).
    # Try UTF-8 first, then fall back to cp1252 / latin-1.
    df = None
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(MCC_OPEN_DATA_URL, encoding=enc)
            log.info(f"  CSV decoded with {enc}")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.error(f"  Failed to download MCC Open Data CSV: {e}")
            return pd.DataFrame()
    if df is None:
        log.error("  Failed to decode MCC Open Data CSV with any known encoding")
        return pd.DataFrame()

    log.info(f"  Downloaded {len(df)} rows from data.mcc.gov")

    # Filter to African countries
    df_africa = df[df["MCCCountry Name"].apply(_is_africa_mcc_country)].copy()
    log.info(f"  Africa rows: {len(df_africa)}")

    if df_africa.empty:
        return pd.DataFrame()

    # Parse dollar columns
    df_africa["Commitment"]   = _parse_dollar_column(df_africa["Commitment"])
    df_africa["Disbursement"] = _parse_dollar_column(df_africa["Disbursement"])

    # Aggregate by country, fund, project, sector
    agg = (
        df_africa.groupby(
            ["MCCCountry Name", "Fund Name", "Project Name", "USG Sector Name"],
            dropna=False,
        )
        .agg(
            Total_Commitment=("Commitment", "sum"),
            Total_Disbursement=("Disbursement", "sum"),
        )
        .reset_index()
        .sort_values(["MCCCountry Name", "Total_Disbursement"], ascending=[True, False])
    )

    agg.columns = [
        "Country", "Fund", "Project", "USG Sector",
        "Total Commitment (USD)", "Total Disbursement (USD)",
    ]
    agg["Total Commitment (USD millions)"]   = (agg["Total Commitment (USD)"]   / 1_000_000).round(2)
    agg["Total Disbursement (USD millions)"] = (agg["Total Disbursement (USD)"] / 1_000_000).round(2)

    agg.to_excel(FILES["mcc_open_data_sectors"], index=False)
    log.info(f"  Africa sector data saved → {FILES['mcc_open_data_sectors']} ({len(agg)} rows)")
    return agg


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 55)
    log.info("  MCC Africa Data Collection")
    log.info("=" * 55)

    df_awards          = scrape_usaspending_awards()
    df_recipients      = scrape_usaspending_recipients()
    df_grants          = scrape_usaspending_mcc_grants()
    df_sam             = scrape_samgov()
    df_countries       = scrape_mcc_country_pages()
    df_compact_sectors = scrape_mcc_compact_sectors()
    df_open_data       = scrape_mcc_open_data()

    log.info("\n" + "=" * 55)
    log.info("  SUMMARY")
    log.info("=" * 55)
    log.info(f"  USASpending Africa awards : {len(df_awards)} rows")
    log.info(f"  MCC Africa grants         : {len(df_grants)} rows")
    log.info(f"  Top MCC recipients        : {len(df_recipients)} firms")
    log.info(f"  SAM.gov opportunities     : {len(df_sam)} rows")
    log.info(f"  MCC country pages         : {len(df_countries)} countries")
    log.info(f"  Compact sector rows       : {len(df_compact_sectors)} rows")
    log.info(f"  Open Data sector rows     : {len(df_open_data)} rows")
    log.info("\n  Output files:")
    for key, path in FILES.items():
        if path.exists():
            log.info(f"    {path.relative_to(path.parent.parent)}")


if __name__ == "__main__":
    main()
