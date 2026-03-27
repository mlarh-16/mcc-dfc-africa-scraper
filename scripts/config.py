"""
scripts/config.py
-----------------
Central configuration for the MCC + DFC Africa scrapers.
Edit this file to change API keys, paths, and target countries.
"""

from pathlib import Path

# -- Project root paths -----------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
RAW_DIR    = BASE_DIR / "raw_data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

# -- API keys ---------------------------------------------------------
# Get your free SAM.gov key at: https://sam.gov/profile/details
SAM_GOV_API_KEY = "YOUR_SAM_GOV_API_KEY"

# -- Request settings -------------------------------------------------
REQUEST_TIMEOUT = 30
CRAWL_DELAY     = 1.0
API_DELAY       = 0.3
MAX_PAGES       = 20

# -- USASpending settings ---------------------------------------------
USASPENDING_URL  = "https://api.usaspending.gov/api/v2"
MCC_AGENCY_NAME  = "Millennium Challenge Corporation"
AWARD_TYPE_CODES = ["A", "B", "C", "D"]

# -- SAM.gov settings -------------------------------------------------
SAM_GOV_URL = "https://api.sam.gov/opportunities/v2/search"

# -- Scraper headers --------------------------------------------------
SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Research Bot - contact: your@email.com)"
}

# -- MCC country pages ------------------------------------------------
MCC_BASE_URL = "https://www.mcc.gov"

AFRICA_MCC_COUNTRIES = [
    ("ghana",        "https://www.mcc.gov/where-we-work/country/ghana"),
    ("tanzania",     "https://www.mcc.gov/where-we-work/country/tanzania"),
    ("morocco",      "https://www.mcc.gov/where-we-work/country/morocco"),
    ("mozambique",   "https://www.mcc.gov/where-we-work/country/mozambique"),
    ("lesotho",      "https://www.mcc.gov/where-we-work/country/lesotho"),
    ("senegal",      "https://www.mcc.gov/where-we-work/country/senegal"),
    ("namibia",      "https://www.mcc.gov/where-we-work/country/namibia"),
    ("ethiopia",     "https://www.mcc.gov/where-we-work/country/ethiopia"),
    ("mali",         "https://www.mcc.gov/where-we-work/country/mali"),
    ("rwanda",       "https://www.mcc.gov/where-we-work/country/rwanda"),
    ("benin",        "https://www.mcc.gov/where-we-work/country/benin"),
    ("burkina-faso", "https://www.mcc.gov/where-we-work/country/burkina-faso"),
    ("sierra-leone", "https://www.mcc.gov/where-we-work/country/sierra-leone"),
    ("cote-divoire", "https://www.mcc.gov/where-we-work/country/cote-divoire"),
    ("zambia",       "https://www.mcc.gov/where-we-work/country/zambia"),
    ("cabo-verde",   "https://www.mcc.gov/where-we-work/country/cabo-verde"),
    ("niger",        "https://www.mcc.gov/where-we-work/country/niger"),
    ("kenya",        "https://www.mcc.gov/where-we-work/country/kenya"),
    ("malawi",       "https://www.mcc.gov/where-we-work/country/malawi"),
    ("liberia",      "https://www.mcc.gov/where-we-work/country/liberia"),
    ("madagascar",   "https://www.mcc.gov/where-we-work/country/madagascar"),
    ("tunisia",      "https://www.mcc.gov/where-we-work/country/tunisia"),
]

AFRICA_KEYWORDS = [c for c, _ in AFRICA_MCC_COUNTRIES] + [
    "Africa", "Sub-Saharan", "MCA-", "Millennium Challenge Account"
]

SAM_SEARCH_KEYWORDS = [
    "Ghana", "Tanzania", "Morocco", "Mozambique", "Senegal",
    "Rwanda", "Ethiopia", "Zambia", "Lesotho", "Namibia",
    "Millennium Challenge", "MCA-", "MCC Africa",
]

# -- DFC settings -----------------------------------------------------
DFC_AGENCY_NAME    = "US International Development Finance Corporation"
DFC_BASE_URL       = "https://www.dfc.gov"
DFC_ACTIVE_PROJECTS  = "https://www.dfc.gov/what-we-do/active-projects"
DFC_WHERE_WE_WORK    = "https://www.dfc.gov/what-we-offer/work-with-us/where-we-work"
DFC_BOARD_MEETINGS   = "https://www.dfc.gov/who-we-are/our-people/board-directors"
DFC_IMPACT_PAGE      = "https://www.dfc.gov/what-we-do/investment-stories"
DFC_TRANSACTION_DATA = "https://www.dfc.gov/our-impact/transaction-data"
DFC_TRANSPARENCY     = "https://www.dfc.gov/our-impact/transparency"
DFC_REPORTS          = "https://www.dfc.gov/our-impact/reports"
DFC_NEWSROOM         = "https://www.dfc.gov/media/newsroom"
DFC_PRESS_RELEASES   = "https://www.dfc.gov/media/press-releases"
FEDERAL_REGISTER_API = "https://www.federalregister.gov/api/v1/documents.json"

# -- DFC sector pages -------------------------------------------------
DFC_SECTORS = {
    "Energy":                             "https://www.dfc.gov/our-work/energy",
    "Food Security & Agribusiness":       "https://www.dfc.gov/our-work/food-security-and-agribusiness",
    "Health":                             "https://www.dfc.gov/our-work/health",
    "Infrastructure & Critical Minerals": "https://www.dfc.gov/our-work/infrastructure-and-critical-minerals",
    "Small Business & Financial Services":"https://www.dfc.gov/our-work/small-business-and-financial-services",
}

# -- Federal Register search terms ------------------------------------
FEDERAL_REGISTER_TERMS = [
    "Development Finance Corporation",
    "OPIC Africa",
    "Overseas Private Investment Corporation",
    "DFC Nigeria", "DFC Kenya", "DFC Ghana",
    "DFC Mozambique", "DFC South Africa",
    "DFC Ethiopia", "DFC Rwanda", "DFC Senegal",
]

# -- African countries for DFC filtering ------------------------------
AFRICA_DFC_COUNTRIES = [
    "Nigeria", "Kenya", "Ghana", "Ethiopia", "South Africa",
    "Mozambique", "Tanzania", "Rwanda", "Senegal", "Morocco",
    "Egypt", "Tunisia", "Zambia", "Uganda", "Cote d'Ivoire",
    "Côte d'Ivoire", "DRC", "Congo", "Angola", "Cameroon",
    "Madagascar", "Malawi", "Niger", "Mali", "Burkina Faso",
    "Sierra Leone", "Liberia", "Guinea", "Benin", "Togo",
    "Mauritania", "Namibia", "Botswana", "Zimbabwe", "Lesotho",
    "Eswatini", "Djibouti", "Somalia", "Sudan", "Chad",
    "Equatorial Guinea", "Gabon", "Cabo Verde", "Mauritius",
    "Seychelles", "Comoros", "Eritrea", "Burundi",
]

# -- Output file names ------------------------------------------------
FILES = {
    # MCC
    "usaspending_all":        RAW_DIR    / "usaspending_mcc_all.xlsx",
    "usaspending_africa":     OUTPUT_DIR / "usaspending_mcc_africa.xlsx",
    "usaspending_recipients": OUTPUT_DIR / "usaspending_mcc_top_recipients.xlsx",
    "samgov":                 OUTPUT_DIR / "samgov_mcc_africa.xlsx",
    "mcc_countries":          OUTPUT_DIR / "mcc_country_pages.xlsx",
    # DFC
    "dfc_active_projects":    OUTPUT_DIR / "dfc_active_projects_africa.xlsx",
    "dfc_board_notices":      RAW_DIR    / "dfc_board_notices_raw.xlsx",
    "dfc_board_africa":       OUTPUT_DIR / "dfc_press_releases_africa.xlsx",
    "dfc_impact_stories":     OUTPUT_DIR / "dfc_impact_stories.xlsx",
    "dfc_federal_register":   OUTPUT_DIR / "dfc_federal_register_africa.xlsx",
    "dfc_usaspending":        OUTPUT_DIR / "usaspending_dfc_africa.xlsx",
    "dfc_usaspending_all":    RAW_DIR    / "usaspending_dfc_all.xlsx",
    "dfc_sectors":            OUTPUT_DIR / "dfc_sectors_africa.xlsx",
    # Shared
    "run_log":                OUTPUT_DIR / "run_log.txt",
}
