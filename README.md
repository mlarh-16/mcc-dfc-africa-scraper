# MCC + DFC Africa Data Scraper

A Python data pipeline that collects and consolidates U.S. government investment data in Africa from the **Millennium Challenge Corporation (MCC)** and the **U.S. International Development Finance Corporation (DFC)** into a single styled Excel workbook.

## What it does

The pipeline pulls from multiple official U.S. government sources, filters to the African continent (53 countries), and produces an Excel master workbook with ~25 sheets covering:

- **MCC** — contract awards, compact grants, recipients, country pages, compact sector breakdowns
- **DFC** — active projects, press releases, Federal Register notices, investment stories, sector pages, USASpending contracts

## Data sources

| Source | What it provides |
|---|---|
| [USASpending.gov API](https://api.usaspending.gov) | MCC contract awards + compact grants, DFC contracts |
| [MCC.gov](https://www.mcc.gov) | Country compact pages and compact component budget tables |
| [data.mcc.gov Open Data Catalog](https://data.mcc.gov) | Cumulative obligations & disbursements by program and sector |
| [DFC.gov](https://www.dfc.gov) | Active project data (FY24), impact stories, press releases, sector pages |
| [Federal Register API](https://www.federalregister.gov/developers/api/v1) | DFC/OPIC agency notices |
| [SAM.gov](https://sam.gov) | MCC procurement opportunities (requires free API key) |

## Project structure

```
mcc_data/
├── scripts/
│   ├── config.py           # country lists, API URLs, file paths
│   ├── mcc_scraper.py      # MCC data collection
│   ├── dfc_scraper.py      # DFC data collection
│   ├── consolidate.py      # builds the master Excel workbook
│   ├── run_all.py          # runs the full pipeline
│   └── logger.py
├── output/                 # final output files (Africa-filtered)
├── raw_data/               # unfiltered raw pulls
├── pyproject.toml
└── README.md
```

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/mlarh-16/mcc-dfc-africa-scraper.git
cd mcc-dfc-africa-scraper
```

### 2. Create and activate a virtual environment

**Windows (PowerShell):**
```powershell
python -m venv venv
& .\venv\Scripts\Activate.ps1
```

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -e .
```

### 4. (Optional) Add a SAM.gov API key

Get a free key at https://sam.gov/profile/details and set it in `scripts/config.py`:

```python
SAM_GOV_API_KEY = "your_key_here"
```

Without a key, the SAM.gov step is skipped — everything else works.

## Usage

Run the full pipeline (MCC scrape → DFC scrape → consolidate):

```bash
python -m scripts.run_all
```

Or run each step individually:

```bash
python -m scripts.mcc_scraper
python -m scripts.dfc_scraper
python -m scripts.consolidate
```

Expected run time: **5–15 minutes** depending on network conditions.

## Output

The pipeline produces a dated master workbook in `output/`:

```
output/MCC_DFC_Africa_Master_YYYY-MM-DD.xlsx
```

Plus individual source files in `output/` and `raw_data/`.

### Key sheets in the master workbook

**Summary & QA**
- `00 Summary` — high-level totals
- `00 QA Checks` — reconciliation checks between raw totals and summary tables

**MCC**
- Awards by Year / Country / Sector / Firm Country
- Top Recipients (Africa)
- All Africa Awards
- Grants by Year / Country
- All Africa Grants
- Sectors (MCC.gov) — scraped from compact program pages
- Sectors (Open Data) — from data.mcc.gov (country / fund / project / sector / obligations / disbursements)
- Country Page Summaries

**DFC**
- Projects by Year / Country / Sector
- Regional / Worldwide Projects
- Federal Register notices
- Press Releases
- Investment Stories
- Sector Stories

## Africa country coverage

All 54 AU-recognized sovereign states are supported across the scrapers. Country name matching handles common aliases (Ivory Coast / Côte d'Ivoire, Swaziland / Eswatini, Cape Verde / Cabo Verde, The Gambia / Gambia, etc.).

## Limitations

- **MCC compact grants** on USASpending are aggregate financial assistance records with no date fields populated — fiscal years are derived from Award ID patterns where possible, and unresolvable rows are flagged as "Not Specified".
- **MCC compact sector breakdowns** on mcc.gov program pages are only available for a subset of active/recent compacts. Older compacts' sector data lives in PDF documents and is not parsed. The Open Data sheet from data.mcc.gov provides complete coverage.
- **DFC project data** reflects the FY24 Annual Project Data (published annually). More recent projects may not appear until the next release.
- **Multi-country contracts** on USASpending are attributed to a single place-of-performance code, which can inflate that country's totals. DFC multi-country projects are split proportionally.

## License

MIT — see [LICENSE](LICENSE)
