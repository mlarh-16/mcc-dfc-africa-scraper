"""
scripts/consolidate.py
───────────────────────
Builds a single master Excel workbook from all MCC and DFC
output files. Includes a summary dashboard sheet.

Run from project root:
  python -m scripts.consolidate
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from openpyxl import load_workbook
from openpyxl.styles import (
    PatternFill, Font, Alignment, Border, Side
)
from openpyxl.utils import get_column_letter

from scripts.config import FILES, OUTPUT_DIR
from scripts.logger import get_logger

log = get_logger()

TODAY       = datetime.now(timezone.utc).strftime("%Y-%m-%d")
MASTER_PATH = OUTPUT_DIR / f"MCC_DFC_Africa_Master_{TODAY}.xlsx"

# ── Colour palette ────────────────────────────────────────────
BLUE_DARK   = "0C447C"   # header background
BLUE_MID    = "185FA5"   # MCC accent
TEAL_DARK   = "0F6E56"   # DFC accent
WHITE       = "FFFFFF"
LIGHT_BLUE  = "E6F1FB"   # MCC row fill
LIGHT_TEAL  = "E1F5EE"   # DFC row fill
LIGHT_GRAY  = "F1EFE8"   # summary row fill
AMBER       = "BA7517"   # highlight


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _header_fill(hex_color: str) -> PatternFill:
    return PatternFill("solid", fgColor=hex_color)


def _header_font(bold: bool = True, color: str = WHITE) -> Font:
    return Font(bold=bold, color=color, size=11)


def _border() -> Border:
    thin = Side(style="thin", color="D3D1C7")
    return Border(left=thin, right=thin, top=thin, bottom=thin)


def _style_header_row(ws, row: int, fill_color: str, font_color: str = WHITE):
    """Apply header styling to a worksheet row."""
    for cell in ws[row]:
        if cell.value is not None:
            cell.fill    = _header_fill(fill_color)
            cell.font    = _header_font(color=font_color)
            cell.border  = _border()
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _style_data_rows(ws, start_row: int, fill_color: str):
    """Apply alternating fill to data rows."""
    for i, row in enumerate(ws.iter_rows(min_row=start_row, max_row=ws.max_row)):
        for cell in row:
            cell.border = _border()
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            if i % 2 == 0:
                cell.fill = _header_fill(fill_color)


def _auto_width(ws, min_width: int = 12, max_width: int = 50):
    """Auto-fit column widths."""
    for col in ws.columns:
        max_len = max(
            (len(str(cell.value)) if cell.value else 0 for cell in col),
            default=min_width
        )
        col_letter = get_column_letter(col[0].column)
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, min_width), max_width)


def _freeze(ws, cell: str = "A2"):
    ws.freeze_panes = cell


def _load(key: str) -> pd.DataFrame:
    """Safely load an output file, return empty df if missing."""
    path = FILES.get(key)
    if not path or not path.exists():
        log.warning(f"  File not found: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_excel(path)
        log.info(f"  Loaded {key}: {len(df)} rows")
        return df
    except Exception as e:
        log.error(f"  Error loading {key}: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# MCC year-by-year analysis
# ─────────────────────────────────────────────────────────────

def _mcc_by_year(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Start Date" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce")
    df["Year"] = df["Start Date"].dt.year
    by_year = (
        df.groupby("Year")
        .agg(
            Contracts=("Award ID", "count"),
            Total_USD=("Award Amount", "sum"),
        )
        .reset_index()
    )
    by_year["Total_USD_M"] = by_year["Total_USD"].apply(
        lambda x: round(x / 1_000_000, 2)
    )
    by_year.rename(columns={
        "Year":        "Fiscal Year",
        "Contracts":   "Number of Contracts",
        "Total_USD":   "Total Award (USD)",
        "Total_USD_M": "Total Award (USD millions)",
    }, inplace=True)
    return by_year


def _mcc_top_recipients_africa(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    if df.empty or "Recipient Name" not in df.columns:
        return pd.DataFrame()
    top = (
        df.groupby("Recipient Name")["Award Amount"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )
    top.columns = ["Recipient Name", "Total Award (USD)"]
    top["Total Award (USD millions)"] = top["Total Award (USD)"].apply(
        lambda x: round(x / 1_000_000, 2)
    )
    top["Rank"] = range(1, len(top) + 1)
    return top[["Rank", "Recipient Name", "Total Award (USD)", "Total Award (USD millions)"]]


def _mcc_by_country(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Place of Performance Country Name" not in df.columns:
        return pd.DataFrame()
    by_country = (
        df.groupby("Place of Performance Country Name")
        .agg(
            Contracts=("Award ID", "count"),
            Total_USD=("Award Amount", "sum"),
        )
        .sort_values("Total_USD", ascending=False)
        .reset_index()
    )
    by_country.columns = [
        "Country", "Number of Contracts", "Total Award (USD)"
    ]
    by_country["Total Award (USD millions)"] = by_country["Total Award (USD)"].apply(
        lambda x: round(x / 1_000_000, 2)
    )
    return by_country


# ─────────────────────────────────────────────────────────────
# DFC year-by-year analysis
# ─────────────────────────────────────────────────────────────

def _dfc_by_year(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Fiscal Year" not in df.columns:
        return pd.DataFrame()
    by_year = (
        df.groupby("Fiscal Year")
        .agg(
            Projects=("Project Name", "count"),
            Total_Committed=("Committed", "sum"),
        )
        .reset_index()
        .sort_values("Fiscal Year")
    )
    by_year["Total_Committed_M"] = by_year["Total_Committed"].apply(
        lambda x: round(x / 1_000_000, 2) if pd.notna(x) else 0
    )
    by_year.rename(columns={
        "Projects":          "Number of Projects",
        "Total_Committed":   "Total Committed (USD)",
        "Total_Committed_M": "Total Committed (USD millions)",
    }, inplace=True)
    return by_year


def _dfc_by_country(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Country" not in df.columns:
        return pd.DataFrame()
    by_country = (
        df.groupby("Country")
        .agg(
            Projects=("Project Name", "count"),
            Total_Committed=("Committed", "sum"),
        )
        .sort_values("Total_Committed", ascending=False)
        .reset_index()
    )
    by_country.columns = [
        "Country", "Number of Projects", "Total Committed (USD)"
    ]
    by_country["Total Committed (USD millions)"] = by_country["Total Committed (USD)"].apply(
        lambda x: round(x / 1_000_000, 2) if pd.notna(x) else 0
    )
    return by_country


def _dfc_by_sector(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "NAICS Sector" not in df.columns:
        return pd.DataFrame()
    by_sector = (
        df.groupby("NAICS Sector")
        .agg(
            Projects=("Project Name", "count"),
            Total_Committed=("Committed", "sum"),
        )
        .sort_values("Total_Committed", ascending=False)
        .reset_index()
    )
    by_sector.columns = [
        "NAICS Sector", "Number of Projects", "Total Committed (USD)"
    ]
    by_sector["Total Committed (USD millions)"] = by_sector["Total Committed (USD)"].apply(
        lambda x: round(x / 1_000_000, 2) if pd.notna(x) else 0
    )
    return by_sector


# ─────────────────────────────────────────────────────────────
# Summary sheet builder
# ─────────────────────────────────────────────────────────────

def _build_summary(
    df_mcc: pd.DataFrame,
    df_dfc: pd.DataFrame,
    df_recipients: pd.DataFrame,
    df_sectors: pd.DataFrame,
) -> pd.DataFrame:
    """Build a high-level summary table."""
    rows = []

    # MCC stats
    if not df_mcc.empty:
        rows.append({
            "Category":    "MCC Africa",
            "Metric":      "Total contracts (USASpending)",
            "Value":       len(df_mcc),
            "USD":         df_mcc["Award Amount"].sum() if "Award Amount" in df_mcc.columns else 0,
            "Notes":       f"Start dates: {df_mcc['Start Date'].min()} to {df_mcc['Start Date'].max()}" if "Start Date" in df_mcc.columns else "",
        })

    if not df_recipients.empty:
        rows.append({
            "Category": "MCC Africa",
            "Metric":   "Top recipient firms tracked",
            "Value":    len(df_recipients),
            "USD":      0,
            "Notes":    "From USASpending recipient breakdown",
        })

    # DFC stats
    if not df_dfc.empty:
        rows.append({
            "Category": "DFC Africa",
            "Metric":   "Total projects (FY2024 database)",
            "Value":    len(df_dfc),
            "USD":      df_dfc["Committed"].sum() if "Committed" in df_dfc.columns else 0,
            "Notes":    "Source: DFC Annual Project Data FY2024",
        })

    if not df_sectors.empty:
        rows.append({
            "Category": "DFC Africa",
            "Metric":   "Sector story pages scraped",
            "Value":    len(df_sectors),
            "USD":      0,
            "Notes":    "Energy, Agri, Health, Infrastructure, Finance",
        })

    df_summary = pd.DataFrame(rows)
    if not df_summary.empty:
        df_summary["USD (millions)"] = df_summary["USD"].apply(
            lambda x: round(x / 1_000_000, 2) if pd.notna(x) and x > 0 else ""
        )
    return df_summary


# ─────────────────────────────────────────────────────────────
# Write master workbook
# ─────────────────────────────────────────────────────────────

def build_master_workbook():
    log.info("=" * 55)
    log.info("  Building master Excel workbook")
    log.info("=" * 55)

    # ── Load all source files ─────────────────────────────────
    df_mcc_africa      = _load("usaspending_africa")
    df_mcc_recipients  = _load("usaspending_recipients")
    df_mcc_countries   = _load("mcc_countries")
    df_dfc_projects    = _load("dfc_active_projects")
    df_dfc_stories     = _load("dfc_impact_stories")
    df_dfc_press       = _load("dfc_board_africa")
    df_dfc_fr          = _load("dfc_federal_register")
    df_dfc_spending    = _load("dfc_usaspending")
    df_dfc_sectors     = _load("dfc_sectors")

    # ── Derived tables ────────────────────────────────────────
    df_mcc_by_year     = _mcc_by_year(df_mcc_africa)
    df_mcc_top         = _mcc_top_recipients_africa(df_mcc_africa)
    df_mcc_by_country  = _mcc_by_country(df_mcc_africa)
    df_dfc_by_year     = _dfc_by_year(df_dfc_projects)
    df_dfc_by_country  = _dfc_by_country(df_dfc_projects)
    df_dfc_by_sector   = _dfc_by_sector(df_dfc_projects)
    df_summary         = _build_summary(
        df_mcc_africa, df_dfc_projects,
        df_mcc_recipients, df_dfc_sectors
    )

    # ── Sheet definitions ─────────────────────────────────────
    # (sheet_name, dataframe, header_color, row_fill_color)
    sheets = [
        ("00 Summary",                   df_summary,          BLUE_DARK,  LIGHT_GRAY),
        ("MCC — Awards by Year",          df_mcc_by_year,      BLUE_MID,   LIGHT_BLUE),
        ("MCC — Awards by Country",       df_mcc_by_country,   BLUE_MID,   LIGHT_BLUE),
        ("MCC — Top Recipients (Africa)", df_mcc_top,          BLUE_MID,   LIGHT_BLUE),
        ("MCC — All Africa Awards",       df_mcc_africa,       BLUE_MID,   LIGHT_BLUE),
        ("MCC — All Recipients",          df_mcc_recipients,   BLUE_MID,   LIGHT_BLUE),
        ("MCC — Country Pages",           df_mcc_countries,    BLUE_MID,   LIGHT_BLUE),
        ("DFC — Projects by Year",        df_dfc_by_year,      TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Projects by Country",     df_dfc_by_country,   TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Projects by Sector",      df_dfc_by_sector,    TEAL_DARK,  LIGHT_TEAL),
        ("DFC — All Africa Projects",     df_dfc_projects,     TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Investment Stories",      df_dfc_stories,      TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Press Releases",          df_dfc_press,        TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Federal Register",        df_dfc_fr,           TEAL_DARK,  LIGHT_TEAL),
        ("DFC — USASpending Contracts",   df_dfc_spending,     TEAL_DARK,  LIGHT_TEAL),
        ("DFC — Sector Stories",          df_dfc_sectors,      TEAL_DARK,  LIGHT_TEAL),
    ]

    # ── Write to Excel ────────────────────────────────────────
    with pd.ExcelWriter(MASTER_PATH, engine="openpyxl") as writer:
        for sheet_name, df, header_color, row_color in sheets:
            if df.empty:
                log.warning(f"  Skipping empty sheet: {sheet_name}")
                continue
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            log.info(f"  Written: '{sheet_name}' ({len(df)} rows)")

    # ── Apply styling ─────────────────────────────────────────
    log.info("  Applying styles...")
    wb = load_workbook(MASTER_PATH)

    for sheet_name, df, header_color, row_color in sheets:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        _style_header_row(ws, row=1, fill_color=header_color)
        _style_data_rows(ws, start_row=2, fill_color=row_color)
        _auto_width(ws)
        _freeze(ws)
        ws.row_dimensions[1].height = 30

    wb.save(MASTER_PATH)
    log.info(f"\n  Master workbook saved -> {MASTER_PATH}")
    written = [name for name, df, _, _ in sheets if not df.empty]
    log.info(f"  Sheets written: {len(written)}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    build_master_workbook()
    log.info("\n  Done! Open the master workbook in Excel.")
    log.info(f"  File: {MASTER_PATH.name}")


if __name__ == "__main__":
    main()
