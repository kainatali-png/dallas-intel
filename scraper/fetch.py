"""
Dallas County, TX — Motivated Seller Lead Scraper
Portal: https://dallas.tx.ds.search.govos.com/
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    import sys
    sys.exit("pip install playwright && python -m playwright install chromium")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL     = "https://dallas.tx.ds.search.govos.com/"
OUTPUT_PATHS  = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH  = Path("data/ghl_export.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dallas_scraper")

TARGET_DOC_TYPES: dict[str, dict] = {
    "LP":       {"label": "Lis Pendens",            "cat": "foreclosure"},
    "NOFC":     {"label": "Notice of Foreclosure",  "cat": "foreclosure"},
    "RELLP":    {"label": "Release Lis Pendens",    "cat": "foreclosure"},
    "TAXDEED":  {"label": "Tax Deed",               "cat": "tax"},
    "JUD":      {"label": "Judgment",               "cat": "judgment"},
    "CCJ":      {"label": "Certified Judgment",     "cat": "judgment"},
    "DRJUD":    {"label": "Domestic Judgment",      "cat": "judgment"},
    "LNCORPTX": {"label": "Corp Tax Lien",          "cat": "lien"},
    "LNIRS":    {"label": "IRS Lien",               "cat": "lien"},
    "LNFED":    {"label": "Federal Lien",           "cat": "lien"},
    "LN":       {"label": "Lien",                   "cat": "lien"},
    "LNMECH":   {"label": "Mechanic Lien",          "cat": "lien"},
    "LNHOA":    {"label": "HOA Lien",               "cat": "lien"},
    "MEDLN":    {"label": "Medicaid Lien",          "cat": "lien"},
    "PRO":      {"label": "Probate Document",       "cat": "probate"},
    "NOC":      {"label": "Notice of Commencement", "cat": "other"},
}

LABEL_FUZZY: dict[str, str] = {
    "LIS PENDENS":           "LP",
    "NOTICE OF FORECLOSURE": "NOFC",
    "RELEASE LIS PENDENS":   "RELLP",
    "TAX DEED":              "TAXDEED",
    "JUDGMENT":              "JUD",
    "CERTIFIED JUDGMENT":    "CCJ",
    "DOMESTIC":              "DRJUD",
    "CORP TAX":              "LNCORPTX",
    "IRS":                   "LNIRS",
    "FEDERAL LIEN":          "LNFED",
    "MECHANIC":              "LNMECH",
    "HOA":                   "LNHOA",
    "MEDICAID":              "MEDLN",
    "PROBATE":               "PRO",
    "COMMENCEMENT":          "NOC",
}

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
    "Motivated Seller Flags", "Source", "Public Records URL",
]

STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',   { get: () => [1,2,3,4,5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
}
"""


def compute_score(rec: dict) -> tuple[list[str], int]:
    flags: list[str] = []
    cat  = rec.get("cat", "")
    code = rec.get("doc_type", "")
    if cat == "foreclosure" and code != "RELLP":
        flags.append("Lis pendens")
    if code == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat == "tax":
        flags.append("Tax lien")
    if code == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    owner = rec.get("owner", "")
    if owner and re.search(r"\b(LLC|CORP|INC|LTD|TRUST|LP|LLP|PLLC)\b", owner, re.I):
        flags.append("LLC / corp owner")
    try:
        filed_dt = datetime.strptime(rec.get("filed", ""), "%m/%d/%Y")
        if (datetime.utcnow() - filed_dt).days <= 7:
            flags.append("New this week")
    except ValueError:
        pass
    score = 30
    core  = [f for f in flags if f not in ("New this week", "LLC / corp owner")]
    score += 10 * len(core)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    try:
        amt = float(re.sub(r"[^\d.]", "", rec.get("amount", "") or ""))
        if amt > 100_000: score += 15
        elif amt > 50_000: score += 10
    except ValueError:
        pass
    if "New this week" in flags: score += 5
    if rec.get("prop_address"):
        flags.append("Has address")
        score += 5
    return flags, min(score, 100)


def _match_doc_type(raw: str) -> Optional[dict]:
    upper = raw.upper().strip()
    if upper in TARGET_DOC_TYPES:
        return {"code": upper, **TARGET_DOC_TYPES[upper]}
    for fragment, code in LABEL_FUZZY.items():
        if fragment in upper:
            return {"code": code, **TARGET_DOC_TYPES[code]}
    return None


class ClerkScraper:

    BASE = "https://dallas.tx.ds.search.govos.com"

    # Search directly via URL — the portal URL reveals the exact format
    # From logs: /results?department=RP&recordedDateRange=20010129%2C20260116&searchType=advancedSearch
    # We just need to build the correct URL with our date range
    SEARCH_URL = (
        "https://dallas.tx.ds.search.govos.com/results?"
        "department=RP"
        "&recordedDateRange={date_from}%2C{date_to}"
        "&searchType=advancedSearch"
        "&docTypes={doc_types}"
    )

    async def fetch_all(self, date_from: str, date_to: str) -> list[dict]:
        records: list[dict] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            await ctx.add_init_script(STEALTH_JS)
            page = await ctx.new_page()
            try:
                records = await self._scrape(page, date_from, date_to)
            except Exception as e:
                log.error("Fatal: %s", e)
                import traceback; log.error(traceback.format_exc())
                try: await page.screenshot(path="/tmp/fatal.png")
                except Exception: pass
            finally:
                await browser.close()
        return records

    async def _scrape(self, page, date_from: str, date_to: str) -> list[dict]:
        records: list[dict] = []

        # Convert dates to YYYYMMDD format used in the URL
        dt_from = datetime.strptime(date_from, "%m/%d/%Y")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y")
        url_from = dt_from.strftime("%Y%m%d")
        url_to   = dt_to.strftime("%Y%m%d")

        log.info("Date range: %s → %s (URL format: %s → %s)",
                 date_from, date_to, url_from, url_to)

        # Build search URL with our exact date range
        # No doc type filter first — get ALL records in date range, filter after
        search_url = (
            f"{self.BASE}/results?"
            f"department=RP"
            f"&recordedDateRange={url_from}%2C{url_to}"
            f"&searchType=advancedSearch"
        )

        log.info("Loading search URL: %s", search_url)
        await page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(5000)

        # Dismiss any popup
        for sel in ['[aria-label="Close"]', 'button:has-text("Close")',
                    'button:has-text("No thanks")']:
            try:
                await page.click(sel, timeout=2000)
            except Exception:
                pass

        await page.screenshot(path="/tmp/results_page.png")
        log.info("URL loaded: %s", page.url)
        log.info("Title: %s", await page.title())

        # Collect all pages
        page_num = 1
        while True:
            html = await page.content()
            log.info("Page %d — HTML length: %d chars", page_num, len(html))

            page_records = self._parse_table(html)
            records.extend(page_records)
            log.info("Page %d: %d records (total: %d)",
                     page_num, len(page_records), len(records))

            if not page_records:
                # Log body text to understand what's on the page
                soup = BeautifulSoup(html, "lxml")
                log.info("Page text preview: %s", soup.get_text()[:800])
                break

            has_next = await self._next_page(page)
            if not has_next:
                log.info("No more pages")
                break
            page_num += 1
            await page.wait_for_timeout(2000)

        return records

    def _parse_table(self, html: str) -> list[dict]:
        soup    = BeautifulSoup(html, "lxml")
        records = []
        table   = None

        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(h in headers for h in ["grantor","grantee","doc type","recorded date","doc number"]):
                table = tbl
                log.info("Table headers: %s", headers)
                break

        if not table:
            log.warning("No results table found")
            return records

        rows = table.find_all("tr")
        if len(rows) < 2:
            return records

        # Map headers to column indices
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if any(x in h for x in ["doc number","instrument","doc #"]): col.setdefault("doc_num",  i)
            if "doc type" in h or h == "type":                            col.setdefault("doc_type", i)
            if "recorded" in h or "filed" in h:                          col.setdefault("filed",    i)
            if "grantor" in h:                                            col.setdefault("grantor",  i)
            if "grantee" in h:                                            col.setdefault("grantee",  i)
            if "amount" in h or "consideration" in h:                    col.setdefault("amount",   i)
            if "legal" in h:                                              col.setdefault("legal",    i)
            if "town" in h or "city" in h:                               col.setdefault("town",     i)
            if "address" in h:                                            col.setdefault("address",  i)

        log.info("Column map: %s", col)

        for row in rows[1:]:
            try:
                cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 2:
                    continue

                def g(k, fb):
                    idx = col.get(k, fb)
                    return cells[idx] if 0 <= idx < len(cells) else ""

                raw_type = g("doc_type", 1)
                meta = _match_doc_type(raw_type)
                if not meta:
                    # Log skipped types so we can add them
                    if raw_type:
                        log.info("Skipped doc type: %s", raw_type)
                    continue

                # Get direct link to document
                clerk_url = self.BASE
                for a in row.find_all("a", href=True):
                    h = a["href"]
                    clerk_url = h if h.startswith("http") else f"{self.BASE}/{h.lstrip('/')}"
                    break

                # Extract property address from legal description or address column
                legal       = g("legal",   -1)
                prop_addr   = g("address", -1)
                town        = g("town",    -1)

                # Try to parse street address from legal description
                street = self._extract_address(legal) if not prop_addr else prop_addr

                records.append({
                    "doc_num":      g("doc_num",  0),
                    "doc_type":     meta["code"],
                    "filed":        g("filed",    2),
                    "cat":          meta["cat"],
                    "cat_label":    meta["label"],
                    "owner":        g("grantor",  3),
                    "grantee":      g("grantee",  4),
                    "amount":       g("amount",   5),
                    "legal":        legal,
                    "clerk_url":    clerk_url,
                    "prop_address": street,
                    "prop_city":    town or "Dallas",
                    "prop_state":   "TX",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "TX",
                    "mail_zip":     "",
                })
            except Exception as e:
                log.warning("Row parse error: %s", e)

        log.info("Parsed %d matching records from table", len(records))
        return records

    def _extract_address(self, legal: str) -> str:
        """Try to extract a street address from a legal description."""
        if not legal:
            return ""
        # Match patterns like "123 MAIN ST" or "456 ELM AVE"
        m = re.search(
            r"\b(\d+\s+[A-Z][A-Z0-9\s]{2,30}"
            r"(?:ST|AVE|BLVD|DR|LN|RD|CT|WAY|PL|TRL|CIR|HWY)\b)",
            legal.upper()
        )
        return m.group(1).strip() if m else ""

    async def _next_page(self, page) -> bool:
        for sel in [
            'button:has-text("Next")',
            'a:has-text("Next")',
            '[aria-label="Next page"]',
            '[aria-label="next"]',
            'button[title*="next" i]',
            '.pagination-next',
            '[data-testid*="next"]',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    disabled = await btn.get_attribute("disabled")
                    cls      = await btn.get_attribute("class") or ""
                    if disabled or "disabled" in cls:
                        return False
                    await btn.click(timeout=10_000)
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                    await page.wait_for_timeout(1500)
                    return True
            except Exception:
                pass
        return False


def save_records(records: list[dict], date_from: str, date_to: str) -> list[dict]:
    enriched = []
    for rec in records:
        try:
            flags, score = compute_score(rec)
            rec["flags"] = flags
            rec["score"] = score
        except Exception:
            rec["flags"] = []
            rec["score"] = 30
        enriched.append(rec)
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Dallas County Clerk — Official Public Records",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(enriched),
        "with_address": sum(1 for r in enriched if r.get("prop_address")),
        "records":      enriched,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Saved %d records → %s", len(enriched), path)
    return enriched


def split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split(None, 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (full, "")


def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        writer.writeheader()
        for rec in records:
            try:
                first, last = split_name(rec.get("owner", ""))
                writer.writerow({
                    "First Name":             first,
                    "Last Name":              last,
                    "Mailing Address":        rec.get("mail_address", ""),
                    "Mailing City":           rec.get("mail_city", ""),
                    "Mailing State":          rec.get("mail_state", "TX"),
                    "Mailing Zip":            rec.get("mail_zip", ""),
                    "Property Address":       rec.get("prop_address", ""),
                    "Property City":          rec.get("prop_city", "Dallas"),
                    "Property State":         rec.get("prop_state", "TX"),
                    "Property Zip":           rec.get("prop_zip", ""),
                    "Lead Type":              rec.get("cat_label", ""),
                    "Document Type":          rec.get("doc_type", ""),
                    "Date Filed":             rec.get("filed", ""),
                    "Document Number":        rec.get("doc_num", ""),
                    "Amount/Debt Owed":       rec.get("amount", ""),
                    "Seller Score":           rec.get("score", 30),
                    "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                    "Source":                 "Dallas County Clerk",
                    "Public Records URL":     rec.get("clerk_url", ""),
                })
            except Exception as e:
                log.warning("CSV error: %s", e)
    log.info("GHL CSV: %d rows → %s", len(records), path)


async def main():
    today     = datetime.now(tz=timezone.utc)
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info("=" * 50)
    log.info("Dallas County Lead Scraper")
    log.info("Date range: %s – %s", date_from, date_to)
    log.info("=" * 50)
    scraper  = ClerkScraper()
    raw      = await scraper.fetch_all(date_from, date_to)
    log.info("Total raw records: %d", len(raw))
    enriched = save_records(raw, date_from, date_to)
    export_ghl_csv(enriched, GHL_CSV_PATH)
    log.info("Done. %d leads saved.", len(enriched))


if __name__ == "__main__":
    asyncio.run(main())
