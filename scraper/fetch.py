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

        # Step 1 — Load portal
        log.info("Loading portal...")
        await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(4000)

        # Step 2 — Dismiss popups
        for sel in ['[aria-label="Close"]', 'button:has-text("Close")',
                    'button:has-text("No thanks")', '.modal-close']:
            try:
                await page.click(sel, timeout=2000)
                log.info("Dismissed popup: %s", sel)
            except Exception:
                pass

        await page.screenshot(path="/tmp/step1_home.png")
        log.info("Title: %s", await page.title())

        # Step 3 — Click Advanced Search
        log.info("Clicking Advanced Search...")
        for sel in ['a:has-text("Advanced Search")', 'span:has-text("Advanced Search")',
                    'button:has-text("Advanced")']:
            try:
                await page.click(sel, timeout=5000)
                await page.wait_for_timeout(3000)
                log.info("Clicked Advanced Search")
                break
            except Exception:
                pass

        await page.screenshot(path="/tmp/step2_advanced.png")

        # Step 4 — Fill date range using EXACT IDs from the logs
        # Logs showed: id=recordedDateRange-start and id=recordedDateRange-end
        dt_from = datetime.strptime(date_from, "%m/%d/%Y")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y")
        fmt_from = f"{dt_from.month}/{dt_from.day}/{dt_from.year}"
        fmt_to   = f"{dt_to.month}/{dt_to.day}/{dt_to.year}"
        log.info("Filling dates: %s → %s", fmt_from, fmt_to)

        date_filled = False
        try:
            start_el = await page.wait_for_selector(
                "#recordedDateRange-start", timeout=10_000
            )
            end_el = await page.query_selector("#recordedDateRange-end")
            if start_el and end_el:
                await start_el.triple_click()
                await page.wait_for_timeout(200)
                await start_el.type(fmt_from, delay=80)
                log.info("Filled start date: %s", fmt_from)

                await end_el.triple_click()
                await page.wait_for_timeout(200)
                await end_el.type(fmt_to, delay=80)
                log.info("Filled end date: %s", fmt_to)
                date_filled = True
        except Exception as e:
            log.warning("Date fill with exact IDs failed: %s", e)

        if not date_filled:
            log.warning("Trying fallback date selectors...")
            for s, e in [("#dateRangeStart","#dateRangeEnd"),
                         ('[placeholder*="start" i]','[placeholder*="end" i]')]:
                try:
                    se = await page.query_selector(s)
                    ee = await page.query_selector(e)
                    if se and ee:
                        await se.triple_click()
                        await se.type(fmt_from, delay=80)
                        await ee.triple_click()
                        await ee.type(fmt_to, delay=80)
                        date_filled = True
                        log.info("Dates filled via fallback: %s", s)
                        break
                except Exception:
                    pass

        await page.screenshot(path="/tmp/step3_dates_filled.png")

        # Step 5 — Add each doc type using the docTypes-input field
        # Logs showed: id=docTypes-input placeholder=Filter Document Types
        log.info("Adding document type filters...")
        doc_types_to_search = [
            "LIS PENDENS", "NOTICE OF FORECLOSURE", "JUDGMENT",
            "LIEN", "IRS LIEN", "TAX DEED", "PROBATE",
            "MECHANIC LIEN", "HOA LIEN", "FEDERAL LIEN",
        ]
        try:
            doc_input = await page.query_selector("#docTypes-input")
            if doc_input:
                for dtype in doc_types_to_search:
                    try:
                        await doc_input.click()
                        await doc_input.fill(dtype)
                        await page.wait_for_timeout(1000)
                        # Click first suggestion in dropdown
                        for drop_sel in [
                            '[role="option"]:first-child',
                            '.autocomplete-suggestion:first-child',
                            'li[role="option"]:first-child',
                            'ul[role="listbox"] li:first-child',
                        ]:
                            try:
                                await page.click(drop_sel, timeout=2000)
                                log.info("Selected doc type: %s", dtype)
                                await page.wait_for_timeout(500)
                                break
                            except Exception:
                                pass
                    except Exception as e:
                        log.warning("Doc type '%s' failed: %s", dtype, e)
            else:
                log.warning("docTypes-input not found")
        except Exception as e:
            log.warning("Doc type filter error: %s", e)

        await page.screenshot(path="/tmp/step4_doctypes.png")

        # Step 6 — Submit search
        log.info("Submitting search...")
        submitted = False
        for sel in ['button:has-text("Search")', '#searchBtn',
                    'button[type="submit"]', 'input[type="submit"]']:
            try:
                await page.click(sel, timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=30_000)
                await page.wait_for_timeout(3000)
                log.info("Search submitted via: %s", sel)
                submitted = True
                break
            except Exception:
                pass

        if not submitted:
            log.warning("Submit button not found — trying Enter key")
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)

        await page.screenshot(path="/tmp/step5_results.png")
        log.info("URL after search: %s", page.url)

        # Step 7 — Extract results
        page_num = 1
        while True:
            html = await page.content()
            log.info("Page %d HTML length: %d", page_num, len(html))

            # Try table parse first
            page_records = self._parse_table(html)

            # Try React/card parse if no table
            if not page_records:
                page_records = await self._parse_react(page)

            # Log raw HTML snippet to see what's there
            if not page_records:
                log.warning("No records on page %d — HTML preview:", page_num)
                soup = BeautifulSoup(html, "lxml")
                body_text = soup.get_text()[:1000]
                log.info("Body text: %s", body_text)

            records.extend(page_records)
            log.info("Page %d: %d records (total: %d)",
                     page_num, len(page_records), len(records))

            if not page_records:
                break

            # Next page
            has_next = await self._next_page(page)
            if not has_next:
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
            if any(h in headers for h in ["grantor","grantee","instrument","type","recorded"]):
                table = tbl
                log.info("Table found, headers: %s", headers)
                break

        if not table:
            return records

        rows = table.find_all("tr")
        if len(rows) < 2:
            return records

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if any(x in h for x in ["instrument","doc #","number"]): col.setdefault("doc_num",  i)
            if "type" in h:                                            col.setdefault("doc_type", i)
            if any(x in h for x in ["recorded","filed","date"]):      col.setdefault("filed",    i)
            if "grantor" in h:                                         col.setdefault("grantor",  i)
            if "grantee" in h:                                         col.setdefault("grantee",  i)
            if any(x in h for x in ["amount","consideration"]):        col.setdefault("amount",   i)
            if "legal" in h:                                           col.setdefault("legal",    i)

        for row in rows[1:]:
            try:
                cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 2: continue
                def g(k,fb): idx=col.get(k,fb); return cells[idx] if 0<=idx<len(cells) else ""
                meta = _match_doc_type(g("doc_type",1))
                if not meta: continue
                clerk_url = self.BASE
                for a in row.find_all("a", href=True):
                    h = a["href"]
                    clerk_url = h if h.startswith("http") else f"{self.BASE}/{h.lstrip('/')}"
                    break
                records.append({
                    "doc_num":      g("doc_num",  0),
                    "doc_type":     meta["code"],
                    "filed":        g("filed",    2),
                    "cat":          meta["cat"],
                    "cat_label":    meta["label"],
                    "owner":        g("grantor",  3),
                    "grantee":      g("grantee",  4),
                    "amount":       g("amount",   5),
                    "legal":        g("legal",    6),
                    "clerk_url":    clerk_url,
                    "prop_address": "", "prop_city": "Dallas",
                    "prop_state":   "TX", "prop_zip": "",
                    "mail_address": "", "mail_city": "",
                    "mail_state":   "TX", "mail_zip": "",
                })
            except Exception as e:
                log.warning("Row error: %s", e)
        return records

    async def _parse_react(self, page) -> list[dict]:
        records: list[dict] = []
        try:
            # GovOS renders results as list items with data attributes
            items = await page.query_selector_all(
                '[class*="result"], [class*="record"], [class*="SearchResult"], '
                '[data-testid*="result"], tr[class*="row"], .search-result-item'
            )
            log.info("React items found: %d", len(items))
            for item in items:
                try:
                    text = await item.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if len(lines) < 2: continue
                    full = " ".join(lines).upper()
                    meta = None
                    for line in lines:
                        meta = _match_doc_type(line)
                        if meta: break
                    if not meta: continue
                    clerk_url = self.BASE
                    for a in await item.query_selector_all("a[href]"):
                        href = await a.get_attribute("href") or ""
                        if href:
                            clerk_url = href if href.startswith("http") else f"{self.BASE}{href}"
                            break
                    date_m  = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", full)
                    amt_m   = re.search(r"\$[\d,]+\.?\d*", full)
                    records.append({
                        "doc_num":      lines[0],
                        "doc_type":     meta["code"],
                        "filed":        date_m.group(1) if date_m else "",
                        "cat":          meta["cat"],
                        "cat_label":    meta["label"],
                        "owner":        lines[1] if len(lines) > 1 else "",
                        "grantee":      lines[2] if len(lines) > 2 else "",
                        "amount":       amt_m.group(0) if amt_m else "",
                        "legal":        "",
                        "clerk_url":    clerk_url,
                        "prop_address": "", "prop_city": "Dallas",
                        "prop_state":   "TX", "prop_zip": "",
                        "mail_address": "", "mail_city": "",
                        "mail_state":   "TX", "mail_zip": "",
                    })
                except Exception:
                    continue
        except Exception as e:
            log.warning("React parse error: %s", e)
        return records

    async def _next_page(self, page) -> bool:
        for sel in ['button:has-text("Next")', 'a:has-text("Next")',
                    '[aria-label="Next page"]', '[aria-label="next"]',
                    'button[title*="next" i]']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    disabled = await btn.get_attribute("disabled")
                    cls      = await btn.get_attribute("class") or ""
                    if disabled or "disabled" in cls: return False
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
