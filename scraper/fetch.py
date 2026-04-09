"""
Dallas County, TX — Motivated Seller Lead Scraper
Portal: https://dallas.tx.ds.search.govos.com/
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
except ImportError:
    import sys
    sys.exit("pip install dbfread")

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    import sys
    sys.exit("pip install playwright && python -m playwright install chromium")

# ─── CONFIG ───────────────────────────────────────────────────────────────────

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL     = "https://dallas.tx.ds.search.govos.com/"
PARCEL_URL    = "https://www.taxnetusa.com/data/"
OUTPUT_PATHS  = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH  = Path("data/ghl_export.csv")
PARCEL_CACHE  = Path("/tmp/dallas_parcels.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dallas_scraper")

# ─── DOCUMENT TYPE MAPPING ────────────────────────────────────────────────────

TARGET_DOC_TYPES: dict[str, dict] = {
    "LP":       {"label": "Lis Pendens",             "cat": "foreclosure"},
    "NOFC":     {"label": "Notice of Foreclosure",   "cat": "foreclosure"},
    "RELLP":    {"label": "Release Lis Pendens",     "cat": "foreclosure"},
    "TAXDEED":  {"label": "Tax Deed",                "cat": "tax"},
    "JUD":      {"label": "Judgment",                "cat": "judgment"},
    "CCJ":      {"label": "Certified Judgment",      "cat": "judgment"},
    "DRJUD":    {"label": "Domestic Judgment",       "cat": "judgment"},
    "LNCORPTX": {"label": "Corp Tax Lien",           "cat": "lien"},
    "LNIRS":    {"label": "IRS Lien",                "cat": "lien"},
    "LNFED":    {"label": "Federal Lien",            "cat": "lien"},
    "LN":       {"label": "Lien",                    "cat": "lien"},
    "LNMECH":   {"label": "Mechanic Lien",           "cat": "lien"},
    "LNHOA":    {"label": "HOA Lien",                "cat": "lien"},
    "MEDLN":    {"label": "Medicaid Lien",           "cat": "lien"},
    "PRO":      {"label": "Probate Document",        "cat": "probate"},
    "NOC":      {"label": "Notice of Commencement",  "cat": "other"},
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

# ─── SCORING ──────────────────────────────────────────────────────────────────

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
    amount = _parse_amount(rec.get("amount", ""))
    if amount and amount > 100_000:
        score += 15
    elif amount and amount > 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        flags.append("Has address")
        score += 5

    return flags, min(score, 100)


def _parse_amount(raw: str) -> Optional[float]:
    if not raw:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", raw))
    except ValueError:
        return None


def _match_doc_type(raw: str) -> Optional[dict]:
    upper = raw.upper().strip()
    if upper in TARGET_DOC_TYPES:
        return {"code": upper, **TARGET_DOC_TYPES[upper]}
    for fragment, code in LABEL_FUZZY.items():
        if fragment in upper:
            return {"code": code, **TARGET_DOC_TYPES[code]}
    return None


# ─── PARCEL LOOKUP ────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._index: dict[str, dict] = {}

    def load(self) -> int:
        if PARCEL_CACHE.exists():
            age = time.time() - PARCEL_CACHE.stat().st_mtime
            if age < 86400:
                log.info("Loading parcels from cache...")
                self._index = json.loads(PARCEL_CACHE.read_text())
                log.info("Cached parcels: %d", len(self._index))
                return len(self._index)

        log.info("Downloading parcel data from TaxNetUSA...")
        data = self._download()
        if not data:
            log.warning("Parcel download failed — addresses will be empty")
            return 0
        count = self._parse(data)
        PARCEL_CACHE.write_text(json.dumps(self._index))
        log.info("Parcel index: %d records", count)
        return count

    def _download(self) -> Optional[bytes]:
        s = requests.Session()
        s.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        )
        for attempt in range(3):
            try:
                r = s.get(PARCEL_URL, timeout=30)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "lxml")
                for a in soup.find_all("a", href=True):
                    text = a.get_text(strip=True).upper()
                    href = a["href"]
                    if "DALLAS" in text and any(x in href.upper() for x in ["DBF", "ZIP", "DATA"]):
                        url = href if href.startswith("http") else f"https://www.taxnetusa.com{href}"
                        log.info("Downloading parcel file: %s", url)
                        dl = s.get(url, timeout=120)
                        dl.raise_for_status()
                        return dl.content
                log.warning("Attempt %d: No Dallas link found on TaxNetUSA", attempt + 1)
            except Exception as e:
                log.warning("Parcel attempt %d: %s", attempt + 1, e)
                time.sleep(2 ** attempt)
        return None

    def _parse(self, data: bytes) -> int:
        try:
            raw = data
            if data[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
                    if not names:
                        return 0
                    raw = zf.read(names[0])
            tmp = Path("/tmp/dallas_parcels.dbf")
            tmp.write_bytes(raw)
            count = 0
            for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                try:
                    owner = str(rec.get("OWNER") or rec.get("OWN1") or "").strip().upper()
                    if not owner:
                        continue
                    entry = {
                        "prop_address": str(rec.get("SITE_ADDR") or rec.get("SITEADDR") or "").strip(),
                        "prop_city":    str(rec.get("SITE_CITY") or "Dallas").strip(),
                        "prop_state":   "TX",
                        "prop_zip":     str(rec.get("SITE_ZIP") or "").strip(),
                        "mail_address": str(rec.get("ADDR_1") or rec.get("MAILADR1") or "").strip(),
                        "mail_city":    str(rec.get("CITY") or rec.get("MAILCITY") or "").strip(),
                        "mail_state":   str(rec.get("STATE") or "TX").strip(),
                        "mail_zip":     str(rec.get("ZIP") or rec.get("MAILZIP") or "").strip(),
                    }
                    parts = owner.split()
                    keys = [owner]
                    if len(parts) >= 2:
                        keys += [
                            f"{parts[0]} {' '.join(parts[1:])}",
                            f"{parts[-1]} {' '.join(parts[:-1])}",
                            f"{parts[-1]}, {' '.join(parts[:-1])}",
                        ]
                    for k in keys:
                        self._index.setdefault(k, entry)
                    count += 1
                except Exception:
                    continue
            return count
        except Exception as e:
            log.error("DBF parse error: %s", e)
            return 0

    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        key = owner.strip().upper()
        if key in self._index:
            return self._index[key]
        parts = key.split()
        if parts:
            for k, v in self._index.items():
                if k.startswith(parts[-1]):
                    return v
        return {}


# ─── CLERK SCRAPER ────────────────────────────────────────────────────────────

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
                log.error("Fatal scraper error: %s", e)
                import traceback; log.error(traceback.format_exc())
                try: await page.screenshot(path="/tmp/fatal.png")
                except Exception: pass
            finally:
                await browser.close()
        return records

    async def _scrape(self, page, date_from: str, date_to: str) -> list[dict]:
        records: list[dict] = []

        # ── Load portal ──
        log.info("Loading portal: %s", CLERK_URL)
        await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(4000)

        # Dismiss any popup/tour
        for sel in ['button:has-text("Close")', 'button:has-text("No")',
                    '[aria-label="Close"]', '.close', 'button:has-text("Dismiss")']:
            try:
                await page.click(sel, timeout=2000)
                log.info("Dismissed popup: %s", sel)
                break
            except Exception:
                pass

        await page.screenshot(path="/tmp/portal_home.png")
        log.info("Page title: %s", await page.title())

        # ── Click Advanced Search ──
        log.info("Looking for Advanced Search link...")
        clicked_advanced = False
        for sel in [
            'a:has-text("Advanced Search")',
            'span:has-text("Advanced Search")',
            'button:has-text("Advanced")',
            'a[href*="advanced" i]',
        ]:
            try:
                await page.click(sel, timeout=5000)
                await page.wait_for_timeout(3000)
                log.info("Clicked Advanced Search via: %s", sel)
                clicked_advanced = True
                break
            except Exception:
                pass

        if not clicked_advanced:
            log.warning("Could not click Advanced Search — logging all links")
            links = await page.query_selector_all("a, button")
            for lnk in links[:30]:
                try:
                    txt = (await lnk.inner_text()).strip()
                    href = await lnk.get_attribute("href") or ""
                    log.info("  element: '%s' href=%s", txt[:60], href[:60])
                except Exception:
                    pass

        await page.screenshot(path="/tmp/advanced_search.png")

        # ── Log all form elements ──
        all_inputs = await page.query_selector_all("input, select, textarea")
        log.info("Form elements found: %d", len(all_inputs))
        for el in all_inputs[:40]:
            try:
                t = await el.get_attribute("type") or "?"
                i = await el.get_attribute("id") or ""
                n = await el.get_attribute("name") or ""
                p = await el.get_attribute("placeholder") or ""
                log.info("  input type=%s id=%s name=%s placeholder=%s", t, i, n, p)
            except Exception:
                pass

        # ── Set date range ──
        # Format needed: M/D/YYYY (e.g. 4/2/2026)
        dt_from = datetime.strptime(date_from, "%m/%d/%Y")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y")
        fmt_from = f"{dt_from.month}/{dt_from.day}/{dt_from.year}"
        fmt_to   = f"{dt_to.month}/{dt_to.day}/{dt_to.year}"
        log.info("Setting date range: %s → %s", fmt_from, fmt_to)

        date_filled = False

        # Try by placeholder pattern (the screenshot shows M/D/YYYY style dates)
        for sel_pair in [
            ("#dateRangeStart", "#dateRangeEnd"),
            ("#startDate",      "#endDate"),
            ("#fromDate",       "#toDate"),
            ('[placeholder*="start" i]', '[placeholder*="end" i]'),
            ('[placeholder*="from" i]',  '[placeholder*="to" i]'),
        ]:
            try:
                s, e = sel_pair
                start_el = await page.query_selector(s)
                end_el   = await page.query_selector(e)
                if start_el and end_el:
                    # Clear and type into date range fields
                    await start_el.triple_click()
                    await start_el.type(fmt_from, delay=60)
                    await page.wait_for_timeout(300)
                    await end_el.triple_click()
                    await end_el.type(fmt_to, delay=60)
                    log.info("Filled dates via selectors: %s / %s", s, e)
                    date_filled = True
                    break
            except Exception:
                pass

        # GovOS date range — often rendered as two inputs side by side
        if not date_filled:
            date_inputs = await page.query_selector_all('input[type="text"], input[type="date"], input:not([type])')
            log.info("Fallback: found %d generic inputs", len(date_inputs))
            # Find the two that look like date fields (contain "/" or are near "Date Range" label)
            date_like = []
            for el in date_inputs:
                try:
                    val = await el.get_attribute("value") or ""
                    ph  = await el.get_attribute("placeholder") or ""
                    if "/" in val or "/" in ph or "date" in ph.lower():
                        date_like.append(el)
                except Exception:
                    pass
            log.info("Date-like inputs found: %d", len(date_like))
            if len(date_like) >= 2:
                await date_like[0].triple_click()
                await date_like[0].type(fmt_from, delay=60)
                await page.wait_for_timeout(300)
                await date_like[1].triple_click()
                await date_like[1].type(fmt_to, delay=60)
                date_filled = True
                log.info("Filled dates via date-like fallback")

        if not date_filled:
            log.warning("Could not fill date fields — will try document type search only")

        # ── Select doc types ──
        # Try selecting each target doc type from a dropdown or checkbox list
        await self._select_doc_types(page)
        await page.wait_for_timeout(500)

        # ── Submit search ──
        submitted = False
        for sel in [
            'button:has-text("Search")',
            'input[type="submit"]',
            '#searchBtn',
            '#btnSearch',
            'button[type="submit"]',
            '.search-btn',
        ]:
            try:
                await page.click(sel, timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=30_000)
                await page.wait_for_timeout(3000)
                log.info("Submitted search via: %s", sel)
                submitted = True
                break
            except Exception:
                pass

        if not submitted:
            log.warning("Could not submit search form")
            await page.screenshot(path="/tmp/submit_failed.png")
            return records

        await page.screenshot(path="/tmp/results.png")

        # ── Collect results across all pages ──
        page_num = 1
        while True:
            html = await page.content()
            page_records = self._parse_results(html)

            # Also try React/JSON results
            if not page_records:
                page_records = await self._extract_react_results(page)

            records.extend(page_records)
            log.info("Page %d: %d records (total: %d)", page_num, len(page_records), len(records))

            if not page_records:
                break

            has_next = await self._next_page(page)
            if not has_next:
                break
            page_num += 1
            await page.wait_for_timeout(2000)

        return records

    async def _select_doc_types(self, page):
        """Try to select target document types in the search form."""
        # Look for a doc type dropdown
        for sel in [
            'select[name*="type" i]',
            'select[id*="type" i]',
            'select[name*="doc" i]',
            '#docType',
            '#documentType',
            '#instrumentType',
        ]:
            try:
                el = await page.query_selector(sel)
                if el:
                    options = await el.query_selector_all("option")
                    log.info("Doc type dropdown found with %d options", len(options))
                    for opt in options:
                        val  = await opt.get_attribute("value") or ""
                        text = (await opt.inner_text()).strip().upper()
                        meta = _match_doc_type(text) or _match_doc_type(val)
                        if meta:
                            await page.select_option(sel, value=val)
                            log.info("Selected doc type: %s", text)
                    return
            except Exception:
                pass

        # Look for checkboxes
        checkboxes = await page.query_selector_all('input[type="checkbox"]')
        log.info("Checkboxes found: %d", len(checkboxes))
        for cb in checkboxes:
            try:
                label = await cb.get_attribute("aria-label") or ""
                name  = await cb.get_attribute("name") or ""
                val   = await cb.get_attribute("value") or ""
                meta  = _match_doc_type(label) or _match_doc_type(name) or _match_doc_type(val)
                if meta:
                    is_checked = await cb.is_checked()
                    if not is_checked:
                        await cb.click()
                        log.info("Checked: %s", label or name or val)
            except Exception:
                pass

    async def _extract_react_results(self, page) -> list[dict]:
        """Extract results from React-rendered DOM by reading result cards/rows."""
        records: list[dict] = []
        try:
            # GovOS/Neuo portals often render results as card elements
            result_items = await page.query_selector_all(
                '[class*="result" i], [class*="record" i], [class*="item" i], '
                '[data-testid*="result" i], [data-testid*="record" i]'
            )
            log.info("React result items found: %d", len(result_items))

            for item in result_items:
                try:
                    text = await item.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if len(lines) < 2:
                        continue

                    # Try to find doc type in the text
                    full_text = " ".join(lines).upper()
                    meta = None
                    for line in lines:
                        meta = _match_doc_type(line)
                        if meta:
                            break
                    if not meta:
                        continue

                    # Extract URL
                    clerk_url = self.BASE
                    a_tags = await item.query_selector_all("a[href]")
                    for a in a_tags:
                        href = await a.get_attribute("href") or ""
                        if href:
                            clerk_url = href if href.startswith("http") else f"{self.BASE}{href}"
                            break

                    records.append({
                        "doc_num":      lines[0] if lines else "",
                        "doc_type":     meta["code"],
                        "filed":        self._extract_date(full_text),
                        "cat":          meta["cat"],
                        "cat_label":    meta["label"],
                        "owner":        self._extract_name(lines, "grantor"),
                        "grantee":      self._extract_name(lines, "grantee"),
                        "amount":       self._extract_amount(full_text),
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
            log.warning("React extraction error: %s", e)
        return records

    def _extract_date(self, text: str) -> str:
        m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", text)
        return m.group(1) if m else ""

    def _extract_amount(self, text: str) -> str:
        m = re.search(r"\$[\d,]+\.?\d*", text)
        return m.group(0) if m else ""

    def _extract_name(self, lines: list[str], role: str) -> str:
        for i, line in enumerate(lines):
            if role.lower() in line.lower() and i + 1 < len(lines):
                return lines[i + 1]
        return lines[1] if len(lines) > 1 else ""

    def _parse_results(self, html: str) -> list[dict]:
        """Parse HTML table results."""
        soup    = BeautifulSoup(html, "lxml")
        records = []
        table   = None

        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(h in headers for h in ["grantor", "grantee", "instrument", "type", "recorded"]):
                table = tbl
                log.info("Table headers: %s", headers)
                break

        if not table:
            return records

        rows = table.find_all("tr")
        if len(rows) < 2:
            return records

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if any(x in h for x in ["instrument", "doc #", "number"]): col.setdefault("doc_num",  i)
            if "type" in h:                                              col.setdefault("doc_type", i)
            if any(x in h for x in ["recorded", "filed", "date"]):     col.setdefault("filed",    i)
            if "grantor" in h:                                           col.setdefault("grantor",  i)
            if "grantee" in h:                                           col.setdefault("grantee",  i)
            if any(x in h for x in ["amount", "consideration"]):        col.setdefault("amount",   i)
            if "legal" in h:                                             col.setdefault("legal",    i)

        for row in rows[1:]:
            try:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue

                def g(k, fb):
                    idx = col.get(k, fb)
                    return cells[idx] if 0 <= idx < len(cells) else ""

                raw_type = g("doc_type", 1)
                meta = _match_doc_type(raw_type)
                if not meta:
                    continue

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

    async def _next_page(self, page) -> bool:
        for sel in [
            'button:has-text("Next")', 'a:has-text("Next")',
            '[aria-label="Next page"]', '[aria-label="next"]',
            'button[title*="next" i]', '.pagination-next',
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


# ─── SAVE & EXPORT ────────────────────────────────────────────────────────────

def enrich_with_parcels(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    matched = 0
    for rec in records:
        addr = parcel.lookup(rec.get("owner", ""))
        if addr:
            rec.update(addr)
            matched += 1
    log.info("Parcel enrichment: %d / %d matched", matched, len(records))
    return records


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
                log.warning("CSV row error: %s", e)
    log.info("GHL CSV: %d rows → %s", len(records), path)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    today     = datetime.now(tz=timezone.utc)
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")

    log.info("=" * 50)
    log.info("Dallas County Lead Scraper")
    log.info("Date range: %s – %s", date_from, date_to)
    log.info("=" * 50)

    parcel = ParcelLookup()
    parcel.load()

    scraper  = ClerkScraper()
    raw      = await scraper.fetch_all(date_from, date_to)
    log.info("Raw records: %d", len(raw))

    raw      = enrich_with_parcels(raw, parcel)
    enriched = save_records(raw, date_from, date_to)
    export_ghl_csv(enriched, GHL_CSV_PATH)

    log.info("=" * 50)
    log.info("Done. %d leads saved.", len(enriched))
    log.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
