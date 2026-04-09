"""
Dallas County, TX — Motivated Seller Lead Scraper
Clerk Portal : https://dallas.tx.ds.search.govos.com/
Parcel Data  : https://www.taxnetusa.com/data/
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

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL       = "https://dallas.tx.ds.search.govos.com/"
PARCEL_URL      = "https://www.taxnetusa.com/data/"
OUTPUT_PATHS    = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH    = Path("data/ghl_export.csv")
PARCEL_CACHE    = Path("/tmp/dallas_parcels.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dallas_scraper")

# ─── DOCUMENT TYPE MAPPING ────────────────────────────────────────────────────

TARGET_DOC_TYPES: dict[str, dict] = {
    # Foreclosure
    "LP":      {"label": "Lis Pendens",              "cat": "foreclosure"},
    "NOFC":    {"label": "Notice of Foreclosure",    "cat": "foreclosure"},
    "RELLP":   {"label": "Release Lis Pendens",      "cat": "foreclosure"},
    # Tax
    "TAXDEED": {"label": "Tax Deed",                 "cat": "tax"},
    # Judgments
    "JUD":     {"label": "Judgment",                 "cat": "judgment"},
    "CCJ":     {"label": "Certified Judgment",       "cat": "judgment"},
    "DRJUD":   {"label": "Domestic Judgment",        "cat": "judgment"},
    # Liens
    "LNCORPTX":{"label": "Corp Tax Lien",            "cat": "lien"},
    "LNIRS":   {"label": "IRS Lien",                 "cat": "lien"},
    "LNFED":   {"label": "Federal Lien",             "cat": "lien"},
    "LN":      {"label": "Lien",                     "cat": "lien"},
    "LNMECH":  {"label": "Mechanic Lien",            "cat": "lien"},
    "LNHOA":   {"label": "HOA Lien",                 "cat": "lien"},
    "MEDLN":   {"label": "Medicaid Lien",            "cat": "lien"},
    # Other
    "PRO":     {"label": "Probate Document",         "cat": "probate"},
    "NOC":     {"label": "Notice of Commencement",   "cat": "other"},
}

# Also match by partial/fuzzy label from the site
LABEL_FUZZY: dict[str, str] = {
    "LIS PENDENS":            "LP",
    "NOTICE OF FORECLOSURE":  "NOFC",
    "RELEASE LIS PENDENS":    "RELLP",
    "TAX DEED":               "TAXDEED",
    "JUDGMENT":               "JUD",
    "CERTIFIED JUDGMENT":     "CCJ",
    "DOMESTIC":               "DRJUD",
    "CORP TAX":               "LNCORPTX",
    "IRS":                    "LNIRS",
    "FEDERAL LIEN":           "LNFED",
    "MECHANIC":               "LNMECH",
    "HOA":                    "LNHOA",
    "MEDICAID":               "MEDLN",
    "PROBATE":                "PRO",
    "COMMENCEMENT":           "NOC",
}

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
    "Motivated Seller Flags", "Source", "Public Records URL",
]

# ─── STEALTH JS ───────────────────────────────────────────────────────────────

STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    const orig = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : orig(p);
}
"""

# ─── SCORING ──────────────────────────────────────────────────────────────────

def compute_score(rec: dict) -> tuple[list[str], int]:
    flags: list[str] = []
    cat    = rec.get("cat", "")
    code   = rec.get("doc_type", "")
    amount = _parse_amount(rec.get("amount", ""))

    if cat == "foreclosure" and code not in ("RELLP",):
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
    core_flags = [f for f in flags if f not in ("New this week", "LLC / corp owner")]
    score += 10 * len(core_flags)

    # LP + FC combo bonus
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

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
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _match_doc_type(raw_type: str) -> Optional[dict]:
    """Match a raw doc type string to our TARGET_DOC_TYPES."""
    upper = raw_type.upper().strip()

    # Exact code match first
    if upper in TARGET_DOC_TYPES:
        return {"code": upper, **TARGET_DOC_TYPES[upper]}

    # Fuzzy label match
    for label_fragment, code in LABEL_FUZZY.items():
        if label_fragment in upper:
            return {"code": code, **TARGET_DOC_TYPES[code]}

    return None


# ─── PARCEL LOOKUP ────────────────────────────────────────────────────────────

class ParcelLookup:
    """Downloads Dallas County bulk parcel DBF and builds owner→address lookup."""

    def __init__(self):
        self._index: dict[str, dict] = {}

    def load(self) -> int:
        # Try cached version first
        if PARCEL_CACHE.exists():
            age = time.time() - PARCEL_CACHE.stat().st_mtime
            if age < 86400:  # 24 hours
                log.info("Loading parcels from cache...")
                self._index = json.loads(PARCEL_CACHE.read_text())
                log.info("Loaded %d parcels from cache", len(self._index))
                return len(self._index)

        log.info("Downloading parcel data from TaxNetUSA...")
        dbf_bytes = self._download_dbf()
        if not dbf_bytes:
            log.warning("Could not download parcel data — addresses will be empty")
            return 0

        count = self._parse_dbf(dbf_bytes)
        PARCEL_CACHE.write_text(json.dumps(self._index))
        log.info("Parcel index built: %d records", count)
        return count

    def _download_dbf(self) -> Optional[bytes]:
        """Try to download Dallas County parcel DBF from TaxNetUSA."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

        for attempt in range(3):
            try:
                resp = session.get(PARCEL_URL, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")

                # Look for Dallas County download link
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True).upper()
                    if "DALLAS" in text and ("DBF" in text or "ZIP" in href.upper() or "DBF" in href.upper()):
                        dl_url = href if href.startswith("http") else f"https://www.taxnetusa.com{href}"
                        log.info("Found Dallas parcel link: %s", dl_url)
                        dl_resp = session.get(dl_url, timeout=120, stream=True)
                        dl_resp.raise_for_status()
                        return dl_resp.content

                # Try __doPostBack for dynamic pages
                viewstate = ""
                vs_input = soup.find("input", {"name": "__VIEWSTATE"})
                if vs_input:
                    viewstate = vs_input.get("value", "")

                eventval = ""
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "__doPostBack" in href and "DALLAS" in a.get_text(strip=True).upper():
                        match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
                        if match:
                            post_data = {
                                "__EVENTTARGET":   match.group(1),
                                "__EVENTARGUMENT": match.group(2),
                                "__VIEWSTATE":     viewstate,
                            }
                            pb_resp = session.post(PARCEL_URL, data=post_data, timeout=120)
                            pb_resp.raise_for_status()
                            return pb_resp.content

                log.warning("Attempt %d: No Dallas parcel link found", attempt + 1)

            except Exception as e:
                log.warning("Parcel download attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 ** attempt)

        return None

    def _parse_dbf(self, data: bytes) -> int:
        """Parse DBF bytes (possibly zipped) into owner index."""
        try:
            # Try ZIP first
            if data[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
                    if not dbf_names:
                        log.warning("No DBF file found in ZIP")
                        return 0
                    dbf_bytes = zf.read(dbf_names[0])
            else:
                dbf_bytes = data

            # Write to temp file (dbfread needs a file path)
            tmp = Path("/tmp/dallas_parcels.dbf")
            tmp.write_bytes(dbf_bytes)

            table = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            count = 0

            for rec in table:
                try:
                    owner = (
                        str(rec.get("OWNER") or rec.get("OWN1") or "").strip().upper()
                    )
                    if not owner:
                        continue

                    entry = {
                        "prop_address": str(rec.get("SITE_ADDR") or rec.get("SITEADDR") or "").strip(),
                        "prop_city":    str(rec.get("SITE_CITY") or "").strip() or "Dallas",
                        "prop_state":   "TX",
                        "prop_zip":     str(rec.get("SITE_ZIP") or "").strip(),
                        "mail_address": str(rec.get("ADDR_1") or rec.get("MAILADR1") or "").strip(),
                        "mail_city":    str(rec.get("CITY") or rec.get("MAILCITY") or "").strip(),
                        "mail_state":   str(rec.get("STATE") or "TX").strip(),
                        "mail_zip":     str(rec.get("ZIP") or rec.get("MAILZIP") or "").strip(),
                    }

                    # Index by multiple name formats
                    parts = owner.split()
                    if len(parts) >= 2:
                        first_last = f"{parts[0]} {' '.join(parts[1:])}"
                        last_first = f"{parts[-1]} {' '.join(parts[:-1])}"
                        last_comma = f"{parts[-1]}, {' '.join(parts[:-1])}"
                        for key in [owner, first_last, last_first, last_comma]:
                            self._index.setdefault(key, entry)
                    else:
                        self._index[owner] = entry

                    count += 1
                except Exception:
                    continue

            return count

        except Exception as e:
            log.error("DBF parse error: %s", e)
            return 0

    def lookup(self, owner_name: str) -> dict:
        """Return address data for owner, or empty dict."""
        if not owner_name:
            return {}
        key = owner_name.strip().upper()
        result = self._index.get(key, {})
        if result:
            return result
        # Try last name only
        parts = key.split()
        if parts:
            for k, v in self._index.items():
                if k.startswith(parts[-1]):
                    return v
        return {}


# ─── CLERK SCRAPER ────────────────────────────────────────────────────────────

class ClerkScraper:
    """Scrapes Dallas County clerk portal via Playwright."""

    BASE = "https://dallas.tx.ds.search.govos.com"

    async def fetch_all(self, date_from: str, date_to: str) -> list[dict]:
        records: list[dict] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            await context.add_init_script(STEALTH_JS)
            page = await context.new_page()

            try:
                records = await self._scrape(page, date_from, date_to)
            except Exception as e:
                log.error("Clerk scraper fatal error: %s", e)
                import traceback
                log.error(traceback.format_exc())
                try:
                    await page.screenshot(path="/tmp/fatal_error.png")
                except Exception:
                    pass
            finally:
                await browser.close()

        return records

    async def _scrape(self, page, date_from: str, date_to: str) -> list[dict]:
        records: list[dict] = []

        log.info("Loading clerk portal: %s", CLERK_URL)
        await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(4000)

        title = await page.title()
        log.info("Portal title: %s", title)
        await page.screenshot(path="/tmp/clerk_home.png")

        # Navigate to document search
        search_url = await self._find_search_url(page)
        if search_url and search_url != page.url:
            log.info("Navigating to search: %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(3000)

        # Try to interact with the search form
        form_found = await self._fill_search_form(page, date_from, date_to)
        if not form_found:
            log.warning("Search form not found — trying direct URL approach")
            records = await self._try_direct_search(page, date_from, date_to)
            return records

        await page.screenshot(path="/tmp/clerk_results.png")

        # Parse all result pages
        page_num = 1
        while True:
            html = await page.content()
            page_records = self._parse_results_page(html)
            records.extend(page_records)
            log.info("Page %d: %d records (total so far: %d)", page_num, len(page_records), len(records))

            if not page_records:
                break

            # Try next page
            has_next = await self._go_next_page(page)
            if not has_next:
                break
            page_num += 1
            await page.wait_for_timeout(1500)

        return records

    async def _find_search_url(self, page) -> Optional[str]:
        """Find the document search link on the portal home page."""
        for sel in [
            'a:has-text("Document Search")',
            'a:has-text("Search Records")',
            'a:has-text("Official Records")',
            'a:has-text("Search")',
            'a[href*="search"]',
        ]:
            try:
                el = await page.query_selector(sel)
                if el:
                    href = await el.get_attribute("href")
                    if href:
                        return href if href.startswith("http") else f"{self.BASE}{href}"
            except Exception:
                pass

        # Log all links for debugging
        links = await page.query_selector_all("a[href]")
        log.info("Found %d links on home page:", len(links))
        for lnk in links[:20]:
            try:
                href = await lnk.get_attribute("href") or ""
                text = (await lnk.inner_text()).strip()[:60]
                log.info("  Link: %s → %s", text, href)
            except Exception:
                pass

        return None

    async def _fill_search_form(self, page, date_from: str, date_to: str) -> bool:
        """Fill and submit the date range search form."""
        await page.wait_for_timeout(3000)

        # Log all form elements
        all_inputs = await page.query_selector_all("input, select, textarea")
        log.info("Form elements found: %d", len(all_inputs))
        for el in all_inputs[:30]:
            try:
                t = await el.get_attribute("type") or "?"
                i = await el.get_attribute("id") or ""
                n = await el.get_attribute("name") or ""
                p = await el.get_attribute("placeholder") or ""
                log.info("  input type=%s id=%s name=%s placeholder=%s", t, i, n, p)
            except Exception:
                pass

        filled = False

        # Try date range fields
        date_selectors = [
            ('#dateFrom', '#dateTo'),
            ('#startDate', '#endDate'),
            ('#beginDate', '#endDate'),
            ('input[name="dateFrom"]', 'input[name="dateTo"]'),
            ('input[name="startDate"]', 'input[name="endDate"]'),
            ('input[placeholder*="MM/DD" i]', None),
        ]

        for from_sel, to_sel in date_selectors:
            try:
                from_el = await page.query_selector(from_sel)
                if not from_el:
                    continue

                await from_el.click()
                await from_el.fill("")
                await from_el.type(date_from, delay=60)
                log.info("Filled start date via: %s", from_sel)

                if to_sel:
                    to_el = await page.query_selector(to_sel)
                    if to_el:
                        await to_el.click()
                        await to_el.fill("")
                        await to_el.type(date_to, delay=60)
                        log.info("Filled end date via: %s", to_sel)
                else:
                    # Two MM/DD inputs — fill second one
                    date_inputs = await page.query_selector_all('input[placeholder*="MM/DD" i]')
                    if len(date_inputs) >= 2:
                        await date_inputs[1].click()
                        await date_inputs[1].fill("")
                        await date_inputs[1].type(date_to, delay=60)

                filled = True
                break
            except Exception:
                pass

        if not filled:
            log.warning("Could not fill date fields")
            return False

        await page.wait_for_timeout(500)

        # Submit
        for sel in [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Search")',
            'a:has-text("Search")',
            '#btnSearch',
            '#searchBtn',
        ]:
            try:
                await page.click(sel, timeout=3000)
                await page.wait_for_load_state("networkidle", timeout=30_000)
                log.info("Submitted search via: %s", sel)
                return True
            except Exception:
                pass

        # Try F2 as last resort
        await page.keyboard.press("F2")
        await page.wait_for_timeout(3000)
        return True

    async def _try_direct_search(self, page, date_from: str, date_to: str) -> list[dict]:
        """Try common URL patterns for date-range document search."""
        records: list[dict] = []

        # GovOS / iDocMarket common patterns
        url_patterns = [
            f"{self.BASE}/search/index?RecordedDateFrom={date_from}&RecordedDateTo={date_to}",
            f"{self.BASE}/search/index?startDate={date_from}&endDate={date_to}",
            f"{self.BASE}/api/search?dateFrom={date_from}&dateTo={date_to}&county=dallas",
        ]

        for url in url_patterns:
            try:
                log.info("Trying direct URL: %s", url)
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(3000)
                html = await page.content()
                if len(html) > 5000:
                    found = self._parse_results_page(html)
                    if found:
                        records.extend(found)
                        log.info("Direct URL found %d records", len(found))
                        break
            except Exception as e:
                log.warning("Direct URL failed: %s", e)

        return records

    async def _go_next_page(self, page) -> bool:
        """Click the Next button if available."""
        for sel in [
            'a:has-text("Next")',
            'a:has-text(">")',
            'input[value="Next"]',
            'button:has-text("Next")',
            '[aria-label="Next page"]',
            '.pagination a:last-child',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    is_disabled = await btn.get_attribute("disabled")
                    classes = await btn.get_attribute("class") or ""
                    if is_disabled or "disabled" in classes:
                        return False
                    await btn.click(timeout=10_000)
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                    await page.wait_for_timeout(1000)
                    return True
            except Exception:
                pass
        return False

    def _parse_results_page(self, html: str) -> list[dict]:
        """Parse a results page and return matching records."""
        soup = BeautifulSoup(html, "lxml")
        records: list[dict] = []

        # Find results table
        results_table = None
        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            text    = tbl.get_text().lower()
            if any(h in headers for h in ["grantor", "grantee", "instrument", "type", "recorded", "filed"]):
                results_table = tbl
                log.info("Results table headers: %s", headers)
                break
            if "grantor" in text and "grantee" in text and len(tbl.find_all("tr")) > 2:
                results_table = tbl
                break

        # Also try JSON results (some GovOS portals return JSON)
        if not results_table:
            records = self._parse_json_results(html)
            if records:
                return records
            log.warning("No results table found")
            return records

        rows = results_table.find_all("tr")
        if len(rows) < 2:
            return records

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if any(x in h for x in ["instrument", "instr", "doc #", "doc#", "number"]):
                col.setdefault("doc_num",  i)
            if any(x in h for x in ["type", "document type"]):
                col.setdefault("doc_type", i)
            if any(x in h for x in ["recorded", "filed", "date"]):
                col.setdefault("filed",    i)
            if "grantor" in h:
                col.setdefault("grantor",  i)
            if "grantee" in h:
                col.setdefault("grantee",  i)
            if any(x in h for x in ["consideration", "amount", "value"]):
                col.setdefault("amount",   i)
            if any(x in h for x in ["legal", "description"]):
                col.setdefault("legal",    i)

        log.info("Column map: %s", col)

        for row in rows[1:]:
            try:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue

                def g(key: str, fallback: int) -> str:
                    idx = col.get(key, fallback)
                    return cells[idx] if 0 <= idx < len(cells) else ""

                doc_num  = g("doc_num",  0)
                raw_type = g("doc_type", 1)
                filed    = g("filed",    2)
                grantor  = g("grantor",  3)
                grantee  = g("grantee",  4)
                amount   = g("amount",   5)
                legal    = g("legal",    6)

                if not doc_num and not filed:
                    continue

                meta = _match_doc_type(raw_type)
                if not meta:
                    continue

                # Get direct link
                clerk_url = f"{self.BASE}/search/index"
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    clerk_url = href if href.startswith("http") else f"{self.BASE}/{href.lstrip('/')}"
                    break

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  meta["code"],
                    "filed":     filed,
                    "cat":       meta["cat"],
                    "cat_label": meta["label"],
                    "owner":     grantor,
                    "grantee":   grantee,
                    "amount":    amount,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                    # Address fields filled later by parcel lookup
                    "prop_address": "",
                    "prop_city":    "Dallas",
                    "prop_state":   "TX",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "TX",
                    "mail_zip":     "",
                })
            except Exception as e:
                log.warning("Row parse error: %s", e)
                continue

        return records

    def _parse_json_results(self, html: str) -> list[dict]:
        """Try to parse JSON embedded in page or returned as API response."""
        records: list[dict] = []
        try:
            data = json.loads(html)
            items = data if isinstance(data, list) else data.get("records", data.get("results", []))
            for item in items:
                if not isinstance(item, dict):
                    continue
                raw_type = str(item.get("docType") or item.get("documentType") or item.get("type") or "")
                meta = _match_doc_type(raw_type)
                if not meta:
                    continue
                doc_num = str(item.get("docNumber") or item.get("instrumentNumber") or item.get("id") or "")
                records.append({
                    "doc_num":      doc_num,
                    "doc_type":     meta["code"],
                    "filed":        str(item.get("recordedDate") or item.get("filedDate") or ""),
                    "cat":          meta["cat"],
                    "cat_label":    meta["label"],
                    "owner":        str(item.get("grantor") or item.get("owner") or ""),
                    "grantee":      str(item.get("grantee") or ""),
                    "amount":       str(item.get("amount") or item.get("consideration") or ""),
                    "legal":        str(item.get("legalDescription") or item.get("legal") or ""),
                    "clerk_url":    str(item.get("url") or item.get("link") or self.BASE),
                    "prop_address": "", "prop_city": "Dallas", "prop_state": "TX", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                })
        except (json.JSONDecodeError, Exception):
            pass
        return records


# ─── ENRICH WITH PARCEL DATA ──────────────────────────────────────────────────

def enrich_with_parcels(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    matched = 0
    for rec in records:
        owner = rec.get("owner", "")
        if not owner:
            continue
        addr = parcel.lookup(owner)
        if addr:
            rec.update(addr)
            matched += 1
    log.info("Parcel match: %d / %d records enriched", matched, len(records))
    return records


# ─── SAVE & EXPORT ────────────────────────────────────────────────────────────

def save_records(records: list[dict], date_from: str, date_to: str) -> list[dict]:
    enriched = []
    for rec in records:
        try:
            flags, score = compute_score(rec)
            rec["flags"] = flags
            rec["score"] = score
            enriched.append(rec)
        except Exception as e:
            log.warning("Score error for record %s: %s", rec.get("doc_num"), e)
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
                flags = rec.get("flags", [])
                score = rec.get("score", 30)
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
                    "Seller Score":           score,
                    "Motivated Seller Flags": "; ".join(flags),
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

    # Step 1: Load parcel data for address enrichment
    parcel = ParcelLookup()
    parcel.load()

    # Step 2: Scrape clerk portal
    scraper = ClerkScraper()
    raw = await scraper.fetch_all(date_from, date_to)
    log.info("Total raw records fetched: %d", len(raw))

    # Step 3: Enrich with parcel addresses
    raw = enrich_with_parcels(raw, parcel)

    # Step 4: Score and save
    enriched = save_records(raw, date_from, date_to)

    # Step 5: Export GHL CSV
    export_ghl_csv(enriched, GHL_CSV_PATH)

    log.info("=" * 50)
    log.info("Done. %d leads saved.", len(enriched))
    log.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
