#!/usr/bin/env python3
"""
Playwright-based parallel scraper (no company page navigation)
Extracts project page details directly from project-profile, with robust waits
for dynamically injected tables and horizontally scalable concurrency.
"""

import os
import asyncio
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin
import re
import random

from bs4 import BeautifulSoup


BASE_HOST = "https://mininghub.com"


@dataclass
class ParallelScrapedProjectRecord:
    gid: str
    project_name: Optional[str] = None
    operator: Optional[str] = None

    company_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None

    commodities: Optional[str] = None
    stage: Optional[str] = None
    ticker_exchange: Optional[str] = None
    project_summary_href: Optional[str] = None

    # Map + geocoding placeholders (not resolved in this parallel scraper)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    map_zoom: Optional[float] = None
    map_lib: Optional[str] = None

    # Enriched location placeholders
    country: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    iso3166_2: Optional[str] = None
    county: Optional[str] = None
    territory: Optional[str] = None
    location_source: Optional[str] = None

    # Page metadata
    project_url: Optional[str] = None
    company_profile_link_found: bool = False

    scrape_source: str = "playwright_parallel_scraper"


class PlaywrightParallelScraper:
    """
    Parallel Playwright scraper to extract project page details directly
    from project-profile without navigating to the company page.
    """

    def __init__(self, goto_timeout_ms: int = 45000):
        self.goto_timeout_ms = goto_timeout_ms

    async def _launch(self, headless: bool = True):
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-gpu"])
        self._context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="UTC",
        )
        try:
            await self._context.set_extra_http_headers({
                "Accept-Language": "en-US,en;q=0.9",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "Referer": "https://mininghub.com/",
            })
        except Exception:
            pass

    async def _close(self):
        try:
            await self._context.close()
        except Exception:
            pass
        try:
            await self._browser.close()
        except Exception:
            pass
        try:
            await self._pw.stop()
        except Exception:
            pass

    async def _new_page(self):
        page = await self._context.new_page()

        async def route_blocker(route):
            rt = route.request.resource_type
            u = route.request.url
            if rt in {"image", "media", "font"}:
                return await route.abort()
            if any(s in u for s in ["/tile/", "/tiles/", "/{z}/", "/wmts", "/arcgis/", "/basemaps/", "/mapbox/"]):
                return await route.abort()
            return await route.continue_()

        await page.route("**/*", route_blocker)

        # Apply stealth if available (best-effort)
        try:
            from playwright_stealth import stealth_async  # type: ignore
            try:
                await stealth_async(page)
            except Exception:
                pass
        except Exception:
            pass
        return page

    async def _safe_goto(self, page, url: str) -> bool:
        """Navigate with retries/backoff and flexible wait_until strategies."""
        strategies = ["domcontentloaded", "load", "commit"]
        timeouts = [15000, 25000, self.goto_timeout_ms]
        for wait_until in strategies:
            for to in timeouts:
                try:
                    jitter = random.uniform(50, 200)
                    await page.wait_for_timeout(jitter)
                    await page.goto(url, wait_until=wait_until, timeout=to)
                    return True
                except Exception:
                    await page.wait_for_timeout(random.uniform(100, 300))
            # small pause before switching strategy
            await page.wait_for_timeout(random.uniform(150, 350))
        return False

    async def _wait_for_properties_table(self, page, max_total_ms: int = 30000) -> Optional[str]:
        """Wait with exponential backoff for dynamically injected properties table and return its outerHTML."""
        delays = [2000, 3000, 5000, 8000, 12000]
        elapsed = 0
        for d in delays:
            remaining = max_total_ms - elapsed
            if remaining <= 0:
                break
            try:
                # Wait either the table or a possible container, then prefer table
                await page.wait_for_selector(".properties-wrapper-table", timeout=min(d, remaining))
                el = await page.query_selector("table.properties-wrapper-table")
                if not el:
                    el = await page.query_selector("div.properties-wrapper-table")
                if el:
                    # Ensure it has content (more than header)
                    row_count = await page.evaluate("(n) => (n.tagName==='TABLE' ? n.querySelectorAll('tr').length : n.querySelectorAll('tr').length)", el)
                    outer = await el.evaluate("(node) => node.outerHTML")
                    if row_count and row_count >= 2:
                        return outer
                    # If structure present but few rows, still return for debugging
                    if outer:
                        return outer
            except Exception:
                pass
            await page.wait_for_timeout(250)  # small gap between attempts
            elapsed += d
        return None

    async def _reveal_tables_by_scrolling(self, page) -> None:
        """Attempt to reveal lazy-loaded table rows by scrolling containers and window."""
        try:
            # Try scrolling main container
            for sel in [".main-profile-container", "#right-sider", "body"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.scroll_into_view_if_needed()
                        # Incremental scroll within element if scrollable
                        await page.evaluate(
                            "(n) => { try { n.scrollTop = 0; } catch(e){} }",
                            el
                        )
                        for _ in range(6):
                            await page.evaluate(
                                "(n) => { try { n.scrollBy ? n.scrollBy(0, 800) : (n.scrollTop += 800); } catch(e){} }",
                                el
                            )
                            await page.wait_for_timeout(250)
                except Exception:
                    continue
            # Fallback: scroll window
            for _ in range(8):
                await page.evaluate("() => window.scrollBy(0, 800)")
                await page.wait_for_timeout(200)
        except Exception:
            pass

    async def _collect_properties_tables(self, page, max_total_ms: int = 30000) -> List[str]:
        """Collect all properties tables' outerHTMLs with a similar wait/backoff strategy."""
        delays = [2000, 3000, 5000, 8000, 12000]
        elapsed = 0
        tables_html: List[str] = []
        seen: set[str] = set()
        for d in delays:
            remaining = max_total_ms - elapsed
            if remaining <= 0:
                break
            try:
                await page.wait_for_selector(".properties-wrapper-table", timeout=min(d, remaining))
                elements = await page.query_selector_all("table.properties-wrapper-table")
                if not elements or len(elements) == 0:
                    elements = await page.query_selector_all("div.properties-wrapper-table")
                for el in elements or []:
                    try:
                        outer = await el.evaluate("(node) => node.outerHTML")
                        if outer and outer not in seen:
                            seen.add(outer)
                            tables_html.append(outer)
                    except Exception:
                        continue
            except Exception:
                pass
            await page.wait_for_timeout(250)
            elapsed += d
        return tables_html

    def _parse_properties_table(self, table_html: str, result: ParallelScrapedProjectRecord) -> None:
        """Parse the properties table HTML to fill project fields and companies."""
        soup = BeautifulSoup(table_html, "lxml")
        field_mapping = {
            "project": "project_name",
            "operator": "operator",
            "commodit": "commodities",
            "stage": "stage",
            "ticker": "ticker_exchange",
        }

        def _norm(s: str) -> str:
            return " ".join((s or "").strip().lower().split())

        rows = soup.find_all("tr")
        if not rows:
            return

        # Detect header-style (projects table) vs key-value (main table)
        first_cells = rows[0].find_all(["td", "th"]) if rows else []
        header_labels = [c.get_text(strip=True).lower() for c in first_cells]
        is_header_table = (
            len(first_cells) >= 3 and
            any("project" in h for h in header_labels) and
            any("commodit" in h for h in header_labels)
        )

        primary_company_candidates: List[Dict[str, str]] = []

        if is_header_table:
            # Build column index map
            col_index: Dict[str, int] = {}
            for idx, h in enumerate(header_labels):
                for key, attr in field_mapping.items():
                    if key in h and attr not in col_index:
                        col_index[attr] = idx
                        break

            # Try to find row by current gid first (robust), else by project name
            target_row = None
            # 1) by gid link
            gid_anchor = soup.find("a", href=re.compile(rf"project-profile\\?gid={re.escape(str(result.gid))}\\b"))
            if gid_anchor:
                target_row = gid_anchor.find_parent("tr")
            # 2) by project name match in project column
            if not target_row and "project_name" in col_index and result.project_name:
                pcol = col_index["project_name"]
                for r in rows[1:]:
                    cells = r.find_all(["td", "th"])
                    if len(cells) <= pcol:
                        continue
                    pname = cells[pcol].get_text(strip=True)
                    if _norm(pname) == _norm(result.project_name):
                        target_row = r
                        break
            # 3) fallback: first data row
            if not target_row and len(rows) > 1:
                target_row = rows[1]

            if target_row:
                cells = target_row.find_all(["td", "th"])
                # project name from table if missing
                if "project_name" in col_index and not result.project_name and len(cells) > col_index["project_name"]:
                    val = cells[col_index["project_name"]].get_text(strip=True)
                    if val:
                        result.project_name = val
                # commodities
                if "commodities" in col_index and getattr(result, "commodities", None) is None:
                    idx = col_index["commodities"]
                    if len(cells) > idx:
                        val = cells[idx].get_text(strip=True)
                        if val and val != "-":
                            result.commodities = val
                # stage
                if "stage" in col_index and getattr(result, "stage", None) is None:
                    idx = col_index["stage"]
                    if len(cells) > idx:
                        val = cells[idx].get_text(strip=True)
                        if val and val != "-":
                            result.stage = val

            # Ownership/company extraction rarely present in header table, skip here
        else:
            # Key-value main table parsing and ownership link extraction
            for tr in rows:
                tds = tr.find_all(["td", "th"])
                if len(tds) < 2:
                    continue
                label = tds[0].get_text(strip=True).lower()
                value = tds[1].get_text(strip=True)
                if not value or value == "-":
                    continue

                # Map key fields
                for key, attr in field_mapping.items():
                    if key in label:
                        if attr == "project_name" and not result.project_name:
                            result.project_name = value
                        elif attr == "operator" and not result.operator:
                            result.operator = value
                        elif attr in {"commodities", "stage", "ticker_exchange"} and getattr(result, attr) is None:
                            setattr(result, attr, value)
                        break

                # Extract companies from ownership cell
                if "ownership" in label:
                    for link in tds[1].find_all("a"):
                        company_name = link.get_text(strip=True)
                        if not company_name:
                            continue
                        company_id = None
                        for attr_val in [link.get("onclick", ""), link.get("href", "")]:
                            if "gid=" in attr_val:
                                m = re.search(r"gid=(\d+)", attr_val)
                                if m:
                                    company_id = m.group(1)
                                    break
                        if company_id:
                            company_url = f"{BASE_HOST}/company-profile?gid={company_id}"
                            primary_company_candidates.append({
                                "id": company_id,
                                "name": company_name,
                                "url": company_url,
                            })

            # Choose primary company (prefer operator name match, else first)
            if primary_company_candidates and not result.company_id:
                operator_name = getattr(result, "operator", None)
                primary = primary_company_candidates[0]
                if operator_name:
                    for c in primary_company_candidates:
                        if operator_name.lower() in c["name"].lower():
                            primary = c
                            break
                result.company_id = primary["id"]
                result.company_name = result.company_name or result.operator or primary["name"]
                result.company_url = primary["url"]

    def _create_company_slug(self, company_name: str) -> str:
        replacements = {" ": "-", ".": "", ",": "", "&": "and", "(": "", ")": "", "'": "", '"': "", "/": "-"}
        slug = (company_name or "").lower()
        for old, new in replacements.items():
            slug = slug.replace(old, new)
        return slug

    async def _extract_fast_company_from_attrs(self, page, result: ParallelScrapedProjectRecord) -> None:
        """Extract company id quickly from known elements' attributes without parsing the table."""
        selectors = [("#company-news-btn", "href"), ("#project-news-btn", "href"), ("#project-map", "src")]
        for sel, attr in selectors:
            try:
                el = await page.query_selector(sel)
                if not el:
                    continue
                val = await el.get_attribute(attr)
                if not val:
                    continue

                company_id = None
                if attr == "href":
                    # Only accept company ids from company-profile links, never from project-profile
                    if "company-profile" in val:
                        m = re.search(r"[?&]gid=(\d+)", val)
                        if m:
                            company_id = m.group(1)
                elif attr == "src":
                    # Map src can contain both gid (project) and companyId; only use companyId here
                    m = re.search(r"[?&]companyId=(\d+)", val)
                    if m:
                        company_id = m.group(1)

                if company_id:
                    result.company_id = company_id
                    if not result.company_name and getattr(result, "operator", None):
                        result.company_name = result.operator
                    result.company_url = f"{BASE_HOST}/company-profile?gid={company_id}"
                    return
            except Exception:
                continue

        # Fallback: look for any anchor that links to company-profile?gid=...
        try:
            anchors = await page.query_selector_all('a[href*="company-profile?gid="]')
            for a in anchors:
                href = await a.get_attribute('href')
                if not href:
                    continue
                m = re.search(r"[?&]gid=(\d+)", href)
                if m:
                    company_id = m.group(1)
                    result.company_id = company_id
                    if not result.company_name and getattr(result, "operator", None):
                        result.company_name = result.operator
                    result.company_url = f"{BASE_HOST}/company-profile?gid={company_id}"
                    return
        except Exception:
            pass

    async def scrape_one(self, gid: str, headless: bool = True, verbose: bool = True) -> ParallelScrapedProjectRecord:
        url = urljoin(BASE_HOST, f"/project-profile?gid={gid}")
        rec = ParallelScrapedProjectRecord(gid=str(gid), project_url=url)

        page = await self._new_page()
        try:
            ok = await self._safe_goto(page, url)
            if not ok and verbose:
                print(f"[DEBUG {gid}] goto() failed across strategies; continuing to attempt parse.")
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

            # Try to reveal dynamic content by scrolling
            await self._reveal_tables_by_scrolling(page)

            # project title, if available
            try:
                h1 = await page.query_selector("h1#project-title, h1#project_title, h1")
                if h1:
                    txt = await h1.inner_text()
                    if txt and not rec.project_name:
                        rec.project_name = txt.strip()
            except Exception:
                pass

            # presence of company link id
            try:
                rec.company_profile_link_found = bool(await page.query_selector("#company-news-btn"))
            except Exception:
                rec.company_profile_link_found = False

            # try to read company name from h3 explicitly
            try:
                h3 = await page.wait_for_selector("h3#company-name", timeout=4000)
                if h3:
                    name_txt = (await h3.inner_text()) or ""
                    name_txt = name_txt.strip()
                    if name_txt:
                        rec.company_name = rec.company_name or name_txt
            except Exception:
                pass

            # collect and parse all properties tables
            tables_html = await self._collect_properties_tables(page, max_total_ms=35000)

            if tables_html:
                for table_html in tables_html:
                    self._parse_properties_table(table_html, rec)
            # If key fields missing, try another scroll and second pass
            # If key fields still missing, try a second reveal/collect/parse pass regardless of table count
            if (rec.commodities is None or rec.stage is None):
                await self._reveal_tables_by_scrolling(page)
                tables_html2 = await self._collect_properties_tables(page, max_total_ms=15000)
                for table_html in tables_html2:
                    self._parse_properties_table(table_html, rec)
            else:
                # Fallback: parse container text (no debug printing)
                try:
                    container = await page.query_selector(".main-profile-container")
                    if container:
                        text = await container.inner_text()
                        lines = [l.strip() for l in (text or "").split("\n") if l.strip()]
                        mapping = {"project:": "project_name", "operator:": "operator", "commodities:": "commodities", "stage:": "stage"}
                        for i, line in enumerate(lines):
                            for key, attr in mapping.items():
                                if key in line.lower() and i + 1 < len(lines):
                                    val = lines[i + 1]
                                    if attr == "project_name" and not rec.project_name:
                                        rec.project_name = val
                                    elif attr == "operator" and not getattr(rec, "operator", None):
                                        rec.operator = val
                                    elif attr in {"commodities", "stage"} and getattr(rec, attr) is None:
                                        setattr(rec, attr, val)
                                    break
                except Exception:
                    pass

            # Ensure company_name set from operator if still missing
            if not rec.company_name and getattr(rec, "operator", None):
                rec.company_name = rec.operator

            # Fast company extraction from known attributes if still not set
            if not rec.company_id:
                await self._extract_fast_company_from_attrs(page, rec)

            # Normalize company_name to Proper Case if captured
            try:
                if rec.company_name and isinstance(rec.company_name, str):
                    rec.company_name = self._to_proper_case(rec.company_name)
                if not rec.operator and rec.company_name:
                    rec.operator = rec.company_name
            except Exception:
                pass

            return rec
        finally:
            try:
                await page.close()
            except Exception:
                pass

    @staticmethod
    def to_dict(rec: ParallelScrapedProjectRecord) -> Dict[str, Any]:
        return asdict(rec)

    async def scrape_many_parallel(self, gids: List[str], max_concurrency: int = 4, headless: bool = True, verbose: bool = True) -> List[ParallelScrapedProjectRecord]:
        await self._launch(headless=headless)
        results: List[ParallelScrapedProjectRecord] = []
        semaphore = asyncio.Semaphore(max_concurrency)

        async def bound_scrape(gid: str):
            async with semaphore:
                try:
                    if verbose:
                        print(f"🧭 Fetching GID {gid}…", flush=True)
                    rec = await self.scrape_one(gid, headless=headless, verbose=verbose)
                    results.append(rec)
                except Exception as e:
                    if verbose:
                        print(f"❌ Error scraping {gid}: {e}", flush=True)

        try:
            await asyncio.gather(*(bound_scrape(g) for g in gids))
        finally:
            await self._close()
        return results

    def _to_proper_case(self, s: str) -> str:
        try:
            return ' '.join(part.capitalize() for part in s.split())
        except Exception:
            return s


