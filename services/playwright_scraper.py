#!/usr/bin/env python3
"""
Playwright-based scraper service
Extracts project/company details and map center, with BeautifulSoup parsing
and geocoding integration via services.geocoding.
"""

import os
import asyncio
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup


BASE_HOST = "https://mininghub.com"


@dataclass
class ScrapedProjectRecord:
    gid: str
    project_name: Optional[str] = None

    company_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None

    commodities: Optional[str] = None
    stage: Optional[str] = None
    project_summary_href: Optional[str] = None

    # Map + geocoding
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    map_zoom: Optional[float] = None
    map_lib: Optional[str] = None

    # Enriched location
    country: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    iso3166_2: Optional[str] = None
    county: Optional[str] = None
    territory: Optional[str] = None
    location_source: Optional[str] = None  # scraper_map | geocode

    # Page metadata
    project_url: Optional[str] = None
    company_profile_link_found: bool = False

    scrape_source: str = "playwright_scraper"


class PlaywrightScraper:
    """
    Playwright scraper to extract project page details, company profile info
    (commodities/stage for the project), and map center; then geocode.
    """

    def __init__(self, goto_timeout_ms: int = 25000, map_ready_timeout_ms: int = 7000):
        self.goto_timeout_ms = goto_timeout_ms
        self.map_ready_timeout_ms = map_ready_timeout_ms

        # Geocoding service (sync)
        try:
            from services.geocoding import GeocodingService, GeocodingConfig
            self.geocoder = GeocodingService(GeocodingConfig())
        except Exception:
            self.geocoder = None

    async def _launch(self, headless: bool = True):
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-gpu"])
        self._context = await self._browser.new_context(viewport={"width": 1280, "height": 900})

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

    async def _fetch_page_html(self, url: str, wait_selectors: Optional[List[str]] = None, wait_ms: int = 2000) -> str:
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

        await page.goto(url, wait_until="domcontentloaded", timeout=min(self.goto_timeout_ms, 12000))
        # Try short waits for expected selectors so dynamic content can settle
        if wait_selectors:
            for sel in wait_selectors:
                try:
                    await page.wait_for_selector(sel, timeout=wait_ms)
                except Exception:
                    pass
        html = await page.content()
        await page.close()
        return html

    async def _fetch_project_basics(self, gid: str) -> Dict[str, Any]:
        """Fetch project page and parse project title, company link/name from right-sider."""
        url = urljoin(BASE_HOST, f"/project-profile?gid={gid}")
        html = await self._fetch_page_html(
            url,
            wait_selectors=["#right-sider", "h3#company-name", "h1#project-title", "h1#project_title", "#modal-nav-buttons"],
            wait_ms=2000
        )
        soup = BeautifulSoup(html, "html.parser")

        # Project name (try common variants)
        project_name = None
        for sel in ["h1#project-title", "h1#project_title", "h1"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                project_name = el.get_text(strip=True)
                break

        # Company profile link in right-sider
        right = soup.find(id="right-sider")
        company_url = None
        company_id = None
        company_name = None
        company_link_found = False

        if right:
            # company name h3#company-name
            h3 = right.find("h3", id="company-name")
            if h3 and h3.get_text(strip=True):
                company_name = h3.get_text(strip=True)

            a = right.find("a", id="company-news-btn")
            if a and a.get("href"):
                href = a.get("href").strip()
                company_url = urljoin(BASE_HOST, href)
                company_link_found = True
                if "company-profile?gid=" in href:
                    try:
                        part = href.split("company-profile?gid=")[1]
                        company_id = part.split("&")[0]
                    except Exception:
                        company_id = None

        # Fallback: modal nav buttons (sometimes data-url is used instead of href)
        if not company_url:
            nav = soup.find(id="modal-nav-buttons") or soup.find(id="modal_nav_buttons")
            if nav:
                company_anchor = nav.find("a", id="company_profile")
                if company_anchor:
                    data_url = (company_anchor.get("data-url") or "").strip()
                    href = (company_anchor.get("href") or "").strip()
                    candidate = data_url or href
                    if candidate:
                        company_url = urljoin(BASE_HOST, candidate)
                        company_link_found = True
                        if "company-profile?gid=" in candidate:
                            try:
                                part = candidate.split("company-profile?gid=")[1]
                                company_id = part.split("&")[0]
                            except Exception:
                                company_id = None

        # Last resort: any anchor containing company-profile?gid
        if not company_url:
            a_any = soup.find("a", href=lambda x: isinstance(x, str) and "company-profile?gid=" in x)
            if not a_any:
                a_any = soup.find("a", attrs={"data-url": True})
            if a_any:
                candidate = (a_any.get("href") or a_any.get("data-url") or "").strip()
                if candidate:
                    company_url = urljoin(BASE_HOST, candidate)
                    company_link_found = True
                    if "company-profile?gid=" in candidate:
                        try:
                            part = candidate.split("company-profile?gid=")[1]
                            company_id = part.split("&")[0]
                        except Exception:
                            company_id = None

        return {
            "url": url,
            "html": html,
            "project_name": project_name,
            "company_url": company_url,
            "company_id": company_id,
            "company_name": company_name,
            "company_link_found": company_link_found,
        }

    def _parse_company_projects_table(self, company_html: str, target_gid: str, target_project_name: Optional[str]):
        soup = BeautifulSoup(company_html, "html.parser")

        # Find the right table
        table = soup.find("div", {"class": "properties-wrapper-table"})

        for t in soup.find_all("table", {"class": "properties-wrapper-table"}):
            header_row = t.find("tr")
            if not header_row:
                continue
            headers = [c.get_text(strip=True).lower() for c in header_row.find_all(["td", "th"])]
            header_str = ",".join(headers)
            if "project" in header_str and "commodit" in header_str and "stage" in header_str:
                table = t
                break
        if not table:
            return {"commodities": None, "stage": None, "table_html": None, "matched_by": None, "project_summary_href": None}

        def normalize_name(s: str) -> str:
            return " ".join((s or "").strip().lower().split())

        target_name_norm = normalize_name(target_project_name) if target_project_name else None

        rows = table.find_all("tr")
        best = {"commodities": None, "stage": None, "project_summary_href": None, "matched_by": None}

        for tr in rows[1:]:  # skip header
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            project_cell, _, commodities_cell, stage_cell, *rest = tds
            link_cell = rest[0] if rest else None

            # Try link gid match
            link_gid = None
            if link_cell:
                a = link_cell.find("a", href=True)
                if a and "project-profile?gid=" in a["href"]:
                    link_gid = a["href"].split("project-profile?gid=")[1].split("&")[0]

            if link_gid and str(link_gid) == str(target_gid):
                return {
                    "commodities": commodities_cell.get_text(strip=True) or None,
                    "stage": stage_cell.get_text(strip=True) or None,
                    "project_summary_href": a["href"] if a else None,
                    "matched_by": "gid",
                    "table_html": str(table)
                }

            # Fallback to name match
            project_name_text = project_cell.get_text(" ", strip=True)
            if target_name_norm and normalize_name(project_name_text) == target_name_norm:
                best = {
                    "commodities": commodities_cell.get_text(strip=True) or None,
                    "stage": stage_cell.get_text(strip=True) or None,
                    "project_summary_href": a["href"] if link_cell and (a := link_cell.find("a", href=True)) else None,
                    "matched_by": "name",
                    "table_html": str(table)
                }

        if "table_html" not in best:
            best["table_html"] = str(table)
        return best

    async def _fetch_company_profile_html(self, company_url: str) -> str:
        return await self._fetch_page_html(
            company_url,
            wait_selectors=["table.properties-wrapper-table"],
            wait_ms=2000
        )

    async def _fetch_map_center(self, gid: str) -> Dict[str, Any]:
        """Fetch map center from /map?gid= using Playwright and robust map ready waits."""
        from playwright.async_api import TimeoutError as PWTimeout
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

        url = urljoin(BASE_HOST, f"/map?gid={gid}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self.goto_timeout_ms)
        except PWTimeout:
            await page.close()
            return {"status": "timeout", "url": url}

        # Wait for map object presence
        try:
            await page.wait_for_function(
                """
                () => {
                    const w = window;
                    if (w.map && typeof w.map.getCenter==='function') return true;
                    for (const v of Object.values(w)){
                        if (v && typeof v.getCenter==='function') return true;
                    }
                    return false;
                }
                """,
                timeout=self.map_ready_timeout_ms
            )
        except PWTimeout:
            pass

        RUNTIME_JS_ASYNC = """
        async () => {
          function findLeaflet(){
            const m = (window.map && typeof window.map.getCenter==='function')
              ? window.map
              : Object.values(window).find(v => v && typeof v.getCenter==='function' && typeof v.eachLayer==='function');
            return m || null;
          }
          function findMapbox(){
            for (const v of Object.values(window)){
              try{
                if (v && typeof v.getCenter==='function' && typeof v.getZoom==='function' && typeof v.on==='function'){
                  return v;
                }
              }catch(e){}
            }
            return null;
          }
          const Lmap = findLeaflet();
          if (Lmap){
            if (!Lmap._loaded && typeof Lmap.whenReady==='function'){
              await new Promise(res => Lmap.whenReady(res));
            }
            const c = Lmap.getCenter();
            const z = (typeof Lmap.getZoom==='function') ? Lmap.getZoom() : null;
            return {lib:'Leaflet', lat:c.lat, lng:c.lng, zoom:z};
          }
          const Mmap = findMapbox();
          if (Mmap){
            if (!Mmap.loaded && typeof Mmap.once==='function'){
              await new Promise(res => Mmap.once('load', res));
            }
            const c = Mmap.getCenter();
            return {lib:'MapboxGL', lat:c.lat, lng:c.lng, zoom:Mmap.getZoom()};
          }
          return null;
        }
        """

        try:
            center = await page.evaluate(RUNTIME_JS_ASYNC)
        except Exception:
            await page.close()
            return {"status": "eval_error", "url": url}
        finally:
            try:
                await page.close()
            except Exception:
                pass

        if center:
            return {"status": "ok", **center, "url": url}
        return {"status": "no_center", "url": url}

    def _geocode_if_possible(self, rec: ScrapedProjectRecord) -> ScrapedProjectRecord:
        if not self.geocoder or rec.latitude is None or rec.longitude is None:
            return rec
        try:
            data = self.geocoder.reverse_geocode(rec.latitude, rec.longitude)
            if data and 'address' in data:
                addr = data['address']
                rec.country = addr.get('country') or rec.country
                for key in ['state', 'state_district', 'region', 'province', 'territory']:
                    if addr.get(key):
                        rec.state = addr.get(key)
                        break
                rec.postcode = addr.get('postcode')
                rec.iso3166_2 = addr.get('ISO3166-2-lvl4') or addr.get('ISO3166-2-lvl6')
                rec.county = addr.get('county')
                rec.territory = addr.get('territory')
                rec.location_source = rec.location_source or 'geocode'
        except Exception:
            pass
        return rec

    async def scrape_project(self, gid: str, headless: bool = True) -> ScrapedProjectRecord:
        await self._launch(headless=headless)
        try:
            basics = await self._fetch_project_basics(gid)
            rec = ScrapedProjectRecord(
                gid=str(gid),
                project_name=basics.get('project_name'),
                company_id=basics.get('company_id'),
                company_name=basics.get('company_name'),
                company_url=basics.get('company_url'),
                project_url=basics.get('url'),
                company_profile_link_found=bool(basics.get('company_link_found')),
            )

            # Company profile table parsing to get commodities/stage
            if rec.company_url:
                company_html = await self._fetch_company_profile_html(rec.company_url)
                props = self._parse_company_projects_table(company_html, target_gid=gid, target_project_name=rec.project_name)
                rec.commodities = props.get('commodities')
                rec.stage = props.get('stage')
                rec.project_summary_href = props.get('project_summary_href')
                rec.debug_company_table_html = props.get('table_html')

            # Map center
            m = await self._fetch_map_center(gid)
            if m.get('status') == 'ok':
                rec.latitude = float(m.get('lat')) if m.get('lat') is not None else None
                rec.longitude = float(m.get('lng')) if m.get('lng') is not None else None
                rec.map_zoom = m.get('zoom')
                rec.map_lib = m.get('lib')
                rec.location_source = 'scraper_map'

            # Geocode
            rec = self._geocode_if_possible(rec)
            return rec
        finally:
            await self._close()

    @staticmethod
    def to_dict(rec: ScrapedProjectRecord) -> Dict[str, Any]:
        return asdict(rec)

    async def scrape_many(self, gids: List[str], headless: bool = True, verbose: bool = True) -> List[ScrapedProjectRecord]:
        """Scrape multiple project pages using a single browser session for speed."""
        await self._launch(headless=headless)
        results: List[ScrapedProjectRecord] = []
        try:
            total = len(gids)
            for idx, gid in enumerate(gids, 1):
                try:
                    if verbose:
                        print(f"[{idx}/{total}] 🧭 Fetching basics for GID {gid}…", flush=True)
                    basics = await self._fetch_project_basics(gid)
                    rec = ScrapedProjectRecord(
                        gid=str(gid),
                        project_name=basics.get('project_name'),
                        company_id=basics.get('company_id'),
                        company_name=basics.get('company_name'),
                        company_url=basics.get('company_url'),
                        project_url=basics.get('url'),
                        company_profile_link_found=bool(basics.get('company_link_found')),
                    )

                    if rec.company_url:
                        if verbose:
                            print(f"[{idx}/{total}] 🏢 Loading company profile to match project row…", flush=True)
                        company_html = await self._fetch_company_profile_html(rec.company_url)
                        props = self._parse_company_projects_table(company_html, target_gid=str(gid), target_project_name=rec.project_name)
                        rec.commodities = props.get('commodities')
                        rec.stage = props.get('stage')
                        rec.project_summary_href = urljoin(BASE_HOST, props.get('project_summary_href') or "") if props.get('project_summary_href') else None
                    else:
                        if verbose:
                            print(f"[{idx}/{total}] ⚠️ No company profile link found on project page.", flush=True)

                    if verbose:
                        print(f"[{idx}/{total}] 🗺️ Fetching map center… (5s max)", flush=True)
                    try:
                        m = await asyncio.wait_for(self._fetch_map_center(str(gid)), timeout=5.0)
                    except asyncio.TimeoutError:
                        m = {"status": "timeout_overall"}
                    if m.get('status') == 'ok':
                        rec.latitude = float(m.get('lat')) if m.get('lat') is not None else None
                        rec.longitude = float(m.get('lng')) if m.get('lng') is not None else None
                        rec.map_zoom = m.get('zoom')
                        rec.map_lib = m.get('lib')
                        rec.location_source = 'scraper_map'
                    else:
                        if verbose:
                            print(f"[{idx}/{total}] ⚠️ Map center status: {m.get('status')}", flush=True)

                    if verbose and rec.latitude is not None and rec.longitude is not None:
                        print(f"[{idx}/{total}] 🌍 Reverse geocoding lat={rec.latitude:.6f}, lon={rec.longitude:.6f}…", flush=True)
                    rec = self._geocode_if_possible(rec)

                    if verbose:
                        print(f"[{idx}/{total}] ✅ Done: project='{rec.project_name or ''}', company='{rec.company_name or ''}', stage='{rec.stage or ''}', commodities='{rec.commodities or ''}'", flush=True)

                    results.append(rec)
                except Exception as e:
                    if verbose:
                        print(f"[{idx}/{total}] ❌ Error scraping {gid}: {e}", flush=True)
        finally:
            await self._close()
        return results


