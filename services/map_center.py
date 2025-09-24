#!/usr/bin/env python3
"""
Map Center Fetch Service (Playwright)
Fetches lat/lon/zoom from the MiningHub map page for a given project gid.
Headless by default. Safe to call from assembler as a quick, single-gid op.
"""

import asyncio
import logging
from typing import Optional, Dict, Any


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


async def _fetch_one_async(gid: str, headless: bool, goto_timeout_ms: int, ready_timeout_ms: int) -> Optional[Dict[str, Any]]:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    logger = logging.getLogger(__name__)

    base_url = f"https://mininghub.com/map?gid={gid}"
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-gpu"])  # type: ignore
        context = await browser.new_context(viewport={"width": 1280, "height": 900})
        page = await context.new_page()

        async def route_blocker(route):
            rt = route.request.resource_type
            u = route.request.url
            if rt in {"image", "media", "font"}:
                return await route.abort()
            if any(s in u for s in ["/tile/", "/tiles/", "/{z}/", "/wmts", "/arcgis/", "/basemaps/", "/mapbox/"]):
                return await route.abort()
            return await route.continue_()

        await page.route("**/*", route_blocker)

        # Small retry loop for navigation (mirrors legacy script behavior)
        last_err = None
        for attempt in range(2):
            try:
                await page.goto(base_url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
                break
            except PWTimeout as e:
                last_err = e
                if attempt == 1:
                    logger.debug(f"map_center.goto timeout for gid={gid} after {goto_timeout_ms}ms")
                    await context.close(); await browser.close()
                    return None

        try:
            await page.wait_for_function(
                """() => {
                    const w = window;
                    if (w.map && typeof w.map.getCenter==='function') return true;
                    for (const v of Object.values(w)){
                        if (v && typeof v.getCenter==='function') return true;
                    }
                    return false;
                }""",
                timeout=ready_timeout_ms
            )
        except PWTimeout:
            # Proceed to evaluate anyway; some maps are ready but predicate fails
            logger.debug(f"map_center.wait_for_function timeout for gid={gid} after {ready_timeout_ms}ms")
            pass

        try:
            center = await page.evaluate(RUNTIME_JS_ASYNC)
        except Exception:
            center = None

        await context.close(); await browser.close()
        if center and center.get("lat") is not None and center.get("lng") is not None:
            logger.info(f"Map center for gid={gid}: lat={center['lat']}, lng={center['lng']} via {center.get('lib')}")
            return {"latitude": float(center["lat"]), "longitude": float(center["lng"]), "map_zoom": center.get("zoom"), "map_lib": center.get("lib")}
        logger.warning(f"No map center found for gid={gid}")
        return None


def fetch_map_center(
    gid: str,
    headless: bool = True,
    goto_timeout_ms: int = 7000,
    ready_timeout_ms: int = 7000,
    overall_timeout_ms: int = 7000,
) -> Optional[Dict[str, Any]]:
    """Synchronous helper to fetch a single map center for a gid."""
    import asyncio as _asyncio
    import logging as _logging
    logger = _logging.getLogger(__name__)

    async def _runner():
        return await _asyncio.wait_for(
            _fetch_one_async(str(gid), headless=headless, goto_timeout_ms=goto_timeout_ms, ready_timeout_ms=ready_timeout_ms),
            timeout=max(0.1, overall_timeout_ms / 1000.0)
        )

    try:
        return _asyncio.run(_runner())
    except _asyncio.TimeoutError:
        logger.warning(f"Map center overall timeout for gid={gid} after {overall_timeout_ms}ms")
        return None
    except RuntimeError:
        # If already in an event loop, create a new one
        try:
            loop = _asyncio.get_event_loop()
            return loop.run_until_complete(_runner())
        except _asyncio.TimeoutError:
            logger.warning(f"Map center overall timeout (loop) for gid={gid} after {overall_timeout_ms}ms")
            return None


