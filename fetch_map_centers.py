#!/usr/bin/env python3
# pip install playwright pandas
# python -m playwright install chromium

import asyncio, csv
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

INPUT_CSV = Path("reports/missing_ids_report.csv")
OUTPUT_CSV = Path("reports/map_centers.csv")
BASE_URL  = "https://mininghub.com/map?gid={gid}"

# Async JS: wait for Leaflet .whenReady() or MapboxGL 'load', then read center
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

def read_ids() -> list[str]:
    df = pd.read_csv(INPUT_CSV)
    if "ID" not in df.columns:
        raise ValueError(f'"ID" column not found in {INPUT_CSV}')
    return [str(x).strip() for x in df["ID"].dropna().astype(str)]

async def fetch_one(page, gid: str, goto_timeout_ms: int, map_ready_timeout_ms: int):
    url = BASE_URL.format(gid=gid)

    # Block heavy stuff, BUT allow stylesheets so Leaflet can size/initialize
    async def route_blocker(route):
        rt = route.request.resource_type
        u  = route.request.url
        if rt in {"image","media","font"}:
            return await route.abort()
        # crude tile patterns
        if any(s in u for s in ["/tile/", "/tiles/", "/{z}/", "/wmts", "/arcgis/", "/basemaps/", "/mapbox/"]):
            return await route.abort()
        return await route.continue_()
    await page.route("**/*", route_blocker)

    # small retry loop for navigation
    last_err = ""
    for attempt in range(2):  # 0,1
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            break
        except PWTimeout as e:
            last_err = f"goto>{goto_timeout_ms}ms (attempt {attempt+1})"
            if attempt == 1:
                return {"ID": gid, "url": url, "status": "timeout_goto", "error": last_err}

    # Wait explicitly for a map object to exist, then call the async getter
    try:
        # first, wait until a candidate map object appears
        await page.wait_for_function(
            """() => {
                const w = window;
                if (w.map && typeof w.map.getCenter==='function') return true;
                for (const v of Object.values(w)){
                    if (v && typeof v.getCenter==='function') return true;
                }
                return false;
            }""",
            timeout=map_ready_timeout_ms
        )
    except PWTimeout:
        # We'll still try evaluation—some apps are ready but predicate failed
        pass

    try:
        center = await page.evaluate(RUNTIME_JS_ASYNC)
    except Exception as e:
        return {"ID": gid, "url": url, "status": "eval_error", "error": str(e)[:300]}

    if center:
        return {
            "ID": gid, "url": url,
            "lat": center.get("lat"), "lng": center.get("lng"),
            "zoom": center.get("zoom"), "via": center.get("lib"),
            "status": "ok", "error": ""
        }
    else:
        return {"ID": gid, "url": url, "status": "no_center", "error": ""}

async def main_async(limit, concurrency, goto_timeout_ms, map_ready_timeout_ms, headful):
    ids = read_ids()
    if limit is not None:
        ids = ids[:limit]
    total = len(ids)
    print(f"Starting {total} ID(s) with concurrency={concurrency}…")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["ID","url","lat","lng","zoom","via","status","error"]
    f = OUTPUT_CSV.open("w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); f.flush()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headful, args=["--no-sandbox","--disable-gpu"])
        context = await browser.new_context(viewport={"width":1280,"height":900})
        sem = asyncio.Semaphore(concurrency)
        counter = {"done": 0}

        async def worker(gid: str):
            async with sem:
                page = await context.new_page()
                try:
                    res = await fetch_one(page, gid, goto_timeout_ms, map_ready_timeout_ms)
                finally:
                    await page.close()
                w.writerow(res); f.flush()
                counter["done"] += 1
                i = counter["done"]
                if res["status"] == "ok":
                    print(f"[{i}/{total}] {gid} ✓ lat={res['lat']:.6f}, lng={res['lng']:.6f} (via {res.get('via')})")
                else:
                    msg = res.get("error","")
                    print(f"[{i}/{total}] {gid} • {res['status']} {('— '+msg) if msg else ''}")

        await asyncio.gather(*(worker(g) for g in ids))
        await context.close(); await browser.close()

    f.close()
    print(f"Done. Results → {OUTPUT_CSV.resolve()}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Concurrent map-center fetcher (Playwright) with proper map readiness waits.")
    ap.add_argument("--limit", type=int, default=None, help="Process only the first N IDs")
    ap.add_argument("--concurrency", type=int, default=12, help="Parallel pages (8–16 good on laptops)")
    ap.add_argument("--goto-timeout-ms", type=int, default=25000, help="Timeout for page.goto")
    ap.add_argument("--map-ready-timeout-ms", type=int, default=7000, help="Timeout waiting for map object/ready")
    ap.add_argument("--headful", action="store_true", help="Show browser (debugging)")
    args = ap.parse_args()

    asyncio.run(main_async(
        limit=args.limit,
        concurrency=args.concurrency,
        goto_timeout_ms=args.goto_timeout_ms,
        map_ready_timeout_ms=args.map_ready_timeout_ms,
        headful=args.headful
    ))