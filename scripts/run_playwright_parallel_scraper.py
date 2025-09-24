#!/usr/bin/env python3
"""
Runner to scrape a list of project GIDs with PlaywrightParallelScraper and save JSON.
Supports configurable concurrency and headful mode for debugging.
"""

import os
import sys
import json
import asyncio
import random
from typing import List

import pandas as pd

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.playwright_parallel_scraper import PlaywrightParallelScraper


OUTPUT_DIR = os.path.join("outputs", "json_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_random_gids_from_urls(limit: int = 50) -> List[str]:
    path = os.path.join(os.getcwd(), "found_urls.xlsx")
    try:
        df = pd.read_excel(path, sheet_name="Projects")
        ids = [str(x).strip() for x in df["ID"].dropna().astype(str).tolist()]
        random.shuffle(ids)
        return ids[:limit]
    except Exception as e:
        print(f"⚠️ Could not read {path}: {e}")
        return []


async def main_async(gids: List[str], concurrency: int, headful: bool):
    scraper = PlaywrightParallelScraper()
    recs = await scraper.scrape_many_parallel(gids, max_concurrency=concurrency, headless=not headful, verbose=True)
    results = [PlaywrightParallelScraper.to_dict(r) for r in recs]

    out_path = os.path.join(OUTPUT_DIR, "playwright_parallel_scraped_projects.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"scraped_projects": results}, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved {len(results)} scraped projects → {out_path}")


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Run Playwright parallel scraper for a list of GIDs")
    ap.add_argument("--gids", type=str, default="", help="Comma-separated list of GIDs to scrape")
    ap.add_argument("--limit", type=int, default=20, help="Random sample size from found_urls.xlsx if --gids omitted")
    ap.add_argument("--concurrency", type=int, default=4, help="Max concurrent Playwright pages")
    ap.add_argument("--headful", action="store_true", help="Show browser UI")
    args = ap.parse_args()

    if args.gids:
        gids = [g.strip() for g in args.gids.split(",") if g.strip()]
    else:
        gids = load_random_gids_from_urls(limit=args.limit)

    if not gids:
        print("❌ No GIDs to scrape")
        sys.exit(1)

    asyncio.run(main_async(gids, concurrency=args.concurrency, headful=args.headful))


if __name__ == "__main__":
    main()


