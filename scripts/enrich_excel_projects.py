#!/usr/bin/env python3
"""
Scenario A Enrichment Script

Updates outputs/excel_outputs/enrich1.xlsx (sheet "Projects") in place by:
- Loading all countries from countries.json
- Prefetching API projects by country and building gid->project map
- For rows missing any of: stage, commodities, latitude, longitude
  - Fill from API where available
  - If latitude/longitude still missing, fetch map center (7s timeouts)
  - If coordinates present, reverse geocode to enrich state/country/postcode/ISO fields

Writes back to the same Excel file after creating a timestamped backup.

Requirements: pandas, openpyxl, python-dotenv, requests, playwright (installed), bs4
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(name)s"}',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


# Ensure project root (parent of scripts/) is on sys.path so 'services' imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()
logger = configure_logging()


def load_all_countries(countries_file: str) -> List[str]:
    try:
        with open(countries_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        countries = data.get('country', [])
        if not isinstance(countries, list):
            raise ValueError("countries.json malformed: 'country' must be a list")
        logger.info(f"Loaded {len(countries)} countries from {countries_file}")
        return countries
    except Exception as e:
        logger.error(f"Failed to load countries from {countries_file}: {e}")
        # Minimal fallback to avoid hard failure
        return ["Australia", "Canada", "United States of America", "Brazil"]


def fetch_all_projects_by_country(countries: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch projects for all countries and return a gid->project map."""
    from services.api_client import MiningHubClient

    jwt_token = os.getenv('JWT_TOKEN')
    if not jwt_token:
        raise ValueError("JWT_TOKEN must be set in environment or .env")

    client = MiningHubClient(
        base_url=os.getenv('API_BASE_URL', 'https://mininghub.com/api'),
        jwt_token=jwt_token,
        timeout=int(os.getenv('API_TIMEOUT', '30')),
        retry_attempts=int(os.getenv('API_RETRY_ATTEMPTS', '3')),
    )

    gid_to_project: Dict[str, Dict[str, Any]] = {}
    for country in countries:
        try:
            projects = client.get_projects_by_country(country)
            for proj in projects or []:
                gid = str(proj.get('gid', '')).strip()
                if not gid:
                    continue
                # Prefer first-seen project; avoid overwriting with duplicates from other countries
                if gid not in gid_to_project:
                    gid_to_project[gid] = proj
        except Exception as e:
            logger.warning(f"Failed fetching projects for {country}: {e}")
    client.close()
    logger.info(f"Prefetched {len(gid_to_project)} unique projects across {len(countries)} countries")
    return gid_to_project


def case_insensitive_get(df: pd.DataFrame, column_name: str) -> Optional[str]:
    """Return real column name in df matching column_name case-insensitively, or None."""
    target = column_name.lower()
    for col in df.columns:
        if str(col).lower() == target:
            return col
    return None


def ensure_columns(df: pd.DataFrame, columns: List[str]) -> None:
    for name in columns:
        real = case_insensitive_get(df, name)
        if real is None:
            df[name] = None


def parse_api_location(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse API location string like "State, Country" into (state, country)."""
    if not location or not isinstance(location, str):
        return None, None
    parts = [p.strip() for p in location.split(',') if p and p.strip()]
    if len(parts) >= 2:
        # Heuristic: first is state/province, last is country
        state = parts[0]
        country = parts[-1]
        return state or None, country or None
    # If only one part, assume it's country
    if len(parts) == 1:
        return None, parts[0]
    return None, None


def extract_coords_from_api(project: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    centroid = project.get('centroid') if isinstance(project, dict) else None
    if centroid and isinstance(centroid, dict):
        coords = centroid.get('coordinates') or []
        if isinstance(coords, list) and len(coords) >= 2:
            # API uses [lon, lat]
            lon = coords[0]
            lat = coords[1]
            try:
                return float(lat), float(lon)
            except Exception:
                return None, None
    return None, None


def enrich_excel(
    excel_path: str,
    sheet_name: str,
    gid_to_project: Dict[str, Dict[str, Any]],
    enable_geocoding: bool = True,
) -> None:
    from services.map_center import fetch_map_center
    from services.geocoding import GeocodingService

    logger.info(f"Reading Excel: {excel_path} [{sheet_name}]")
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Normalize columns and ensure required columns exist
    gid_col = case_insensitive_get(df, 'gid') or case_insensitive_get(df, 'id')
    if not gid_col:
        raise ValueError("Could not find a 'gid' or 'id' column in the Projects sheet")

    ensure_columns(df, [
        'stage', 'commodities', 'latitude', 'longitude', 'area_m2',
        'country', 'state', 'postcode', 'iso3166_2', 'county', 'territory',
    ])

    # Resolve actual column names after ensuring
    stage_col = case_insensitive_get(df, 'stage') or 'stage'
    commodities_col = case_insensitive_get(df, 'commodities') or 'commodities'
    lat_col = case_insensitive_get(df, 'latitude') or 'latitude'
    lon_col = case_insensitive_get(df, 'longitude') or 'longitude'
    country_col = case_insensitive_get(df, 'country') or 'country'
    state_col = case_insensitive_get(df, 'state') or 'state'
    postcode_col = case_insensitive_get(df, 'postcode') or 'postcode'
    iso_col = case_insensitive_get(df, 'iso3166_2') or 'iso3166_2'
    county_col = case_insensitive_get(df, 'county') or 'county'
    territory_col = case_insensitive_get(df, 'territory') or 'territory'
    area_col = case_insensitive_get(df, 'area_m2') or 'area_m2'

    geocoder = GeocodingService() if enable_geocoding else None

    updated_rows = 0
    total_rows = len(df)

    for idx, row in df.iterrows():
        gid = str(row[gid_col]).strip() if pd.notna(row[gid_col]) else ''
        if not gid:
            continue

        needs_stage = pd.isna(row[stage_col]) or row[stage_col] in (None, '', '-')
        needs_commodities = pd.isna(row[commodities_col]) or row[commodities_col] in (None, '', '-')
        needs_lat = pd.isna(row[lat_col]) or row[lat_col] in (None, '')
        needs_lon = pd.isna(row[lon_col]) or row[lon_col] in (None, '')
        needs_area = pd.isna(row[area_col]) or row[area_col] in (None, '')

        # We'll still pass through to possibly fill state/country from API even if only area is present
        if not (needs_stage or needs_commodities or needs_lat or needs_lon or needs_area):
            continue

        api_project = gid_to_project.get(gid)
        if api_project:
            if needs_stage:
                api_stage = api_project.get('stage')
                if api_stage:
                    df.at[idx, stage_col] = api_stage
            if needs_commodities:
                api_commodities = api_project.get('commodities')
                if api_commodities:
                    df.at[idx, commodities_col] = api_commodities
            if needs_area:
                area_val = api_project.get('area_m2')
                if area_val not in (None, ''):
                    try:
                        # Convert to float if numeric string; otherwise keep as is
                        df.at[idx, area_col] = float(area_val)
                    except Exception:
                        df.at[idx, area_col] = area_val
            if needs_lat or needs_lon:
                lat_api, lon_api = extract_coords_from_api(api_project)
                if lat_api is not None and lon_api is not None:
                    df.at[idx, lat_col] = lat_api
                    df.at[idx, lon_col] = lon_api
            # Fill state/country from API location, if present and missing
            api_location = api_project.get('location')
            if api_location:
                state_from_api, country_from_api = parse_api_location(api_location)
                if state_from_api and (pd.isna(row[state_col]) or not row[state_col]):
                    df.at[idx, state_col] = state_from_api
                if country_from_api and (pd.isna(row[country_col]) or not row[country_col]):
                    df.at[idx, country_col] = country_from_api

        # If still missing coordinates, try map center
        needs_lat = pd.isna(df.at[idx, lat_col]) or df.at[idx, lat_col] in (None, '')
        needs_lon = pd.isna(df.at[idx, lon_col]) or df.at[idx, lon_col] in (None, '')
        if needs_lat or needs_lon:
            try:
                mc = fetch_map_center(gid=str(gid), headless=True)
                if mc and mc.get('latitude') is not None and mc.get('longitude') is not None:
                    df.at[idx, lat_col] = float(mc['latitude'])
                    df.at[idx, lon_col] = float(mc['longitude'])
                    logger.info(f"Map center fetched for GID {gid}: lat={mc['latitude']}, lon={mc['longitude']}")
            except Exception as e:
                logger.warning(f"Map center fetch failed for GID {gid}: {e}")

        # If coordinates present and geocoding enabled, enrich address fields
        try:
            lat_val = df.at[idx, lat_col]
            lon_val = df.at[idx, lon_col]
            if geocoder and pd.notna(lat_val) and pd.notna(lon_val):
                data = geocoder.reverse_geocode(float(lat_val), float(lon_val))
                if data and isinstance(data, dict) and 'address' in data:
                    addr = data['address']
                    state = (
                        addr.get('state')
                        or addr.get('state_district')
                        or addr.get('region')
                        or addr.get('province')
                        or addr.get('territory')
                    )
                    if state and (pd.isna(row[state_col]) or not row[state_col]):
                        df.at[idx, state_col] = state
                    if addr.get('country') and (pd.isna(row[country_col]) or not row[country_col]):
                        df.at[idx, country_col] = addr.get('country')
                    if addr.get('postcode') and (pd.isna(row[postcode_col]) or not row[postcode_col]):
                        df.at[idx, postcode_col] = addr.get('postcode')
                    iso_val = addr.get('ISO3166-2-lvl4') or addr.get('ISO3166-2-lvl6')
                    if iso_val and (pd.isna(row[iso_col]) or not row[iso_col]):
                        df.at[idx, iso_col] = iso_val
                    if addr.get('county') and (pd.isna(row[county_col]) or not row[county_col]):
                        df.at[idx, county_col] = addr.get('county')
                    if addr.get('territory') and (pd.isna(row[territory_col]) or not row[territory_col]):
                        df.at[idx, territory_col] = addr.get('territory')
        except Exception as e:
            logger.warning(f"Geocoding failed for GID {gid}: {e}")

        updated_rows += 1

        if updated_rows % 25 == 0:
            logger.info(f"Progress: updated {updated_rows} rows out of {total_rows}")

    # Backup original and write updated sheet
    backup_path = excel_path.replace('.xlsx', f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    try:
        import shutil
        shutil.copy2(excel_path, backup_path)
        logger.info(f"Created backup: {backup_path}")
    except Exception as e:
        logger.warning(f"Failed to create backup: {e}")

    with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    logger.info(f"Enrichment complete. Updated rows: {updated_rows}. File saved: {excel_path}")


def main():
    base_dir = os.getcwd()
    excel_path = os.path.join(base_dir, 'outputs', 'excel_outputs', 'enrich1.xlsx')
    sheet_name = os.getenv('ENRICH_SHEET', 'Projects')
    countries_file = os.path.join(base_dir, 'countries.json')

    if not os.path.exists(excel_path):
        logger.error(f"Excel file not found: {excel_path}")
        sys.exit(1)

    countries = load_all_countries(countries_file)
    gid_to_project = fetch_all_projects_by_country(countries)
    enrich_excel(excel_path, sheet_name, gid_to_project, enable_geocoding=os.getenv('ENABLE_GEOCODING', 'true').lower() == 'true')


if __name__ == '__main__':
    main()


