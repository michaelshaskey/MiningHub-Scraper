import os
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from companyids import company_ids
import re
import pandas as pd


# Load environment variables (expects JWT_TOKEN in .env or environment)
load_dotenv()

BASE_URL = os.getenv(
    "COMPANY_API_BASE_URL",
    "https://mininghub.com/api/companies/editor/retrieve/",
).rstrip("/") + "/"

JWT_TOKEN = os.getenv("JWT_TOKEN", "").strip()
TIMEOUT_SECS = int(os.getenv("API_TIMEOUT", "30"))
RETRY_ATTEMPTS = int(os.getenv("API_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF = float(os.getenv("API_RETRY_DELAY", "1.0"))
RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "0.3"))

# Output paths
OUTPUT_DIR = Path("outputs/json_outputs")
REPORTS_DIR = Path("outputs/reports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def build_session() -> requests.Session:
    session = requests.Session()

    retry_strategy = Retry(
        total=RETRY_ATTEMPTS,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Default headers similar to our other services
    default_headers: Dict[str, str] = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.0 Safari/537.36"
        ),
        "DNT": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://mininghub.com/",
    }
    # Prefer Authorization if provided (fallback to query token later)
    if JWT_TOKEN:
        default_headers["Authorization"] = f"Bearer {JWT_TOKEN}"

    session.headers.update(default_headers)
    return session


SESSION = build_session()


def _dump_debug_response(gid: int | str, text: str, content_type: Optional[str]) -> None:
    """Save non-JSON responses for debugging."""
    suffix = "html" if content_type and "html" in content_type.lower() else "txt"
    path = REPORTS_DIR / f"company_raw_{gid}.{suffix}"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"📝 Saved debug response for {gid} → {path}")
    except Exception as e:
        print(f"⚠️ Failed to write debug for {gid}: {e}")


def get_company_data(gid: int | str) -> Optional[Dict[str, Any]]:
    """Fetch company editor data by company GID.

    Strategy:
    1) GET {BASE_URL}{gid} with Authorization header if JWT is available.
    2) If 401/403 and JWT available, retry with `?token=JWT` query param.
    3) Validate Content-Type is JSON; if not, attempt json decode; else dump for debug.
    """
    time.sleep(RATE_LIMIT_DELAY)
    url = f"{BASE_URL}{gid}"

    try:
        # Attempt 1: plain GET (Authorization header, if set)
        resp = SESSION.get(url, timeout=TIMEOUT_SECS)
        if resp.status_code in (401, 403) and JWT_TOKEN:
            # Attempt 2: retry with token query param
            url_with_token = f"{url}?token={JWT_TOKEN}"
            resp = SESSION.get(url_with_token, timeout=TIMEOUT_SECS)

        # Raise for non-2xx
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        text = resp.text.strip() if resp.text is not None else ""

        # Prefer content-type check
        if "application/json" in content_type.lower():
            return resp.json()

        # Fallback: try to parse JSON if it looks like JSON
        if text.startswith("{") or text.startswith("["):
            try:
                return json.loads(text)
            except Exception:
                pass

        # Not JSON — dump for debugging and return None
        _dump_debug_response(gid, text, content_type)
        return None

    except requests.exceptions.JSONDecodeError as e:
        print(f"❌ JSON decode error for {gid}: {e}")
        try:
            _dump_debug_response(gid, resp.text, resp.headers.get("Content-Type"))  # type: ignore[name-defined]
        except Exception:
            pass
        return None
    except requests.RequestException as e:
        print(f"❌ HTTP error for {gid}: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error for {gid}: {e}")
        return None


def save_company_data_aggregated(records: List[Dict[str, Any]], path: Path) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved {len(records)} companies → {path}")
    except Exception as e:
        print(f"❌ Failed to save aggregated results: {e}")


def _sanitize_string(value: str, max_len: int = 400) -> str:
    """Remove img source data and truncate long strings.

    - Removes <img ...> tags entirely
    - Neutralizes data:image sources
    - Truncates to max_len characters
    """
    if not value:
        return value

    s = value
    # Remove <img ...> tags
    s = re.sub(r"<\s*img[^>]*>", "", s, flags=re.IGNORECASE | re.DOTALL)
    # Neutralize data:image sources present in strings
    s = re.sub(r"data:image/[^'\" )>]+", "data:image/removed", s, flags=re.IGNORECASE)
    # Also remove explicit src attributes inside any tags
    s = re.sub(r"src\s*=\s*\"[^\"]*\"", 'src=""', s, flags=re.IGNORECASE)
    s = re.sub(r"src\s*=\s*'[^']*'", "src=''", s, flags=re.IGNORECASE)
    # Truncate
    if len(s) > max_len:
        s = s[:max_len]
    return s


def _clean_json_value(value: Any, max_len: int = 400) -> Any:
    """Recursively sanitize strings and truncate values in JSON-like structures."""
    if isinstance(value, str):
        return _sanitize_string(value, max_len=max_len)
    if isinstance(value, list):
        return [_clean_json_value(v, max_len=max_len) for v in value]
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for k, v in value.items():
            cleaned[k] = _clean_json_value(v, max_len=max_len)
        return cleaned
    # passthrough for numbers, booleans, None
    return value


def _infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


def _build_links_schema(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Infer a JSON schema for the links array from observed data."""
    key_to_types: Dict[str, set] = {}
    for rec in records:
        links = rec.get("links")
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict):
                    for k, v in link.items():
                        key_to_types.setdefault(k, set()).add(_infer_type(v))

    link_properties: Dict[str, Any] = {}
    for k, types in key_to_types.items():
        link_properties[k] = {"type": sorted(list(types))}

    schema: Dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "CompanyData",
        "type": "object",
        "properties": {
            "company_id": {"type": ["integer", "string", "null"]},
            "companyName": {"type": ["string", "null"]},
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": link_properties,
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": True,
    }
    return schema


def main(company_ids: List[int]) -> None:
    if not JWT_TOKEN:
        print("⚠️ JWT_TOKEN not set; endpoint may require auth. Proceeding anyway.")

    results: List[Dict[str, Any]] = []
    processed = 0
    max_to_fetch = int(os.getenv("MAX_COMPANIES", "10"))  # default: process 10 for testing

    for gid in company_ids:
        data = get_company_data(gid)
        if data is not None:
            # ensure gid is present for reference
            if isinstance(data, dict) and "gid" not in data:
                data["gid"] = gid
            # sanitize content
            cleaned = _clean_json_value(data, max_len=400)
            results.append(cleaned)
        else:
            print(f"⚠️ Skipped company {gid} (no JSON data)")

        processed += 1
        if processed >= max_to_fetch:
            break

    # Build links schema from observed data and save
    links_schema = _build_links_schema(results)
    schema_path = REPORTS_DIR / "company_links_schema.json"
    try:
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(links_schema, f, ensure_ascii=False, indent=2)
        print(f"🧩 Saved inferred links schema → {schema_path}")
    except Exception as e:
        print(f"⚠️ Failed to save schema: {e}")

    # Save sanitized aggregated JSON
    out_path = OUTPUT_DIR / "company_data.json"
    save_company_data_aggregated(results, out_path)

    # Flatten to Excel: company-level on one sheet, links on another
    try:
        # Company sheet: remove heavy arrays/objects, keep primitives + top-level ids/names
        def flatten_company(rec: Dict[str, Any]) -> Dict[str, Any]:
            flat: Dict[str, Any] = {}
            for k, v in rec.items():
                if k == "links":
                    continue
                if isinstance(v, (str, int, float, bool)) or v is None:
                    flat[k] = v
                else:
                    # store a short preview string for nested structures
                    preview = json.dumps(v) if not isinstance(v, str) else v
                    flat[k] = _sanitize_string(preview, max_len=200)
            return flat

        companies_rows = [flatten_company(r) for r in results if isinstance(r, dict)]

        # Links sheet: explode each link item, attach company_id if missing
        links_rows: List[Dict[str, Any]] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            company_id = r.get("company_id") or r.get("gid")
            links = r.get("links")
            if isinstance(links, list):
                for link in links:
                    if isinstance(link, dict):
                        row = {**link}
                        if "company_id" not in row:
                            row["company_id"] = company_id
                        # sanitize all string fields in link rows
                        for lk, lv in list(row.items()):
                            row[lk] = _clean_json_value(lv, max_len=400)
                        links_rows.append(row)

        companies_df = pd.DataFrame(companies_rows) if companies_rows else pd.DataFrame()
        links_df = pd.DataFrame(links_rows) if links_rows else pd.DataFrame()

        xlsx_path = OUTPUT_DIR / "company_data.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            companies_df.to_excel(writer, sheet_name="companies", index=False)
            links_df.to_excel(writer, sheet_name="links", index=False)
        print(f"📊 Saved Excel with companies and links → {xlsx_path}")
    except Exception as e:
        print(f"⚠️ Failed to create Excel export: {e}")


if __name__ == "__main__":
    main(company_ids)

