"""
Geocoding Service
Provides reverse/forward geocoding with caching, English results, and polite rate limiting.
"""

import os
import json
import time
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass
class GeocodingConfig:
    provider: str = "nominatim"
    delay_seconds: float = 1.0
    cache_dir: str = os.path.join("outputs", "cache")
    timeout: int = 10


class GeocodingService:
    """
    Stateless geocoding client with local file cache and 1s rate limit.
    Returns raw provider JSON; enrichment layer maps fields.
    """

    def __init__(self, config: Optional[GeocodingConfig] = None):
        self.config = config or GeocodingConfig()
        os.makedirs(self.config.cache_dir, exist_ok=True)
        self.cache_path = os.path.join(self.config.cache_dir, "geocoding_cache.json")
        self.cache: Dict[str, Any] = self._load_cache()
        self.last_request_time = 0.0
        self.session = requests.Session()
        # Force English responses
        self.session.headers.update({
            'User-Agent': 'MiningHub-Geocoder/1.0',
            'Accept-Language': 'en-US,en;q=0.9'
        })

    def _load_cache(self) -> Dict[str, Any]:
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load geocoding cache: {e}")
        return {}

    def _save_cache(self) -> None:
        try:
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save geocoding cache: {e}")

    def _respect_rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.config.delay_seconds:
            time.sleep(self.config.delay_seconds - elapsed)

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        if latitude is None or longitude is None:
            return None
        # Use full precision string keys to avoid precision loss
        key = f"rev:{latitude:.8f},{longitude:.8f}"
        if key in self.cache:
            return self.cache[key]

        try:
            self._respect_rate_limit()
            params = {
                'lat': f"{latitude:.8f}",
                'lon': f"{longitude:.8f}",
                'format': 'json',
                'addressdetails': 1,
                'zoom': 10
            }
            resp = self.session.get("https://nominatim.openstreetmap.org/reverse", params=params, timeout=self.config.timeout)
            self.last_request_time = time.time()
            if resp.status_code == 200:
                data = resp.json()
                self.cache[key] = data
                self._save_cache()
                return data
            logger.warning(f"Reverse geocode failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Reverse geocode error: {e}")
        self.cache[key] = None
        self._save_cache()
        return None

    def forward_geocode(self, location: str) -> Optional[Dict[str, Any]]:
        if not location:
            return None
        norm = ' '.join(location.strip().split())
        key = f"fwd:{norm.lower()}"
        if key in self.cache:
            return self.cache[key]

        try:
            self._respect_rate_limit()
            params = {
                'q': norm,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1
            }
            resp = self.session.get("https://nominatim.openstreetmap.org/search", params=params, timeout=self.config.timeout)
            self.last_request_time = time.time()
            if resp.status_code == 200:
                arr = resp.json()
                data = arr[0] if isinstance(arr, list) and arr else None
                self.cache[key] = data
                self._save_cache()
                return data
            logger.warning(f"Forward geocode failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Forward geocode error: {e}")
        self.cache[key] = None
        self._save_cache()
        return None


