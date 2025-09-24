"""
MiningHub API Client Service
Implements Factor 4 (Backing Services) and Factor 14 (Security).
"""

import requests
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configuration for API client."""
    base_url: str
    jwt_token: str
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 2.0
    rate_limit_delay: float = 0.5


class MiningHubClient:
    """
    Stateless API client for MiningHub services.
    Implements proper retry logic, rate limiting, and error handling.
    """
    
    def __init__(self, base_url: str, jwt_token: str, **kwargs):
        self.config = APIConfig(
            base_url=base_url.rstrip('/'),
            jwt_token=jwt_token,
            **kwargs
        )
        
        # Validate configuration (Factor 14: Security)
        if not self.config.jwt_token:
            raise ValueError("JWT token is required")
        
        # Configure session with connection pooling (Factor 4: Backing Services)
        self.session = requests.Session()
        
        # Add retry strategy
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'MiningHub-DataProcessor/1.0',
            'Accept': 'application/json'
        })
        
        logger.info("API client initialized", extra={
            "base_url": self.config.base_url,
            "timeout": self.config.timeout
        })
    
    def get_projects_by_country(self, country: str) -> List[Dict[str, Any]]:
        """
        Fetch projects for a specific country.
        Returns list of project dictionaries from API response.
        """
        payload = {
            "filters": {
                "country": country,
                "marketcap": {"min": 0, "max": 10000},
                "outstandingshares": {"min": 0, "max": 10000},
                "projectSize": [None, None],
                "commoditiesWhere": "any"
            },
            "token": self.config.jwt_token
        }
        
        logger.debug(f"Fetching projects for country: {country}")
        
        try:
            response = self._make_request(
                method="POST",
                endpoint="/projects/filter",
                json_data=payload,
                description=f"Projects for {country}"
            )
            
            if response and isinstance(response, list):
                logger.info(f"Retrieved {len(response)} projects for {country}")
                return response
            else:
                logger.warning(f"Unexpected response format for {country}: {type(response)}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to fetch projects for {country}: {e}")
            return []
    
    def get_project_relationships(self, gid: str) -> Optional[Dict[str, Any]]:
        """
        Fetch relationship data for a specific project.
        Returns company relationship information.
        """
        payload = {"gid": gid}
        
        logger.debug(f"Fetching relationships for project: {gid}")
        
        try:
            response = self._make_request(
                method="POST",
                endpoint="/project/relationships",
                json_data=payload,
                description=f"Relationships for GID {gid}"
            )
            
            if response:
                logger.debug(f"Retrieved relationships for {gid}")
                return response
            else:
                logger.warning(f"No relationships data for {gid}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to fetch relationships for {gid}: {e}")
            return None
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        description: str = "API call"
    ) -> Optional[Any]:
        """
        Make HTTP request with retry logic and rate limiting.
        Implements proper error handling and logging.
        """
        url = f"{self.config.base_url}{endpoint}"
        
        # Rate limiting (Factor 8: Concurrency)
        time.sleep(self.config.rate_limit_delay)
        
        for attempt in range(self.config.retry_attempts + 1):
            try:
                logger.debug(f"Making {method} request to {endpoint} (attempt {attempt + 1})")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json_data,
                    params=params,
                    timeout=self.config.timeout
                )
                
                # Check for successful response
                response.raise_for_status()
                
                # Parse JSON response
                try:
                    data = response.json()
                    logger.debug(f"Successful {description}")
                    return data
                except ValueError as e:
                    logger.error(f"Invalid JSON in response for {description}: {e}")
                    return None
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout for {description} (attempt {attempt + 1})")
                if attempt < self.config.retry_attempts:
                    time.sleep(self.config.retry_delay * (attempt + 1))  # Exponential backoff
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else None
                
                if status_code == 429:  # Rate limited
                    logger.warning(f"Rate limited for {description} (attempt {attempt + 1})")
                    if attempt < self.config.retry_attempts:
                        time.sleep(self.config.retry_delay * 2 * (attempt + 1))
                elif 500 <= status_code < 600:  # Server errors
                    logger.warning(f"Server error {status_code} for {description} (attempt {attempt + 1})")
                    if attempt < self.config.retry_attempts:
                        time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(f"HTTP error {status_code} for {description}: {e}")
                    return None
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {description} (attempt {attempt + 1}): {e}")
                if attempt < self.config.retry_attempts:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                
            except Exception as e:
                logger.error(f"Unexpected error for {description}: {e}")
                return None
        
        logger.error(f"{description} failed after {self.config.retry_attempts + 1} attempts")
        return None
    
    def health_check(self) -> bool:
        """
        Perform health check against the API.
        Returns True if API is accessible.
        """
        try:
            # Simple GET request to check API availability
            response = self.session.get(
                f"{self.config.base_url}/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def close(self):
        """Close the session and cleanup resources."""
        if self.session:
            self.session.close()
            logger.debug("API client session closed")
