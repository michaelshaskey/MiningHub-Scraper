"""
Project Discovery Service
Implements Factor 6 (Stateless Processes) and Factor 4 (Backing Services).
"""

import logging
from typing import Set, List, Dict, Any
from dataclasses import dataclass
import pandas as pd
import os

from .models import DataSource, ProcessingMetrics

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryConfig:
    """Configuration for project discovery."""
    countries: List[str]
    max_projects: int = None
    api_base_url: str = "https://mininghub.com/api"
    jwt_token: str = ""
    found_urls_file: str = "found_urls.xlsx"


class ProjectDiscovery:
    """
    Stateless project discovery service.
    Finds all unique project GIDs from API and URL files.
    """
    
    def __init__(self, config):
        self.config = config
        self.metrics = ProcessingMetrics()
        
        # Validate configuration (Factor 14: Security)
        if not config.jwt_token:
            raise ValueError("JWT_TOKEN is required for API access")
    
    def find_all_gids(self) -> Set[str]:
        """
        Find all unique project GIDs from all sources.
        Returns deduplicated set of GIDs.
        """
        logger.info("Starting project discovery")
        self.metrics.start_time = pd.Timestamp.now()
        
        try:
            # Get GIDs from API
            api_gids = self._get_gids_from_api()
            logger.info(f"Found {len(api_gids)} unique GIDs from API")
            
            # Get GIDs from URL file
            url_gids = self._get_gids_from_urls()
            logger.info(f"Found {len(url_gids)} GIDs from URL file")
            
            # Combine and deduplicate
            all_gids = api_gids.union(url_gids)
            
            # Apply limits if in test mode
            if self.config.max_projects and len(all_gids) > self.config.max_projects:
                # Prioritize API GIDs since we have data for them
                api_gids_list = list(api_gids)[:self.config.max_projects]
                all_gids = set(api_gids_list)
                logger.info(f"Limited to {self.config.max_projects} projects from API for testing")
            
            self.metrics.total_projects = len(all_gids)
            self.metrics.api_projects = len(api_gids)
            self.metrics.end_time = pd.Timestamp.now()
            
            logger.info("Project discovery completed", extra=self.metrics.to_dict())
            
            return all_gids
            
        except Exception as e:
            logger.error(f"Project discovery failed: {e}")
            self.metrics.add_error("discovery_failed")
            raise
    
    def _get_gids_from_api(self) -> Set[str]:
        """
        Extract unique GIDs from API responses.
        Deduplicates immediately to avoid processing duplicates.
        """
        gids = set()
        
        try:
            # Import API client here to avoid circular dependencies
            from services.api_client import MiningHubClient
            
            client = MiningHubClient(
                base_url=self.config.api_base_url,
                jwt_token=self.config.jwt_token
            )
            
            countries = self.config.countries or self._get_all_countries()
            
            for country in countries:
                logger.info(f"Fetching projects for {country}")
                
                try:
                    projects = client.get_projects_by_country(country)
                    
                    # Extract GIDs only, ignore all company data
                    country_gids = {str(project.get('gid', '')) for project in projects}
                    country_gids.discard('')  # Remove empty GIDs
                    
                    logger.info(f"Found {len(country_gids)} unique GIDs in {country}")
                    gids.update(country_gids)
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch projects for {country}: {e}")
                    self.metrics.add_error(f"api_fetch_failed_{country}")
            
            return gids
            
        except Exception as e:
            logger.error(f"API GID extraction failed: {e}")
            self.metrics.add_error("api_gids_failed")
            return set()
    
    def _get_gids_from_urls(self) -> Set[str]:
        """
        Extract GIDs from found_urls.xlsx file.
        Provides fallback for projects not in API.
        """
        try:
            file_path = os.path.join(os.getcwd(), getattr(self.config, 'found_urls_file', 'found_urls.xlsx'))
            
            if not os.path.exists(file_path):
                logger.warning(f"URL file not found: {file_path}")
                return set()
            
            # Read Projects sheet
            projects_df = pd.read_excel(file_path, sheet_name='Projects')
            
            # Extract GIDs, convert to string and remove empty
            gids = {str(gid) for gid in projects_df['ID'].dropna()}
            gids.discard('')  # Remove empty strings
            
            logger.info(f"Extracted {len(gids)} GIDs from URL file")
            return gids
            
        except Exception as e:
            logger.error(f"URL GID extraction failed: {e}")
            self.metrics.add_error("url_gids_failed")
            return set()
    
    def _get_all_countries(self) -> List[str]:
        """Load all countries from countries.json file."""
        try:
            countries_file = os.path.join(os.getcwd(), "countries.json")
            
            if os.path.exists(countries_file):
                import json
                with open(countries_file, 'r') as f:
                    data = json.load(f)
                    return data.get("country", [])
            else:
                # Fallback countries
                return ["Australia", "Canada", "United States of America", "Brazil"]
                
        except Exception as e:
            logger.warning(f"Failed to load countries: {e}")
            return ["Australia", "Canada"]  # Minimal fallback
    
    def get_metrics(self) -> ProcessingMetrics:
        """Get discovery metrics for observability."""
        return self.metrics
