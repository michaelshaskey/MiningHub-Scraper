#!/usr/bin/env python3

# ======================= CONFIG PANEL =======================
CONFIG = {
    # ===================== MODE CONFIGURATION =====================
    "MODE": "PRODUCTION",  # "TEST" or "PRODUCTION" - Controls all limits and behavior
    
    # Country configuration
    "SPECIFIC_COUNTRY": "",  # Set to specific country name (e.g., "Australia")
    "SELECTED_COUNTRIES": [],  # List of countries (e.g., ["Australia", "Canada"])
    # If both above are empty, will run for ALL countries in countries.json
    
    # Output settings
    "COMBINE_ALL_COUNTRIES": True,  # Create one combined file for all countries
    "INDIVIDUAL_COUNTRY_FILES": False,  # Create separate file for each country
    "CREATE_COMPANY_JSON": True,  # Create nested JSON with companies and their projects
    
    # Column filtering
    "EXCLUDE_COLUMNS": [
        "bbox", "available_for_jv_option_sale", "geom", "user_id", 
        "mineralized_zones", "deposits", "parent_id", "default_project", 
        "vrify", "project_subscribed"
    ],
    
    # Processing settings (automatically adjusted by MODE)
    "ENABLE_GEOCODING": True,  # Enable geocoding for missing location data
    "MAX_GEOCODING_REQUESTS": None,  # Limit geocoding requests per country
    "FETCH_COMPANY_RELATIONSHIPS": True,  # Fetch company relationship data
    "MAX_COMPANY_ENRICHMENTS": 40,  # Limit companies to enrich (overridden by MODE)
    "MAX_PROJECTS_PER_COUNTRY": 40,  # Limit API projects per country (overridden by MODE)
    
    # Missing projects scraping settings (automatically adjusted by MODE)
    "SCRAPE_MISSING_PROJECTS": True,  # Enable parallel scraping of missing projects
    "MAX_MISSING_PROJECTS": 40,  # Limit missing projects to scrape (overridden by MODE)
    "SCRAPER_WORKERS": 4,  # Number of parallel scraper workers
    
    # Map center fetching settings
    "FETCH_MAP_CENTERS": True,  # Enable map center fetching for scraped projects
    "MAP_FETCH_CONCURRENCY": 12,  # Concurrent Playwright pages
    "MAP_FETCH_TIMEOUT": 25000,  # Page load timeout (ms)
    "MAP_READY_TIMEOUT": 7000,  # Map readiness timeout (ms)
    
    # API retry and rate limiting
    "API_RETRY_ATTEMPTS": 3,  # Retry attempts for failed API calls
    "API_RETRY_DELAY": 2,  # Seconds between retries
    "RELATIONSHIPS_API_PAUSE_INTERVAL": 200,  # Pause every N relationship API calls
    "RELATIONSHIPS_API_PAUSE_DURATION": 60,  # 1-minute pause
    
    # Progress display
    "SHOW_DETAILED_PROGRESS": True,  # Show progress bars
    "SHOW_COUNTRY_STATS": True,  # Show stats for each country
}
# ===================== END CONFIG PANEL =====================

import json
import pandas as pd
import os
import requests
import time
import datetime
import asyncio
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import Playwright for map center fetching
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ Playwright not available - map center fetching disabled")

# Import schema validator
try:
    from json_schema_validator import MiningDataSchemaValidator
    SCHEMA_VALIDATION_ENABLED = True
except ImportError:
    SCHEMA_VALIDATION_ENABLED = False
    print("⚠️ Schema validation disabled - json_schema_validator.py not found")


class MiningProjectsProcessor:
    """Refactored mining projects processor with all functionality."""
    
    def __init__(self, config: Dict):
        self.config = self._apply_mode_configuration(config)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.dirs = self._create_directories()
        self.countries = self._load_countries()
        self.project_urls, self.company_urls = self._load_urls()
        self.geocoder = Geocoder(self.config) if self.config["ENABLE_GEOCODING"] else None
        
        # Data storage
        self.all_data = []
        self.company_data = {}
        self.orphaned_projects = []
        self.api_calls_count = 0
        self.failed_enrichments = []  # Track companies that failed enrichment
        
        # Schema validation
        self.schema_validator = MiningDataSchemaValidator() if SCHEMA_VALIDATION_ENABLED else None
        
        # Map center fetching
        self.map_fetcher = MapCenterFetcher(self.config) if PLAYWRIGHT_AVAILABLE else None
    
    def _apply_mode_configuration(self, config: Dict) -> Dict:
        """Apply mode-specific configuration adjustments."""
        adjusted_config = config.copy()
        mode = config.get("MODE", "PRODUCTION").upper()
        
        print(f"🎯 Running in {mode} mode")
        
        if mode == "TEST":
            # Test mode: Use configured limits for comprehensive testing
            print(f"   🧪 Test limits: {config.get('MAX_PROJECTS_PER_COUNTRY', 'unlimited')} API projects, "
                  f"{config.get('MAX_MISSING_PROJECTS', 'unlimited')} scraped projects, "
                  f"{config.get('MAX_COMPANY_ENRICHMENTS', 'unlimited')} enrichments")
            
            # In test mode, we'll create a synthetic missing IDs report for testing
            adjusted_config["_TEST_MODE_ACTIVE"] = True
            
        elif mode == "PRODUCTION":
            # Production mode: Remove all limits
            print(f"   🚀 Production mode: All limits removed")
            adjusted_config["SPECIFIC_COUNTRY"] = ""
            adjusted_config["MAX_PROJECTS_PER_COUNTRY"] = None
            adjusted_config["MAX_MISSING_PROJECTS"] = None
            adjusted_config["MAX_COMPANY_ENRICHMENTS"] = None
            adjusted_config["_TEST_MODE_ACTIVE"] = False
            
        else:
            raise ValueError(f"Invalid MODE: {mode}. Must be 'TEST' or 'PRODUCTION'")
        
        return adjusted_config
    
    def _create_directories(self) -> Dict[str, str]:
        """Create organized output directories."""
        # Use test_mode_output for test runs
        if self.config.get("_TEST_MODE_ACTIVE"):
            base_output = os.path.join(self.base_dir, 'test_mode_output')
            print(f"🧪 Test mode: Using separate output directory: {base_output}")
        else:
            base_output = self.base_dir
        
        dirs = {
            'excel': os.path.join(base_output, 'excel_outputs'),
            'json': os.path.join(base_output, 'json_outputs'),
            'reports': os.path.join(base_output, 'reports'),
            'debug': os.path.join(base_output, 'debug')
        }
        
        for dir_path in dirs.values():
            os.makedirs(dir_path, exist_ok=True)
        
        # Clear working data files at start of test runs
        if self.config.get("_TEST_MODE_ACTIVE"):
            self._clear_test_working_files(dirs)
        
        return dirs
    
    def _clear_test_working_files(self, dirs: Dict[str, str]):
        """Clear working data files at start of test runs (preserve master files)."""
        print(f"🧹 Test mode: Clearing working data files...")
        
        files_to_clear = [
            os.path.join(dirs['json'], 'companies_with_projects.json'),
            os.path.join(dirs['json'], 'scraped_projects.json'),
            os.path.join(dirs['json'], 'orphaned_projects.json'),
            os.path.join(dirs['reports'], 'missing_ids_report.csv'),
            os.path.join(dirs['reports'], 'data_coverage_summary.csv'),
        ]
        
        cleared_count = 0
        for file_path in files_to_clear:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    cleared_count += 1
                except Exception as e:
                    print(f"   ⚠️  Could not clear {os.path.basename(file_path)}: {e}")
        
        print(f"   🧹 Cleared {cleared_count} working files")
        print(f"   ✅ Master files (found_urls.xlsx, countries.json) preserved")
    
    def _get_timestamped_filename(self, base_name: str, extension: str, final_output: bool = False) -> str:
        """Generate timestamped filename for final outputs only."""
        if base_name == "missing_ids_report" or not final_output:
            # Don't timestamp working files or missing_ids_report.csv
            return f"{base_name}.{extension}"
        
        # Only timestamp final output files
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{timestamp}.{extension}"
    
    def _load_countries(self) -> List[str]:
        """Load countries from countries.json."""
        try:
            with open(os.path.join(self.base_dir, "countries.json"), 'r') as f:
                return json.load(f).get("country", [])
        except FileNotFoundError:
            return ["Australia", "Canada", "United States of America", "Brazil"]
    
    def _load_urls(self) -> Tuple[Dict, Dict]:
        """Load URLs from found_urls.xlsx."""
        try:
            projects_df = pd.read_excel(os.path.join(self.base_dir, 'found_urls.xlsx'), sheet_name='Projects')
            companies_df = pd.read_excel(os.path.join(self.base_dir, 'found_urls.xlsx'), sheet_name='Companies')
            
            project_urls = dict(zip(projects_df['ID'].astype(str), projects_df['URL']))
            company_urls = dict(zip(companies_df['ID'].astype(str), companies_df['URL']))
            
            print(f"📁 Loaded {len(project_urls)} project URLs, {len(company_urls)} company URLs")
            return project_urls, company_urls
        except Exception as e:
            print(f"⚠️  Error loading URLs: {e}")
            return {}, {}
    
    def _get_countries_to_process(self) -> List[str]:
        """Determine which countries to process."""
        if self.config["SPECIFIC_COUNTRY"]:
            return [self.config["SPECIFIC_COUNTRY"]]
        elif self.config["SELECTED_COUNTRIES"]:
            return self.config["SELECTED_COUNTRIES"]
        else:
            return self.countries
    
    def _api_call_with_retry(self, url: str, payload: Dict, description: str = "API call") -> Optional[Dict]:
        """Make API call with retry logic."""
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        max_retries = self.config.get("API_RETRY_ATTEMPTS", 3)
        retry_delay = self.config.get("API_RETRY_DELAY", 2)
        
        for attempt in range(max_retries + 1):
            try:
                response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
                response.raise_for_status()
                return response.json()
                
            except requests.RequestException as e:
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    print(f"❌ {description} failed after {max_retries + 1} attempts")
                    return None
        return None
    
    def fetch_country_data(self, country: str) -> List[Dict]:
        """Fetch projects for a country."""
        # Get JWT token from environment
        jwt_token = os.getenv('JWT_TOKEN')
        if not jwt_token:
            raise ValueError("JWT_TOKEN environment variable is required. Please check your .env file.")
        
        payload = {
            "filters": {
                "country": country,
                "marketcap": {"min": 0, "max": 10000},
                "outstandingshares": {"min": 0, "max": 10000},
                "projectSize": [None, None],
                "commoditiesWhere": "any"
            },
            "token": jwt_token
        }
        
        data = self._api_call_with_retry("https://mininghub.com/api/projects/filter", payload, f"Projects for {country}")
        return data or []
    
    def process_location(self, location_str: str, centroid_data: Dict, country: str) -> Dict:
        """Process location with optional geocoding."""
        result = {'state': None, 'country': country, 'geocoded': False, 'postcode': None, 'iso_code': None, 'county': None, 'territory': None}
        
        # Parse existing location
        if location_str and pd.notna(location_str):
            parts = location_str.strip().split(', ')
            if len(parts) >= 2:
                result['state'] = parts[0].strip()
                result['country'] = parts[1].strip()
                return result
            elif len(parts) == 1 and parts[0].strip().lower() != country.lower():
                result['state'] = parts[0].strip()
        
        # Use geocoding if enabled and coordinates available
        if (self.geocoder and centroid_data and 'coordinates' in centroid_data and 
            len(centroid_data['coordinates']) >= 2):
            
            lon, lat = centroid_data['coordinates'][0], centroid_data['coordinates'][1]
            geocode_result = self.geocoder.reverse_geocode(lat, lon)
            
            if geocode_result and 'address' in geocode_result:
                address = geocode_result['address']
                for field in ['state', 'state_district', 'territory', 'region', 'province']:
                    if field in address and address[field]:
                        result['state'] = address[field].strip()
                        break
                
                result['country'] = address.get('country', result['country'])
                result['postcode'] = address.get('postcode')
                result['iso_code'] = address.get('ISO3166-2-lvl4')
                result['county'] = address.get('county')
                result['territory'] = address.get('territory')
                result['geocoded'] = True
        
        return result
    
    def fetch_company_relationships(self, project_gid: str) -> Optional[Dict]:
        """Fetch company relationship data with rate limiting."""
        # Rate limiting
        if self.api_calls_count > 0 and self.api_calls_count % self.config["RELATIONSHIPS_API_PAUSE_INTERVAL"] == 0:
            print(f"\n⏸️  Pausing for {self.config['RELATIONSHIPS_API_PAUSE_DURATION']} seconds after {self.api_calls_count} API calls...")
            time.sleep(self.config["RELATIONSHIPS_API_PAUSE_DURATION"])
        
        self.api_calls_count += 1
        payload = {"gid": project_gid}
        data = self._api_call_with_retry("https://mininghub.com/api/project/relationships", payload, f"Relationships for GID {project_gid}")
        
        if data:
            additional_company_info = None
            if 'jv' in data and len(data['jv']) > 0:
                additional_company_info = data['jv'][0]
            
            return {
                'relationships_data': data,
                'primary_company_info': additional_company_info,
                'fetched_using_project_gid': project_gid,
                'api_response_timestamp': datetime.datetime.now().isoformat()
            }
        return None
    
    def process_country(self, country: str) -> Tuple[pd.DataFrame, Dict]:
        """Process all data for a single country."""
        print(f"\n🌍 Processing {country}...")
        
        # Fetch data
        data = self.fetch_country_data(country)
        if not data:
            return pd.DataFrame(), {}
        
        print(f"📊 Found {len(data)} projects in {country}")
        
        # Optional: Limit projects per country if configured
        max_projects = self.config.get("MAX_PROJECTS_PER_COUNTRY")
        if max_projects and len(data) > max_projects:
            data = data[:max_projects]
            print(f"   🔬 Limited to first {max_projects} projects per configuration")
        
        df = pd.DataFrame(data)
        df['source_country'] = country
        
        # Filter unwanted columns
        exclude_cols = [col for col in self.config["EXCLUDE_COLUMNS"] if col in df.columns]
        if exclude_cols:
            df = df.drop(columns=exclude_cols)
        
        # Process locations
        location_results = []
        needs_geocoding = 0
        geocoded_count = 0
        
        progress_iter = tqdm(df.iterrows(), desc=f"🗺️  Processing {country} locations", total=len(df), 
                           leave=False) if self.config["SHOW_DETAILED_PROGRESS"] else df.iterrows()
        
        for _, row in progress_iter:
            location_str = row.get('location')
            needs_geo = (not location_str or pd.isna(location_str) or 
                        (isinstance(location_str, str) and location_str.strip().count(',') == 0))
            
            if needs_geo:
                needs_geocoding += 1
                max_geo = self.config.get("MAX_GEOCODING_REQUESTS")
                if max_geo and geocoded_count >= max_geo:
                    result = {'state': None, 'country': country, 'geocoded': False, 'postcode': None, 'iso_code': None, 'county': None, 'territory': None}
                else:
                    result = self.process_location(location_str, row.get('centroid'), country)
                    if result['geocoded']:
                        geocoded_count += 1
            else:
                result = self.process_location(location_str, row.get('centroid'), country)
            
            location_results.append([result['state'], result['country'], result['geocoded'],
                                   result['postcode'], result['iso_code'], result['county'], result['territory']])
        
        # Add location columns
        new_cols = ['State', 'Country', 'Geocoded', 'Postcode', 'ISO3166_2_Code', 'County', 'Territory']
        for i, col in enumerate(new_cols):
            df[col] = [row[i] for row in location_results]
        
        # Add URLs
        df['project_url'] = df['gid'].astype(str).map(self.project_urls)
        df['company_url'] = df['company_id'].astype(str).map(self.company_urls)
        
        # Organize by companies
        self._organize_by_companies(df, country)
        
        # Stats
        stats = {
            'total_projects': len(df),
            'needed_geocoding': needs_geocoding,
            'successfully_geocoded': geocoded_count,
            'failed_geocoding': needs_geocoding - geocoded_count,
            'unique_states': df['State'].nunique(),
            'unique_companies': df['company_id'].nunique()
        }
        
        if self.config["SHOW_COUNTRY_STATS"]:
            print(f"📈 {country} Stats: {stats['total_projects']} projects, {stats['successfully_geocoded']} geocoded, {stats['unique_states']} states, {stats['unique_companies']} companies")
        
        return df, stats
    
    def _organize_by_companies(self, df: pd.DataFrame, country: str):
        """Organize projects by companies, separating orphaned ones."""
        for _, row in df.iterrows():
            company_id = str(row.get('company_id', ''))
            company_name = row.get('company_name', '')
            
            if pd.isna(company_id) or pd.isna(company_name):
                continue
            
            project_dict = row.to_dict()
            for key, value in project_dict.items():
                if pd.isna(value):
                    project_dict[key] = None
            
            # Check if company has URL
            if company_id not in self.company_urls:
                self.orphaned_projects.append({
                    'project_data': project_dict,
                    'company_info': {'company_id': company_id, 'company_name': company_name, 'company_url': None},
                    'country': country,
                    'orphaned_timestamp': datetime.datetime.now().isoformat()
                })
                continue
            
            # Regular company with URL
            if company_id not in self.company_data:
                self.company_data[company_id] = {
                    'company_id': company_id,
                    'company_name': company_name,
                    'company_url': self.company_urls[company_id],
                    'countries': set(),
                    'projects': [],
                    'additional_company_data': None
                }
            
            self.company_data[company_id]['countries'].add(country)
            self.company_data[company_id]['projects'].append(project_dict)
    
    def enrich_companies(self):
        """Enrich companies with relationship data."""
        if not self.config["FETCH_COMPANY_RELATIONSHIPS"] or not self.company_data:
            return
        
        companies_list = list(self.company_data.items())
        max_enrichments = self.config.get("MAX_COMPANY_ENRICHMENTS")
        if max_enrichments:
            companies_list = companies_list[:max_enrichments]
        
        print(f"\n🔗 Enriching {len(companies_list)} companies with relationship data...")
        
        enriched_count = 0
        companies_iter = tqdm(companies_list, desc="🔗 Enriching companies", leave=False) if self.config["SHOW_DETAILED_PROGRESS"] else companies_list
        
        for company_id, company_info in companies_iter:
            # Rate limiting check
            if self.api_calls_count > 0 and self.api_calls_count % self.config["RELATIONSHIPS_API_PAUSE_INTERVAL"] == 0:
                print(f"\n⏸️  Pausing for {self.config['RELATIONSHIPS_API_PAUSE_DURATION']} seconds after {self.api_calls_count} API calls...")
                time.sleep(self.config["RELATIONSHIPS_API_PAUSE_DURATION"])
            
            self.api_calls_count += 1
            
            # Get sample project for API call
            sample_project = company_info['projects'][0] if company_info['projects'] else None
            if not sample_project:
                continue
            
            project_gid = sample_project.get('gid')
            if not project_gid:
                continue
            
            # Fetch relationship data
            payload = {"gid": project_gid}
            relationship_data = self._api_call_with_retry("https://mininghub.com/api/project/relationships", payload, f"Relationships for company {company_id}")
            
            if relationship_data:
                # Extract only company-wide data (not project-specific relationships)
                company_wide_data = self._extract_company_data_only(relationship_data)
                
                if company_wide_data:
                    company_info['additional_company_data'] = {
                        'company_info': company_wide_data,
                        'fetched_using_project_gid': project_gid,
                        'api_response_timestamp': datetime.datetime.now().isoformat(),
                        'enrichment_source': 'main_script'
                    }
                    enriched_count += 1
                else:
                    # Track failed enrichment for standalone tool
                    self.failed_enrichments.append({
                        'company_id': company_id,
                        'company_name': company_info.get('company_name', ''),
                        'tried_project_gid': project_gid,
                        'failure_reason': 'No usable company data in relationships response'
                    })
            else:
                # Track failed enrichment for standalone tool
                self.failed_enrichments.append({
                    'company_id': company_id,
                    'company_name': company_info.get('company_name', ''),
                    'tried_project_gid': project_gid,
                    'failure_reason': 'Relationships API call failed'
                })
        
        print(f"✅ Enriched {enriched_count}/{len(companies_list)} companies")
        
        # Save enrichment status for standalone tool
        if self.failed_enrichments:
            status_data = {
                'failed_enrichments': self.failed_enrichments,
                'total_companies': len(self.company_data),
                'enriched_companies': enriched_count,
                'failed_companies': len(self.failed_enrichments),
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            with open(os.path.join(self.dirs['reports'], 'enrichment_status.json'), 'w') as f:
                json.dump(status_data, f, indent=2)
            
            print(f"📋 Enrichment failures logged: {len(self.failed_enrichments)} companies need retry")
    
    def _extract_company_data_only(self, relationships_response: Dict) -> Optional[Dict]:
        """Extract only company-wide data, filtering out project-specific relationships."""
        if not relationships_response or 'jv' not in relationships_response:
            return None
        
        jv_companies = relationships_response.get('jv', [])
        if not jv_companies:
            return None
        
        # Get the primary company (usually the first JV entry)
        primary_company = jv_companies[0]
        
        # Extract only company-wide fields (not project-specific data)
        company_data = {
            'company_name': primary_company.get('company_name'),
            'website': primary_company.get('website'),
            'root_ticker': primary_company.get('root_ticker'),
            'exchange': primary_company.get('exchange'),
            'root_ticker_02': primary_company.get('root_ticker_02'),
            'exchange_02': primary_company.get('exchange_02'),
            'root_ticker_03': primary_company.get('root_ticker_03'),
            'exchange_03': primary_company.get('exchange_03'),
            'ceo': primary_company.get('ceo'),
            'headquarters': primary_company.get('headquarters'),
            'phone': primary_company.get('phone'),
            'address': primary_company.get('address'),
            'country': primary_company.get('country'),
            'state': primary_company.get('state'),
            'city': primary_company.get('city'),
            'zip': primary_company.get('zip'),
            'industry': primary_company.get('industry'),
            'sector': primary_company.get('sector'),
            'primary_sector': primary_company.get('primary_sector'),
            'is_delisted': primary_company.get('is_delisted'),
            # Exclude project-specific fields: percentage, projectCompanyOwnership, projectCompanyNsr
        }
        
        # Remove None values
        return {k: v for k, v in company_data.items() if v is not None}
    
    def create_summaries(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create summary reports."""
        summaries = {}
        
        # Company summary
        if 'company_id' in df.columns:
            summaries['company'] = df.groupby(['company_id', 'company_name', 'source_country']).size().reset_index(name='Number_of_Projects')
            summaries['company'] = summaries['company'].sort_values('Number_of_Projects', ascending=False)
        
        # Location summary
        summaries['location'] = df.groupby(['State', 'Country', 'source_country']).agg({
            'gid': 'count' if 'gid' in df.columns else 'size',
            'company_id': 'nunique' if 'company_id' in df.columns else 'size'
        }).reset_index()
        summaries['location'].columns = ['State', 'Country', 'Source_Country', 'Number_of_Projects', 'Number_of_Companies']
        summaries['location'] = summaries['location'].sort_values('Number_of_Projects', ascending=False)
        
        # Geocoding summary
        geocoded_count = df['Geocoded'].sum()
        incomplete_count = (df['location'].isna() | (df['location'].astype(str).str.count(',') == 0)).sum()
        
        summaries['geocoding'] = pd.DataFrame({
            'Metric': ['Total Projects', 'Had Complete Location', 'Had Incomplete Location', 'Successfully Geocoded', 'Failed to Geocode'],
            'Count': [len(df), len(df) - incomplete_count, incomplete_count, geocoded_count, incomplete_count - geocoded_count]
        })
        
        return summaries
    
    def _get_scraped_projects_dataframe(self) -> Optional[pd.DataFrame]:
        """Create DataFrame from scraped projects for Excel export."""
        try:
            scraped_file = os.path.join(self.dirs['json'], 'scraped_projects.json')
            if not os.path.exists(scraped_file):
                return None
            
            with open(scraped_file, 'r') as f:
                scraped_data = json.load(f)
            
            projects = scraped_data.get('scraped_projects', [])
            successful_projects = [p for p in projects if p.get('scrape_success', False)]
            
            if not successful_projects:
                return None
            
            # Flatten projects data for Excel
            rows = []
            for project in successful_projects:
                # Use primary company data for clean CRM structure
                row = {
                    'gid': project.get('gid'),
                    'project_name': project.get('project_name'),
                    'operator': project.get('operator'),
                    'commodities': project.get('commodities'),
                    'stage': project.get('stage'),
                    'root_ticker': project.get('ticker_exchange', '').split(':')[1] if ':' in str(project.get('ticker_exchange', '')) else None,
                    'exchange': project.get('ticker_exchange', '').split(':')[0] if ':' in str(project.get('ticker_exchange', '')) else None,
                    'primary_company_id': project.get('primary_company_id'),
                    'primary_company_name': project.get('primary_company_name'),
                    'primary_company_url': project.get('primary_company_url'),
                    'project_url': project.get('url'),
                    'latitude': project.get('centroid', {}).get('coordinates', [None, None])[1] if project.get('centroid') else None,
                    'longitude': project.get('centroid', {}).get('coordinates', [None, None])[0] if project.get('centroid') else None,
                    'location': project.get('location'),
                    'State': project.get('State'),
                    'Country': project.get('Country'),
                    'Geocoded': project.get('Geocoded', False),
                    'map_center_source': project.get('map_center_source'),
                    'scrape_timestamp': project.get('scrape_timestamp'),
                    'worker_id': project.get('worker_id')
                }
                rows.append(row)
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            print(f"⚠️ Error creating scraped projects DataFrame: {e}")
            return None
    
    def _get_all_projects_combined_dataframe(self) -> Optional[pd.DataFrame]:
        """Create combined DataFrame from companies JSON for Excel export."""
        try:
            companies_file = os.path.join(self.dirs['json'], 'companies_with_projects.json')
            if not os.path.exists(companies_file):
                return None
            
            with open(companies_file, 'r') as f:
                companies_data = json.load(f)
            
            # Flatten all projects from all companies
            rows = []
            for company in companies_data:
                for project in company.get('projects', []):
                    row = project.copy()  # Start with all project fields
                    
                    # Add company-level information
                    row['company_total_projects'] = company.get('total_projects', 0)
                    row['company_countries'] = ', '.join(company.get('countries', []))
                    
                    # Add enrichment status
                    if company.get('additional_company_data'):
                        row['company_enriched'] = True
                        enrichment = company['additional_company_data'].get('company_info', {})
                        row['company_website'] = enrichment.get('website')
                        row['company_ceo'] = enrichment.get('ceo')
                        row['company_headquarters'] = enrichment.get('headquarters')
                        row['company_industry'] = enrichment.get('industry')
                    else:
                        row['company_enriched'] = False
                    
                    rows.append(row)
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            print(f"⚠️ Error creating combined projects DataFrame: {e}")
            return None
    
    def _get_companies_dataframe(self) -> Optional[pd.DataFrame]:
        """Create companies DataFrame for Excel export."""
        try:
            companies_file = os.path.join(self.dirs['json'], 'companies_with_projects.json')
            if not os.path.exists(companies_file):
                return None
            
            with open(companies_file, 'r') as f:
                companies_data = json.load(f)
            
            rows = []
            for company in companies_data:
                row = {
                    'company_id': company.get('company_id'),
                    'company_name': company.get('company_name'),
                    'company_url': company.get('company_url'),
                    'countries': ', '.join(company.get('countries', [])),
                    'total_projects': company.get('total_projects', 0),
                    'data_source': company.get('data_source', 'api'),
                    'enriched': bool(company.get('additional_company_data'))
                }
                
                # Add enrichment data if available
                if company.get('additional_company_data'):
                    enrichment = company['additional_company_data'].get('company_info', {})
                    row.update({
                        'website': enrichment.get('website'),
                        'ceo': enrichment.get('ceo'),
                        'headquarters': enrichment.get('headquarters'),
                        'industry': enrichment.get('industry'),
                        'sector': enrichment.get('sector'),
                        'root_ticker': enrichment.get('root_ticker'),
                        'exchange': enrichment.get('exchange'),
                        'is_delisted': enrichment.get('is_delisted'),
                        'enrichment_source': company['additional_company_data'].get('enrichment_source'),
                        'enrichment_timestamp': company['additional_company_data'].get('api_response_timestamp')
                    })
                
                rows.append(row)
            
            # Sort by total projects descending
            df = pd.DataFrame(rows)
            return df.sort_values('total_projects', ascending=False)
            
        except Exception as e:
            print(f"⚠️ Error creating companies DataFrame: {e}")
            return None
    
    def _get_orphaned_projects_dataframe(self) -> Optional[pd.DataFrame]:
        """Create orphaned projects DataFrame for Excel export."""
        try:
            if not self.orphaned_projects:
                return None
            
            rows = []
            for orphan in self.orphaned_projects:
                project_data = orphan.get('project_data', {})
                company_info = orphan.get('company_info', {})
                
                row = project_data.copy()
                row.update({
                    'orphaned_company_id': company_info.get('company_id'),
                    'orphaned_company_name': company_info.get('company_name'),
                    'orphaned_reason': 'Company URL not found in found_urls.xlsx',
                    'orphaned_timestamp': orphan.get('orphaned_timestamp'),
                    'orphaned_country': orphan.get('country')
                })
                rows.append(row)
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            print(f"⚠️ Error creating orphaned projects DataFrame: {e}")
            return None
    
    def _get_all_projects_unified_dataframe(self) -> Optional[pd.DataFrame]:
        """Create unified DataFrame with ALL projects (API + scraped + orphaned) with source tracking."""
        try:
            all_rows = []
            
            # 1. Add API projects
            if self.all_data:
                for df in self.all_data:
                    for _, row in df.iterrows():
                        project_row = row.to_dict()
                        # Clean NaN values
                        for key, value in project_row.items():
                            if pd.isna(value):
                                project_row[key] = None
                        
                        project_row['data_source'] = 'api'
                        project_row['has_company_url'] = bool(project_row.get('company_url'))
                        all_rows.append(project_row)
            
            # 2. Add scraped projects from companies JSON
            companies_file = os.path.join(self.dirs['json'], 'companies_with_projects.json')
            if os.path.exists(companies_file):
                with open(companies_file, 'r') as f:
                    companies_data = json.load(f)
                
                for company in companies_data:
                    for project in company.get('projects', []):
                        if project.get('scrape_source') == 'parallel_scraper':
                            project_row = project.copy()
                            project_row['data_source'] = 'scraped'
                            project_row['has_company_url'] = bool(project.get('company_url'))
                            all_rows.append(project_row)
            
            # 3. Add orphaned projects (and try to resolve company URLs)
            for orphan in self.orphaned_projects:
                project_data = orphan.get('project_data', {})
                company_info = orphan.get('company_info', {})
                
                project_row = project_data.copy()
                # Clean NaN values
                for key, value in project_row.items():
                    if pd.isna(value):
                        project_row[key] = None
                
                project_row['data_source'] = 'orphaned'
                project_row['orphaned_reason'] = 'Company URL not found in found_urls.xlsx'
                project_row['orphaned_timestamp'] = orphan.get('orphaned_timestamp')
                
                # Try to resolve company URL from scraped data
                company_id = str(company_info.get('company_id', ''))
                resolved_url = self._try_resolve_orphaned_company_url(company_id)
                if resolved_url:
                    project_row['company_url'] = resolved_url
                    project_row['company_url_source'] = 'resolved_from_scraped'
                    project_row['has_company_url'] = True
                else:
                    project_row['has_company_url'] = False
                
                all_rows.append(project_row)
            
            if not all_rows:
                return None
            
            df = pd.DataFrame(all_rows)
            
            # Sort by data_source (api first, then scraped, then orphaned) and company name
            source_order = {'api': 1, 'scraped': 2, 'orphaned': 3}
            df['_sort_order'] = df['data_source'].map(source_order)
            
            # Fill missing values to prevent KeyError during sorting
            df['company_name'] = df['company_name'].fillna('Unknown Company')
            df['project_name'] = df['project_name'].fillna('Unknown Project')
            
            df = df.sort_values(['_sort_order', 'company_name', 'project_name'])
            df = df.drop('_sort_order', axis=1)
            
            return df
            
        except Exception as e:
            print(f"⚠️ Error creating unified projects DataFrame: {e}")
            return None
    
    def _try_resolve_orphaned_company_url(self, company_id: str) -> Optional[str]:
        """Try to resolve orphaned company URL from scraped projects or companies data."""
        try:
            # Check if this company was found during scraping
            companies_file = os.path.join(self.dirs['json'], 'companies_with_projects.json')
            if os.path.exists(companies_file):
                with open(companies_file, 'r') as f:
                    companies_data = json.load(f)
                
                for company in companies_data:
                    if str(company.get('company_id', '')) == company_id:
                        return company.get('company_url')
            
            # Check scraped projects for this company
            scraped_file = os.path.join(self.dirs['json'], 'scraped_projects.json')
            if os.path.exists(scraped_file):
                with open(scraped_file, 'r') as f:
                    scraped_data = json.load(f)
                
                for project in scraped_data.get('scraped_projects', []):
                    if project.get('scrape_success') and project.get('primary_company_id') == company_id:
                        return project.get('primary_company_url')
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error resolving orphaned company URL for {company_id}: {e}")
            return None
    
    def save_outputs(self, combined_df: pd.DataFrame = None) -> List[str]:
        """Save all outputs to organized directories."""
        results = []
        
        # Save Excel file with ALL projects (API + scraped) and companies
        excel_filename = self._get_timestamped_filename('all_countries_projects', 'xlsx', final_output=True)
        excel_path = os.path.join(self.dirs['excel'], excel_filename)
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # All Projects Combined sheet (main sheet with everything)
            all_projects_df = self._get_all_projects_unified_dataframe()
            if all_projects_df is not None and not all_projects_df.empty:
                all_projects_df.to_excel(writer, sheet_name='All_Projects', index=False)
                print(f"📊 Excel: Added {len(all_projects_df)} total projects")
                
                # Verify counts
                api_count = len(all_projects_df[all_projects_df['data_source'] == 'api'])
                scraped_count = len(all_projects_df[all_projects_df['data_source'] == 'scraped'])
                orphaned_count = len(all_projects_df[all_projects_df['data_source'] == 'orphaned'])
                print(f"   📊 Breakdown: {api_count} API + {scraped_count} scraped + {orphaned_count} orphaned = {api_count + scraped_count + orphaned_count}")
            
            # Companies sheet
            companies_df = self._get_companies_dataframe()
            if companies_df is not None and not companies_df.empty:
                companies_df.to_excel(writer, sheet_name='Companies', index=False)
                print(f"📊 Excel: Added {len(companies_df)} companies")
            
            # Summary sheets
            if combined_df is not None and not combined_df.empty:
                summaries = self.create_summaries(combined_df)
                for name, summary_df in summaries.items():
                    summary_df.to_excel(writer, sheet_name=f"{name.title()}_Summary", index=False)
        
        results.append(excel_path)
        
        # Save company JSON
        if self.config["CREATE_COMPANY_JSON"] and self.company_data:
            companies_json = []
            for company_info in self.company_data.values():
                company_copy = company_info.copy()
                company_copy['countries'] = list(company_info['countries'])
                company_copy['total_projects'] = len(company_info['projects'])
                companies_json.append(company_copy)
            
            companies_json.sort(key=lambda x: x['total_projects'], reverse=True)
            
            company_filename = self._get_timestamped_filename('companies_with_projects', 'json', final_output=True)
            company_path = os.path.join(self.dirs['json'], company_filename)
            with open(company_path, 'w', encoding='utf-8') as f:
                json.dump(companies_json, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Company JSON: {len(companies_json)} companies")
            results.append(company_path)
        
        # Save orphaned projects JSON
        if self.orphaned_projects:
            orphaned_data = {
                'summary': {
                    'total_orphaned_projects': len(self.orphaned_projects),
                    'orphaned_companies': len(set(p['company_info']['company_id'] for p in self.orphaned_projects)),
                    'countries_affected': list(set(p['country'] for p in self.orphaned_projects))
                },
                'orphaned_projects': self.orphaned_projects
            }
            
            orphaned_filename = self._get_timestamped_filename('orphaned_projects', 'json', final_output=True)
            orphaned_path = os.path.join(self.dirs['json'], orphaned_filename)
            with open(orphaned_path, 'w', encoding='utf-8') as f:
                json.dump(orphaned_data, f, indent=2, ensure_ascii=False)
            
            print(f"🔗 Orphaned projects: {len(self.orphaned_projects)} projects from {orphaned_data['summary']['orphaned_companies']} companies")
            results.append(orphaned_path)
        
        # Note: Coverage report is created separately by _create_coverage_report()
        
        return results
    
    def _create_coverage_report(self):
        """Create data coverage analysis report."""
        api_project_ids = set()
        api_company_ids = set()
        
        for df in self.all_data:
            api_project_ids.update(df['gid'].astype(str).tolist())
            api_company_ids.update(df['company_id'].astype(str).tolist())
        
        url_project_ids = set(self.project_urls.keys())
        url_company_ids = set(self.company_urls.keys())
        
        # Calculate coverage
        projects_with_urls = len(api_project_ids & url_project_ids)
        companies_with_urls = len(api_company_ids & url_company_ids)
        
        summary_data = [
            {'Metric': 'Total Projects in API', 'Count': len(api_project_ids)},
            {'Metric': 'Projects with URLs', 'Count': projects_with_urls},
            {'Metric': 'Projects coverage %', 'Count': f"{projects_with_urls/len(api_project_ids)*100:.1f}%" if len(api_project_ids) > 0 else "0.0%"},
            {'Metric': 'Total Companies in API', 'Count': len(api_company_ids)},
            {'Metric': 'Companies with URLs', 'Count': companies_with_urls},
            {'Metric': 'Companies coverage %', 'Count': f"{companies_with_urls/len(api_company_ids)*100:.1f}%" if len(api_company_ids) > 0 else "0.0%"},
        ]
        
        summary_df = pd.DataFrame(summary_data)
        coverage_filename = self._get_timestamped_filename('data_coverage_summary', 'csv', final_output=True)
        summary_df.to_csv(os.path.join(self.dirs['reports'], coverage_filename), index=False)
        
        project_pct = f"{projects_with_urls/len(api_project_ids)*100:.1f}%" if len(api_project_ids) > 0 else "0.0%"
        company_pct = f"{companies_with_urls/len(api_company_ids)*100:.1f}%" if len(api_company_ids) > 0 else "0.0%"
        print(f"📊 Coverage: Projects {projects_with_urls}/{len(api_project_ids)} ({project_pct}), Companies {companies_with_urls}/{len(api_company_ids)} ({company_pct})")
        
        # Create detailed missing IDs report
        if self.config.get("_TEST_MODE_ACTIVE"):
            self._create_test_missing_ids_report()
        else:
            # Production mode: Generate real missing IDs report
            self._create_detailed_missing_ids_report(api_project_ids, api_company_ids, url_project_ids, url_company_ids)
    
    def _create_detailed_missing_ids_report(self, api_project_ids, api_company_ids, url_project_ids, url_company_ids):
        """Create detailed missing IDs report."""
        missing_data = []
        
        # Projects in API but missing from URL file
        for project_id in api_project_ids - url_project_ids:
            missing_data.append({
                'ID': project_id,
                'Type': 'Project',
                'Status': 'In API but missing from URL file',
                'URL': None,
                'Notes': 'Project exists in API response but no URL found'
            })
        
        # Projects in URL file but missing from API
        for project_id in url_project_ids - api_project_ids:
            missing_data.append({
                'ID': project_id,
                'Type': 'Project',
                'Status': 'In URL file but missing from API',
                'URL': self.project_urls.get(project_id, ''),
                'Notes': 'URL exists but project not found in API response'
            })
        
        # Companies in API but missing from URL file
        for company_id in api_company_ids - url_company_ids:
            missing_data.append({
                'ID': company_id,
                'Type': 'Company',
                'Status': 'In API but missing from URL file',
                'URL': None,
                'Notes': 'Company exists in API response but no URL found'
            })
        
        # Companies in URL file but missing from API
        for company_id in url_company_ids - api_company_ids:
            missing_data.append({
                'ID': company_id,
                'Type': 'Company',
                'Status': 'In URL file but missing from API',
                'URL': self.company_urls.get(company_id, ''),
                'Notes': 'URL exists but company not found in API response'
            })
        
        # Save detailed report
        if missing_data:
            missing_df = pd.DataFrame(missing_data)
            missing_df.to_csv(os.path.join(self.dirs['reports'], 'missing_ids_report.csv'), index=False)
            print(f"📋 Missing IDs report: {len(missing_data)} missing entries saved")
    
    def _create_test_missing_ids_report(self):
        """Create a synthetic missing IDs report for testing purposes."""
        print(f"📋 Test mode: Creating synthetic missing IDs report for testing")
        
        # Get a sample of projects from the URL file that we can use for testing
        max_test_projects = self.config.get("MAX_MISSING_PROJECTS", 40)
        
        # Sample some project IDs from the URL file
        available_project_ids = list(self.project_urls.keys())
        
        # Take a sample for testing (skip the first few to simulate "missing" projects)
        start_idx = 100  # Skip first 100 to get different projects than API
        end_idx = start_idx + max_test_projects
        test_project_ids = available_project_ids[start_idx:end_idx]
        
        missing_data = []
        for project_id in test_project_ids:
            missing_data.append({
                'ID': project_id,
                'Type': 'Project',
                'Status': 'In URL file but missing from API',
                'URL': self.project_urls.get(project_id, ''),
                'Notes': 'Synthetic test data - simulated missing project for testing'
            })
        
        # Save test report
        if missing_data:
            missing_df = pd.DataFrame(missing_data)
            missing_df.to_csv(os.path.join(self.dirs['reports'], 'missing_ids_report.csv'), index=False)
            print(f"📋 Test missing IDs report: {len(missing_data)} synthetic entries created for testing")
            print(f"   🧪 These are NOT real missing projects - just test data")
        else:
            # Create empty report if no test data
            empty_df = pd.DataFrame(columns=['ID', 'Type', 'Status', 'URL', 'Notes'])
            empty_df.to_csv(os.path.join(self.dirs['reports'], 'missing_ids_report.csv'), index=False)
            print(f"📋 Test missing IDs report: Empty report created")
    
    def _scrape_missing_projects(self):
        """Scrape missing projects using parallel scraper."""
        try:
            print(f"\n🔍 Starting parallel scraping of missing projects (Playwright)…")

            # Load missing projects from report
            import pandas as pd
            report_path = os.path.join(self.dirs['reports'], 'missing_ids_report.csv')
            if not os.path.exists(report_path):
                print("⚠️ missing_ids_report.csv not found; skipping scraping")
                return

            df = pd.read_csv(report_path)
            missing_projects = df[
                (df['Type'] == 'Project') &
                (df['Status'] == 'In URL file but missing from API')
            ].copy()

            # Filter out already processed scraped projects
            scraped_file = os.path.join(self.dirs['json'], 'scraped_projects.json')
            existing_gids = set()
            if os.path.exists(scraped_file):
                try:
                    with open(scraped_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    for p in existing_data.get('scraped_projects', []):
                        if p.get('scrape_success', False):
                            existing_gids.add(str(p.get('gid', '')))
                except Exception:
                    pass

            gids_all = [str(x) for x in missing_projects['ID'].astype(str).tolist() if str(x) not in existing_gids]

            max_projects = self.config.get("MAX_MISSING_PROJECTS")
            if max_projects:
                gids = gids_all[:max_projects]
            else:
                gids = gids_all

            if not gids:
                print("📭 No new missing projects to scrape")
                return

            # Run Playwright parallel scraper
            try:
                from services.playwright_parallel_scraper import PlaywrightParallelScraper
            except Exception as e:
                print(f"❌ Playwright scraper not available: {e}")
                print("   Falling back to legacy Selenium scraper…")
                from project_scraper_parallel_compact import ParallelProjectScraper
                legacy = ParallelProjectScraper(base_dir=self.base_dir, max_workers=self.config.get("SCRAPER_WORKERS", 3), batch_size=10, test_mode=self.config.get("_TEST_MODE_ACTIVE", False))
                output_file = legacy.scrape_missing_projects(max_projects=self.config.get("MAX_MISSING_PROJECTS"))
                if output_file:
                    print(f"✅ Missing projects scraping completed!")
                    print(f"   📁 Results saved to: {os.path.relpath(output_file, self.base_dir)}")
                    self._merge_scraped_projects_with_companies()
                return

            concurrency = max(1, int(self.config.get("SCRAPER_WORKERS", 4)))

            async def run_playwright_scrape(ids: list):
                scraper = PlaywrightParallelScraper()
                recs = await scraper.scrape_many_parallel(ids, max_concurrency=concurrency, headless=True, verbose=True)
                return [PlaywrightParallelScraper.to_dict(r) for r in recs]

            rec_dicts = asyncio.run(run_playwright_scrape(gids))

            # Transform to legacy scraped_projects schema for downstream compatibility
            import datetime as _dt
            scraped_projects = []
            success_count = 0
            for r in rec_dicts:
                gid = str(r.get('gid', ''))
                # Normalize company
                company_id = str(r.get('company_id') or '').strip() or None
                company_name = r.get('company_name') or r.get('operator') or None
                company_url = r.get('company_url') if company_id else None
                if company_name and isinstance(company_name, str):
                    company_name = ' '.join([w.capitalize() for w in company_name.split()])
                # Operator fallback
                operator = r.get('operator') or company_name

                proj = {
                    'gid': gid,
                    'url': r.get('project_url') or f"https://mininghub.com/project-profile?gid={gid}",
                    'project_name': r.get('project_name'),
                    'operator': operator,
                    'commodities': r.get('commodities'),
                    'stage': r.get('stage'),
                    'ticker_exchange': r.get('ticker_exchange'),
                    'primary_company_id': company_id,
                    'primary_company_name': company_name,
                    'primary_company_url': company_url,
                    'company_ids': [company_id] if company_id else [],
                    'company_names': [company_name] if company_name else [],
                    'company_urls': [company_url] if company_url else [],
                    'scrape_success': True,
                    'error_message': None,
                    'scrape_timestamp': _dt.datetime.now().isoformat(),
                    'worker_id': None,
                    'scrape_source': 'playwright_parallel_scraper',
                    'data_source': 'scraped',
                    # Placeholders for map/geocoding update step
                    'centroid': None,
                    'location': None,
                    'State': None,
                    'Country': None,
                    'Geocoded': False,
                    'map_center_source': None,
                }
                scraped_projects.append(proj)
                success_count += 1

            out_path = os.path.join(self.dirs['json'], 'scraped_projects.json')
            payload = {
                'summary': {
                    'total_processed': len(scraped_projects),
                    'successful_scrapes': success_count,
                    'failed_scrapes': len(scraped_projects) - success_count,
                    'projects_with_companies': len([p for p in scraped_projects if p.get('company_ids')]),
                    'total_companies': sum(len(p.get('company_ids', [])) for p in scraped_projects),
                    'last_updated': _dt.datetime.now().isoformat(),
                    'scraper_version': 'playwright_parallel_v1.0',
                },
                'scraped_projects': scraped_projects
            }
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)

            print(f"✅ Missing projects scraping completed!")
            print(f"   📁 Results saved to: {os.path.relpath(out_path, self.base_dir)}")

            # Merge scraped data with companies_with_projects.json
            self._merge_scraped_projects_with_companies()
                
        except ImportError as e:
            print(f"❌ Could not import parallel scraper: {e}")
            print(f"   Make sure project_scraper_parallel_compact.py is in the same directory")
        except Exception as e:
            print(f"❌ Error during missing projects scraping: {e}")
    
    def _merge_scraped_projects_with_companies(self):
        """Merge scraped projects with companies data using proper schema transformation."""
        try:
            scraped_file = os.path.join(self.dirs['json'], 'scraped_projects.json')
            companies_file = os.path.join(self.dirs['json'], 'companies_with_projects.json')
            
            if not os.path.exists(scraped_file):
                print("⚠️  No scraped projects file found to merge")
                return
            
            # Load scraped projects
            with open(scraped_file, 'r') as f:
                scraped_data = json.load(f)
            
            scraped_projects = scraped_data.get('scraped_projects', [])
            successful_projects = [p for p in scraped_projects if p.get('scrape_success', False)]
            
            if not successful_projects:
                print("⚠️  No successful scraped projects to merge")
                return
            
            print(f"🔗 Merging {len(successful_projects)} scraped projects with companies data...")
            
            # Load existing companies data
            companies_data = []
            if os.path.exists(companies_file):
                with open(companies_file, 'r') as f:
                    companies_data = json.load(f)
            
            # Create a lookup for existing companies
            companies_lookup = {comp['company_id']: comp for comp in companies_data}
            
            # Process scraped projects with proper schema transformation
            new_companies = 0
            updated_companies = 0
            projects_added = 0
            
            for scraped_project in successful_projects:
                # Use primary company data for clean CRM integration
                primary_company_id = scraped_project.get('primary_company_id') or (scraped_project.get('company_ids', [None])[0])
                primary_company_name = scraped_project.get('primary_company_name') or (scraped_project.get('company_names', [None])[0])
                primary_company_url = scraped_project.get('primary_company_url') or (scraped_project.get('company_urls', [None])[0])
                
                if not primary_company_id:
                    print(f"   ⚠️ Skipping project {scraped_project.get('gid')} - no company data found")
                    continue
                
                # Transform scraped project to match API schema with validation
                if self.schema_validator:
                    is_valid, transformed_project, error_msg = self.schema_validator.transform_and_validate_scraped_project(scraped_project)
                    if not is_valid:
                        print(f"   ⚠️ Schema validation failed for project {scraped_project.get('gid')}: {error_msg}")
                        continue
                else:
                    transformed_project = self._transform_scraped_project_to_api_schema(scraped_project)
                
                # Create clean project record with single company (CRM-ready)
                project_data = transformed_project.copy()
                project_data.update({
                    'company_id': int(primary_company_id) if primary_company_id.isdigit() else primary_company_id,
                    'company_name': primary_company_name or 'Unknown Company',
                    'company_url': primary_company_url
                })
                
                company_id = str(primary_company_id)
                
                if company_id in companies_lookup:
                    # Add project to existing company
                    company = companies_lookup[company_id]
                    if 'projects' not in company:
                        company['projects'] = []
                    
                    # Check if project already exists (by gid)
                    existing_gids = {str(p.get('gid', '')) for p in company['projects']}
                    project_gid = str(project_data.get('gid', ''))
                    
                    if project_gid not in existing_gids:
                        company['projects'].append(project_data)
                        company['total_projects'] = len(company['projects'])
                        projects_added += 1
                        
                        if company_id not in [c['company_id'] for c in companies_data if c.get('updated_by_scraper')]:
                            updated_companies += 1
                            company['updated_by_scraper'] = True
                else:
                    # Create new company entry with proper schema
                    new_company = {
                        'company_id': company_id,
                        'company_name': primary_company_name or 'Unknown Company',
                        'company_url': primary_company_url,
                        'countries': ['Unknown'],  # Will be updated by enrichment if possible
                        'projects': [project_data],
                        'total_projects': 1,
                        'additional_company_data': None,  # Will be populated by enrichment
                        'data_source': 'parallel_scraper'
                    }
                    
                    companies_data.append(new_company)
                    companies_lookup[company_id] = new_company
                    new_companies += 1
                    projects_added += 1
            
            # Clean up temporary flags
            for company in companies_data:
                company.pop('updated_by_scraper', None)
            
            # Sort companies by total projects (descending)
            companies_data.sort(key=lambda x: x.get('total_projects', 0), reverse=True)
            
            # Save merged data
            with open(companies_file, 'w', encoding='utf-8') as f:
                json.dump(companies_data, f, indent=2, ensure_ascii=False)
            
            # Update self.company_data for enrichment process
            self._update_company_data_with_scraped(companies_lookup)
            
            print(f"✅ Successfully merged scraped projects:")
            print(f"   • Projects added: {projects_added}")
            print(f"   • New companies created: {new_companies}")
            print(f"   • Existing companies updated: {updated_companies}")
            print(f"   • Total companies: {len(companies_data)}")
            
        except Exception as e:
            print(f"❌ Error merging scraped projects: {e}")
    
    def _transform_scraped_project_to_api_schema(self, scraped_project: Dict) -> Dict:
        """Transform scraped project data to match API schema."""
        # Parse ticker_exchange field
        root_ticker = None
        exchange = None
        ticker_exchange = scraped_project.get('ticker_exchange')
        if ticker_exchange:
            # Handle formats like "ASX:IND", "TSX:CCO", "TSXV:APN, OTC PINK:ALTPF"
            parts = ticker_exchange.split(',')[0].strip()  # Take first ticker if multiple
            if ':' in parts:
                exchange, root_ticker = parts.split(':', 1)
                exchange = exchange.strip()
                root_ticker = root_ticker.strip()
        
        # Create transformed project matching API schema
        transformed = {
            # Core identifiers (convert gid to number)
            'gid': int(scraped_project['gid']) if str(scraped_project['gid']).isdigit() else scraped_project['gid'],
            'project_name': scraped_project.get('project_name'),
            'company_id': None,  # Will be set per company in the loop
            'company_name': None,  # Will be set per company in the loop
            
            # Mining data
            'commodities': scraped_project.get('commodities'),
            'stage': scraped_project.get('stage'),
            'operator': scraped_project.get('operator'),
            'is_flagship_project': 0,  # Default for scraped projects
            
            # Financial data (parsed from ticker_exchange)
            'root_ticker': root_ticker,
            'exchange': exchange,
            
            # Geographic data (may be available from map center fetching)
            'location': scraped_project.get('location'),
            'source_country': scraped_project.get('source_country', 'Unknown'),
            'centroid': scraped_project.get('centroid'),
            'area_m2': None,  # Not available from scraper
            'mineral_district_camp': None,  # Not available from scraper
            
            # Processed location data (may be populated by geocoding)
            'State': scraped_project.get('State'),
            'Country': scraped_project.get('Country', 'Unknown'),
            'Geocoded': scraped_project.get('Geocoded', False),
            'Postcode': scraped_project.get('Postcode'),
            'ISO3166_2_Code': scraped_project.get('ISO3166_2_Code'),
            'County': scraped_project.get('County'),
            'Territory': scraped_project.get('Territory'),
            
            # URLs
            'project_url': scraped_project.get('url'),
            'company_url': None,  # Will be set per company in the loop
            
            # Scraper metadata (additional fields for tracking)
            'scrape_source': 'parallel_scraper',
            'scrape_timestamp': scraped_project.get('scrape_timestamp'),
            'scraper_version': scraped_project.get('worker_id', 'unknown'),
            
            # Map center metadata (if available)
            'map_center_source': scraped_project.get('map_center_source'),
            'map_zoom': scraped_project.get('map_zoom')
        }
        
        return transformed
    
    def _update_company_data_with_scraped(self, companies_lookup: Dict):
        """Update self.company_data with scraped companies for enrichment process."""
        for company_id, company_info in companies_lookup.items():
            if company_info.get('data_source') == 'parallel_scraper':
                # Add scraped companies to self.company_data so they get enriched
                if company_id not in self.company_data:
                    self.company_data[company_id] = {
                        'company_id': company_id,
                        'company_name': company_info['company_name'],
                        'company_url': company_info['company_url'],
                        'countries': set(['Unknown']),  # Will be updated if we can determine location
                        'projects': company_info['projects'],
                        'additional_company_data': None
                    }
                    print(f"   📝 Added scraped company {company_info['company_name']} to enrichment queue")
    
    def _fetch_map_centers_and_geocode_scraped_projects(self):
        """Fetch map centers for scraped projects and geocode them."""
        if not self.config.get("FETCH_MAP_CENTERS", False) or not self.map_fetcher:
            print("⚠️ Map center fetching disabled or not available")
            return
        
        try:
            # Load scraped projects
            scraped_file = os.path.join(self.dirs['json'], 'scraped_projects.json')
            if not os.path.exists(scraped_file):
                print("⚠️ No scraped projects file found for map center fetching")
                return
            
            with open(scraped_file, 'r') as f:
                scraped_data = json.load(f)
            
            scraped_projects = scraped_data.get('scraped_projects', [])
            successful_projects = [p for p in scraped_projects if p.get('scrape_success', False)]
            
            # Only process projects that don't already have centroid data
            projects_needing_coords = [p for p in successful_projects if not p.get('centroid')]
            
            if not projects_needing_coords:
                print("✅ All scraped projects already have coordinate data")
                return
            
            print(f"\n🗺️ Starting map center fetching for {len(projects_needing_coords)} scraped projects (out of {len(successful_projects)} total)...")
            
            # Get project GIDs for projects needing coordinates
            project_gids = [str(p.get('gid', '')) for p in projects_needing_coords]
            
            # Fetch map centers asynchronously
            map_centers = asyncio.run(
                self.map_fetcher.fetch_map_centers_for_projects(project_gids)
            )
            
            if not map_centers:
                print("⚠️ No map centers were successfully fetched")
                return
            
            # Update scraped projects with centroid data and geocode
            updated_count = 0
            geocoded_count = 0
            
            print(f"🌍 Processing {len(map_centers)} successful coordinate fetches...")
            
            for project in projects_needing_coords:
                gid = str(project.get('gid', ''))
                if gid in map_centers:
                    center_data = map_centers[gid]
                    lat, lng = center_data['lat'], center_data['lng']
                    
                    # Create centroid in GeoJSON format
                    project['centroid'] = {
                        "type": "Point",
                        "coordinates": [lng, lat]  # GeoJSON is [longitude, latitude]
                    }
                    
                    # Add map metadata
                    project['map_center_source'] = center_data.get('map_lib', 'unknown')
                    project['map_zoom'] = center_data.get('zoom')
                    
                    updated_count += 1
                    
                    # Geocode if geocoder is available
                    if self.geocoder:
                        geocode_result = self.geocoder.reverse_geocode(lat, lng)
                        if geocode_result and 'address' in geocode_result:
                            address = geocode_result['address']
                            
                            # Update location fields
                            for field in ['state', 'state_district', 'territory', 'region', 'province']:
                                if field in address and address[field]:
                                    project['State'] = address[field].strip()
                                    break
                            
                            project['Country'] = address.get('country', 'Unknown')
                            project['location'] = f"{project.get('State', '')}, {project['Country']}"
                            project['Postcode'] = address.get('postcode')
                            project['ISO3166_2_Code'] = address.get('ISO3166-2-lvl4')
                            project['County'] = address.get('county')
                            project['Territory'] = address.get('territory')
                            project['Geocoded'] = True
                            project['source_country'] = project['Country']
                            
                            geocoded_count += 1
                            print(f"   🌍 {gid}: {project.get('State', 'Unknown')}, {project['Country']}")
            
            # Save updated scraped projects
            with open(scraped_file, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Map center processing complete:")
            print(f"   • Projects with coordinates: {updated_count}")
            print(f"   • Successfully geocoded: {geocoded_count}")
            print(f"   • Updated scraped projects file")
            
        except Exception as e:
            print(f"❌ Error during map center fetching: {e}")
    
    def _save_final_enriched_companies(self):
        """Save final companies data with enrichment applied."""
        try:
            companies_json = []
            for company_info in self.company_data.values():
                company_copy = company_info.copy()
                company_copy['countries'] = list(company_info['countries'])
                company_copy['total_projects'] = len(company_info['projects'])
                companies_json.append(company_copy)
            
            companies_json.sort(key=lambda x: x['total_projects'], reverse=True)
            
            # Save to timestamped final output (don't overwrite working file)
            company_filename = self._get_timestamped_filename('companies_with_projects_final', 'json', final_output=True)
            company_path = os.path.join(self.dirs['json'], company_filename)
            with open(company_path, 'w', encoding='utf-8') as f:
                json.dump(companies_json, f, indent=2, ensure_ascii=False)
            
            enriched_count = sum(1 for c in companies_json if c.get('additional_company_data'))
            print(f"💾 Final companies JSON saved: {len(companies_json)} companies ({enriched_count} enriched)")
            
            # Validate final JSON schema
            if self.schema_validator:
                print(f"🔍 Validating final JSON schema...")
                validation_results = self.schema_validator.validate_companies_json(companies_json)
                
                if validation_results["valid"]:
                    print(f"✅ Schema validation PASSED!")
                    stats = validation_results["statistics"]
                    print(f"   📊 API projects: {stats['api_projects']}")
                    print(f"   📊 Scraped projects: {stats['scraped_projects']}")
                    print(f"   📊 Enriched companies: {stats['enriched_companies']} ({stats['enrichment_rate']})")
                    print(f"   📊 Scraper contribution: {stats['scraper_contribution']}")
                else:
                    print(f"❌ Schema validation FAILED!")
                    print(f"   Errors: {len(validation_results['errors'])}")
                    for error in validation_results['errors'][:3]:
                        print(f"   • {error['company_name']}: {error['error']}")
            
        except Exception as e:
            print(f"❌ Error saving final enriched companies: {e}")
    
    def run(self):
        """Main processing pipeline."""
        countries_to_process = self._get_countries_to_process()
        
        print(f"🚀 Processing {len(countries_to_process)} countries")
        print(f"🌐 Geocoding: {'Enabled' if self.config['ENABLE_GEOCODING'] else 'Disabled'}")
        
        # Process each country
        all_stats = {}
        countries_iter = tqdm(countries_to_process, desc="🌍 Processing countries") if self.config["SHOW_DETAILED_PROGRESS"] else countries_to_process
        
        for country in countries_iter:
            df, stats = self.process_country(country)
            if not df.empty:
                self.all_data.append(df)
                all_stats[country] = stats
        
        # Create initial coverage report and missing IDs (needed for scraping)
        combined_df = pd.concat(self.all_data, ignore_index=True) if self.all_data else pd.DataFrame()
        self._create_coverage_report()
        
        # Scrape missing projects if enabled (BEFORE enrichment to include scraped companies)
        if self.config.get("SCRAPE_MISSING_PROJECTS", False):
            self._scrape_missing_projects()
            
            # Fetch map centers for scraped projects (immediately after scraping)
            if self.config.get("FETCH_MAP_CENTERS", False):
                self._fetch_map_centers_and_geocode_scraped_projects()
                
                # Re-merge scraped projects with updated location data
                print(f"🔄 Re-merging scraped projects with updated location data...")
                self._merge_scraped_projects_with_companies()
        
        # Enrich companies with relationship data (AFTER scraping and map fetching)
        print(f"\n🔗 Starting company enrichment process...")
        self.enrich_companies()
        
        # Final save with enriched data
        if self.company_data:
            print(f"💾 Saving final enriched companies data...")
            self._save_final_enriched_companies()
        
        # Generate final outputs with ALL data (API + scraped + enriched)
        print(f"📊 Generating final Excel and reports with complete data...")
        final_combined_df = pd.concat(self.all_data, ignore_index=True) if self.all_data else pd.DataFrame()
        results = self.save_outputs(final_combined_df)
        
        print(f"\n✅ Processing complete!")
        print(f"📁 Files created: {len(results)}")
        for result_file in results:
            print(f"   • {os.path.relpath(result_file, self.base_dir)}")


class Geocoder:
    """Simplified geocoder."""
    
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MiningProjectsProcessor/2.0'})
        self.cache = {}
        self.last_request_time = 0
    
    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict]:
        """Geocode coordinates with rate limiting."""
        cache_key = f"{latitude:.6f},{longitude:.6f}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Rate limiting
        current_time = time.time()
        if current_time - self.last_request_time < 1.0:
            time.sleep(1.0 - (current_time - self.last_request_time))
        
        try:
            params = {
                'lat': latitude, 
                'lon': longitude, 
                'format': 'json', 
                'addressdetails': 1, 
                'zoom': 10,
                'accept-language': 'en'  # Force English language responses
            }
            headers = {'Accept-Language': 'en-US,en;q=0.9'}  # Additional language preference
            response = self.session.get("https://nominatim.openstreetmap.org/reverse", params=params, headers=headers, timeout=10)
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                data = response.json()
                self.cache[cache_key] = data
                return data
        except:
            pass
        
        self.cache[cache_key] = None
        return None


class MapCenterFetcher:
    """Async map center fetching using Playwright for scraped projects."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.concurrency = config.get("MAP_FETCH_CONCURRENCY", 12)
        self.goto_timeout = config.get("MAP_FETCH_TIMEOUT", 25000)
        self.map_ready_timeout = config.get("MAP_READY_TIMEOUT", 7000)
        
        # JavaScript for map center extraction (from fetch_map_centers.py)
        self.map_center_js = """
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
    
    async def _fetch_single_map_center(self, page, gid: str) -> Dict:
        """Fetch map center for a single project."""
        url = f"https://mininghub.com/map?gid={gid}"
        
        # Block heavy resources for speed
        async def route_blocker(route):
            rt = route.request.resource_type
            u = route.request.url
            if rt in {"image", "media", "font"}:
                return await route.abort()
            # Block map tiles
            if any(s in u for s in ["/tile/", "/tiles/", "/{z}/", "/wmts", "/arcgis/", "/basemaps/", "/mapbox/"]):
                return await route.abort()
            return await route.continue_()
        
        await page.route("**/*", route_blocker)
        
        try:
            # Navigate to map page
            await page.goto(url, wait_until="domcontentloaded", timeout=self.goto_timeout)
            
            # Wait for map object to be ready
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
                    timeout=self.map_ready_timeout
                )
            except PWTimeout:
                pass  # Try evaluation anyway
            
            # Extract map center
            center = await page.evaluate(self.map_center_js)
            
            if center:
                return {
                    "gid": gid,
                    "lat": center.get("lat"),
                    "lng": center.get("lng"),
                    "zoom": center.get("zoom"),
                    "map_lib": center.get("lib"),
                    "status": "success",
                    "error": None
                }
            else:
                return {"gid": gid, "status": "no_center", "error": "Map center not found"}
                
        except PWTimeout:
            return {"gid": gid, "status": "timeout", "error": "Page load timeout"}
        except Exception as e:
            return {"gid": gid, "status": "error", "error": str(e)[:200]}
    
    async def fetch_map_centers_for_projects(self, project_gids: List[str]) -> Dict[str, Dict]:
        """Fetch map centers for multiple projects with optimized cold start handling."""
        if not PLAYWRIGHT_AVAILABLE:
            print("⚠️ Playwright not available - skipping map center fetching")
            return {}
        
        if not project_gids:
            return {}
        
        print(f"🗺️ Fetching map centers for {len(project_gids)} projects")
        
        results = {}
        
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-gpu"]
            )
            context = await browser.new_context(viewport={"width": 1280, "height": 900})
            
            # OPTIMIZATION 1: Warm up browser with dummy pages
            print("🔥 Warming up browser...")
            warm_up_page = await context.new_page()
            try:
                await warm_up_page.goto("https://mininghub.com", timeout=30000)
                await asyncio.sleep(2)  # Let it settle
            except:
                pass
            finally:
                await warm_up_page.close()
            
            # OPTIMIZATION 2: Staggered concurrency ramp-up
            batches = [
                (project_gids[:20], 4, "🚀 Initial batch (concurrency: 4)"),
                (project_gids[20:60], 8, "⚡ Ramping up (concurrency: 8)"),
                (project_gids[60:], 12, "🔥 Full speed (concurrency: 12)")
            ]
            
            for batch_idx, (batch_gids, concurrency, desc) in enumerate(batches):
                if not batch_gids:
                    print(f"📭 Batch {batch_idx + 1}: Empty, skipping")
                    continue
                
                print(f"{desc} - {len(batch_gids)} projects")
                print(f"   🔧 Creating semaphore with concurrency: {concurrency}")
                sem = asyncio.Semaphore(concurrency)
                
                async def worker(gid: str):
                    async with sem:
                        page = await context.new_page()
                        try:
                            # OPTIMIZATION 3: Try with relaxed timeout first
                            timeout = self.goto_timeout + 10000  # Extra 10s for cold start
                            result = await self._fetch_single_map_center_with_timeout(page, gid, timeout)
                            
                            # OPTIMIZATION 4: One retry for timeouts only
                            if result["status"] == "timeout":
                                await asyncio.sleep(2)  # Brief backoff
                                result = await self._fetch_single_map_center_with_timeout(page, gid, self.goto_timeout)
                            
                            return result
                        finally:
                            await page.close()
                
                # Process batch with robust timeout handling
                print(f"   🚀 Starting {len(batch_gids)} tasks...")
                tasks = set([asyncio.create_task(worker(gid)) for gid in batch_gids])
                print(f"   ⏳ Processing {len(tasks)} tasks with individual timeouts...")
                
                batch_results = []
                completed_count = 0
                start_time = asyncio.get_event_loop().time()
                max_batch_time = 180  # 3 minutes max per batch
                
                while tasks and (asyncio.get_event_loop().time() - start_time) < max_batch_time:
                    try:
                        # Wait for next completion (10s check interval)
                        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=10)
                        
                        # Process completed tasks
                        for task in done:
                            try:
                                result = await task
                                batch_results.append(result)
                                completed_count += 1
                                
                                gid = result["gid"]
                                if result["status"] == "success":
                                    results[gid] = {
                                        "lat": result["lat"],
                                        "lng": result["lng"],
                                        "zoom": result.get("zoom"),
                                        "map_lib": result.get("map_lib")
                                    }
                                    print(f"   ✅ {gid}: lat={result['lat']:.6f}, lng={result['lng']:.6f} ({completed_count}/{len(batch_gids)})")
                                else:
                                    print(f"   ❌ {gid}: {result['status']} ({completed_count}/{len(batch_gids)})")
                            
                            except Exception as e:
                                print(f"   ❌ Task error: {str(e)[:100]} ({completed_count + 1}/{len(batch_gids)})")
                                completed_count += 1
                        
                        # Update remaining tasks
                        tasks = pending
                        
                        # Progress update every 10 tasks
                        if completed_count % 10 == 0 and completed_count > 0:
                            elapsed = asyncio.get_event_loop().time() - start_time
                            print(f"   📊 Progress: {completed_count}/{len(batch_gids)} completed in {elapsed:.1f}s, {len(tasks)} remaining")
                    
                    except asyncio.TimeoutError:
                        # 10s check interval timeout - continue checking
                        continue
                
                # Cancel any remaining hanging tasks
                if tasks:
                    print(f"   🚨 Cancelling {len(tasks)} hanging tasks after {max_batch_time}s")
                    for task in tasks:
                        task.cancel()
                    
                    # Wait briefly for cancellations
                    try:
                        await asyncio.wait(tasks, timeout=5)
                    except:
                        pass
                
                batch_success = len([r for r in batch_results if r["status"] == "success"])
                print(f"   📊 Batch {batch_idx + 1} complete: {batch_success}/{len(batch_gids)} successful ({completed_count} total processed)")
                
                # Small pause between batches
                if batch_idx < len(batches) - 1:  # Not the last batch
                    print(f"   ⏸️  Pausing 2s before next batch...")
                    await asyncio.sleep(2)
            
            await context.close()
            await browser.close()
        
        success_count = len(results)
        print(f"🗺️ Map center fetching complete: {success_count}/{len(project_gids)} successful ({success_count/len(project_gids)*100:.1f}%)")
        
        return results
    
    async def _fetch_single_map_center_with_timeout(self, page, gid: str, timeout: int) -> Dict:
        """Fetch map center with custom timeout."""
        url = f"https://mininghub.com/map?gid={gid}"
        
        # Block heavy resources for speed (allow CSS for proper layout)
        async def route_blocker(route):
            rt = route.request.resource_type
            u = route.request.url
            if rt in {"image", "media", "font"}:
                return await route.abort()
            # Block map tiles but allow CSS
            if any(s in u for s in ["/tile/", "/tiles/", "/{z}/", "/wmts", "/arcgis/", "/basemaps/", "/mapbox/"]):
                return await route.abort()
            return await route.continue_()
        
        await page.route("**/*", route_blocker)
        
        try:
            # Navigate with custom timeout
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            
            # Wait for map object to be ready
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
                    timeout=self.map_ready_timeout
                )
            except PWTimeout:
                pass  # Try evaluation anyway
            
            # Extract map center
            center = await page.evaluate(self.map_center_js)
            
            if center:
                return {
                    "gid": gid,
                    "lat": center.get("lat"),
                    "lng": center.get("lng"),
                    "zoom": center.get("zoom"),
                    "map_lib": center.get("lib"),
                    "status": "success",
                    "error": None
                }
            else:
                return {"gid": gid, "status": "no_center", "error": "Map center not found"}
                
        except PWTimeout:
            return {"gid": gid, "status": "timeout", "error": "Page load timeout"}
        except Exception as e:
            return {"gid": gid, "status": "error", "error": str(e)[:200]}


def main():
    """Main entry point."""
    print("🌍 Mining Projects Processor (Refactored)")
    print("=" * 50)
    
    processor = MiningProjectsProcessor(CONFIG)
    processor.run()


if __name__ == "__main__":
    main()
