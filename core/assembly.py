"""
Project Assembly Service
Builds complete Project objects with authoritative company data.
Implements Factor 6 (Stateless Processes) and proper error handling.
"""

import logging
import os
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

from .models import Project, Company, CompanyRelationship, ProjectLocation, DataSource, ProcessingStage, ProcessingMetrics, RelationshipType

logger = logging.getLogger(__name__)


@dataclass
class AssemblyResult:
    """Result of processing a batch of projects."""
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    projects: List[Project] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class CompanyResolver:
    """
    Resolves authoritative company data using multiple strategies.
    Implements fallback chain: Relationships API → Scraper → Operator fallback.
    """
    
    def __init__(self, api_client, scraper_headless: bool = True):
        self.api_client = api_client
        self.scraper_headless = scraper_headless
        self.resolution_cache = {}  # Simple in-memory cache
    
    def resolve_companies(self, gid: str, project_data: Dict[str, Any]) -> List[CompanyRelationship]:
        """
        Resolve all company relationships for a project.
        
        Args:
            gid: Project GID
            project_data: Safe project data from API
            
        Returns:
            List of CompanyRelationship objects
        """
        cache_key = f"relationships_{gid}"
        if cache_key in self.resolution_cache:
            return self.resolution_cache[cache_key]
        
        relationships = []
        
        # Strategy 1: Relationships API (authoritative)
        api_relationships = self._resolve_from_relationships(gid)
        if api_relationships:
            relationships.extend(api_relationships)
            logger.debug(f"Resolved {len(api_relationships)} relationships for {gid} via relationships API")
        
        # Strategy 2: Scraper fallback (if relationships not found)
        if not relationships:
            scraped_rel = self._resolve_from_scraper(gid)
            if scraped_rel:
                relationships.extend(scraped_rel)
        
        # Strategy 3: Operator fallback (last resort)
        if not relationships:
            operator = project_data.get('operator')
            if operator:
                operator_company = self._create_company_from_operator(operator)
                operator_relationship = CompanyRelationship(
                    company_id=operator_company['id'],
                    company_name=operator_company['name'],
                    relationship_type=RelationshipType.OPERATOR,
                    percentage=100.0,  # Assume full operational control
                    company_details=Company(**operator_company),
                    data_source=DataSource.API
                )
                relationships.append(operator_relationship)
                logger.debug(f"Created operator relationship for {gid}: {operator}")
        
        self.resolution_cache[cache_key] = relationships
        return relationships
    
    def _resolve_from_relationships(self, gid: str) -> List[CompanyRelationship]:
        """Get all company relationships from relationships endpoint."""
        try:
            relationships = self.api_client.get_project_relationships(gid)
            if not relationships:
                return []
            
            company_relationships = []
            
            # Process JV relationships
            jv_companies = relationships.get('jv', [])
            for jv_company in jv_companies:
                company_details = self._create_company_from_api_data(jv_company)
                relationship = CompanyRelationship(
                    company_id=str(jv_company.get('id', '')),
                    company_name=jv_company.get('company_name', ''),
                    relationship_type=RelationshipType.JV,
                    percentage=jv_company.get('percentage'),
                    ownership_id=jv_company.get('projectCompanyOwnership'),
                    company_details=company_details,
                    data_source=DataSource.RELATIONSHIPS
                )
                company_relationships.append(relationship)
            
            # Process NSR relationships
            nsr_companies = relationships.get('nsrs', [])
            for nsr_company in nsr_companies:
                company_details = self._create_company_from_api_data(nsr_company)
                relationship = CompanyRelationship(
                    company_id=str(nsr_company.get('id', '')),
                    company_name=nsr_company.get('company_name', ''),
                    relationship_type=RelationshipType.NSR,
                    percentage=nsr_company.get('percentage'),
                    ownership_id=nsr_company.get('projectCompanyNsr'),
                    company_details=company_details,
                    data_source=DataSource.RELATIONSHIPS
                )
                company_relationships.append(relationship)
            
            # Process Option relationships (note: nested array structure)
            option_arrays = relationships.get('option', [])
            for option_array in option_arrays:
                if isinstance(option_array, list):
                    for option_company in option_array:
                        company_details = self._create_company_from_api_data(option_company)
                        relationship = CompanyRelationship(
                            company_id=str(option_company.get('id', '')),
                            company_name=option_company.get('company_name', ''),
                            relationship_type=RelationshipType.OPTION,
                            optionee_id=option_company.get('optionee'),
                            ownership_id=option_company.get('projectcompanyoptions'),
                            comments=option_company.get('comments'),
                            company_details=company_details,
                            data_source=DataSource.RELATIONSHIPS
                        )
                        company_relationships.append(relationship)
            
            logger.debug(f"Resolved {len(company_relationships)} relationships for {gid}")
            return company_relationships
            
        except Exception as e:
            logger.warning(f"Relationships API failed for {gid}: {e}")
            return []
    
    def _create_company_from_api_data(self, company_data: Dict[str, Any]) -> Company:
        """Create Company object from API relationship data."""
        return Company(
            id=str(company_data.get('id', '')),
            name=company_data.get('company_name', ''),
            ticker=company_data.get('root_ticker'),
            exchange=company_data.get('exchange'),
            website=company_data.get('website'),
            ceo=company_data.get('ceo'),
            headquarters=company_data.get('headquarters'),
            phone=company_data.get('phone'),
            industry=company_data.get('industry'),
            sector=company_data.get('sector'),
            data_source=DataSource.RELATIONSHIPS
        )
    
    def _resolve_from_scraper(self, gid: str) -> List[CompanyRelationship]:
        """Get minimal company relationship from web scraper as a fallback.
        Uses PlaywrightParallelScraper (headless by default) to extract company id/name.
        """
        try:
            # Lazy import to avoid heavy dependency unless needed
            from services.playwright_parallel_scraper import PlaywrightParallelScraper
            import asyncio

            async def _run() -> list:
                scraper = PlaywrightParallelScraper()
                recs = await scraper.scrape_many_parallel([str(gid)], max_concurrency=1, headless=self.scraper_headless, verbose=False)
                return recs

            recs = asyncio.run(_run())
            if not recs:
                return []
            rec = recs[0]
            company_id = getattr(rec, 'company_id', None)
            company_name = getattr(rec, 'company_name', None) or getattr(rec, 'operator', None)
            if not (company_id or company_name):
                return []

            company_details = Company(
                id=str(company_id or f"operator_{hash(company_name) % 100000}"),
                name=company_name or "Unknown Company",
                data_source=DataSource.SCRAPER
            )
            relationship = CompanyRelationship(
                company_id=company_details.id,
                company_name=company_details.name,
                relationship_type=RelationshipType.OPERATOR,
                company_details=company_details,
                data_source=DataSource.SCRAPER
            )
            logger.debug(f"Scraper fallback created relationship for {gid}: {company_details.name}")
            return [relationship]
        except Exception as e:
            logger.debug(f"Scraper fallback failed for {gid}: {e}")
            return []
    
    def _create_company_from_operator(self, operator: str) -> dict:
        """Create basic company from operator name (fallback)."""
        return {
            'id': f"operator_{hash(operator) % 100000}",  # Generate consistent ID
            'name': operator,
            'data_source': DataSource.API,  # Operator comes from API
            'relationship_type': 'operator'
        }


class ProjectAssembler:
    """
    Stateless project assembly service.
    Processes batches of GIDs into complete Project objects.
    """
    
    def __init__(self, config):
        self.config = config
        self.metrics = ProcessingMetrics()
        
        # Initialize services
        from services.api_client import MiningHubClient
        from services.geocoding import GeocodingService, GeocodingConfig
        
        self.api_client = MiningHubClient(
            base_url=config.api_base_url,
            jwt_token=config.jwt_token,
            timeout=config.api_timeout,
            retry_attempts=config.api_retry_attempts
        )
        
        # Scraper fallback runs headless by default unless SCRAPER_HEADFUL=true
        scraper_headless = os.getenv('SCRAPER_HEADFUL', 'false').lower() != 'true'
        self.company_resolver = CompanyResolver(self.api_client, scraper_headless=scraper_headless)
        # Geocoding service (toggle via config)
        self.enable_geocoding = os.getenv('ENABLE_GEOCODING', 'true').lower() == 'true' if not hasattr(config, 'enable_geocoding') else getattr(config, 'enable_geocoding')
        self.geocoder = GeocodingService(GeocodingConfig()) if self.enable_geocoding else None
        
        # Load project URLs for URL mapping
        self.project_urls = self._load_project_urls()
        
        # Create GID to country mapping for efficient API calls
        self.gid_to_country_cache = {}
        self._preload_api_data()
        
        logger.info("Project assembler initialized", extra={
            "batch_size": config.batch_size,
            "project_urls_loaded": len(self.project_urls),
            "gid_cache_size": len(self.gid_to_country_cache)
        })
    
    def process_batch(self, gids: List[str]) -> AssemblyResult:
        """
        Process a batch of GIDs into Project objects.
        Uses concurrent processing for efficiency.
        """
        logger.info(f"Processing batch of {len(gids)} projects")
        self.metrics.start_time = datetime.now()
        self.metrics.total_projects = len(gids)
        
        result = AssemblyResult()
        
        try:
            # Process projects concurrently
            max_workers = min(4, len(gids)) if len(gids) > 0 else 1
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_gid = {executor.submit(self._process_single_project, gid): gid for gid in gids}
                for future in as_completed(future_to_gid):
                    gid = future_to_gid[future]
                    try:
                        project = future.result()
                        if project:
                            result.projects.append(project)
                            result.completed += 1
                            self.metrics.completed_projects += 1
                            
                            if project.primary_company and project.primary_company.data_source == DataSource.RELATIONSHIPS:
                                self.metrics.relationships_enriched += 1
                            if DataSource.API in project.data_sources:
                                self.metrics.api_projects += 1
                        else:
                            result.failed += 1
                            result.errors.append(f"Failed to process project {gid}")
                            self.metrics.failed_projects += 1
                    except Exception as e:
                        logger.error(f"Error processing {gid}: {e}")
                        result.failed += 1
                        result.errors.append(f"Error processing {gid}: {str(e)}")
                        self.metrics.failed_projects += 1
                        self.metrics.add_error("processing_error")
            
            self.metrics.end_time = datetime.now()
            
            logger.info("Batch processing completed", extra={
                "completed": result.completed,
                "failed": result.failed,
                "duration": self.metrics.duration_seconds()
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            self.metrics.add_error("batch_processing_failed")
            result.errors.append(f"Batch processing failed: {str(e)}")
            return result
    
    def _process_single_project(self, gid: str) -> Optional[Project]:
        """
        Process a single project GID into a complete Project object.
        
        Args:
            gid: Project GID to process
            
        Returns:
            Complete Project object or None if processing fails
        """
        try:
            logger.debug(f"Processing project {gid}")
            
            # Step 1: Get safe project data from API cache (if available)
            project_data = self._get_safe_project_data(gid)
            project: Optional[Project] = None
            if project_data:
                # Create base project from safe data
                project = Project.from_api_data(project_data, gid)
                logger.debug(f"Created project: {project.name} (GID: {gid})")
            
            # Step 3: Add project URL if available
            project_url = self.project_urls.get(gid)
            if project_url:
                # Use dataclass replace to maintain object type
                from dataclasses import replace
                if project is not None:
                    project = replace(project, project_url=project_url)
                logger.debug(f"Added project URL: {project_url}")
            
            # Step 4: Resolve company relationships
            relationships = self.company_resolver.resolve_companies(gid, project_data or {})
            if relationships:
                # Log relationship details for analysis
                rel_summary = []
                for rel in relationships:
                    rel_info = f"{rel.relationship_type.value}:{rel.company_name}"
                    if rel.percentage:
                        rel_info += f" ({rel.percentage}%)"
                    rel_summary.append(rel_info)
                project_name_log = project.name if project else 'unknown'
                logger.info(f"GID {gid} ({project_name_log}): {len(relationships)} relationships - {', '.join(rel_summary)}")
                
                # Find primary company (highest ownership percentage in JV, or first relationship)
                primary_company = None
                if relationships:
                    # Sort by ownership percentage (JV relationships first, then by percentage)
                    jv_relationships = [r for r in relationships if r.relationship_type == RelationshipType.JV]
                    if jv_relationships:
                        primary_relationship = max(jv_relationships, key=lambda r: r.percentage or 0)
                        primary_company = primary_relationship.company_details
                        logger.debug(f"Primary company: {primary_company.name} ({primary_relationship.percentage}% JV)")
                    else:
                        # Use first relationship if no JV found
                        primary_company = relationships[0].company_details
                        logger.debug(f"Primary company (non-JV): {primary_company.name}")
                
                # Add relationships to project (immutable pattern) only if project exists now
                if project is not None:
                    # Determine data sources from relationships
                    new_data_sources = project.data_sources.copy()
                    for rel in relationships:
                        if hasattr(rel, 'data_source'):
                            new_data_sources.add(rel.data_source)
                        else:
                            new_data_sources.add(DataSource.RELATIONSHIPS)  # Safe fallback

                    # Use dataclass replace to maintain object type
                    from dataclasses import replace
                    project = replace(
                        project,
                        company_relationships=relationships,
                        primary_company=primary_company,  # Backward compatibility
                        data_sources=new_data_sources
                    )
                    # Ensure operator fallback from company name if missing
                    if not project.operator:
                        fallback_operator = None
                        if primary_company and primary_company.name:
                            fallback_operator = primary_company.name
                        elif relationships and relationships[0].company_name:
                            fallback_operator = relationships[0].company_name
                        if fallback_operator:
                            project = replace(project, operator=fallback_operator)
            else:
                logger.warning(f"GID {gid} ({project.name if project else 'unknown'}): No relationships found")

            # Step 4: If no API project data, attempt to build minimal project from scraper
            if project is None:
                try:
                    from services.playwright_parallel_scraper import PlaywrightParallelScraper
                    import asyncio

                    async def _run_scrape() -> list:
                        scraper = PlaywrightParallelScraper()
                        return await scraper.scrape_many_parallel([str(gid)], max_concurrency=1, headless=(os.getenv('SCRAPER_HEADFUL', 'false').lower() != 'true'), verbose=False)

                    recs = asyncio.run(_run_scrape())
                    rec = recs[0] if recs else None
                    if rec:
                        # Build minimal project using scraped fields
                        location = ProjectLocation()
                        project = Project(
                            gid=str(gid),
                            name=getattr(rec, 'project_name', '') or '',
                            location=location,
                            stage=getattr(rec, 'stage', None),
                            commodities=getattr(rec, 'commodities', None),
                            operator=getattr(rec, 'operator', None),
                            data_sources={DataSource.SCRAPER},
                            processing_stage=ProcessingStage.DISCOVERED,
                            project_url=self.project_urls.get(gid)
                        )
                        logger.info(f"Built minimal project from scraper for {gid}: {project.name}")
                    else:
                        logger.warning(f"Scraper did not return data for {gid}")
                except Exception as e:
                    logger.warning(f"Scraper fallback failed for {gid}: {e}")

                if project is None:
                    # As a last resort, skip this gid
                    return None

                # Attach relationships to scraped project if any
                if relationships:
                    from dataclasses import replace
                    new_sources = project.data_sources.copy()
                    for rel in relationships:
                        if hasattr(rel, 'data_source'):
                            new_sources.add(rel.data_source)
                    project = replace(project, company_relationships=relationships, data_sources=new_sources)
                    if not project.primary_company and relationships:
                        project = replace(project, primary_company=relationships[0].company_details)
                    # Ensure operator fallback from relationship company name if still missing
                    if not project.operator:
                        op = None
                        if project.primary_company and project.primary_company.name:
                            op = project.primary_company.name
                        elif relationships and relationships[0].company_name:
                            op = relationships[0].company_name
                        if op:
                            project = replace(project, operator=op)
            
            # Step 5: Fetch map center if lat/lon missing, then geocode
            try:
                if project and (not project.location or project.location.latitude is None or project.location.longitude is None):
                    from services.map_center import fetch_map_center
                    mc = fetch_map_center(gid=str(gid), headless=(os.getenv('SCRAPER_HEADFUL', 'false').lower() != 'true'))
                    if mc:
                        from dataclasses import replace
                        loc = project.location or ProjectLocation()
                        loc = replace(
                            loc,
                            latitude=mc.get('latitude', loc.latitude),
                            longitude=mc.get('longitude', loc.longitude),
                            location_source=loc.location_source or 'scraper_map'
                        )
                        project = replace(project, location=loc)
            except Exception as e:
                logger.warning(f"Map center fetch failed for {gid}: {e}")

            project = self._maybe_enrich_location(project)
            
            # Step 6: Update processing stage
            project = project.update_stage(ProcessingStage.COMPLETED)
            
            logger.info(f"✅ Successfully processed {gid}: {project.name} with {len(project.company_relationships)} relationships")
            return project
            
        except Exception as e:
            logger.error(f"Failed to process project {gid}: {e}")
            return None

    def _maybe_enrich_location(self, project: Project) -> Project:
        """Apply geocoding to fill missing state/postcode/ISO and normalize precision.
        Keeps API lat/lon precision (does not round). Adds provenance fields.
        """
        try:
            if not self.geocoder or not project.location:
                return project
            loc = project.location
            have_state = bool(loc.state)
            have_coords = loc.latitude is not None and loc.longitude is not None
            from dataclasses import replace
            enriched = loc
            
            # Reverse geocode if we have coordinates but missing state
            if have_coords and not have_state:
                data = self.geocoder.reverse_geocode(loc.latitude, loc.longitude)
                if data and 'address' in data:
                    addr = data['address']
                    state = (
                        addr.get('state')
                        or addr.get('state_district')
                        or addr.get('region')
                        or addr.get('province')
                        or addr.get('territory')
                    )
                    enriched = replace(
                        enriched,
                        state=state or enriched.state,
                        country=addr.get('country', enriched.country),
                        postcode=addr.get('postcode'),
                        iso3166_2=addr.get('ISO3166-2-lvl4') or addr.get('ISO3166-2-lvl6'),
                        county=addr.get('county'),
                        territory=addr.get('territory'),
                        geocoded=True,
                        location_source=enriched.location_source or 'geocode'
                    )
            # Forward geocode if we have location_string but no coords/state
            elif not have_coords and (loc.location_string and not have_state):
                data = self.geocoder.forward_geocode(loc.location_string)
                if data:
                    lat = float(data.get('lat')) if data.get('lat') else None
                    lon = float(data.get('lon')) if data.get('lon') else None
                    addr = data.get('address', {}) if isinstance(data, dict) else {}
                    state = (
                        addr.get('state')
                        or addr.get('state_district')
                        or addr.get('region')
                        or addr.get('province')
                        or addr.get('territory')
                    )
                    enriched = replace(
                        enriched,
                        latitude=lat if lat is not None else enriched.latitude,
                        longitude=lon if lon is not None else enriched.longitude,
                        state=state or enriched.state,
                        country=addr.get('country', enriched.country),
                        postcode=addr.get('postcode'),
                        iso3166_2=addr.get('ISO3166-2-lvl4') or addr.get('ISO3166-2-lvl6'),
                        county=addr.get('county'),
                        territory=addr.get('territory'),
                        geocoded=True,
                        location_source=enriched.location_source or 'geocode'
                    )
            
            if enriched is not loc:
                from dataclasses import replace as r
                project = r(project, location=enriched)
            return project
        except Exception as e:
            logger.warning(f"Location enrichment failed for {project.gid}: {e}")
            return project
    
    def _preload_api_data(self):
        """Preload API data for efficient GID lookups."""
        try:
            # Determine countries to preload
            countries = list(self.config.countries or [])
            if not countries:
                # Production mode may leave countries empty to mean "all"; load from countries.json
                try:
                    import json
                    countries_file = os.path.join(os.getcwd(), "countries.json")
                    if os.path.exists(countries_file):
                        with open(countries_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            countries = data.get('country', []) or []
                    else:
                        countries = ["Australia", "Canada", "United States of America", "Brazil"]
                except Exception:
                    countries = ["Australia", "Canada"]

            # Load projects from selected countries
            for country in countries:
                projects = self.api_client.get_projects_by_country(country)
                for project in projects:
                    gid = str(project.get('gid', ''))
                    if gid:
                        self.gid_to_country_cache[gid] = {
                            'country': country,
                            'data': project
                        }
            
            logger.info(f"Preloaded {len(self.gid_to_country_cache)} projects for efficient lookup")
            
        except Exception as e:
            logger.warning(f"Failed to preload API data: {e}")
    
    def _get_safe_project_data(self, gid: str) -> Optional[Dict[str, Any]]:
        """
        Get safe project data from cache.
        Uses preloaded data for efficiency.
        """
        try:
            # Use cached data if available
            cached = self.gid_to_country_cache.get(str(gid))
            if cached:
                return cached['data']
            
            logger.warning(f"No cached data found for GID {gid}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get project data for {gid}: {e}")
            return None
    
    def _load_project_urls(self) -> Dict[str, str]:
        """Load project URLs from found_urls.xlsx file."""
        try:
            import pandas as pd
            import os
            
            file_path = os.path.join(os.getcwd(), 'found_urls.xlsx')
            
            if not os.path.exists(file_path):
                logger.warning("found_urls.xlsx not found")
                return {}
            
            projects_df = pd.read_excel(file_path, sheet_name='Projects')
            project_urls = dict(zip(projects_df['ID'].astype(str), projects_df['URL']))
            
            logger.info(f"Loaded {len(project_urls)} project URLs")
            return project_urls
            
        except Exception as e:
            logger.warning(f"Failed to load project URLs: {e}")
            return {}
    
    def get_metrics(self) -> ProcessingMetrics:
        """Get processing metrics for observability."""
        return self.metrics
    
    def close(self):
        """Cleanup resources."""
        if self.api_client:
            self.api_client.close()
