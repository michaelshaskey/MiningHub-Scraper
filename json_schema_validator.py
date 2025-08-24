#!/usr/bin/env python3
"""
JSON Schema Validator for Mining Projects and Companies Data
Implements formal validation based on the documented schema.
"""

import json
from typing import Dict, List, Optional, Any
from jsonschema import validate, ValidationError, Draft7Validator
import datetime


class MiningDataSchemaValidator:
    """Formal JSON schema validator for mining projects and companies data."""
    
    def __init__(self):
        self.project_api_schema = self._create_project_api_schema()
        self.project_scraped_schema = self._create_project_scraped_schema()
        self.company_enrichment_schema = self._create_company_enrichment_schema()
        self.company_schema = self._create_company_schema()
    
    def _create_company_schema(self) -> Dict:
        """Create JSON schema for company objects."""
        return {
            "type": "object",
            "required": ["company_id", "company_name", "company_url", "countries", "projects", "total_projects"],
            "properties": {
                "company_id": {"type": "string"},
                "company_name": {"type": "string"},
                "company_url": {"type": ["string", "null"]},
                "countries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1
                },
                "total_projects": {"type": "integer", "minimum": 0},
                "projects": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/unified_project"}
                },
                "additional_company_data": {
                    "type": ["object", "null"],
                    "properties": {
                        "company_info": {"$ref": "#/definitions/company_enrichment"},
                        "fetched_using_project_gid": {"type": ["integer", "string"]},
                        "api_response_timestamp": {"type": "string", "format": "date-time"},
                        "enrichment_source": {"type": "string", "enum": ["main_script", "standalone_tool"]}
                    }
                },
                "data_source": {"type": "string", "enum": ["api", "parallel_scraper"]}
            },
            "definitions": {
                "unified_project": self._create_unified_project_schema(),
                "company_enrichment": self.company_enrichment_schema
            }
        }
    
    def _create_unified_project_schema(self) -> Dict:
        """Create unified JSON schema that accepts both API and scraped projects."""
        return {
            "type": "object",
            "required": ["gid"],
            "properties": {
                # Core identifiers (flexible types)
                "gid": {"type": ["integer", "string"]},
                "project_name": {"type": ["string", "null"]},
                "company_id": {"type": ["integer", "string", "null"]},
                "company_name": {"type": ["string", "null"]},
                
                # Mining data (all optional)
                "commodities": {"type": ["string", "null"]},
                "stage": {"type": ["string", "null"]},
                "operator": {"type": ["string", "null"]},
                "is_flagship_project": {"type": ["integer", "null"]},
                
                # Financial data (flexible)
                "root_ticker": {"type": ["string", "null"]},
                "exchange": {"type": ["string", "null"]},
                
                # Geographic data (flexible - can be present or null)
                "location": {"type": ["string", "null"]},
                "source_country": {"type": ["string", "null"]},
                "centroid": {
                    "type": ["object", "null"],
                    "properties": {
                        "type": {"type": "string"},
                        "coordinates": {"type": "array", "items": {"type": "number"}}
                    }
                },
                "area_m2": {"type": ["string", "null"]},
                "mineral_district_camp": {"type": ["string", "null"]},
                
                # Processed location data (flexible)
                "State": {"type": ["string", "null"]},
                "Country": {"type": ["string", "null"]},
                "Geocoded": {"type": ["boolean", "null"]},
                "Postcode": {"type": ["string", "null"]},
                "ISO3166_2_Code": {"type": ["string", "null"]},
                "County": {"type": ["string", "null"]},
                "Territory": {"type": ["string", "null"]},
                
                # URLs (flexible)
                "project_url": {"type": ["string", "null"]},
                "company_url": {"type": ["string", "null"]},
                
                # Optional scraper metadata
                "scrape_source": {"type": "string"},
                "scrape_timestamp": {"type": "string"},
                "scraper_version": {"type": ["string", "number", "null"]}
            }
        }
    
    def _create_project_api_schema(self) -> Dict:
        """Create JSON schema for API-sourced projects."""
        return {
            "type": "object",
            "required": ["gid", "project_name", "company_id", "company_name"],
            "properties": {
                # Core identifiers
                "gid": {"type": "integer"},
                "project_name": {"type": ["string", "null"]},
                "company_id": {"type": "integer"},
                "company_name": {"type": "string"},
                
                # Mining data
                "commodities": {"type": ["string", "null"]},
                "stage": {"type": ["string", "null"]},
                "operator": {"type": ["string", "null"]},
                "is_flagship_project": {"type": "integer", "enum": [0, 1]},
                
                # Financial data
                "root_ticker": {"type": ["string", "null"]},
                "exchange": {"type": ["string", "null"]},
                
                # Geographic data
                "location": {"type": ["string", "null"]},
                "source_country": {"type": "string"},
                "centroid": {
                    "type": ["object", "null"],
                    "properties": {
                        "type": {"type": "string", "enum": ["Point"]},
                        "coordinates": {
                            "type": "array",
                            "items": {"type": "number"},
                            "minItems": 2,
                            "maxItems": 2
                        }
                    }
                },
                "area_m2": {"type": ["string", "null"]},
                "mineral_district_camp": {"type": ["string", "null"]},
                
                # Processed location data
                "State": {"type": ["string", "null"]},
                "Country": {"type": "string"},
                "Geocoded": {"type": "boolean"},
                "Postcode": {"type": ["string", "null"]},
                "ISO3166_2_Code": {"type": ["string", "null"]},
                "County": {"type": ["string", "null"]},
                "Territory": {"type": ["string", "null"]},
                
                # URLs
                "project_url": {"type": "string"},
                "company_url": {"type": "string"}
            }
        }
    
    def _create_project_scraped_schema(self) -> Dict:
        """Create JSON schema for scraper-sourced projects."""
        return {
            "type": "object",
            "required": ["gid", "project_name", "company_id", "company_name", "scrape_source"],
            "properties": {
                # Core identifiers
                "gid": {"type": "integer"},
                "project_name": {"type": ["string", "null"]},
                "company_id": {"type": ["integer", "string"]},
                "company_name": {"type": "string"},
                
                # Mining data
                "commodities": {"type": ["string", "null"]},
                "stage": {"type": ["string", "null"]},
                "operator": {"type": ["string", "null"]},
                "is_flagship_project": {"type": "integer", "enum": [0]},
                
                # Financial data (parsed from ticker_exchange)
                "root_ticker": {"type": ["string", "null"]},
                "exchange": {"type": ["string", "null"]},
                
                # Geographic data (defaults for scraped projects)
                "location": {"type": "null"},
                "source_country": {"type": "string", "enum": ["Unknown"]},
                "centroid": {"type": "null"},
                "area_m2": {"type": "null"},
                "mineral_district_camp": {"type": "null"},
                
                # Processed location data (defaults)
                "State": {"type": "null"},
                "Country": {"type": "string", "enum": ["Unknown"]},
                "Geocoded": {"type": "boolean", "enum": [False]},
                "Postcode": {"type": "null"},
                "ISO3166_2_Code": {"type": "null"},
                "County": {"type": "null"},
                "Territory": {"type": "null"},
                
                # URLs
                "project_url": {"type": "string"},
                "company_url": {"type": ["string", "null"]},
                
                # Scraper metadata
                "scrape_source": {"type": "string", "enum": ["parallel_scraper"]},
                "scrape_timestamp": {"type": "string"},
                "scraper_version": {"type": ["string", "number"]}
            }
        }
    
    def _create_company_enrichment_schema(self) -> Dict:
        """Create JSON schema for company enrichment data."""
        return {
            "type": "object",
            "properties": {
                "company_name": {"type": "string"},
                "website": {"type": ["string", "null"]},
                "root_ticker": {"type": ["string", "null"]},
                "exchange": {"type": ["string", "null"]},
                "root_ticker_02": {"type": ["string", "null"]},
                "exchange_02": {"type": ["string", "null"]},
                "root_ticker_03": {"type": ["string", "null"]},
                "exchange_03": {"type": ["string", "null"]},
                "ceo": {"type": ["string", "null"]},
                "headquarters": {"type": ["string", "null"]},
                "phone": {"type": ["string", "null"]},
                "address": {"type": ["string", "null"]},
                "country": {"type": ["string", "null"]},
                "state": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "zip": {"type": ["string", "null"]},
                "industry": {"type": ["string", "null"]},
                "sector": {"type": ["string", "null"]},
                "primary_sector": {"type": ["string", "null"]},
                "is_delisted": {"type": ["boolean", "null"]}
            }
        }
    
    def validate_company(self, company_data: Dict) -> tuple[bool, Optional[str]]:
        """Validate a single company object."""
        try:
            validate(instance=company_data, schema=self.company_schema)
            return True, None
        except ValidationError as e:
            return False, f"Company validation error: {e.message}"
        except Exception as e:
            return False, f"Unexpected validation error: {str(e)}"
    
    def validate_companies_json(self, companies_data: List[Dict]) -> Dict:
        """Validate entire companies JSON array."""
        results = {
            "valid": True,
            "total_companies": len(companies_data),
            "valid_companies": 0,
            "invalid_companies": 0,
            "errors": [],
            "warnings": [],
            "statistics": {}
        }
        
        api_projects = 0
        scraped_projects = 0
        enriched_companies = 0
        
        for i, company in enumerate(companies_data):
            is_valid, error_msg = self.validate_company(company)
            
            if is_valid:
                results["valid_companies"] += 1
                
                # Collect statistics
                if company.get("additional_company_data"):
                    enriched_companies += 1
                
                for project in company.get("projects", []):
                    if project.get("scrape_source") == "parallel_scraper":
                        scraped_projects += 1
                    else:
                        api_projects += 1
            else:
                results["invalid_companies"] += 1
                results["valid"] = False
                results["errors"].append({
                    "company_index": i,
                    "company_id": company.get("company_id", "unknown"),
                    "company_name": company.get("company_name", "unknown"),
                    "error": error_msg
                })
        
        # Add statistics
        results["statistics"] = {
            "api_projects": api_projects,
            "scraped_projects": scraped_projects,
            "enriched_companies": enriched_companies,
            "enrichment_rate": f"{enriched_companies/len(companies_data)*100:.1f}%",
            "scraper_contribution": f"{scraped_projects/(api_projects + scraped_projects)*100:.1f}%" if (api_projects + scraped_projects) > 0 else "0%"
        }
        
        return results
    
    def validate_project_for_transformation(self, scraped_project: Dict) -> tuple[bool, Optional[str]]:
        """Validate scraped project before transformation to API schema."""
        required_fields = ["gid", "url"]
        
        for field in required_fields:
            if field not in scraped_project:
                return False, f"Missing required field: {field}"
        
        # Validate GID can be converted to integer
        gid = scraped_project.get("gid")
        if not (isinstance(gid, int) or (isinstance(gid, str) and gid.isdigit())):
            return False, f"Invalid GID format: {gid} (must be integer or numeric string)"
        
        return True, None
    
    def transform_and_validate_scraped_project(self, scraped_project: Dict) -> tuple[bool, Optional[Dict], Optional[str]]:
        """Transform scraped project to API schema and validate."""
        # Pre-transformation validation
        is_valid, error_msg = self.validate_project_for_transformation(scraped_project)
        if not is_valid:
            return False, None, error_msg
        
        # Transform (using the same logic from mining_projects_refactored.py)
        transformed = self._transform_scraped_to_api_format(scraped_project)
        
        # Skip validation during transformation - will be validated later with company_id set
        # The transformed project will have company_id and company_name set to None initially,
        # but these will be populated in the loop that handles multiple companies per project
        return True, transformed, None
    
    def _transform_scraped_to_api_format(self, scraped_project: Dict) -> Dict:
        """Transform scraped project to API format (matches mining_projects_refactored.py logic)."""
        # Parse ticker_exchange field
        root_ticker = None
        exchange = None
        ticker_exchange = scraped_project.get('ticker_exchange')
        if ticker_exchange:
            parts = ticker_exchange.split(',')[0].strip()
            if ':' in parts:
                exchange, root_ticker = parts.split(':', 1)
                exchange = exchange.strip()
                root_ticker = root_ticker.strip()
        
        return {
            # Core identifiers
            'gid': int(scraped_project['gid']) if str(scraped_project['gid']).isdigit() else scraped_project['gid'],
            'project_name': scraped_project.get('project_name'),
            'company_id': None,  # Set by caller
            'company_name': None,  # Set by caller
            
            # Mining data
            'commodities': scraped_project.get('commodities'),
            'stage': scraped_project.get('stage'),
            'operator': scraped_project.get('operator'),
            'is_flagship_project': 0,
            
            # Financial data
            'root_ticker': root_ticker,
            'exchange': exchange,
            
            # Geographic data (defaults for scraped)
            'location': None,
            'source_country': 'Unknown',
            'centroid': None,
            'area_m2': None,
            'mineral_district_camp': None,
            
            # Processed location data (defaults)
            'State': None,
            'Country': 'Unknown',
            'Geocoded': False,
            'Postcode': None,
            'ISO3166_2_Code': None,
            'County': None,
            'Territory': None,
            
            # URLs
            'project_url': scraped_project.get('url'),
            'company_url': None,  # Set by caller
            
            # Scraper metadata
            'scrape_source': 'parallel_scraper',
            'scrape_timestamp': scraped_project.get('scrape_timestamp'),
            'scraper_version': scraped_project.get('worker_id', 'unknown')
        }


def validate_companies_file(file_path: str) -> Dict:
    """Validate companies JSON file and return detailed results."""
    validator = MiningDataSchemaValidator()
    
    try:
        with open(file_path, 'r') as f:
            companies_data = json.load(f)
        
        if not isinstance(companies_data, list):
            return {
                "valid": False,
                "error": "Root element must be an array of companies",
                "total_companies": 0
            }
        
        results = validator.validate_companies_json(companies_data)
        
        print(f"📋 SCHEMA VALIDATION RESULTS:")
        print(f"=" * 50)
        print(f"File: {file_path}")
        print(f"Valid: {'✅ YES' if results['valid'] else '❌ NO'}")
        print(f"Total companies: {results['total_companies']}")
        print(f"Valid companies: {results['valid_companies']}")
        print(f"Invalid companies: {results['invalid_companies']}")
        
        if results['statistics']:
            stats = results['statistics']
            print(f"\n📊 DATA STATISTICS:")
            print(f"API projects: {stats['api_projects']}")
            print(f"Scraped projects: {stats['scraped_projects']}")
            print(f"Enriched companies: {stats['enriched_companies']}")
            print(f"Enrichment rate: {stats['enrichment_rate']}")
            print(f"Scraper contribution: {stats['scraper_contribution']}")
        
        if results['errors']:
            print(f"\n❌ VALIDATION ERRORS:")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"  Company {error['company_id']}: {error['error']}")
            if len(results['errors']) > 5:
                print(f"  ... and {len(results['errors']) - 5} more errors")
        
        return results
        
    except FileNotFoundError:
        return {"valid": False, "error": f"File not found: {file_path}"}
    except json.JSONDecodeError as e:
        return {"valid": False, "error": f"Invalid JSON: {str(e)}"}
    except Exception as e:
        return {"valid": False, "error": f"Unexpected error: {str(e)}"}


def main():
    """Test schema validation on current companies file."""
    import os
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    companies_file = os.path.join(base_dir, 'json_outputs', 'companies_with_projects.json')
    
    print("🔍 JSON Schema Validator for Mining Data")
    print("=" * 50)
    
    results = validate_companies_file(companies_file)
    
    if results.get("valid"):
        print(f"\n✅ Schema validation PASSED!")
    else:
        print(f"\n❌ Schema validation FAILED!")
        if "error" in results:
            print(f"Error: {results['error']}")


if __name__ == "__main__":
    main()
