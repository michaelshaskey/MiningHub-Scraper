"""
Project Storage Service
Handles data persistence and export in multiple formats.
Implements Factor 4 (Backing Services) with pluggable storage backends.
"""

import json
import os
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import asdict

from .models import Project, Company, ProcessingMetrics

logger = logging.getLogger(__name__)


class ProjectStorage:
    """
    Handles project data storage and export.
    Supports multiple output formats: JSON, Excel, CSV.
    """
    
    def __init__(self, config):
        self.config = config
        self.output_dir = config.output_dir
        
        # Ensure output directories exist (OUTPUT_DIR should be the root, e.g. 'outputs')
        self.json_dir = os.path.join(self.output_dir, 'json_outputs')
        self.excel_dir = os.path.join(self.output_dir, 'excel_outputs')
        self.reports_dir = os.path.join(self.output_dir, 'reports')
        
        for dir_path in [self.json_dir, self.excel_dir, self.reports_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        logger.info("Storage service initialized", extra={
            "output_dir": self.output_dir,
            "json_dir": self.json_dir,
            "excel_dir": self.excel_dir
        })
    
    def save_projects(self, projects: List[Project]) -> str:
        """
        Save projects to JSON file.
        
        Args:
            projects: List of Project objects to save
            
        Returns:
            Path to saved file
        """
        if not projects:
            logger.warning("No projects to save")
            return ""
        
        try:
            # Convert projects to serializable format (objects expected)
            projects_data = [p.to_dict() for p in projects]
            
            # Extract data sources safely and normalize to strings
            all_data_sources = set()
            for project in projects:
                for ds in project.data_sources:
                    all_data_sources.add(ds.value)
            
            # Create output structure
            output_data = {
                'metadata': {
                    'total_projects': len(projects),
                    'generated_at': datetime.now().isoformat(),
                    'data_sources': list(all_data_sources),
                    'version': '2.0'
                },
                'projects': projects_data
            }
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"projects_processed_{timestamp}.json"
            filepath = os.path.join(self.json_dir, filename)
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(projects)} projects to {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to save projects: {e}")
            raise
    
    def save_companies_with_projects(self, projects: List[Project]) -> str:
        """
        Save data organized by companies (CRM-ready format).
        
        Args:
            projects: List of Project objects
            
        Returns:
            Path to saved file
        """
        if not projects:
            logger.warning("No projects to organize by companies")
            return ""
        
        try:
            # Organize projects by company
            companies_data = {}
            
            for project in projects:
                primary_company = project.primary_company
                if not primary_company:
                    continue
                company_id = primary_company.id
                if not company_id:
                    continue
                if company_id not in companies_data:
                    company_url = f"https://mininghub.com/company-profile?gid={company_id}" if company_id else None
                    companies_data[company_id] = {
                        'company_id': company_id,
                        'company_name': primary_company.name or 'Unknown',
                        'company_url': company_url,
                        'countries': set(),
                        'projects': [],
                        'total_projects': 0,
                        'additional_company_data': {
                            'company_info': primary_company.to_dict(),
                            'enrichment_source': 'new_architecture',
                            'api_response_timestamp': datetime.now().isoformat()
                        }
                    }
                companies_data[company_id]['projects'].append(project.to_dict())
                if project.location and project.location.country:
                    companies_data[company_id]['countries'].add(project.location.country)
            
            # Convert to list and finalize
            companies_list = []
            for company_data in companies_data.values():
                company_data['countries'] = list(company_data['countries'])
                company_data['total_projects'] = len(company_data['projects'])
                companies_list.append(company_data)
            
            # Sort by total projects (descending)
            companies_list.sort(key=lambda x: x['total_projects'], reverse=True)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"companies_with_projects_{timestamp}.json"
            filepath = os.path.join(self.json_dir, filename)
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(companies_list, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(companies_list)} companies with projects to {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to save companies data: {e}")
            raise
    
    def export_to_excel(self, projects: List[Project], filename_prefix: str = None) -> str:
        """
        Export projects to Excel with multiple sheets.
        
        Args:
            projects: List of Project objects
            
        Returns:
            Path to saved Excel file
        """
        if not projects:
            logger.warning("No projects to export to Excel")
            return ""
        
        try:
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix = (filename_prefix.strip() if filename_prefix else "mining_projects").rstrip("_")
            filename = f"{prefix}_{timestamp}.xlsx"
            filepath = os.path.join(self.excel_dir, filename)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Sheet 1: Projects (normalized for relational joins)
                projects_df = self._create_projects_dataframe(projects)
                projects_df.to_excel(writer, sheet_name='Projects', index=False)
                
                # Sheet 2: Companies (normalized)
                companies_df = self._create_companies_dataframe(projects)
                companies_df.to_excel(writer, sheet_name='Companies', index=False)
                
                # Sheet 3: Relationships (project-company edges with types)
                relationships_df = self._create_relationships_dataframe(projects)
                relationships_df.to_excel(writer, sheet_name='Relationships', index=False)
                
                # Sheet 4: Data Sources Summary
                sources_df = self._create_sources_dataframe(projects)
                sources_df.to_excel(writer, sheet_name='Data_Sources', index=False)
                
                # Sheet 5: Processing Summary
                summary_df = self._create_processing_summary(projects)
                summary_df.to_excel(writer, sheet_name='Processing_Summary', index=False)
            
            logger.info(f"Exported {len(projects)} projects to Excel: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to export to Excel: {e}")
            raise
    
    def _create_projects_dataframe(self, projects: List[Project]) -> pd.DataFrame:
        """Create flattened DataFrame of all projects (normalized)."""
        rows = []
        
        for project in projects:
            row = {
                # Project data
                'gid': project.gid,
                'project_name': project.name,
                'stage': project.stage,
                'commodities': project.commodities,
                'operator': project.operator,
                'project_url': project.project_url,
                
                # Location data
                'location_string': project.location.location_string if project.location else None,
                'latitude': project.location.latitude if project.location else None,
                'longitude': project.location.longitude if project.location else None,
                'country': project.location.country if project.location else None,
                'state': project.location.state if project.location else None,
                'mineral_district': project.location.mineral_district if project.location else None,
                'area_m2': project.location.area_m2 if project.location else None,
                'location_source': project.location.location_source if project.location else None,
                'iso3166_2': project.location.iso3166_2 if project.location else None,
                'postcode': project.location.postcode if project.location else None,
                'county': project.location.county if project.location else None,
                'territory': project.location.territory if project.location else None,
                
                # Metadata
                'data_sources': ', '.join([ds.value for ds in project.data_sources]),
                'processing_stage': project.processing_stage.value,
                'created_at': project.created_at.isoformat(),
                'updated_at': project.updated_at.isoformat(),
                'has_errors': len(project.errors) > 0,
                'error_count': len(project.errors)
            }
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def _create_companies_dataframe(self, projects: List[Project]) -> pd.DataFrame:
        """Create normalized Companies DataFrame (one row per company)."""
        companies = {}
        
        for project in projects:
            if not project.primary_company:
                continue
            
            company_id = project.primary_company.id
            
            if company_id not in companies:
                company_url = f"https://mininghub.com/company-profile?gid={company_id}" if company_id else None
                companies[company_id] = {
                    'company_id': company_id,
                    'company_name': project.primary_company.name,
                    'company_url': company_url,
                    'ticker': project.primary_company.ticker,
                    'exchange': project.primary_company.exchange,
                    'website': project.primary_company.website,
                    'ceo': project.primary_company.ceo,
                    'headquarters': project.primary_company.headquarters,
                    'industry': project.primary_company.industry,
                    'project_count': 0,
                    'countries': set(),
                    'data_source': project.primary_company.data_source.value
                }
            
            companies[company_id]['project_count'] += 1
            if project.location and project.location.country:
                companies[company_id]['countries'].add(project.location.country)
        
        # Convert to DataFrame
        rows = []
        for company in companies.values():
            company['countries'] = ', '.join(sorted(company['countries']))
            rows.append(company)
        
        df = pd.DataFrame(rows)
        return df.sort_values('project_count', ascending=False)

    def _create_relationships_dataframe(self, projects: List[Project]) -> pd.DataFrame:
        """Create Relationships DataFrame (edges between projects and companies)."""
        rows = []
        for project in projects:
            if not project.company_relationships:
                continue
            for rel in project.company_relationships:
                rows.append({
                    'gid': project.gid,
                    'project_name': project.name,
                    'company_id': rel.company_id,
                    'company_name': rel.company_name,
                    'relationship_type': rel.relationship_type.value if rel.relationship_type else None,
                    'percentage': rel.percentage,
                    'ownership_id': rel.ownership_id,
                    'optionee_id': rel.optionee_id,
                    'comments': rel.comments,
                    'source': rel.data_source.value if rel.data_source else None
                })
        return pd.DataFrame(rows)
    
    def _create_sources_dataframe(self, projects: List[Project]) -> pd.DataFrame:
        """Create data sources summary DataFrame."""
        source_stats = {}
        
        for project in projects:
            for source in project.data_sources:
                source_name = source.value
                if source_name not in source_stats:
                    source_stats[source_name] = 0
                source_stats[source_name] += 1
        
        rows = [{'data_source': source, 'project_count': count} 
                for source, count in source_stats.items()]
        
        return pd.DataFrame(rows).sort_values('project_count', ascending=False)
    
    def _create_processing_summary(self, projects: List[Project]) -> pd.DataFrame:
        """Create processing summary DataFrame."""
        total_projects = len(projects)
        completed_projects = len([p for p in projects if p.processing_stage.value == 'completed'])
        failed_projects = len([p for p in projects if p.processing_stage.value == 'failed'])
        with_companies = len([p for p in projects if p.primary_company])
        with_relationships = len([p for p in projects if p.primary_company and p.primary_company.data_source.value == 'relationships'])
        
        summary_data = [
            {'metric': 'Total Projects', 'value': total_projects},
            {'metric': 'Completed Projects', 'value': completed_projects},
            {'metric': 'Failed Projects', 'value': failed_projects},
            {'metric': 'Projects with Companies', 'value': with_companies},
            {'metric': 'Companies from Relationships API', 'value': with_relationships},
            {'metric': 'Company Resolution Rate', 'value': f"{(with_companies/total_projects)*100:.1f}%"},
            {'metric': 'Relationships API Success Rate', 'value': f"{(with_relationships/total_projects)*100:.1f}%"},
        ]
        
        return pd.DataFrame(summary_data)
    
    def save_metrics(self, metrics: ProcessingMetrics) -> str:
        """Save processing metrics to JSON file."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processing_metrics_{timestamp}.json"
            filepath = os.path.join(self.reports_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metrics.to_dict(), f, indent=2)
            
            logger.info(f"Saved processing metrics to {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")
            raise
    
    def export_all(self, projects: List[Project] = None) -> Dict[str, str]:
        """
        Export all available data in multiple formats.
        
        Args:
            projects: Optional list of projects. If None, loads from latest file.
            
        Returns:
            Dictionary of exported file paths
        """
        if projects is None:
            logger.warning("No projects provided for export")
            return {}
        
        try:
            results = {}
            
            # Export JSON formats
            results['projects_json'] = self.save_projects(projects)
            results['companies_json'] = self.save_companies_with_projects(projects)
            
            # Export Excel
            results['excel_export'] = self.export_to_excel(projects)
            
            logger.info("All exports completed", extra={
                "files_created": len(results),
                "projects_exported": len(projects)
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            raise
