"""
Data models for MiningHub Data Processor.
Following 12/15-Factor principles with immutable, serializable data structures.
"""

from dataclasses import dataclass, field, asdict, replace
from typing import Dict, List, Optional, Any, Set, Union
from datetime import datetime
from enum import Enum
import json


class DataSource(Enum):
    """Enumeration of data sources for tracking data lineage."""
    API = "api"
    RELATIONSHIPS = "relationships"
    SCRAPER = "scraper"
    URL_FILE = "url_file"


class ProcessingStage(Enum):
    """Processing stages for tracking project lifecycle."""
    DISCOVERED = "discovered"
    ASSEMBLING = "assembling"
    ASSEMBLED = "assembled"
    FAILED = "failed"
    COMPLETED = "completed"


class RelationshipType(Enum):
    """Enumeration of company-project relationship types."""
    JV = "jv"  # Joint Venture / Ownership
    OPTION = "option"  # Option Agreement
    NSR = "nsr"  # Net Smelter Return / Royalty
    OPERATOR = "operator"  # Operating company (from API safe data)


@dataclass(frozen=True)  # Immutable for better concurrency
class CompanyRelationship:
    """
    Represents a company's relationship to a specific project.
    Designed for HubSpot CRM integration with association labels.
    """
    company_id: str
    company_name: str
    relationship_type: RelationshipType
    
    # Ownership details
    percentage: Optional[float] = None
    
    # Relationship-specific IDs for tracking
    ownership_id: Optional[int] = None  # projectCompanyOwnership, projectCompanyNsr, etc.
    
    # Option-specific data
    optionee_id: Optional[int] = None
    comments: Optional[str] = None
    
    # Full company details (when available)
    company_details: Optional['Company'] = None
    
    # Data source tracking
    data_source: DataSource = DataSource.RELATIONSHIPS
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['relationship_type'] = self.relationship_type.value
        data['data_source'] = self.data_source.value
        if self.company_details:
            data['company_details'] = self.company_details.to_dict()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CompanyRelationship':
        """Create from dictionary."""
        data = data.copy()
        if 'relationship_type' in data:
            data['relationship_type'] = RelationshipType(data['relationship_type'])
        if 'data_source' in data:
            data['data_source'] = DataSource(data['data_source'])
        if 'company_details' in data and data['company_details']:
            data['company_details'] = Company.from_dict(data['company_details'])
        return cls(**data)


@dataclass(frozen=True)  # Immutable for better concurrency (Factor 8)
class Company:
    """
    Immutable company data structure.
    Contains authoritative company information from relationships API.
    """
    id: str
    name: str
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    website: Optional[str] = None
    ceo: Optional[str] = None
    headquarters: Optional[str] = None
    phone: Optional[str] = None
    industry: Optional[str] = None
    sector: Optional[str] = None
    
    # Relationship-specific data
    ownership_percentage: Optional[float] = None
    relationship_type: Optional[str] = None  # 'jv', 'nsr', 'option'
    
    # Metadata
    data_source: DataSource = DataSource.RELATIONSHIPS
    fetched_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['data_source'] = self.data_source.value
        data['fetched_at'] = self.fetched_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Company':
        """Create Company from dictionary."""
        data = data.copy()
        if 'data_source' in data:
            data['data_source'] = DataSource(data['data_source'])
        if 'fetched_at' in data:
            data['fetched_at'] = datetime.fromisoformat(data['fetched_at'])
        return cls(**data)


@dataclass(frozen=True)  # Immutable for better concurrency
class ProjectLocation:
    """Geographic data for a project."""
    location_string: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    country: Optional[str] = None
    state: Optional[str] = None
    mineral_district: Optional[str] = None
    area_m2: Optional[str] = None
    # Enrichment/provenance fields
    geocoded: Optional[bool] = None
    location_source: Optional[str] = None  # api | scraper_map | geocode
    postcode: Optional[str] = None
    iso3166_2: Optional[str] = None
    county: Optional[str] = None
    territory: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_centroid(cls, centroid: Dict, location: str = None, **kwargs) -> 'ProjectLocation':
        """Create from API centroid data."""
        coords = centroid.get('coordinates', []) if centroid else []
        return cls(
            location_string=location,
            longitude=coords[0] if len(coords) > 0 else None,
            latitude=coords[1] if len(coords) > 1 else None,
            **kwargs
        )


@dataclass(frozen=True)  # Immutable for better concurrency
class Project:
    """
    Immutable project data structure.
    Single source of truth for project information.
    """
    gid: str
    name: str
    
    # Safe project data (consistent across API duplicates)
    location: ProjectLocation
    stage: Optional[str] = None
    commodities: Optional[str] = None
    operator: Optional[str] = None  # Operational company name
    
    # Company relationships (new structure for HubSpot CRM)
    company_relationships: List[CompanyRelationship] = field(default_factory=list)
    
    # Backward compatibility (deprecated but maintained for transition)
    primary_company: Optional[Company] = None
    stakeholders: List[Company] = field(default_factory=list)
    
    # Data lineage tracking (Factor 13: Observability)
    data_sources: Set[DataSource] = field(default_factory=set)
    processing_stage: ProcessingStage = ProcessingStage.DISCOVERED
    
    # URLs
    project_url: Optional[str] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        
        # Handle enums and special types
        data['data_sources'] = [ds.value for ds in self.data_sources]
        data['processing_stage'] = self.processing_stage.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        
        # Handle nested objects
        data['company_relationships'] = [rel.to_dict() for rel in self.company_relationships]
        
        # Backward compatibility
        if self.primary_company:
            data['primary_company'] = self.primary_company.to_dict()
        data['stakeholders'] = [company.to_dict() for company in self.stakeholders]
        data['location'] = self.location.to_dict()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Project':
        """Create Project from dictionary."""
        data = data.copy()
        
        # Handle enums and special types
        if 'data_sources' in data:
            data['data_sources'] = {DataSource(ds) for ds in data['data_sources']}
        if 'processing_stage' in data:
            data['processing_stage'] = ProcessingStage(data['processing_stage'])
        if 'created_at' in data:
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if 'updated_at' in data:
            data['updated_at'] = datetime.fromisoformat(data['updated_at'])
        
        # Handle nested objects
        if 'company_relationships' in data:
            data['company_relationships'] = [CompanyRelationship.from_dict(rel) for rel in data['company_relationships']]
        
        # Backward compatibility
        if 'primary_company' in data and data['primary_company']:
            data['primary_company'] = Company.from_dict(data['primary_company'])
        if 'stakeholders' in data:
            data['stakeholders'] = [Company.from_dict(comp) for comp in data['stakeholders']]
        
        if 'location' in data:
            data['location'] = ProjectLocation(**data['location'])
        
        return cls(**data)
    
    def add_data_source(self, source: DataSource) -> 'Project':
        """Add a data source to tracking (returns new instance for immutability)."""
        new_sources = self.data_sources.copy()
        new_sources.add(source)
        # Preserve nested object types using dataclasses.replace
        return replace(self, data_sources=new_sources, updated_at=datetime.now())
    
    def update_stage(self, stage: ProcessingStage) -> 'Project':
        """Update processing stage (returns new instance for immutability)."""
        # Preserve nested object types using dataclasses.replace
        return replace(self, processing_stage=stage, updated_at=datetime.now())
    
    def add_error(self, error: str) -> 'Project':
        """Add error message (returns new instance for immutability)."""
        new_errors = self.errors.copy()
        new_errors.append(error)
        # Preserve nested object types using dataclasses.replace
        return replace(self, errors=new_errors, updated_at=datetime.now())
    
    @classmethod
    def from_api_data(cls, api_data: Dict[str, Any], gid: str = None) -> 'Project':
        """
        Create Project from API response data.
        Extracts only safe, project-centric fields.
        """
        gid = gid or str(api_data.get('gid', ''))
        
        # Extract safe location data
        location = ProjectLocation.from_centroid(
            centroid=api_data.get('centroid'),
            location=api_data.get('location'),
            country=api_data.get('location', '').split(', ')[-1] if api_data.get('location') else None,
            state=api_data.get('location', '').split(', ')[0] if api_data.get('location') and ', ' in api_data.get('location', '') else None,
            # Prefer mineral_district_camp (API field), fallback to mineral_district if present
            mineral_district=api_data.get('mineral_district_camp') or api_data.get('mineral_district'),
            area_m2=api_data.get('area_m2'),
            location_source='api'
        )
        
        return cls(
            gid=gid,
            name=api_data.get('project_name', ''),
            location=location,
            stage=api_data.get('stage'),
            commodities=api_data.get('commodities'),
            operator=api_data.get('operator'),
            data_sources={DataSource.API},
            processing_stage=ProcessingStage.DISCOVERED
        )


@dataclass
class ProcessingMetrics:
    """
    Metrics for observability (Factor 13: Telemetry/Observability).
    Tracks processing performance and outcomes.
    """
    total_projects: int = 0
    completed_projects: int = 0
    failed_projects: int = 0
    skipped_projects: int = 0
    
    # Source breakdown
    api_projects: int = 0
    scraped_projects: int = 0
    relationships_enriched: int = 0
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # Errors
    error_summary: Dict[str, int] = field(default_factory=dict)
    
    def add_error(self, error_type: str):
        """Track an error occurrence."""
        self.error_summary[error_type] = self.error_summary.get(error_type, 0) + 1
    
    def duration_seconds(self) -> Optional[float]:
        """Calculate processing duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_projects == 0:
            return 0.0
        return (self.completed_projects / self.total_projects) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/monitoring."""
        data = asdict(self)
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        data['duration_seconds'] = self.duration_seconds()
        data['success_rate'] = self.success_rate()
        return data
