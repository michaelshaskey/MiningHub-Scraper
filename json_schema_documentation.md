# JSON Schema Reference

## Overview
Unified JSON schema for mining projects and companies data. This document provides a quick reference for the data structure used throughout the system.

**Note**: For complete technical details, see `SYSTEM_KNOWLEDGE_BASE.md`

## Company Schema

```json
{
  "company_id": "string",           // Primary key
  "company_name": "string",         // Company name
  "company_url": "string|null",     // Company profile URL
  "countries": ["string"],          // Operating countries
  "total_projects": "integer",      // Number of projects
  "projects": [...],                // Project objects (see below)
  "additional_company_data": {      // Enrichment data (optional)
    "company_info": {...},          // CEO, website, headquarters, etc.
    "fetched_using_project_gid": "integer",
    "api_response_timestamp": "ISO 8601 string",
    "enrichment_source": "main_script|standalone_tool"
  }
}
```

## Unified Project Schema

All projects (API and scraped) follow this unified structure after processing:

```json
{
  // Core identifiers
  "gid": "integer",                 // Project identifier
  "project_name": "string|null",    // Project name
  "company_id": "integer|string",   // Company identifier
  "company_name": "string",         // Company name
  
  // Mining data
  "commodities": "string|null",     // e.g., "Au,Cu,Ag", "Lithium", "Uranium"
  "stage": "string|null",           // e.g., "Mining", "Exploration", "Development"
  "operator": "string|null",        // Operating company name
  "is_flagship_project": "integer", // 0 or 1
  
  // Financial data
  "root_ticker": "string|null",     // Stock ticker (e.g., "BHP", "RIO")
  "exchange": "string|null",        // Exchange (e.g., "ASX", "NYSE", "TSX")
  
  // Geographic data (API projects have full data, scraped have defaults)
  "location": "string|null",        // "State, Country"
  "source_country": "string",       // Country from processing
  "centroid": "GeoJSON Point|null", // {"type": "Point", "coordinates": [lon, lat]}
  "area_m2": "string|null",         // Area in square meters
  
  // Processed location data
  "State": "string|null",           // State/province
  "Country": "string",              // Country name
  "Geocoded": "boolean",            // Whether geocoded
  
  // URLs
  "project_url": "string",          // Project profile URL
  "company_url": "string",          // Company profile URL
  
  // Source tracking (scraped projects only)
  "scrape_source": "parallel_scraper", // Identifies scraped projects
  "scrape_timestamp": "ISO 8601 string"
}
```

## Company Enrichment Data

```json
{
  "company_name": "string",
  "website": "string|null",
  "root_ticker": "string|null",
  "exchange": "string|null", 
  "ceo": "string|null",
  "headquarters": "string|null",
  "industry": "string|null",
  "is_delisted": "boolean|null"
  // ... additional fields
}
```

## Key Transformations

### **Scraped → API Format**
- `gid`: string → integer
- `ticker_exchange`: "ASX:BHP" → root_ticker="BHP", exchange="ASX"  
- `primary_company_*`: Single company → normalized fields
- Geographic fields: Set to defaults (null/"Unknown")

### **Data Normalization**
- **One company per project**: No arrays, clean CRM structure
- **Consistent types**: All GIDs as integers, company_ids normalized
- **Schema validation**: Automatic validation ensures compliance

---

**For complete technical details, troubleshooting, and gotchas, see `SYSTEM_KNOWLEDGE_BASE.md`**
