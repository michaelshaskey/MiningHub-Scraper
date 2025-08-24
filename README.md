# Mining Projects Data Cleanup System

A comprehensive data processing pipeline that extracts, processes, and enriches mining project data from MiningHub.com to create CRM-ready datasets.

## 🏗️ System Overview

This system combines API data extraction with parallel web scraping to maximize data coverage, processing mining projects through a 6-phase pipeline:

1. **API Data Collection** - Extract projects by country from MiningHub API
2. **Missing Projects Analysis** - Identify gaps between API and URL data
3. **Parallel Web Scraping** - Scrape missing projects using Selenium
4. **Data Integration** - Merge and transform scraped data with API data
5. **Company Enrichment** - Enrich companies with additional metadata
6. **Final Output** - Generate CRM-ready JSON and Excel outputs

## 🚀 Quick Start

### Prerequisites

```bash
# Install required Python packages
pip3 install pandas openpyxl requests beautifulsoup4 selenium 
pip3 install webdriver-manager tqdm psutil jsonschema
```

### Basic Usage

```bash
# Full production run
python3 mining_projects_refactored.py

# Schema validation
python3 json_schema_validator.py

# Standalone scraper test
python3 project_scraper_parallel_compact.py
```

## 📁 Project Structure

```
├── mining_projects_refactored.py      # Main processing script
├── project_scraper_parallel_compact.py # Parallel web scraper
├── json_schema_validator.py           # Data validation utilities
├── countries.json                     # Countries to process
├── found_urls.xlsx                    # Project and company URLs
├── json_outputs/                      # JSON data outputs
│   ├── companies_with_projects.json   # PRIMARY OUTPUT (CRM-ready)
│   ├── scraped_projects.json          # Raw scraper output
│   └── orphaned_projects.json         # Projects without company URLs
├── excel_outputs/                     # Excel reports
│   └── all_countries_projects.xlsx    # Unified Excel output
└── reports/                           # Analysis reports
    ├── missing_ids_report.csv         # Missing projects analysis
    ├── data_coverage_summary.csv      # Coverage statistics
    └── enrichment_status.json         # Enrichment failures
```

## ⚙️ Configuration

Key configuration options in `mining_projects_refactored.py`:

```python
CONFIG = {
    # Country Selection
    "SPECIFIC_COUNTRY": "",              # Single country (e.g., "Australia")
    "SELECTED_COUNTRIES": [],            # Multiple countries ["Australia", "Canada"]
    
    # Processing Controls
    "ENABLE_GEOCODING": True,            # Reverse geocode coordinates
    "FETCH_COMPANY_RELATIONSHIPS": True, # Enrich companies with metadata
    "SCRAPE_MISSING_PROJECTS": True,     # Enable web scraping
    
    # Performance Settings
    "SCRAPER_WORKERS": 4,                # Parallel Chrome instances
    "MAX_COMPANY_ENRICHMENTS": None,     # Limit enrichment calls
    "MAX_MISSING_PROJECTS": None,        # Limit scraped projects
}
```

## 📊 Primary Output

The main output is `json_outputs/companies_with_projects.json` - a CRM-ready dataset containing:

- **Company Objects** with unified project data
- **Enriched Metadata** (CEO, website, headquarters, ticker info)
- **Geographic Data** (coordinates, addresses, administrative regions)
- **Mining Data** (commodities, stages, operators)
- **Source Tracking** (API vs scraped data attribution)

## 🔧 Key Features

### Parallel Web Scraping
- **4 concurrent Chrome instances** (configurable)
- **Smart retry logic** with exponential backoff
- **Resource monitoring** and cleanup
- **~90% success rate** on missing projects

### Data Integration
- **Schema transformation** from scraped to API format
- **Company normalization** for CRM compatibility  
- **Duplicate detection** and merging
- **100% schema validation** on output

### Rate Limiting & Ethics
- **API rate limiting** (pause every 200 calls)
- **Respectful scraping** (0.2s delays between requests)
- **JWT authentication** with monitoring
- **Error handling** and recovery

## ⚠️ Important Notes

### Critical Dependencies
- **JWT Token**: Hardcoded in line 165, expires 2027-01-19
- **Processing Order**: API → Missing IDs → Scraping → Merging → Enrichment → Final Save
- **Resource Usage**: ~600MB RAM per Chrome worker
- **File Dependencies**: `countries.json` and `found_urls.xlsx` required

### Success Metrics
- **Scraping Success**: >90%
- **Company Extraction**: >95%
- **Schema Validation**: 100%
- **API Coverage**: >95% projects have URLs

## 🛠️ Development

### Testing Configuration
```python
CONFIG = {
    "SPECIFIC_COUNTRY": "Australia",     # Single country for testing
    "MAX_PROJECTS_PER_COUNTRY": 50,     # Limit API projects
    "MAX_MISSING_PROJECTS": 10,         # Limit scraping
    "SCRAPER_WORKERS": 2,               # Fewer workers for testing
}
```

### Common Issues
- **Selenium Timeouts**: Reduce concurrent workers if timeouts occur
- **Memory Issues**: Monitor Chrome processes, reduce `SCRAPER_WORKERS`
- **Schema Validation Failures**: Ensure primary company data extraction
- **Rate Limiting**: Respect API limits to avoid 429 responses

## 📈 Performance

### Typical Performance
- **API Processing**: 2-10 seconds per country
- **Parallel Scraping**: 0.04-0.16 projects/sec per worker
- **Company Enrichment**: 1-2 seconds per company
- **Total Runtime**: Minutes for testing, hours for full production run

### Resource Requirements
- **RAM**: 8GB+ recommended (4GB base + 600MB per worker)
- **CPU**: ~1 core per worker during active scraping
- **Network**: Stable internet for API calls and web scraping

## 📝 Documentation

- `SYSTEM_KNOWLEDGE_BASE.md` - Comprehensive technical documentation
- `json_schema_documentation.md` - Data schema reference
- Inline code comments for implementation details

## 🔒 Security & Compliance

- JWT token authentication for API access
- Respectful web scraping with appropriate delays
- Rate limiting compliance
- User agent identification as browser

## 📄 License

[Add your license information here]

## 🤝 Contributing

[Add contribution guidelines here]

---

**Note**: This system is designed for data processing and analysis. Ensure compliance with MiningHub.com's terms of service and applicable data protection regulations.
