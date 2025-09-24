#!/usr/bin/env python3
"""
High-Performance Parallel Project Scraper - Compact Version
Optimized for speed and resource efficiency with parallel Selenium execution.
"""

import json
import pandas as pd
import os
import time
import datetime
import re
import psutil
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# Load environment variables from .env file
load_dotenv()


@dataclass
class ScrapedProject:
    """Lightweight data structure for scraped projects."""
    gid: str
    url: str
    project_name: Optional[str] = None
    operator: Optional[str] = None
    commodities: Optional[str] = None
    stage: Optional[str] = None
    ticker_exchange: Optional[str] = None
    
    # Primary company data (for CRM normalization)
    primary_company_id: Optional[str] = None
    primary_company_name: Optional[str] = None
    primary_company_url: Optional[str] = None
    
    # Legacy arrays (for backward compatibility)
    company_ids: List[str] = None
    company_names: List[str] = None
    company_urls: List[str] = None
    
    scrape_success: bool = True
    error_message: Optional[str] = None
    scrape_timestamp: str = None
    worker_id: Optional[int] = None
    
    # Source tracking
    scrape_source: str = "parallel_scraper"
    data_source: str = "scraped"
    
    def __post_init__(self):
        for attr in ['company_ids', 'company_names', 'company_urls']:
            if getattr(self, attr) is None:
                setattr(self, attr, [])
        if self.scrape_timestamp is None:
            self.scrape_timestamp = datetime.datetime.now().isoformat()


class ParallelProjectScraper:
    """
    High-performance parallel project scraper - compact version.
    
    Note: Environment variables (including JWT_TOKEN) are loaded and available 
    via os.getenv() for future API integration if needed.
    """
    
    def __init__(self, base_dir: str = None, max_workers: int = 3, batch_size: int = 10, test_mode: bool = False):
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        self.test_mode = test_mode
        
        # Use test_mode_output directory if in test mode
        if test_mode:
            output_base = os.path.join(self.base_dir, 'test_mode_output')
        else:
            output_base = self.base_dir
            
        self.json_dir = os.path.join(output_base, 'json_outputs')
        self.reports_dir = os.path.join(output_base, 'reports')
        self.max_workers = max_workers
        self.batch_size_per_worker = batch_size
        
        # Results and tracking
        self.scraped_projects = []
        self.failed_projects = []
        self.processed_projects = set()
        self.worker_stats = {}
        self.lock = threading.Lock()
        
        print(f"🚀 Parallel Scraper: {self.max_workers} workers, {self.batch_size_per_worker} URLs/worker")
    
    def get_env_var(self, var_name: str, default: str = None) -> Optional[str]:
        """
        Utility method to get environment variables.
        
        Args:
            var_name: Name of the environment variable
            default: Default value if not found
            
        Returns:
            Environment variable value or default
        """
        return os.getenv(var_name, default)
    
    def _get_system_resources(self) -> Dict:
        """Get current system resource usage."""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        chrome_procs = len([p for p in psutil.process_iter(['name']) 
                           if 'chrome' in p.info['name'].lower()])
        
        return {
            'cpu_percent': cpu_percent,
            'memory_available_gb': memory.available / 1024 / 1024 / 1024,
            'memory_percent': memory.percent,
            'chrome_processes': chrome_procs
        }
    
    def _create_driver(self, worker_id: int) -> webdriver.Chrome:
        """Create optimized Chrome driver."""
        options = Options()
        
        # Essential options for performance
        for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", 
                   "--disable-gpu", "--window-size=1280,900", "--memory-pressure-off",
                   "--disable-background-networking", "--disable-extensions", 
                   "--log-level=3", "--silent"]:
            options.add_argument(arg)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(45)  # Increased from 30 to 45 seconds
        return driver
    
    def _scrape_single_project(self, driver: webdriver.Chrome, project_id: str, worker_id: int) -> ScrapedProject:
        """Scrape a single project with optimized approach."""
        url = f"https://mininghub.com/project-profile?gid={project_id}"
        result = ScrapedProject(gid=project_id, url=url, worker_id=worker_id)
        
        try:
            print(f"   🌐 Worker {worker_id}: Loading project {project_id}")
            driver.get(url)
            print(f"   📄 Worker {worker_id}: Page loaded, title: {driver.title[:50]}...")
            
            time.sleep(2)  # Initial wait
            
            # Smart wait for table with exponential backoff (increased for stability)
            wait_times = [2, 3, 5, 8, 12]  # Total ~30s max for more reliability
            table_found = False
            
            print(f"   🔍 Worker {worker_id}: Searching for properties table...")
            for i, wait_time in enumerate(wait_times, 1):
                try:
                    driver.find_element(By.CSS_SELECTOR, "table.properties-wrapper-table")
                    table_found = True
                    print(f"   ✅ Worker {worker_id}: Table found on attempt {i}")
                    break
                except:
                    if i < len(wait_times):
                        print(f"   ⏳ Worker {worker_id}: Attempt {i}/{len(wait_times)} - waiting {wait_time}s...")
                        time.sleep(wait_time)
            
            if table_found:
                self._parse_html_content(driver.page_source, result)
                self._extract_company_id_fast(driver, result)
                
                # Ensure we have primary company data
                self._ensure_primary_company_data(result)
            else:
                # Fallback to container text
                try:
                    container = driver.find_element(By.CSS_SELECTOR, ".main-profile-container")
                    self._parse_fallback_content(container.text, result)
                    result.error_message = "Used fallback extraction"
                except:
                    result.scrape_success = False
                    result.error_message = "No content found"
                    return result
            
            result.scrape_success = True
            
        except Exception as e:
            result.scrape_success = False
            result.error_message = str(e)
        
        return result
    
    def _parse_html_content(self, html: str, result: ScrapedProject):
        """Parse HTML content with BeautifulSoup."""
        soup = BeautifulSoup(html, 'lxml')
        
        # Get project title
        title_element = soup.find('h1', id='project-title')
        if title_element and not result.project_name:
            result.project_name = title_element.get_text(strip=True)
        
        # Parse properties tables
        tables = soup.find_all('table', class_='properties-wrapper-table')
        for table in tables:
            self._parse_properties_table(table, result)
    
    def _parse_properties_table(self, table, result: ScrapedProject):
        """Parse project properties table."""
        field_mapping = {
            'project': 'project_name', 'operator': 'operator', 
            'commodit': 'commodities', 'stage': 'stage', 'ticker': 'ticker_exchange'
        }
        
        for tr in table.find_all('tr'):
            tds = tr.find_all(['td', 'th'])
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True).lower()
                value = tds[1].get_text(strip=True)
                
                if not value or value == '-':
                    continue
                
                # Map fields
                for key, attr in field_mapping.items():
                    if key in label and not getattr(result, attr):
                        setattr(result, attr, value)
                        break
                
                # Extract companies from ownership
                if 'ownership' in label:
                    self._extract_companies_from_cell(tds[1], result)
    
    def _extract_companies_from_cell(self, cell, result: ScrapedProject):
        """Extract company info from ownership cell."""
        companies_found = []
        
        for link in cell.find_all('a'):
            company_name = link.get_text(strip=True)
            if not company_name:
                continue
            
            # Extract company ID from onclick or href
            company_id = None
            for attr in [link.get('onclick', ''), link.get('href', '')]:
                if 'gid=' in attr:
                    match = re.search(r'gid=(\d+)', attr)
                    if match:
                        company_id = match.group(1)
                        break
            
            if company_id:
                company_slug = self._create_company_slug(company_name)
                company_url = f"https://mininghub.com/company/{company_slug}/{company_id}"
                
                companies_found.append({
                    'id': company_id,
                    'name': company_name,
                    'url': company_url
                })
                
                # Add to arrays for backward compatibility
                result.company_ids.append(company_id)
                result.company_names.append(company_name)
                result.company_urls.append(company_url)
        
        # Set primary company (first one found, or operator company)
        if companies_found and not result.primary_company_id:
            # Prefer company that matches operator name, otherwise take first
            primary_company = companies_found[0]
            for company in companies_found:
                if result.operator and result.operator.lower() in company['name'].lower():
                    primary_company = company
                    break
            
            result.primary_company_id = primary_company['id']
            result.primary_company_name = primary_company['name']
            result.primary_company_url = primary_company['url']
    
    def _ensure_primary_company_data(self, result: ScrapedProject):
        """Ensure we have primary company data for CRM integration."""
        # If no primary company set but we have arrays, use first entry
        if not result.primary_company_id and result.company_ids:
            result.primary_company_id = result.company_ids[0]
            result.primary_company_name = result.company_names[0] if result.company_names else result.operator
            result.primary_company_url = result.company_urls[0] if result.company_urls else None
        
        # If still no primary company, create from operator
        if not result.primary_company_id and result.operator:
            # Create a placeholder company ID based on operator name
            company_slug = self._create_company_slug(result.operator)
            result.primary_company_id = "unknown"
            result.primary_company_name = result.operator
            result.primary_company_url = f"https://mininghub.com/company/{company_slug}/unknown"
            
            # Also populate arrays if empty
            if not result.company_ids:
                result.company_ids = [result.primary_company_id]
                result.company_names = [result.primary_company_name]
                result.company_urls = [result.primary_company_url]
    
    def _extract_company_id_fast(self, driver, result: ScrapedProject):
        """Fast company ID extraction from page elements."""
        selectors = [("company-news-btn", "href"), ("project-news-btn", "href"), ("project-map", "src")]
        
        for element_id, attr in selectors:
            try:
                element = driver.find_element(By.ID, element_id)
                attr_value = element.get_attribute(attr)
                
                if attr_value:
                    pattern = r'gid=(\d+)' if 'gid=' in attr_value else r'companyId=(\d+)'
                    match = re.search(pattern, attr_value)
                    if match:
                        # Extract as primary company (not array)
                        company_id = match.group(1)
                        company_name = result.operator or "Unknown Company"
                        company_slug = self._create_company_slug(company_name)
                        company_url = f"https://mininghub.com/company/{company_slug}/{company_id}"
                        
                        # Store as single values, not arrays
                        result.primary_company_id = company_id
                        result.primary_company_name = company_name
                        result.primary_company_url = company_url
                        
                        # Also keep arrays for backward compatibility
                        if not result.company_ids:
                            result.company_ids = [company_id]
                            result.company_names = [company_name]
                            result.company_urls = [company_url]
                        return
            except:
                continue
    
    def _parse_fallback_content(self, text: str, result: ScrapedProject):
        """Parse fallback container text."""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        field_mapping = {'project:': 'project_name', 'operator:': 'operator', 
                        'commodities:': 'commodities', 'stage:': 'stage'}
        
        for i, line in enumerate(lines):
            for key, attr in field_mapping.items():
                if key in line.lower() and i + 1 < len(lines):
                    setattr(result, attr, lines[i + 1])
                    break
    
    def _add_company_to_result(self, result: ScrapedProject, company_id: str):
        """Add company to result."""
        company_name = result.operator or "Unknown Company"
        company_slug = self._create_company_slug(company_name)
        company_url = f"https://mininghub.com/company/{company_slug}/{company_id}"
        
        result.company_ids.append(company_id)
        result.company_names.append(company_name)
        result.company_urls.append(company_url)
    
    def _create_company_slug(self, company_name: str) -> str:
        """Create URL slug from company name."""
        replacements = {' ': '-', '.': '', ',': '', '&': 'and', 
                       '(': '', ')': '', "'": '', '"': '', '/': '-'}
        
        slug = company_name.lower()
        for old, new in replacements.items():
            slug = slug.replace(old, new)
        return slug
    
    def _worker_scrape_batch(self, worker_id: int, project_batch: List[Tuple[str, str]]) -> List[ScrapedProject]:
        """Worker function to scrape a batch of projects."""
        results = []
        driver = None
        
        print(f"🔧 Worker {worker_id}: Starting with {len(project_batch)} projects")
        
        try:
            print(f"🔧 Worker {worker_id}: Creating Chrome driver...")
            driver = self._create_driver(worker_id)
            print(f"✅ Worker {worker_id}: Driver created successfully")
            
            start_time = time.time()
            success_count = 0
            
            for i, (project_id, _) in enumerate(project_batch, 1):
                print(f"🔧 Worker {worker_id}: Processing project {i}/{len(project_batch)} - ID: {project_id}")
                
                # Skip if already processed
                with self.lock:
                    if project_id in self.processed_projects:
                        print(f"⏭️  Worker {worker_id}: Skipping already processed project {project_id}")
                        continue
                    self.processed_projects.add(project_id)
                
                result = self._scrape_single_project(driver, project_id, worker_id)
                results.append(result)
                
                if result.scrape_success:
                    success_count += 1
                    print(f"✅ Worker {worker_id}: Success - {result.project_name or 'Unknown'}")
                else:
                    print(f"❌ Worker {worker_id}: Failed - {result.error_message}")
                
                time.sleep(0.2)  # Be respectful
            
            # Record stats
            duration = time.time() - start_time
            with self.lock:
                self.worker_stats[worker_id] = {
                    'processed': len(project_batch), 'successful': success_count,
                    'duration': duration, 'rate': len(project_batch) / duration if duration > 0 else 0
                }
        
        except Exception as e:
            print(f"❌ Worker {worker_id} failed: {e}")
        
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
        return results
    
    def load_missing_projects(self) -> List[Tuple[str, str]]:
        """Load missing projects, excluding already processed ones."""
        missing_file = os.path.join(self.reports_dir, 'missing_ids_report.csv')
        
        try:
            missing_df = pd.read_csv(missing_file)
            
            # Log what we found in the missing IDs report
            if len(missing_df) == 0:
                print(f"📊 Missing IDs report is empty - no projects to scrape")
                return []
            
            total_entries = len(missing_df)
            project_entries = len(missing_df[missing_df['Type'] == 'Project'])
            company_entries = len(missing_df[missing_df['Type'] == 'Company'])
            
            print(f"📊 Missing IDs report contains: {total_entries} total entries")
            print(f"   📊 Projects: {project_entries}, Companies: {company_entries}")
            
            # Filter for projects that are missing from API (these need scraping)
            missing_projects = missing_df[
                (missing_df['Type'] == 'Project') & 
                (missing_df['Status'] == 'In URL file but missing from API')
            ]
            
            print(f"📊 Projects missing from API (candidates for scraping): {len(missing_projects)}")
            
            # Filter out already processed projects
            existing_file = os.path.join(self.json_dir, 'scraped_projects.json')
            if os.path.exists(existing_file):
                try:
                    with open(existing_file, 'r') as f:
                        existing_data = json.load(f)
                    
                    existing_gids = {str(p.get('gid', '')) for p in existing_data.get('scraped_projects', []) 
                                   if p.get('scrape_success', False)}
                    
                    missing_projects = missing_projects[~missing_projects['ID'].astype(str).isin(existing_gids)]
                    print(f"📊 Filtered out {len(existing_gids)} already processed projects")
                except:
                    pass
            
            final_project_list = list(zip(missing_projects['ID'].astype(str), missing_projects['URL']))
            print(f"📊 Final projects to scrape: {len(final_project_list)}")
            
            return final_project_list
        
        except Exception as e:
            print(f"❌ Error loading missing projects: {e}")
            return []
    
    def save_results(self, merge_with_existing: bool = True, export_csv: bool = True) -> Optional[str]:
        """Save results with optional merging and CSV export."""
        if not self.scraped_projects:
            return None
        
        successful = [p for p in self.scraped_projects if p.scrape_success]
        with_companies = [p for p in successful if p.company_ids]
        
        scraped_data = {
            'summary': {
                'total_processed': len(self.scraped_projects),
                'successful_scrapes': len(successful),
                'failed_scrapes': len(self.scraped_projects) - len(successful),
                'projects_with_companies': len(with_companies),
                'total_companies_extracted': sum(len(p.company_ids) for p in successful),
                'unique_companies': len(set().union(*[p.company_ids for p in successful if p.company_ids])),
                'last_updated': datetime.datetime.now().isoformat(),
                'scraper_version': 'parallel_compact_v1.0',
                'worker_stats': self.worker_stats
            },
            'scraped_projects': [asdict(p) for p in self.scraped_projects]
        }
        
        output_file = os.path.join(self.json_dir, 'scraped_projects.json')
        
        # Merge with existing if requested
        if merge_with_existing and os.path.exists(output_file):
            try:
                with open(output_file, 'r') as f:
                    existing_data = json.load(f)
                
                existing_gids = {str(p.get('gid', '')) for p in existing_data.get('scraped_projects', [])}
                new_projects = [p for p in scraped_data['scraped_projects'] 
                              if str(p.get('gid', '')) not in existing_gids]
                
                scraped_data['scraped_projects'] = existing_data.get('scraped_projects', []) + new_projects
                scraped_data['summary']['total_processed'] += existing_data.get('summary', {}).get('total_processed', 0)
                scraped_data['summary']['successful_scrapes'] = len([p for p in scraped_data['scraped_projects'] if p.get('scrape_success', False)])
                
                print(f"📊 Merged {len(new_projects)} new projects with {len(existing_data.get('scraped_projects', []))} existing")
            except Exception as e:
                print(f"⚠️ Could not merge with existing data: {e}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=2, ensure_ascii=False)
        
        # Export to CSV for analysis
        if export_csv:
            self._export_to_csv(scraped_data['scraped_projects'])
        
        return output_file
    
    def _export_to_csv(self, projects_data: List[Dict]):
        """Export scraped data to CSV for analysis."""
        try:
            # Flatten the data for CSV
            csv_data = []
            for project in projects_data:
                # Handle multiple companies per project
                if project.get('company_ids') and len(project['company_ids']) > 0:
                    for i, company_id in enumerate(project['company_ids']):
                        row = {
                            'gid': project.get('gid'),
                            'project_name': project.get('project_name'),
                            'operator': project.get('operator'),
                            'commodities': project.get('commodities'),
                            'stage': project.get('stage'),
                            'ticker_exchange': project.get('ticker_exchange'),
                            'company_id': company_id,
                            'company_name': project['company_names'][i] if i < len(project['company_names']) else None,
                            'company_url': project['company_urls'][i] if i < len(project['company_urls']) else None,
                            'project_url': project.get('url'),
                            'scrape_success': project.get('scrape_success'),
                            'error_message': project.get('error_message'),
                            'scrape_timestamp': project.get('scrape_timestamp'),
                            'worker_id': project.get('worker_id')
                        }
                        csv_data.append(row)
                else:
                    # Project with no companies
                    row = {
                        'gid': project.get('gid'),
                        'project_name': project.get('project_name'),
                        'operator': project.get('operator'),
                        'commodities': project.get('commodities'),
                        'stage': project.get('stage'),
                        'ticker_exchange': project.get('ticker_exchange'),
                        'company_id': None,
                        'company_name': None,
                        'company_url': None,
                        'project_url': project.get('url'),
                        'scrape_success': project.get('scrape_success'),
                        'error_message': project.get('error_message'),
                        'scrape_timestamp': project.get('scrape_timestamp'),
                        'worker_id': project.get('worker_id')
                    }
                    csv_data.append(row)
            
            # Save to CSV
            csv_file = os.path.join(self.json_dir, 'scraped_projects_analysis.csv')
            df = pd.DataFrame(csv_data)
            df.to_csv(csv_file, index=False, encoding='utf-8')
            
            print(f"📊 CSV exported: {len(csv_data)} rows saved to {os.path.basename(csv_file)}")
            
            # Print sample data for verification
            print(f"\n📋 SAMPLE DATA VERIFICATION (First 5 projects):")
            print("="*80)
            for i, row in enumerate(csv_data[:5], 1):
                print(f"{i}. {row['project_name']} (ID: {row['gid']})")
                print(f"   Operator: {row['operator']}")
                print(f"   Commodities: {row['commodities']}")
                print(f"   Stage: {row['stage']}")
                print(f"   Company: {row['company_name']} (ID: {row['company_id']})")
                print(f"   Company URL: {row['company_url']}")
                print(f"   Ticker: {row['ticker_exchange']}")
                print(f"   Success: {row['scrape_success']}")
                print()
            
            return csv_file
            
        except Exception as e:
            print(f"❌ Error exporting CSV: {e}")
            return None
    
    def scrape_missing_projects(self, max_projects: int = None) -> Optional[str]:
        """Main parallel scraping method."""
        print("🚀 High-Performance Parallel Project Scraper - Compact")
        print("=" * 55)
        
        # System resource check
        resources = self._get_system_resources()
        print(f"💻 System: {resources['memory_available_gb']:.1f}GB RAM available, {resources['cpu_percent']:.1f}% CPU")
        print(f"⚙️  Using {self.max_workers} parallel workers")
        
        # Load and prepare projects
        missing_projects = self.load_missing_projects()
        if not missing_projects:
            print("❌ No missing projects found")
            return None
        
        if max_projects:
            missing_projects = missing_projects[:max_projects]
        
        print(f"📊 Processing {len(missing_projects)} projects")
        
        # Create batches
        batches = [missing_projects[i:i + self.batch_size_per_worker] 
                  for i in range(0, len(missing_projects), self.batch_size_per_worker)]
        print(f"🔄 Created {len(batches)} batches ({self.batch_size_per_worker} projects/batch)")
        
        # Execute parallel processing
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {executor.submit(self._worker_scrape_batch, i, batch): i 
                              for i, batch in enumerate(batches)}
            
            with tqdm(total=len(missing_projects), desc="🔍 Scraping projects") as pbar:
                for future in as_completed(future_to_batch):
                    try:
                        batch_results = future.result()
                        self.scraped_projects.extend(batch_results)
                        pbar.update(len(batch_results))
                        
                        # Track failures
                        for result in batch_results:
                            if not result.scrape_success:
                                self.failed_projects.append((result.gid, result.url, result.error_message))
                    except Exception as e:
                        print(f"❌ Batch failed: {e}")
        
        # Save and report results
        output_file = self.save_results(merge_with_existing=True)
        duration = time.time() - start_time
        successful = sum(1 for p in self.scraped_projects if p.scrape_success)
        with_companies = sum(1 for p in self.scraped_projects if p.scrape_success and p.company_ids)
        total_companies = sum(len(p.company_ids) for p in self.scraped_projects if p.scrape_success)
        
        # Detailed terminal summary for debugging
        print(f"\n" + "="*60)
        print(f"📊 COMPREHENSIVE SCRAPING SUMMARY")
        print(f"="*60)
        print(f"⏱️  PERFORMANCE METRICS:")
        print(f"   • Total Duration: {duration:.1f} seconds")
        print(f"   • Processing Rate: {len(missing_projects)/duration:.2f} projects/sec")
        print(f"   • Average Worker Rate: {sum(stats['rate'] for stats in self.worker_stats.values()) / len(self.worker_stats):.2f} projects/sec/worker" if self.worker_stats else "   • Worker Rate: N/A")
        
        print(f"\n📈 SCRAPING RESULTS:")
        print(f"   • Total Projects Processed: {len(self.scraped_projects)}")
        print(f"   • Successful Scrapes: {successful} ({successful/len(self.scraped_projects)*100:.1f}%)")
        print(f"   • Failed Scrapes: {len(self.failed_projects)} ({len(self.failed_projects)/len(self.scraped_projects)*100:.1f}%)")
        print(f"   • Projects with Company Data: {with_companies} ({with_companies/successful*100:.1f}% of successful)")
        print(f"   • Total Companies Extracted: {total_companies}")
        
        print(f"\n🏢 PROJECT DETAILS:")
        for i, project in enumerate(self.scraped_projects[:10], 1):  # Show first 10
            status = "✅" if project.scrape_success else "❌"
            print(f"   {i:2d}. {status} {project.project_name or 'Unknown'} (ID: {project.gid}) - Companies: {len(project.company_ids)}")
        
        if len(self.scraped_projects) > 10:
            print(f"   ... and {len(self.scraped_projects) - 10} more projects")
        
        print(f"\n💻 SYSTEM RESOURCES:")
        final_resources = self._get_system_resources()
        print(f"   • Final RAM Usage: {final_resources['memory_percent']:.1f}%")
        print(f"   • Final CPU Usage: {final_resources['cpu_percent']:.1f}%")
        print(f"   • Chrome Processes: {final_resources['chrome_processes']}")
        
        print(f"\n📁 OUTPUT:")
        print(f"   • Results saved to: {os.path.basename(output_file) if output_file else 'No file saved'}")
        print(f"="*60)
        
        return output_file


# Integration functions
def scrape_missing_projects_for_main_script(base_dir: str, max_projects: int = None, max_workers: int = 3) -> Optional[str]:
    """Function to be called from main mining script."""
    scraper = ParallelProjectScraper(base_dir, max_workers)
    return scraper.scrape_missing_projects(max_projects)


def main():
    """Standalone testing."""
    print("🧪 Testing High-Performance Parallel Scraper - Compact")
    print("=" * 60)
    
    scraper = ParallelProjectScraper()
    output_file = scraper.scrape_missing_projects(max_projects=10)


if __name__ == "__main__":
    main()
