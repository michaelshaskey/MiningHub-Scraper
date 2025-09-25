#!/usr/bin/env python3
"""
MiningHub Data Processor - 12/15-Factor Compliant Application
Main entry point for the application following cloud-native best practices.
"""

import os
import sys
import logging
import signal
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables first (Factor 3: Config)
load_dotenv()

# Configure structured logging (Factor 11: Logs)
def setup_logging():
    """Setup logging with both file and console handlers."""
    # Create logs directory
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate timestamped log filename
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f'mininghub_processor_{timestamp}.log')
    
    # Configure logging format
    log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(name)s"}'
    
    # Create handlers
    handlers = []
    
    # File handler - always enabled
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))
    handlers.append(file_handler)
    
    # Console handler - optional based on environment
    if os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true':
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format=log_format,
        handlers=handlers,
        force=True  # Override any existing configuration
    )
    
    return log_filename

# Setup logging and get log filename
log_file_path = setup_logging()
logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    """
    Configuration class following Factor 3: Config
    All config comes from environment variables with sensible defaults.
    """
    # Core settings
    environment: str = os.getenv('ENVIRONMENT', 'development')
    debug: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Processing settings
    mode: str = os.getenv('PROCESSING_MODE', 'test')  # test, production
    countries: list = None
    max_projects: Optional[int] = None
    batch_size: int = int(os.getenv('BATCH_SIZE', '100'))
    
    # External services (Factor 4: Backing Services)
    jwt_token: str = os.getenv('JWT_TOKEN', '')
    database_url: str = os.getenv('DATABASE_URL', 'sqlite:///mining_data.db')
    redis_url: str = os.getenv('REDIS_URL', 'redis://localhost:6379')
    
    # API settings
    api_base_url: str = os.getenv('API_BASE_URL', 'https://mininghub.com/api')
    api_timeout: int = int(os.getenv('API_TIMEOUT', '30'))
    api_retry_attempts: int = int(os.getenv('API_RETRY_ATTEMPTS', '3'))
    
    # Output settings
    output_dir: str = os.getenv('OUTPUT_DIR', 'outputs')
    # Geocoding toggles
    enable_geocoding: bool = os.getenv('ENABLE_GEOCODING', 'true').lower() == 'true'
    
    def __post_init__(self):
        """Validate configuration and set mode-specific defaults."""
        if not self.jwt_token:
            raise ValueError("JWT_TOKEN environment variable is required")
        
        # Set mode-specific configurations (Factor 10: Dev/Prod Parity)
        if self.mode == 'test':
            # Allow explicit override via COUNTRIES env (comma-separated) or COUNTRY
            env_countries = os.getenv('COUNTRIES') or os.getenv('COUNTRY')
            if env_countries:
                self.countries = [c.strip() for c in env_countries.split(',') if c.strip()]
            else:
                self.countries = ['Australia']
            # Allow override via env for test runs
            env_max = os.getenv('MAX_PROJECTS')
            self.max_projects = int(env_max) if env_max else 10
        elif self.mode == 'production':
            env_countries = os.getenv('COUNTRIES') or os.getenv('COUNTRY')
            if env_countries:
                self.countries = [c.strip() for c in env_countries.split(',') if c.strip()]
            else:
                self.countries = []  # All countries
            self.max_projects = None
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)


class Application:
    """
    Main application class following 12/15-Factor principles.
    Designed for cloud deployment with proper lifecycle management.
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.running = True
        
        # Register signal handlers for graceful shutdown (Factor 9: Disposability)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        signal.signal(signal.SIGINT, self._shutdown_handler)
        
        logger.info("Application initialized", extra={
            "environment": config.environment,
            "mode": config.mode,
            "max_projects": config.max_projects,
            "log_file": log_file_path
        })
    
    def _shutdown_handler(self, signum, frame):
        """Handle graceful shutdown (Factor 9: Disposability)."""
        logger.info("Shutdown signal received", extra={"signal": signum})
        self.running = False
    
    def health_check(self) -> dict:
        """
        Health check endpoint (Factor 13: Telemetry/Observability).
        Returns application health status.
        """
        return {
            "status": "healthy" if self.running else "shutting_down",
            "environment": self.config.environment,
            "mode": self.config.mode,
            "version": os.getenv('APP_VERSION', 'dev')
        }
    
    def run_discovery(self) -> dict:
        """
        Run project discovery phase.
        Returns metrics for observability.
        """
        logger.info("Starting project discovery phase")
        
        try:
            # Import here to avoid circular dependencies
            from core.discovery import ProjectDiscovery
            
            discovery = ProjectDiscovery(self.config)
            # Ensure we return an indexable, sliceable collection
            gids = list(discovery.find_all_gids())
            
            logger.info("Project discovery completed", extra={
                "total_gids": len(gids),
                "unique_gids": len(set(gids))
            })
            
            return {
                "status": "success",
                "total_gids": len(gids),
                "unique_gids": len(set(gids)),
                "gids": gids
            }
            
        except Exception as e:
            logger.error("Project discovery failed", extra={"error": str(e)})
            return {"status": "error", "error": str(e)}
    
    def run_assembly(self, gids: list = None) -> dict:
        """
        Run project assembly phase.
        Processes projects in batches for scalability.
        """
        if gids is None:
            # Get GIDs from discovery if not provided
            discovery_result = self.run_discovery()
            if discovery_result["status"] != "success":
                return discovery_result
            gids = discovery_result.get("gids", [])
        
        # Normalize to list to support slicing/batching
        if not isinstance(gids, list):
            gids = list(gids)
        
        logger.info("Starting project assembly phase", extra={"total_projects": len(gids)})
        
        try:
            from core.assembly import ProjectAssembler
            from core.storage import ProjectStorage
            
            assembler = ProjectAssembler(self.config)
            storage = ProjectStorage(self.config)
            
            # Process in batches (Factor 8: Concurrency)
            batch_size = self.config.batch_size
            all_projects = []
            total_completed = 0
            total_failed = 0
            
            for i in range(0, len(gids), batch_size):
                if not self.running:  # Check for shutdown signal
                    logger.info("Shutdown requested, stopping assembly")
                    break
                
                batch = gids[i:i + batch_size]
                logger.info("Processing batch", extra={
                    "batch_start": i,
                    "batch_size": len(batch)
                })
                
                results = assembler.process_batch(batch)
                all_projects.extend(results.projects)
                total_completed += results.completed
                total_failed += results.failed
            
            # Save processed projects
            if all_projects:
                storage.save_projects(all_projects)
                storage.save_companies_with_projects(all_projects)
                # Also export to Excel
                storage.export_to_excel(all_projects)
                
                # Save metrics
                metrics = assembler.get_metrics()
                storage.save_metrics(metrics)
            
            assembler.close()
            
            logger.info("Project assembly completed", extra={
                "completed": total_completed,
                "failed": total_failed,
                "projects_saved": len(all_projects)
            })
            
            return {
                "status": "success",
                "completed": total_completed,
                "failed": total_failed,
                "projects_saved": len(all_projects)
            }
            
        except Exception as e:
            logger.error("Project assembly failed", extra={"error": str(e)})
            return {"status": "error", "error": str(e)}
    
    def run_export(self, projects: list = None) -> dict:
        """Export processed data to various formats."""
        logger.info("Starting data export phase")
        
        try:
            from core.storage import ProjectStorage
            
            storage = ProjectStorage(self.config)
            
            if projects:
                results = storage.export_all(projects)
            else:
                results = storage.export_all([])  # Empty list for now
            
            logger.info("Data export completed", extra=results)
            return {"status": "success", **results}
            
        except Exception as e:
            logger.error("Data export failed", extra={"error": str(e)})
            return {"status": "error", "error": str(e)}
    
    def run_full_pipeline(self) -> dict:
        """
        Run the complete data processing pipeline.
        Designed as a stateless process (Factor 6: Processes).
        """
        logger.info("Starting full pipeline")
        
        # Phase 1: Discovery
        discovery_result = self.run_discovery()
        if discovery_result["status"] != "success":
            return discovery_result
        
        # Phase 2: Assembly (if we have GIDs)
        gids = discovery_result.get("gids", [])
        if discovery_result["unique_gids"] > 0 and gids:
            assembly_result = self.run_assembly(gids=gids)
        else:
            assembly_result = {"status": "skipped", "reason": "no_gids_found"}
        
        # Phase 3: Export (projects are saved during assembly, so just report status)
        export_result = {"status": "success", "message": "Data saved during assembly phase"}
        
        return {
            "status": "success",
            "phases": {
                "discovery": discovery_result,
                "assembly": assembly_result,
                "export": export_result
            }
        }


def main():
    """
    Main entry point (Factor 12: Admin Processes).
    Can be run as one-off process or continuous service.
    """
    try:
        # Load configuration
        config = AppConfig()
        
        # Create application
        app = Application(config)
        
        # Determine run mode from command line or environment
        run_mode = os.getenv('RUN_MODE', 'pipeline')
        
        if len(sys.argv) > 1:
            run_mode = sys.argv[1]
        
        # Execute based on mode (Factor 12: Admin Processes)
        if run_mode == 'health':
            result = app.health_check()
            print(f"Health: {result}")
        elif run_mode == 'discovery':
            result = app.run_discovery()
            print(f"Discovery: {result}")
        elif run_mode == 'assembly':
            result = app.run_assembly([])  # Would get GIDs from discovery
            print(f"Assembly: {result}")
        elif run_mode == 'export':
            result = app.run_export()
            print(f"Export: {result}")
        else:  # Default: full pipeline
            result = app.run_full_pipeline()
            print(f"Pipeline: {result}")
        
        # Exit with appropriate code
        sys.exit(0 if result.get("status") == "success" else 1)
        
    except Exception as e:
        logger.error("Application failed", extra={"error": str(e)})
        sys.exit(1)


if __name__ == "__main__":
    main()
