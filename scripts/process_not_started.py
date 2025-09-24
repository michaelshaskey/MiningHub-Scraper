#!/usr/bin/env python3
"""
Scenario B Processor

Reads outputs/excel_outputs/NotStarted.xlsx (sheet "Projects", column "gid"),
then processes those projects end-to-end using the existing Application pipeline
and assembler, preserving company data from Relationships API over scraper
fallbacks. Saves Excel with a custom prefix and timestamp.
"""

import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv

# Ensure project root (parent of scripts/) is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(name)s"}',
)
logger = logging.getLogger(__name__)


def read_gids_from_excel(path: str, sheet: str = 'Projects', col: str = 'gid') -> list[str]:
    df = pd.read_excel(path, sheet_name=sheet)
    if col not in df.columns:
        # try case-insensitive
        cols_lower = {str(c).lower(): c for c in df.columns}
        if col.lower() not in cols_lower:
            raise ValueError(f"Column '{col}' not found in {path}:{sheet}")
        col = cols_lower[col.lower()]
    gids = [str(x).strip() for x in df[col].dropna().tolist()]
    gids = [g for g in gids if g]
    return gids


def main():
    from app import AppConfig, Application
    from core.storage import ProjectStorage

    base_dir = os.getcwd()
    excel_in = os.path.join(base_dir, 'outputs', 'excel_outputs', 'NotStarted.xlsx')
    if not os.path.exists(excel_in):
        raise FileNotFoundError(f"NotStarted.xlsx not found at {excel_in}")

    gids = read_gids_from_excel(excel_in, sheet='Projects', col='gid')
    logger.info(f"Loaded {len(gids)} gids from NotStarted.xlsx")

    # Force production mode and global preload (all countries) for this run
    os.environ['PROCESSING_MODE'] = 'production'
    # Ensure COUNTRIES/COUNTRY envs don't restrict preloading
    os.environ.pop('COUNTRIES', None)
    os.environ.pop('COUNTRY', None)

    # Build config from env, ensure JWT is present
    cfg = AppConfig()
    # Explicitly set countries empty to trigger global preload in assembler
    cfg.countries = []
    app = Application(cfg)

    # Run assembly just for these gids
    result = app.run_assembly(gids=gids)
    logger.info(f"Assembly finished: {result}")

    # Export Excel with custom prefix
    from core.assembly import ProjectAssembler
    assembler = ProjectAssembler(cfg)
    metrics = assembler.get_metrics()
    storage = ProjectStorage(cfg)
    # We didn't keep the projects list here; read from latest projects file is not implemented.
    # Instead, we trigger export via storage using the assembler's last processed projects if available.
    # To ensure we export the projects we just processed, we replicate minimal flow by re-processing in small batches to collect objects.

    # Assembling objects for export
    # (Better: modify Application.run_assembly to return projects; for now, we rebuild quickly with small batches.)
    projects = []
    batch_size = 50
    from core.assembly import ProjectAssembler as PA
    pa = PA(cfg)
    for i in range(0, len(gids), batch_size):
        sub = gids[i:i+batch_size]
        res = pa.process_batch(sub)
        projects.extend(res.projects)
    pa.close()

    if projects:
        storage.export_to_excel(projects, filename_prefix='NotStarted_processed')
        storage.save_projects(projects)
        storage.save_companies_with_projects(projects)
        storage.save_metrics(metrics)
        logger.info("Scenario B export complete")
    else:
        logger.warning("No projects were assembled for export")


if __name__ == '__main__':
    main()


