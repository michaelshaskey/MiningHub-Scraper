#!/usr/bin/env python3
"""
Cache and Output Cleanup Tool
Analyzes and cleans up cache files, old outputs, and temporary data
for cloud deployment preparation.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import shutil

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

def analyze_file_writes() -> Dict:
    """Analyze what files are being written by the application."""
    analysis = {
        'cache_files': [],
        'output_files': {
            'json_outputs': [],
            'excel_outputs': [],
            'reports': []
        },
        'log_files': [],
        'temp_files': [],
        'sizes': {}
    }
    
    # Cache files
    cache_dir = PROJECT_ROOT / 'outputs' / 'cache'
    if cache_dir.exists():
        for file in cache_dir.rglob('*'):
            if file.is_file():
                size = file.stat().st_size
                analysis['cache_files'].append({
                    'path': str(file.relative_to(PROJECT_ROOT)),
                    'size_mb': round(size / 1024 / 1024, 2),
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
                analysis['sizes']['cache'] = analysis['sizes'].get('cache', 0) + size
    
    # Output files
    outputs_dir = PROJECT_ROOT / 'outputs'
    if outputs_dir.exists():
        for subdir in ['json_outputs', 'excel_outputs', 'reports']:
            subdir_path = outputs_dir / subdir
            if subdir_path.exists():
                total_size = 0
                for file in subdir_path.glob('*'):
                    if file.is_file():
                        size = file.stat().st_size
                        total_size += size
                        analysis['output_files'][subdir].append({
                            'name': file.name,
                            'path': str(file.relative_to(PROJECT_ROOT)),
                            'size_mb': round(size / 1024 / 1024, 2),
                            'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                        })
                analysis['sizes'][subdir] = total_size
    
    # Log files
    for pattern in ['*.log', 'logs.txt']:
        for file in PROJECT_ROOT.glob(pattern):
            if file.is_file():
                size = file.stat().st_size
                analysis['log_files'].append({
                    'path': str(file.relative_to(PROJECT_ROOT)),
                    'size_mb': round(size / 1024 / 1024, 2),
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
                analysis['sizes']['logs'] = analysis['sizes'].get('logs', 0) + size
    
    # Analysis/report files
    for pattern in ['*_analysis.json', '*_report.json', 'main_app_dependencies.json']:
        for file in PROJECT_ROOT.glob(pattern):
            if file.is_file():
                size = file.stat().st_size
                analysis['temp_files'].append({
                    'path': str(file.relative_to(PROJECT_ROOT)),
                    'size_mb': round(size / 1024 / 1024, 2),
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
                analysis['sizes']['temp'] = analysis['sizes'].get('temp', 0) + size
    
    return analysis

def get_cleanup_recommendations(analysis: Dict) -> Dict:
    """Generate cleanup recommendations."""
    recommendations = {
        'safe_to_remove': [],
        'consider_removing': [],
        'keep_for_cloud': [],
        'total_savings_mb': 0
    }
    
    # Cache files - can be regenerated
    for cache_file in analysis['cache_files']:
        if cache_file['size_mb'] > 1:  # Large cache files
            recommendations['safe_to_remove'].append({
                'file': cache_file['path'],
                'reason': 'Geocoding cache - can be regenerated',
                'size_mb': cache_file['size_mb']
            })
            recommendations['total_savings_mb'] += cache_file['size_mb']
    
    # Old output files (keep only latest)
    cutoff_date = datetime.now() - timedelta(days=7)
    
    for subdir, files in analysis['output_files'].items():
        # Sort by modification date
        files_by_date = sorted(files, key=lambda x: x['modified'], reverse=True)
        
        # Keep latest 2 files, mark others for removal
        for i, file in enumerate(files_by_date):
            file_date = datetime.fromisoformat(file['modified'])
            if i >= 2 or file_date < cutoff_date:
                recommendations['consider_removing'].append({
                    'file': file['path'],
                    'reason': f'Old {subdir} file (keep latest 2)',
                    'size_mb': file['size_mb']
                })
                recommendations['total_savings_mb'] += file['size_mb']
    
    # Log files - safe to remove for deployment
    for log_file in analysis['log_files']:
        recommendations['safe_to_remove'].append({
            'file': log_file['path'],
            'reason': 'Log file - not needed for deployment',
            'size_mb': log_file['size_mb']
        })
        recommendations['total_savings_mb'] += log_file['size_mb']
    
    # Temp analysis files
    for temp_file in analysis['temp_files']:
        recommendations['safe_to_remove'].append({
            'file': temp_file['path'],
            'reason': 'Analysis/report file - not needed for deployment',
            'size_mb': temp_file['size_mb']
        })
        recommendations['total_savings_mb'] += temp_file['size_mb']
    
    # Files to keep for cloud deployment
    recommendations['keep_for_cloud'] = [
        'countries.json',
        'found_urls.xlsx',
        'requirements.txt',
        '.env (create from envexample.txt)',
        'app.py and core/, services/ modules'
    ]
    
    return recommendations

def execute_cleanup(recommendations: Dict, dry_run: bool = True) -> Dict:
    """Execute the cleanup plan."""
    results = {
        'removed_files': [],
        'failed_removals': [],
        'total_freed_mb': 0
    }
    
    files_to_remove = (
        recommendations['safe_to_remove'] + 
        recommendations['consider_removing']
    )
    
    for item in files_to_remove:
        file_path = PROJECT_ROOT / item['file']
        
        if dry_run:
            if file_path.exists():
                results['removed_files'].append(f"[DRY RUN] Would remove: {item['file']} ({item['size_mb']} MB)")
                results['total_freed_mb'] += item['size_mb']
        else:
            try:
                if file_path.exists():
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path)
                    
                    results['removed_files'].append(f"Removed: {item['file']} ({item['size_mb']} MB)")
                    results['total_freed_mb'] += item['size_mb']
            except Exception as e:
                results['failed_removals'].append(f"Failed to remove {item['file']}: {e}")
    
    return results

def print_analysis(analysis: Dict, recommendations: Dict):
    """Print the analysis results."""
    print("="*70)
    print("📋 FILE WRITE ANALYSIS & CLEANUP RECOMMENDATIONS")
    print("="*70)
    
    # Current file usage
    print(f"\n📊 CURRENT FILE USAGE:")
    total_size = sum(analysis['sizes'].values())
    for category, size in analysis['sizes'].items():
        size_mb = round(size / 1024 / 1024, 2)
        percentage = (size / total_size * 100) if total_size > 0 else 0
        print(f"  • {category.capitalize()}: {size_mb} MB ({percentage:.1f}%)")
    
    print(f"  • Total: {round(total_size / 1024 / 1024, 2)} MB")
    
    # Cache analysis
    if analysis['cache_files']:
        print(f"\n💾 CACHE FILES:")
        for cache in analysis['cache_files']:
            print(f"  • {cache['path']}: {cache['size_mb']} MB")
    
    # Output file counts
    print(f"\n📁 OUTPUT FILE COUNTS:")
    for subdir, files in analysis['output_files'].items():
        print(f"  • {subdir}: {len(files)} files")
    
    # Cleanup recommendations
    print(f"\n🗑️ SAFE TO REMOVE ({len(recommendations['safe_to_remove'])} files):")
    for item in recommendations['safe_to_remove']:
        print(f"  • {item['file']} - {item['reason']} ({item['size_mb']} MB)")
    
    if recommendations['consider_removing']:
        print(f"\n⚠️ CONSIDER REMOVING ({len(recommendations['consider_removing'])} files):")
        for item in recommendations['consider_removing'][:5]:  # Show first 5
            print(f"  • {item['file']} - {item['reason']} ({item['size_mb']} MB)")
        if len(recommendations['consider_removing']) > 5:
            print(f"  ... and {len(recommendations['consider_removing']) - 5} more old output files")
    
    print(f"\n✅ KEEP FOR CLOUD DEPLOYMENT:")
    for item in recommendations['keep_for_cloud']:
        print(f"  • {item}")
    
    print(f"\n💡 CLEANUP SUMMARY:")
    print(f"  🗑️ Total files to remove: {len(recommendations['safe_to_remove']) + len(recommendations['consider_removing'])}")
    print(f"  💾 Potential space saved: {recommendations['total_savings_mb']:.1f} MB")
    print(f"  📦 Deployment size reduction: ~{(recommendations['total_savings_mb'] / (total_size / 1024 / 1024)) * 100:.1f}%")

def main():
    """Run the cleanup analysis."""
    print("🔍 Analyzing file writes and cache usage...")
    
    analysis = analyze_file_writes()
    recommendations = get_cleanup_recommendations(analysis)
    
    # Save analysis
    with open(PROJECT_ROOT / 'cleanup_analysis.json', 'w') as f:
        json.dump({
            'analysis': analysis,
            'recommendations': recommendations,
            'generated_at': datetime.now().isoformat()
        }, f, indent=2)
    
    print_analysis(analysis, recommendations)
    
    # Ask for cleanup execution
    print(f"\n❓ CLEANUP OPTIONS:")
    print(f"  1. Dry run (show what would be removed)")
    print(f"  2. Execute cleanup (actually remove files)")
    print(f"  3. Just analysis (no changes)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        print(f"\n🔍 DRY RUN RESULTS:")
        results = execute_cleanup(recommendations, dry_run=True)
        for result in results['removed_files']:
            print(f"  {result}")
        print(f"\nTotal space that would be freed: {results['total_freed_mb']:.1f} MB")
    
    elif choice == "2":
        confirm = input(f"\n⚠️ This will permanently delete {len(recommendations['safe_to_remove']) + len(recommendations['consider_removing'])} files. Continue? (y/N): ").strip().lower()
        if confirm == 'y':
            print(f"\n🗑️ EXECUTING CLEANUP:")
            results = execute_cleanup(recommendations, dry_run=False)
            for result in results['removed_files']:
                print(f"  ✅ {result}")
            if results['failed_removals']:
                for failure in results['failed_removals']:
                    print(f"  ❌ {failure}")
            print(f"\n✨ Cleanup complete! Freed {results['total_freed_mb']:.1f} MB")
        else:
            print("Cleanup cancelled.")
    
    print(f"\n📄 Analysis saved to: cleanup_analysis.json")

if __name__ == '__main__':
    main()
