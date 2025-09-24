#!/usr/bin/env python3
"""
Simple and Reliable Dependency Tracer
Uses grep and file system checks to trace actual dependencies from app.py
"""

import os
import subprocess
import json
from pathlib import Path
from typing import Set, List, Dict

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

def find_all_python_files() -> List[str]:
    """Find all Python files in the project."""
    python_files = []
    for path in PROJECT_ROOT.rglob('*.py'):
        if '__pycache__' not in str(path):
            relative_path = str(path.relative_to(PROJECT_ROOT))
            python_files.append(relative_path)
    return sorted(python_files)

def extract_imports_from_file(file_path: Path) -> List[str]:
    """Extract all import statements from a file using simple text parsing."""
    imports = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
                
            # Find import statements
            if line.startswith('import ') or line.startswith('from '):
                imports.append(line)
                
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return imports

def resolve_import_to_files(import_statement: str) -> List[str]:
    """Resolve import statement to actual project files."""
    resolved_files = []
    
    # Parse import statement
    if import_statement.startswith('from '):
        # from module import something
        parts = import_statement.split()
        if len(parts) >= 2:
            module_name = parts[1]
        else:
            return []
    elif import_statement.startswith('import '):
        # import module
        parts = import_statement.split()
        if len(parts) >= 2:
            module_name = parts[1].split('.')[0]  # Take first part
        else:
            return []
    else:
        return []
    
    # Skip standard library and external packages
    external_modules = {
        'os', 'sys', 'json', 'time', 'datetime', 'logging', 'signal', 'typing',
        'dataclasses', 'pathlib', 'collections', 'concurrent', 'asyncio',
        'requests', 'pandas', 'openpyxl', 'beautifulsoup4', 'bs4', 'selenium',
        'playwright', 'tqdm', 'psutil', 'jsonschema', 'dotenv', 'urllib3',
        'webdriver_manager', 'importlib', 'random'
    }
    
    if module_name in external_modules:
        return []
    
    # Look for actual files in project
    module_parts = module_name.split('.')
    
    # Try direct file match
    potential_file = PROJECT_ROOT / f"{module_parts[0]}.py"
    if potential_file.exists():
        resolved_files.append(str(potential_file.relative_to(PROJECT_ROOT)))
    
    # Try package directory
    potential_init = PROJECT_ROOT / module_parts[0] / '__init__.py'
    if potential_init.exists():
        resolved_files.append(str(potential_init.relative_to(PROJECT_ROOT)))
        
        # Also check for specific module files in the package
        if len(module_parts) > 1:
            for i in range(1, len(module_parts)):
                nested_file = PROJECT_ROOT / module_parts[0] / f"{module_parts[i]}.py"
                if nested_file.exists():
                    resolved_files.append(str(nested_file.relative_to(PROJECT_ROOT)))
    
    return resolved_files

def trace_dependencies_recursive(file_path: str, visited: Set[str] = None) -> Set[str]:
    """Recursively trace all dependencies from a file."""
    if visited is None:
        visited = set()
    
    if file_path in visited:
        return set()
    
    visited.add(file_path)
    all_dependencies = {file_path}
    
    print(f"📄 Tracing: {file_path}")
    
    full_path = PROJECT_ROOT / file_path
    if not full_path.exists():
        return all_dependencies
    
    # Get imports from this file
    imports = extract_imports_from_file(full_path)
    
    for import_stmt in imports:
        resolved_files = resolve_import_to_files(import_stmt)
        for resolved_file in resolved_files:
            print(f"  📦 {import_stmt.strip()} → {resolved_file}")
            # Recursively trace dependencies
            sub_deps = trace_dependencies_recursive(resolved_file, visited.copy())
            all_dependencies.update(sub_deps)
    
    return all_dependencies

def categorize_files(all_files: List[str], used_files: Set[str]) -> Dict:
    """Categorize files into used/unused groups."""
    unused_files = set(all_files) - used_files
    
    # Categorize unused files
    old_scripts = [f for f in unused_files if f.startswith('Old Scripts/')]
    scripts_unused = [f for f in unused_files if f.startswith('scripts/') and not f.endswith('simple_dependency_tracer.py')]
    core_unused = [f for f in unused_files if f.startswith('core/')]
    services_unused = [f for f in unused_files if f.startswith('services/')]
    other_unused = [f for f in unused_files if not any(f.startswith(prefix) for prefix in ['Old Scripts/', 'scripts/', 'core/', 'services/'])]
    
    return {
        'total_files': len(all_files),
        'used_files': sorted(list(used_files)),
        'unused_files': sorted(list(unused_files)),
        'old_scripts_unused': sorted(old_scripts),
        'scripts_unused': sorted(scripts_unused),
        'core_unused': sorted(core_unused),
        'services_unused': sorted(services_unused),
        'other_unused': sorted(other_unused)
    }

def print_results(results: Dict):
    """Print analysis results."""
    print("\n" + "="*70)
    print("📋 DEPENDENCY ANALYSIS FROM app.py")
    print("="*70)
    
    print(f"📁 Total Python files: {results['total_files']}")
    print(f"✅ Used by app.py: {len(results['used_files'])}")
    print(f"❌ Unused files: {len(results['unused_files'])}")
    
    print(f"\n✅ FILES USED BY app.py:")
    for file in results['used_files']:
        print(f"  • {file}")
    
    if results['old_scripts_unused']:
        print(f"\n🗑️ OLD SCRIPTS (safe to remove - {len(results['old_scripts_unused'])} files):")
        for file in results['old_scripts_unused']:
            print(f"  • {file}")
    
    if results['core_unused']:
        print(f"\n⚠️ UNUSED CORE FILES ({len(results['core_unused'])} files):")
        for file in results['core_unused']:
            print(f"  • {file}")
    
    if results['services_unused']:
        print(f"\n⚠️ UNUSED SERVICES FILES ({len(results['services_unused'])} files):")
        for file in results['services_unused']:
            print(f"  • {file}")
    
    if results['scripts_unused']:
        print(f"\n📜 UNUSED SCRIPTS ({len(results['scripts_unused'])} files):")
        for file in results['scripts_unused']:
            print(f"  • {file}")
    
    if results['other_unused']:
        print(f"\n❓ OTHER UNUSED ({len(results['other_unused'])} files):")
        for file in results['other_unused']:
            print(f"  • {file}")
    
    print(f"\n💡 CLOUD DEPLOYMENT RECOMMENDATIONS:")
    total_removable = len(results['old_scripts_unused']) + len(results['scripts_unused'])
    if total_removable > 0:
        print(f"  🗑️ Safe to remove: {total_removable} files")
        print(f"     - Old Scripts: {len(results['old_scripts_unused'])}")
        print(f"     - Unused Scripts: {len(results['scripts_unused'])}")
    
    if results['core_unused'] or results['services_unused']:
        print(f"  ⚠️ Review before removing: {len(results['core_unused']) + len(results['services_unused'])} core/services files")
    
    usage_pct = len(results['used_files']) / results['total_files'] * 100
    print(f"\n✨ Your main app uses {len(results['used_files'])}/{results['total_files']} files ({usage_pct:.1f}%)")

def main():
    """Run the dependency analysis."""
    print("🔍 Starting dependency analysis from app.py...")
    
    # Find all Python files
    all_files = find_all_python_files()
    print(f"Found {len(all_files)} Python files")
    
    # Trace dependencies from app.py
    if not (PROJECT_ROOT / 'app.py').exists():
        print("❌ app.py not found!")
        return
    
    used_files = trace_dependencies_recursive('app.py')
    
    # Categorize results
    results = categorize_files(all_files, used_files)
    
    # Save results
    with open(PROJECT_ROOT / 'dependency_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print results
    print_results(results)
    
    print(f"\n📄 Results saved to: dependency_analysis.json")

if __name__ == '__main__':
    main()
