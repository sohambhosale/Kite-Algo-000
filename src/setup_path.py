"""
setup_path.py
-------------
Utility to make sure project imports work inside Jupyter or VS Code Interactive mode.

Usage:
    from src.setup_path import enable_project_imports
    enable_project_imports()
"""

import sys
import os


def enable_project_imports():
    """
    Adds the project root (one level above src/) to sys.path
    so that imports like `from src.websocket import LiveWebSocket`
    work in notebooks or interactive sessions.
    """
    # Find project root (one level above this file)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    if project_root not in sys.path:
        sys.path.append(project_root)
        print(f"🔧 Added project root to sys.path: {project_root}")
    else:
        print("✅ Project root already in sys.path.")
