"""
FiniexDataCollector - CLI Entry Point
Command line interface for collector operations.

Location: python/cli/collector_cli.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.main import main


if __name__ == "__main__":
    main()
