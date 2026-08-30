"""``python -m runrecord`` -> the read-only run-record query CLI (BL-0005)."""

import sys

from .query import main

if __name__ == '__main__':
    sys.exit(main())
