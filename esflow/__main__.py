"""支持 python -m esflow 调用 CLI。"""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
