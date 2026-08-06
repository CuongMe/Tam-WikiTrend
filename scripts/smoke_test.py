from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.pageviews import parse_pageview_line


def main() -> None:
    record = parse_pageview_line("en Main_Page 100 2048", "2026-01-01", 0)
    if record is None:
        raise SystemExit("parser smoke test failed")
    print(record.to_dict())


if __name__ == "__main__":
    main()
