from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.pageviews import parse_dump_filename


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_gzip(path: Path) -> None:
    with gzip.open(path, "rb") as handle:
        for _chunk in iter(lambda: handle.read(1024 * 1024), b""):
            pass


def build_manifest(
    raw_dir: Path,
    verify_crc: bool,
    project_root: Path | None = None,
) -> dict[str, object]:
    root = project_root or Path(__file__).resolve().parents[1]
    files = sorted(raw_dir.rglob("*.gz"))
    if not files:
        raise FileNotFoundError(f"No Bronze gzip files found under {raw_dir}")
    records = []
    for index, path in enumerate(files, start=1):
        date_value, hour = parse_dump_filename(path.name)
        if verify_crc:
            verify_gzip(path)
        digest = sha256_file(path)
        records.append(
            {
                "path": path.relative_to(root).as_posix(),
                "date": date_value,
                "hour": hour,
                "size_bytes": path.stat().st_size,
                "sha256": digest,
            }
        )
        print(f"Hashed Bronze file {index}/{len(files)}: {path.name}")
    snapshot_digest = hashlib.sha256(
        "".join(
            f"{record['path']}:{record['size_bytes']}:{record['sha256']}"
            for record in records
        ).encode("ascii")
    ).hexdigest()
    return {
        "manifest_version": 1,
        "snapshot_id": f"sha256:{snapshot_digest}",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "raw_dir": raw_dir.relative_to(root).as_posix(),
        "file_count": len(records),
        "total_bytes": sum(int(record["size_bytes"]) for record in records),
        "gzip_crc_verified": verify_crc,
        "files": records,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hash the immutable Bronze source snapshot.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/pageviews"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/manifests/bronze_snapshot.json"),
    )
    parser.add_argument(
        "--verify-gzip",
        action="store_true",
        help="Decompress every file to verify its CRC before hashing it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    raw_dir = args.raw_dir if args.raw_dir.is_absolute() else root / args.raw_dir
    output = args.output if args.output.is_absolute() else root / args.output
    payload = build_manifest(raw_dir, args.verify_gzip)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".part")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(f"Wrote {output} ({payload['snapshot_id']})")


if __name__ == "__main__":
    main()
