"""Share one observation-time implementation with the v4 compatibility adapter."""
from pathlib import Path
import argparse
import hashlib


def sync(v3, v4, *, check=False):
    pairs = [(Path(v3) / "src/stream_core/common" / name,
              Path(v4) / "src/stream_monitoring_v4/compatibility" / name)
             for name in ("observation_time.py", "viewer_evidence.py")]
    if any(source.is_symlink() or target.is_symlink() for source, target in pairs):
        raise ValueError("observation_contract_symlink")
    equal = True
    for source, target in pairs:
        expected = source.read_bytes()
        matches = target.is_file() and target.read_bytes() == expected
        equal = equal and matches
        if not matches and not check:
            target.write_bytes(expected)
    return equal if check else True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v4-root", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if not sync(root, args.v4_root, check=args.check):
        raise SystemExit("observation_contract_out_of_sync")
    print("observation contract: PASS")
