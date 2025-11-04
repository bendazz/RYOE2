#!/usr/bin/env python3
import csv
import hashlib
import os
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def list_csvs(directory: str) -> List[str]:
    return [
        os.path.join(directory, f)
        for f in sorted(os.listdir(directory))
        if os.path.isfile(os.path.join(directory, f)) and f.lower().endswith('.csv')
    ]


def sniff_dialect(path: str) -> csv.Dialect:
    with open(path, 'r', encoding='utf-8-sig', newline='') as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            return csv.Sniffer().sniff(sample)
        except csv.Error:
            return csv.excel


def read_header(path: str, dialect: Optional[csv.Dialect] = None) -> List[str]:
    if dialect is None:
        dialect = sniff_dialect(path)
    with open(path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f, dialect)
        try:
            return next(reader)
        except StopIteration:
            return []


def build_index_map(src_header: List[str], dst_header: List[str]) -> List[Optional[int]]:
    # For each column in dst_header, find its index in src_header; None if missing
    idx_by_name: Dict[str, int] = {name: i for i, name in enumerate(src_header)}
    return [idx_by_name.get(name) for name in dst_header]


def row_to_dst(row: List[str], idx_map: List[Optional[int]]) -> List[str]:
    out: List[str] = []
    for idx in idx_map:
        if idx is None:
            out.append('')
        else:
            if idx < len(row):
                out.append(row[idx])
            else:
                out.append('')
    return out


def hash_row(cells: Iterable[str]) -> bytes:
    h = hashlib.sha256()
    # Use a non-occurring separator to avoid ambiguities; NUL is safe for CSV cell content
    h.update('\0'.join(cells).encode('utf-8'))
    return h.digest()


def _normalize_for_key(val: str) -> str:
    """Normalize a cell value for key comparison.

    - trim whitespace
    - unify case for strings
    - collapse floats with .0 to integer-like strings
    """
    s = (val or '').strip()
    # common float-as-int pattern (e.g., '1.0' -> '1')
    if s.replace('.', '', 1).isdigit():
        try:
            f = float(s)
            if f.is_integer():
                return str(int(f))
        except Exception:
            pass
    return s.lower()


def combine_csvs(input_dir: str, output_csv: str) -> Dict[str, int]:
    files = list_csvs(input_dir)
    if not files:
        raise SystemExit(f'No CSV files found in {input_dir}')

    # Determine canonical header from the first non-empty CSV
    canonical_header: List[str] = []
    first_header_path: Optional[str] = None
    for path in files:
        hdr = read_header(path)
        if hdr:
            canonical_header = hdr
            first_header_path = path
            break
    if not canonical_header:
        raise SystemExit('All CSV files are empty; nothing to combine.')

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    # Duplicate tracking
    seen_full_rows = set()  # fall back to full-row hash when key not available
    seen_keys: set[Tuple[str, ...]] = set()

    # Prefer deduplication by these key columns when present
    KEY_COLUMNS: Sequence[str] = ("game_id", "play_id")
    key_idx: Optional[List[Optional[int]]] = None
    totals = {
        'files': 0,
        'rows_in': 0,
        'duplicates_skipped': 0,
        'rows_out': 0,
    }

    with open(output_csv, 'w', encoding='utf-8', newline='') as out_f:
        writer = csv.writer(out_f)
        writer.writerow(canonical_header)

        # Build key index map once based on canonical header
        idx_by_name: Dict[str, int] = {name: i for i, name in enumerate(canonical_header)}
        key_idx = [idx_by_name.get(name) for name in KEY_COLUMNS]
        use_key = all(i is not None for i in key_idx)

        for path in files:
            totals['files'] += 1
            dialect = sniff_dialect(path)
            with open(path, 'r', encoding='utf-8-sig', newline='') as in_f:
                reader = csv.reader(in_f, dialect)
                try:
                    src_header = next(reader)
                except StopIteration:
                    continue  # empty file

                # Build per-file index map from src->canonical
                idx_map = build_index_map(src_header, canonical_header)

                for row in reader:
                    totals['rows_in'] += 1
                    out_row = row_to_dst(row, idx_map)
                    is_dup = False
                    if use_key:
                        # Construct normalized key tuple
                        k_tuple = tuple(_normalize_for_key(out_row[i]) for i in key_idx if i is not None)
                        if k_tuple in seen_keys:
                            totals['duplicates_skipped'] += 1
                            is_dup = True
                        else:
                            seen_keys.add(k_tuple)
                    if not use_key and not is_dup:
                        # Fallback to full-row hash equality
                        key_hash = hash_row(out_row)
                        if key_hash in seen_full_rows:
                            totals['duplicates_skipped'] += 1
                            is_dup = True
                        else:
                            seen_full_rows.add(key_hash)
                    if is_dup:
                        continue
                    writer.writerow(out_row)
                    totals['rows_out'] += 1

    return totals


def main(argv: List[str]) -> int:
    if len(argv) not in (3,):
        print('Usage: combine_csvs.py <input_dir> <output_csv>')
        return 2
    input_dir = argv[1]
    output_csv = argv[2]
    if not os.path.isdir(input_dir):
        print(f'Not a directory: {input_dir}')
        return 2

    stats = combine_csvs(input_dir, output_csv)
    print(f"Processed {stats['files']} file(s)")
    print(f"Rows read: {stats['rows_in']}")
    print(f"Duplicates removed: {stats['duplicates_skipped']}")
    print(f"Rows written: {stats['rows_out']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
