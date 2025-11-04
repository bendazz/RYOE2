#!/usr/bin/env python3
import csv
import os
import sys
import tempfile
from typing import List


def find_csv_files(directory: str) -> List[str]:
    files = []
    for entry in sorted(os.listdir(directory)):
        path = os.path.join(directory, entry)
        if os.path.isfile(path) and entry.lower().endswith('.csv'):
            files.append(path)
    return files


def remove_unnamed_columns(csv_path: str) -> int:
    # Returns number of columns removed; 0 if none.
    # Use utf-8-sig to transparently handle BOM in header.
    # Detect dialect to preserve delimiter/quoting.
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(f, dialect)
        try:
            header = next(reader)
        except StopIteration:
            # Empty file - nothing to do
            return 0

        keep_idx = [i for i, h in enumerate(header) if not (h or '').strip().startswith('Unnamed')]
        removed = len(header) - len(keep_idx)
        if removed == 0:
            return 0

        # Write filtered data to a temporary file in the same directory
        dir_name = os.path.dirname(csv_path)
        with tempfile.NamedTemporaryFile('w', delete=False, dir=dir_name, suffix='.csv', encoding='utf-8', newline='') as tmp:
            writer = csv.writer(tmp, dialect)
            # Write filtered header
            writer.writerow([header[i] for i in keep_idx])
            # Write filtered rows
            for row in reader:
                # Pad row if short (defensive)
                if len(row) < len(header):
                    row = row + [''] * (len(header) - len(row))
                writer.writerow([row[i] for i in keep_idx])
            tmp_path = tmp.name

    # Atomically replace original file
    os.replace(tmp_path, csv_path)
    return removed


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print('Usage: remove_unnamed_columns.py <directory>')
        return 2

    target_dir = argv[1]
    if not os.path.isdir(target_dir):
        print(f'Not a directory: {target_dir}')
        return 2

    total_removed_cols = 0
    files = find_csv_files(target_dir)
    for path in files:
        removed = remove_unnamed_columns(path)
        total_removed_cols += removed
        print(f'{os.path.basename(path)}: removed {removed} Unnamed column(s)')

    print(f'TOTAL columns removed across files: {total_removed_cols}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
