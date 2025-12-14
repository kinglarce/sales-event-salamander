import csv
from pathlib import Path

# File paths
# Note: Codes thats been given to partners
DISTRIBUTED_CODES_PATH = Path('codes/partner_distributed_codes.csv')
# Note: Codes from the DB
ORIG_SPEC_PATH = Path('codes/codes_series.csv')
OUTPUT_PATH = Path('codes/redeemed.csv')


def load_used_codes(orig_spec_path):
    """
    Load codes from the original spec CSV where 'used' == '1'.
    Returns a set of redeemed codes.
    """
    used_codes = set()
    with open(orig_spec_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            code = row.get('code')
            used = row.get('used')
            if code and used == '1':
                used_codes.add(code)
    return used_codes


def process_distributed_codes(distributed_codes_path, used_codes, output_path):
    """
    Read the distributed codes CSV, add a 'redeemed' column based on used_codes, and write to output_path.
    """
    with open(distributed_codes_path, newline='', encoding='utf-8') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        header = next(reader)
        # Find the code column index (assume it's the 4th column, index 3)
        code_col_idx = 3 if len(header) > 3 else None
        # Add 'redeemed' column
        new_header = header + ['redeemed']
        writer.writerow(new_header)

        for row in reader:
            # Defensive: skip empty or malformed rows
            if not row or (code_col_idx is not None and len(row) <= code_col_idx):
                writer.writerow(row + [''])
                continue
            code = row[code_col_idx].strip()
            redeemed = 'yes' if code in used_codes else 'no'
            writer.writerow(row + [redeemed])


def main():
    used_codes = load_used_codes(ORIG_SPEC_PATH)
    process_distributed_codes(DISTRIBUTED_CODES_PATH, used_codes, OUTPUT_PATH)
    print(f"Updated file written to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main() 