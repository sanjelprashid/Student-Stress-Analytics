import os
import sys
import shutil
from pathlib import Path
from time import time

from etl.utility.utility import setup_logger, format_seconds, snake_case

# expected CSV filenames that should exist in the raw data folder
EXPECTED = ["stress_dataset.csv", "stressleveldataset.csv"]

def find_csvs(src_dir):
    # collect all CSV files in the given folder and map them by lowercase name
    files = {}
    for p in Path(src_dir).glob("*.csv"):
        files[p.name.lower()] = str(p)
    return files

def main():
    t0 = time()
    logger = setup_logger("extract")

    # check arguments: need <source_dir> and <extract_out_dir>
    if len(sys.argv) < 3:
        print("ERROR: Missing args\nUsage: python etl/extract/execute.py <source_dir> <extract_out_dir>")
        sys.exit(1)

    src = sys.argv[1]   # input directory with CSV files
    out = sys.argv[2]   # output directory for extracted files
    os.makedirs(out, exist_ok=True)

    files = find_csvs(src)

    # try to match found files against expected names (exact or snake_case)
    matched = {}
    for want in EXPECTED:
        if want in files:  # exact match
            matched[want] = files[want]
            continue
        for k, v in files.items():  # snake_case match
            if snake_case(k) == want:
                matched[want] = v
                break

    # exit if one or more expected files are missing
    missing = [f for f in EXPECTED if f not in matched]
    if missing:
        logger.error(f"Missing required CSV(s) in {src}: {missing}")
        logger.error("Place your two Kaggle files in data/raw and re-run.")
        sys.exit(2)

    # copy matched files into the extract directory with normalized names
    for norm_name, src_path in matched.items():
        dst_path = os.path.join(out, norm_name)
        shutil.copy2(src_path, dst_path)
        logger.info(f"Copied {src_path} -> {dst_path}")

    # log total execution time
    logger.info(f"Extract completed in {format_seconds(time()-t0)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())