#!/usr/bin/env python3
"""Download and prepare a Kaggle network-traffic dataset for MapReduce.

This script fetches a dataset from Kaggle that contains network flow records,
extracts/renames the required columns (source IP, destination IP, frame length
aka bytes) to the canonical names expected by `mapper.py` (`ip.src`, `ip.dst`,
`frame.len`) and writes the cleaned CSV to `logs.csv` (or a user-specified
path).

Prerequisites:
1. Install the Kaggle Python client and pandas (see `requirements.txt`).
2. Set the environment variables `KAGGLE_USERNAME` and `KAGGLE_KEY` with your
   Kaggle API credentials (https://www.kaggle.com/docs/api).

Usage example:
    python download_dataset.py \
        --dataset ravikumargattu/network-traffic-dataset \
        --file packetflows.csv \
        --output logs.csv

After generating `logs.csv`, upload it to HDFS:
    hdfs dfs -mkdir -p /user/$USER/network_logs
    hdfs dfs -put -f logs.csv /user/$USER/network_logs/
Then run the Hadoop job with `run_hadoop_command.sh` (defaults will pick up the
same HDFS location).
"""
import argparse
import os
import subprocess
import sys
import tempfile

import pandas as pd


DEFAULT_DATASET = "ravikumargattu/network-traffic-dataset"
DEFAULT_FILE = "packetflows.csv"
DEFAULT_OUTPUT = "logs.csv"

# Possible column aliases across datasets -> canonical column names
COLUMN_ALIASES = {
    # common names
    "Source IP": "ip.src",
    "Src IP": "ip.src",
    "srcip": "ip.src",
    "src_ip": "ip.src",
    "ip.src": "ip.src",

    "Destination IP": "ip.dst",
    "Dst IP": "ip.dst",
    "dstip": "ip.dst",
    "dst_ip": "ip.dst",
    "ip.dst": "ip.dst",

    "Frame Length": "frame.len",
    "Bytes": "frame.len",
    "Total Bytes": "frame.len",
    "bytes": "frame.len",
    "frame.len": "frame.len",
}

REQUIRED_COLS = {"ip.src", "ip.dst", "frame.len"}


def download_and_extract(dataset: str, path: str):
    """Download and unzip a Kaggle dataset into the given directory."""
    print(f"Downloading Kaggle dataset '{dataset}' → {path} …", file=sys.stderr)
    subprocess.check_call([
        "kaggle",
        "datasets",
        "download",
        "-d",
        dataset,
        "--unzip",
        "-p",
        path,
    ])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Kaggle dataset identifier (owner/dataset-name)")
    parser.add_argument("--file", default=DEFAULT_FILE, help="CSV file inside the dataset ZIP containing the flows")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path for cleaned logs")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        download_and_extract(args.dataset, tmpdir)
        csv_path = os.path.join(tmpdir, args.file)
        if not os.path.exists(csv_path):
            # fall back: try lower-case name
            alt_path = os.path.join(tmpdir, args.file.lower())
            if os.path.exists(alt_path):
                csv_path = alt_path
            else:
                raise FileNotFoundError(f"{args.file} not found inside dataset")

        print(f"Reading {csv_path} …", file=sys.stderr)
        df = pd.read_csv(csv_path)

        # Rename columns to canonical names where aliases are present
        rename_map = {col: COLUMN_ALIASES[col] for col in df.columns if col in COLUMN_ALIASES}
        df = df.rename(columns=rename_map)

        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(f"Dataset missing required columns: {', '.join(missing)}")

        cleaned = df[list(REQUIRED_COLS)].copy()
        # Ensure frame.len is integer
        cleaned["frame.len"] = cleaned["frame.len"].astype(int, errors="ignore")

        cleaned.to_csv(args.output, index=False)
        print(f"Wrote cleaned logs to {args.output} [{len(cleaned)} rows]")


if __name__ == "__main__":
    main()
