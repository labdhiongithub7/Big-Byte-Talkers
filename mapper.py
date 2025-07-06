#!/usr/bin/env python3
"""Hadoop Streaming mapper for Top Talkers by Bandwidth.
Reads CSV lines (ip.src, ip.dst, frame.len) from stdin and emits
<IP>\t<bytes> for both source and destination addresses."""
import sys
import csv


def main():
    reader = csv.reader(sys.stdin)
    # Skip header if present
    header = next(reader, None)
    if header and header[:3] != ["ip.src", "ip.dst", "frame.len"]:
        # header was actually data; rewind
        sys.stdin.seek(0)
        reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) < 3:
            continue
        src, dst, size = row[0], row[1], row[2]
        try:
            size_int = int(size)
        except ValueError:
            continue
        for ip in (src, dst):
            if ip:
                print(f"{ip}\t{size_int}")


if __name__ == "__main__":
    main()