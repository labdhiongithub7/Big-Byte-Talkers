#!/usr/bin/env python3
"""Hadoop Streaming reducer for Top Talkers by Bandwidth.
Aggregates total bytes per IP from mapper output (<IP>\t<bytes>)."""
import sys

current_ip = None
current_total = 0

for line in sys.stdin:
    try:
        ip, bytes_str = line.rstrip().split("\t", 1)
        bytes_val = int(bytes_str)
    except ValueError:
        # skip malformed line
        continue

    if ip == current_ip:
        current_total += bytes_val
    else:
        if current_ip is not None:
            print(f"{current_ip}\t{current_total}")
        current_ip = ip
        current_total = bytes_val

# output last key
if current_ip is not None:
    print(f"{current_ip}\t{current_total}")