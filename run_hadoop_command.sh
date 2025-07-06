#!/usr/bin/env bash
# run_hadoop_command.sh - Submit Hadoop Streaming job for Top Talkers
# Usage: ./run_hadoop_command.sh <hdfs_csv_path> <hdfs_output_dir>
# Defaults:
#   input  = /user/$USER/network_logs/network_logs.csv
#   output = /user/$USER/output/top_talkers
set -euo pipefail

INPUT=${1:-/user/$USER/network_logs/network_logs.csv}
OUTPUT=${2:-/user/$USER/output/top_talkers}

# Remove existing output to avoid job failure
hdfs dfs -rm -r -f "$OUTPUT" || true

echo "Running Hadoop Streaming job..."

hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-"*.jar \
    -D mapreduce.job.name="TopTalkers" \
    -input  "$INPUT" \
    -output "$OUTPUT" \
    -mapper  mapper.py \
    -reducer reducer.py \
    -file    mapper.py \
    -file    reducer.py

echo "Job finished. Output stored at $OUTPUT"