#!/bin/bash
#for converting list of json data into serializing them into a line-oriented format
INPUT_DIR="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data/datafiles"
OUTPUT_DIR="/home/ra-terminal/Desktop/portfolio_projects/crypto_project/app/data/line_datafiles"

mkdir -p "$OUTPUT_DIR"

for file in "$INPUT_DIR"/*.json; do
    filename=$(basename "$file")
    jq -c '.[]' "$file" > "$OUTPUT_DIR/line_oriented_$filename"
done