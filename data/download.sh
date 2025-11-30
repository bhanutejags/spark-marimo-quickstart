#!/usr/bin/env bash
# Download sample dataset for the quickstart
# Using NYC Taxi data (small sample) as an example

set -euo pipefail

DATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

echo "Downloading NYC Taxi sample data..."
curl -L -o "${DATA_DIR}/yellow_tripdata_2024-01.parquet" "${SAMPLE_URL}"

echo "Done! Data saved to ${DATA_DIR}/yellow_tripdata_2024-01.parquet"
