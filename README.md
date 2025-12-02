# Spark marimo Quickstart

Local-first Spark development with **marimo** and **Spark 4.0 Connect**.

Develop locally, execute on remote clusters. Same code, different endpoint.

## What's This?

A practical example of the Spark Connect architecture:

- **Thin client** - PySpark runs locally, no Spark runtime needed
- **Remote execution** - Jobs run remotely via Spark Connect server
- **Reactive notebooks** - marimo's `.py` files are git-friendly and reproducible

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [mise](https://mise.jdx.dev/) or Python 3.14+
- [uv](https://docs.astral.sh/uv/)

### Setup

```bash
# Clone the repo
git clone https://github.com/yourusername/spark-marimo-quickstart.git
cd spark-marimo-quickstart

# Install dependencies
mise install  # or ensure Python 3.14+ is available
uv sync

# Start Spark Connect server
docker compose up -d

# Wait for Spark to be ready (~30 seconds on first run)
docker compose logs -f spark-connect
```

### Run the Notebook

```bash
# Open in marimo editor
uv run marimo edit notebooks/spark_feature_eng.py

# Or run headless
uv run marimo run notebooks/spark_feature_eng.py
```

## What's in the Notebook?

The `spark_feature_eng.py` notebook demonstrates:

1. **Connect to Spark** - One-line connection to Spark Connect
2. **Load data from S3** - NYC Taxi data from AWS Open Data
3. **ETL transformations** - Clean and filter trip records
4. **Feature engineering** - Time features, trip metrics, window aggregations
5. **Display results** - marimo's reactive DataFrame viewer

### Data Source

NYC TLC Trip Record Data (Parquet format):

- **Source**: [AWS Open Data Registry](https://registry.opendata.aws/nyc-tlc-trip-records-pds/)
- **S3 Path**: `s3a://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet`
- **Size**: ~45MB per month

## Project Structure

```
spark-marimo-quickstart/
├── src/spark_marimo_quickstart/  # Python package (uv_build layout)
├── notebooks/
│   └── spark_feature_eng.py      # marimo notebook
├── data/
│   └── download.sh               # Optional: download data locally
├── docker-compose.yml            # Spark Connect server
├── pyproject.toml                # uv project config
├── mise.toml                     # Python version (3.14)
└── README.md
```

## Configuration

### Local Development (Docker)

The default configuration connects to a local Spark Connect server:

```python
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("my-app") \
    .getOrCreate()
```

### S3 Access

The Docker setup uses anonymous credentials for the public NYC TLC bucket. For private buckets, configure AWS credentials:

```yaml
# docker-compose.yml
environment:
  - AWS_ACCESS_KEY_ID=your-key
  - AWS_SECRET_ACCESS_KEY=your-secret
```

## Commands

```bash
# Start Spark Connect
docker compose up -d

# Stop Spark Connect
docker compose down

# View Spark UI
open http://localhost:4040

# Run notebook
uv run marimo edit notebooks/spark_feature_eng.py

# Run tests
uv run pytest
```

## Resources

- [Spark Connect Documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [marimo Documentation](https://docs.marimo.io/)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Blog Post: Local-First Spark Development](#) <!-- TODO: Add link -->

## License

MIT
