# Spark Connect + marimo Quickstart

Local-first Spark development using Spark 4.0 Connect and marimo notebooks.

## Quick Start

```bash
# Install dependencies
uv sync

# Start Spark Connect server
docker compose up -d

# Open notebook
uv run marimo edit src/spark_marimo_quickstart/spark_feature_eng.py
```

## Commands

```bash
uv sync                  # Install dependencies
uv run marimo edit ...   # Open notebook editor
uv run marimo check ...  # Lint notebook
uv run ruff check .      # Lint Python
uv run ruff format .     # Format Python
```

## Ports

- `15002` - Spark Connect (gRPC)
- `4040` - Spark UI
