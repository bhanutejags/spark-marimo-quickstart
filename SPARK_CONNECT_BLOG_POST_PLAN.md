# Spark Connect + marimo Blog Post Plan

## Status: Companion Repo Complete, Blog Post Pending

## Completed

### Companion Repository (`/Volumes/workplace/spark-marimo-quickstart/`)

```
spark-marimo-quickstart/
├── mise.toml                              # Python 3.14
├── pyproject.toml                         # uv_build, ruff (NPY, PD, AIR, PERF)
├── docker-compose.yml                     # Spark 4.0 Connect + UI
├── .gitignore
├── README.md
├── src/spark_marimo_quickstart/__init__.py
├── notebooks/spark_feature_eng.py         # marimo notebook
└── data/
    ├── download.sh
    └── yellow_tripdata_2024-01.parquet    # 47MB, 2.9M rows
```

### Tested & Working

- Spark Connect server starts ✓
- PySpark connection works ✓
- Full ETL pipeline (2.9M → 2.7M rows) ✓
- Feature engineering (time features, speed, fare metrics) ✓
- Ruff format/lint passes ✓
- Spark UI accessible at http://localhost:4040 ✓

### Docker Compose Ports

- `15002` - Spark Connect
- `4040` - Spark UI

## Remaining Tasks

1. **Git init companion repo** - Initialize and commit
2. **Write blog post** - `src/content/blog/marimo-spark-connect.mdx` in acceptable-beginner repo
3. **Add marimo embeds** - iframe embeds per marimo docs
4. **SEO checklist** - Title, meta description, headings

## Blog Post Structure (from plan)

1. Introduction (~150 words) - Hook, thesis
2. Spark Connect Architecture (~250 words) - Client-server model
3. Setting Up Spark Connect (~250 words) - Docker setup
4. marimo Setup (~150 words) - Brief intro, install
5. Practical Example (~500 words) - ETL + feature engineering
6. Local-to-Remote Workflow (~200 words) - Same code, different endpoint
7. What's Next (~150 words) - SageMaker tease
8. Conclusion (~100 words)

## Frontmatter

```yaml
title: "Local-First Spark Development with marimo and Spark 4.0 Connect"
description: "Run your notebook locally while executing Spark jobs on remote clusters. A practical guide to the Spark Connect architecture with marimo."
tags: ["spark", "marimo", "python", "data-engineering", "spark-connect"]
```

## Notes

- S3 direct access had permission issues with nyc-tlc bucket
- Using local file download instead (CloudFront URL works)
- AWS packages removed from docker-compose for simplicity
