"""
NYC Taxi Feature Engineering with Spark Connect and marimo

A practical example of local-first Spark development:
- Connect to Spark 4.0 Connect server (local Docker or remote cluster)
- Load NYC Taxi trip data
- Perform ETL transformations
- Engineer features for ML

Run with: marimo edit notebooks/spark_feature_eng.py
"""

import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")


@app.cell
def introduction():
    import marimo as mo

    mo.md(
        """
        # NYC Taxi Feature Engineering with Spark Connect

        This notebook demonstrates **local-first Spark development** using:
        - **Spark 4.0 Connect** - thin client, remote execution
        - **marimo** - reactive notebooks as pure Python

        The same code works locally (Docker) or against a remote cluster (EMR, Databricks).
        Just change the connection string!
        """
    )
    return (mo,)


@app.cell
def connect_to_spark():
    from pyspark.sql import SparkSession

    # For local Docker: sc://localhost:15002
    # For remote cluster: sc://your-cluster:15002
    SPARK_CONNECT_URL = "sc://localhost:15002"

    spark = (
        SparkSession.builder.remote(SPARK_CONNECT_URL).appName("nyc-taxi-features").getOrCreate()
    )

    # Prove we're using Spark Connect (not local driver)
    print(f"Spark version: {spark.version}")
    print(f"Session type: {type(spark).__module__}.{type(spark).__name__}")
    print(f"Using Spark Connect: {'connect' in type(spark).__module__}")
    return (spark,)


@app.cell
def load_data_header(mo):
    mo.md("""
    ## Load NYC Taxi Data

    We're using the official NYC TLC trip record data (Parquet format).
    Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    """)
    return


@app.cell
def load_data(spark):
    """Load the taxi trip data."""
    # Local path (download with: ./data/download.sh)
    DATA_PATH = "/opt/spark/work-dir/data/yellow_tripdata_2024-01.parquet"

    # Alternative: S3 path (requires AWS credentials or public bucket access)
    # DATA_PATH = "s3a://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet"

    raw_df = spark.read.parquet(DATA_PATH)
    print(f"Loaded {raw_df.count():,} trips")
    raw_df.printSchema()
    return (raw_df,)


@app.cell
def raw_data_preview_header(mo, raw_df):
    """Preview the raw data."""
    mo.md("### Raw Data Sample")
    # Convert sample to pandas for marimo display
    sample_df = raw_df.limit(1000).toPandas()
    return (sample_df,)


@app.cell
def raw_data_table(sample_df):
    sample_df
    return


@app.cell
def etl_header(mo):
    mo.md("""
    ## ETL: Clean and Transform

    Common data quality issues in taxi data:
    - Negative or zero fares
    - Impossible trip distances
    - Missing pickup/dropoff times
    - Outlier passenger counts
    """)
    return


@app.cell
def clean_data(raw_df):
    """Clean the data: remove invalid records."""
    from pyspark.sql import functions as F

    cleaned_df = (
        raw_df.filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 100)  # Remove outliers
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("passenger_count") <= 6)
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
    )

    original_count = raw_df.count()
    cleaned_count = cleaned_df.count()
    removed_pct = (original_count - cleaned_count) / original_count * 100

    print(f"Original: {original_count:,} rows")
    print(f"Cleaned: {cleaned_count:,} rows")
    print(f"Removed: {removed_pct:.1f}%")
    return F, cleaned_df


@app.cell
def feature_engineering_header(mo):
    mo.md("""
    ## Feature Engineering

    Create ML-ready features from the trip data:
    1. **Time features** - hour, day of week, is_weekend
    2. **Trip features** - duration, speed, fare per mile
    3. **Rolling features** - using window functions
    """)
    return


@app.cell
def engineer_time_features(F, cleaned_df):
    """Engineer time-based features."""
    df_with_time = cleaned_df.withColumns(
        {
            # Time features
            "pickup_hour": F.hour("tpep_pickup_datetime"),
            "pickup_day": F.dayofweek("tpep_pickup_datetime"),
            "is_weekend": F.when(F.dayofweek("tpep_pickup_datetime").isin([1, 7]), 1).otherwise(0),
            # Trip duration in minutes
            "trip_duration_min": (
                F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60,
        }
    )
    return (df_with_time,)


@app.cell
def engineer_trip_features(F, df_with_time):
    """Engineer trip-based features."""
    df_with_features = df_with_time.withColumns(
        {
            # Speed (mph)
            "avg_speed_mph": F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") / (F.col("trip_duration_min") / 60),
            ).otherwise(0),
            # Fare efficiency
            "fare_per_mile": F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount") / F.col("trip_distance"),
            ).otherwise(0),
            # Total amount per passenger
            "amount_per_passenger": F.col("total_amount") / F.col("passenger_count"),
        }
    ).filter(
        # Remove unrealistic speeds
        F.col("avg_speed_mph") < 80
    )
    return (df_with_features,)


@app.cell
def add_window_features(F, df_with_features):
    """Add window-based features: hourly aggregates."""
    from pyspark.sql.window import Window

    # Window: per pickup location, per hour
    hourly_window = Window.partitionBy("PULocationID", "pickup_hour")

    df_final = df_with_features.withColumns(
        {
            "avg_fare_by_location_hour": F.avg("fare_amount").over(hourly_window),
            "trip_count_by_location_hour": F.count("*").over(hourly_window),
        }
    )

    # Select final feature columns
    feature_cols = [
        "tpep_pickup_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        # Engineered features
        "pickup_hour",
        "pickup_day",
        "is_weekend",
        "trip_duration_min",
        "avg_speed_mph",
        "fare_per_mile",
        "amount_per_passenger",
        "avg_fare_by_location_hour",
        "trip_count_by_location_hour",
    ]

    df_final = df_final.select(feature_cols)
    return (df_final,)


@app.cell
def compute_hourly_trip_counts(F, df_with_features):
    """Aggregate trips by hour of day for visualization."""
    hourly_trips = (
        df_with_features.groupBy("pickup_hour")
        .agg(F.count("*").alias("trips"))
        .orderBy("pickup_hour")
        .toPandas()
    )
    return (hourly_trips,)


@app.cell
def visualize_hourly_trips(hourly_trips, mo):
    import altair as alt

    chart = (
        alt.Chart(hourly_trips)
        .mark_bar()
        .encode(
            x=alt.X("pickup_hour:O", title="Hour of Day"),
            y=alt.Y("trips:Q", title="Number of Trips"),
        )
        .properties(title="Trips by Hour of Day", width=600)
    )
    mo.ui.altair_chart(chart)
    return (alt,)


@app.cell
def visualize_fare_vs_distance(alt, features_sample, mo):
    chart2 = (
        alt.Chart(features_sample.sample(500))
        .mark_circle(opacity=0.5)
        .encode(
            x=alt.X("trip_distance:Q", title="Distance (miles)"),
            y=alt.Y("fare_amount:Q", title="Fare ($)"),
            color="pickup_hour:O",
        )
        .properties(title="Fare vs Distance", width=600)
    )
    mo.ui.altair_chart(chart2)
    return


@app.cell
def features_preview_header(mo):
    mo.md("""
    ### Engineered Features Preview
    """)
    return


@app.cell
def features_table(df_final):
    """Display sample of the final feature set."""
    features_sample = df_final.limit(1000).toPandas()
    features_sample
    return (features_sample,)


@app.cell
def summary_statistics(F, df_final, mo):
    """Summary statistics for engineered features."""
    stats = df_final.select(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("avg_speed_mph").alias("avg_speed"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
    ).toPandas()

    mo.md(
        f"""
        ## Summary Statistics

        | Metric | Value |
        |--------|-------|
        | Total Trips | {stats["total_trips"][0]:,} |
        | Avg Distance | {stats["avg_distance"][0]:.2f} miles |
        | Avg Fare | ${stats["avg_fare"][0]:.2f} |
        | Avg Speed | {stats["avg_speed"][0]:.1f} mph |
        | Avg Duration | {stats["avg_duration_min"][0]:.1f} min |
        """
    )
    return


@app.cell
def next_steps(mo):
    mo.md("""
    ## Next Steps

    The feature DataFrame is ready for:
    - **ML Training** - export to Parquet, use with Spark MLlib or sklearn
    - **Further Analysis** - aggregations, visualizations
    - **Production Pipeline** - same code runs on EMR/Databricks

    ```python
    # Save features to Parquet
    df_final.write.parquet("s3://your-bucket/features/taxi_features.parquet")
    ```
    """)
    return


@app.cell
def spark_session_cleanup():
    """Clean up Spark session when done."""
    # Uncomment to stop the session when done
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
