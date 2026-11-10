"""Shared SparkSession factory for the IMDb project."""
from pyspark.sql import SparkSession


def get_spark(app_name="imdb-project", driver_memory="2g"):
    """Create or reuse a local SparkSession for an 8 GB machine."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "16")
        # Tolerate long pauses on a memory-constrained laptop.
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
