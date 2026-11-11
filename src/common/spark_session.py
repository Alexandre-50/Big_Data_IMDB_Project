"""Shared SparkSession factory for the IMDb project."""
import os

from pyspark.sql import SparkSession

# Number of local cores used by Spark. Lower it (e.g. SPARK_MASTER=local[2])
# on machines with little memory: fewer parallel tasks use less RAM.
DEFAULT_MASTER = os.environ.get("SPARK_MASTER", "local[*]")


def get_spark(app_name="imdb-project", driver_memory="2g",
              master=DEFAULT_MASTER):
    """Create or reuse a local SparkSession for an 8 GB machine."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "16")
        # Tolerate long pauses on a memory-constrained laptop.
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
