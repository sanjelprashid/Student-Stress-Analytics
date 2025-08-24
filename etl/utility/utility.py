import logging
import sys
from datetime import timedelta

# setup logger that prints messages with timestamp and level
def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # add stream handler if none exists
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch.setFormatter(ch_fmt)
        logger.addHandler(ch)
    return logger

# format elapsed time in hh:mm:ss style
def format_seconds(seconds: float) -> str:
    td = timedelta(seconds=int(seconds))
    return str(td)

# create or get SparkSession with local configs
def spark_session(app_name="StressETL", master=None):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    builder = builder.config("spark.driver.memory", "2g")
    spark = builder.getOrCreate()
    return spark

# convert column names to lower_snake_case
def snake_case(s: str) -> str:
    return (
        s.strip()
         .lower()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("/", "_")
         .replace("(", "")
         .replace(")", "")
         .replace(".", "_")
    )

# map Spark dtypes to Postgres dtypes (best effort)
def map_spark_to_pg(dtype: str) -> str:
    d = dtype.lower()
    if d in ("string", "binary", "null"):
        return "text"
    if d in ("boolean",):
        return "boolean"
    if d in ("byte", "short", "integer", "int"):
        return "integer"
    if d in ("long",):
        return "bigint"
    if d in ("float",):
        return "real"
    if d in ("double", "decimal"):
        return "double precision"
    if d in ("date",):
        return "date"
    if d in ("timestamp", "timestamp_ntz"):
        return "timestamp"
    return "text"  # fallback type