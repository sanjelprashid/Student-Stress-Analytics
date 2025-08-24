import os, sys, time
from pyspark.sql import functions as F, types as T
from pyspark.sql.utils import AnalysisException

from etl.utility.utility import setup_logger, format_seconds, spark_session, snake_case

# normalize column names to snake_case
def normalize_columns(df):
    return df.toDF(*[snake_case(c) for c in df.columns])

# attempt to cast string columns into numeric, date, or timestamp where possible
def try_cast_columns(df):
    for c, dt in df.dtypes:
        # skip if already numeric or date/time
        if dt in ("int", "bigint", "double", "float", "boolean", "date", "timestamp"):
            continue
        # try numeric cast
        df = df.withColumn(c, F.when(F.col(c).rlike(r'^-?\d+(\.\d+)?$'), F.col(c).cast("double")).otherwise(F.col(c)))
        # try timestamp/date cast if column name suggests time or date
        if "date" in c or "time" in c:
            df = df.withColumn(c, F.coalesce(F.to_timestamp(c), F.col(c)))
            df = df.withColumn(c, F.coalesce(F.to_date(c), F.col(c)))
    return df

# add stress_bucket column based on quantiles if stress_level is numeric
def add_quantile_stress_bucket(df):
    if "stress_level" not in [c for c, _ in df.dtypes]:
        return df
    dtype = dict(df.dtypes).get("stress_level", "string")
    if dtype not in ("int", "bigint", "double", "float"):
        return df
    q = df.approxQuantile("stress_level", [0.33, 0.66], 0.05)
    if len(q) < 2:
        return df
    q1, q2 = q
    return df.withColumn(
        "stress_bucket",
        F.when(F.col("stress_level") <= F.lit(q1), F.lit("low"))
         .when(F.col("stress_level") <= F.lit(q2), F.lit("moderate"))
         .otherwise(F.lit("high"))
    )

# read a CSV with header and schema inference
def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def main():
    t0 = time.time()
    logger = setup_logger("transform")

    # check required args
    if len(sys.argv) < 3:
        print("ERROR: Missing args\nUsage: python etl/transform/execute.py <extract_dir> <transform_out_dir>")
        sys.exit(1)

    in_dir = sys.argv[1]    # input directory with extracted CSVs
    out_dir = sys.argv[2]   # output directory for parquet
    os.makedirs(out_dir, exist_ok=True)

    # start Spark session
    spark = spark_session("StressETL-Transform")

    # read both CSV files from extract step
    f1 = os.path.join(in_dir, "stress_dataset.csv")
    f2 = os.path.join(in_dir, "stressleveldataset.csv")
    df1 = read_csv(spark, f1)
    df2 = read_csv(spark, f2)

    # normalize column names
    df1 = normalize_columns(df1)
    df2 = normalize_columns(df2)

    # try casting and add ingestion timestamp
    df1 = try_cast_columns(df1).withColumn("ingested_at", F.current_timestamp())
    df2 = try_cast_columns(df2).withColumn("ingested_at", F.current_timestamp())

    # remove duplicates
    df1 = df1.dropDuplicates()
    df2 = df2.dropDuplicates()

    # derive stress_bucket on df1 if applicable
    df1 = add_quantile_stress_bucket(df1)

    # write curated parquet outputs
    out_curated_student = os.path.join(out_dir, "curated_student")
    out_curated_levels = os.path.join(out_dir, "curated_stress_levels")
    df1.write.mode("overwrite").parquet(out_curated_student)
    df2.write.mode("overwrite").parquet(out_curated_levels)

    # build master table with best-effort join
    master = df1
    join_key = None
    for key in ["student_id", "id", "user_id", "uid", "record_id", "stress_level"]:
        if key in df1.columns and key in df2.columns:
            join_key = key
            break
    if join_key:
        master = df1.join(df2, on=join_key, how="left")

    # write master table parquet
    out_master = os.path.join(out_dir, "master_table")
    master.write.mode("overwrite").parquet(out_master)

    # log outputs and runtime
    logger.info(f"Wrote: {out_curated_student}")
    logger.info(f"Wrote: {out_curated_levels}")
    logger.info(f"Wrote: {out_master}")
    logger.info(f"Transform completed in {format_seconds(time.time()-t0)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())