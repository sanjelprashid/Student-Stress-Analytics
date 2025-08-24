import os, sys, time
import psycopg2
from psycopg2 import sql
from etl.utility.utility import setup_logger, format_seconds, spark_session, map_spark_to_pg

# create table in Postgres if it does not exist
def ensure_table(conn, table_name, schema):
    # schema: list of (column_name, spark_type)
    cols = [sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(map_spark_to_pg(t))) for c, t in schema]
    create = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(cols)
    )
    with conn.cursor() as cur:
        cur.execute(create)
    conn.commit()

# extract schema from a Spark dataframe as (column, type) pairs
def df_schema(df):
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]

# write a dataframe into Postgres using Spark JDBC
def write_jdbc(df, table, user, pwd, host, db, port):
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    props = {
        "user": user,
        "password": pwd,
        "driver": "org.postgresql.Driver"
    }
    df.write.mode("overwrite").jdbc(url=url, table=table, properties=props)

def main():
    t0 = time.time()
    logger = setup_logger("load")

    # check required args
    if len(sys.argv) < 7:
        print("ERROR: Missing args\nUsage: python etl/load/execute.py <transform_dir> <pg_user> <pg_password> <pg_host> <pg_db> <pg_port>")
        sys.exit(1)

    # unpack CLI args
    tdir, user, pwd, host, db, port = sys.argv[1:7]

    # start Spark session
    spark = spark_session("StressETL-Load")

    # read parquet outputs from transform step
    paths = {
        "curated_student": os.path.join(tdir, "curated_student"),
        "curated_stress_levels": os.path.join(tdir, "curated_stress_levels"),
        "master_table": os.path.join(tdir, "master_table"),
    }
    dfs = {name: spark.read.parquet(path) for name, path in paths.items()}

    # open Postgres connection
    conn = psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)

    try:
        # ensure table exists and load each dataframe
        for table, df in dfs.items():
            schema = df_schema(df)
            logger.info(f"Ensuring table {table} ...")
            ensure_table(conn, table, schema)
            logger.info(f"Loading {table} ({df.count()} rows) ...")
            write_jdbc(df, table, user, pwd, host, db, port)
            logger.info(f"Loaded {table}")
    finally:
        # close connection
        conn.close()

    # log total runtime
    logger.info(f"Load completed in {format_seconds(time.time()-t0)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())