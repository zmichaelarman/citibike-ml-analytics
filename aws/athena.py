"""Run the Citi Bike queries on Amazon Athena and return pandas DataFrames.

Setup steps (see RUN_AWS.md): python aws/athena.py export | tables | check | measure
"""
import sys
from pathlib import Path

import awswrangler as wr
import boto3
import duckdb

SESSION = boto3.Session(profile_name="citibike", region_name="us-east-1")  # assumes the pipeline role; no keys in code
BUCKET = f"citibike-{SESSION.client('sts').get_caller_identity()['Account']}"
SQL = Path(__file__).with_name("athena.sql").read_text()


def sql(name, **values):
    """Return one named query from athena.sql, without comment lines, with the bucket and other values filled in."""
    block = SQL.split(f"-- name: {name}\n")[1].split("-- name:")[0]
    lines = [line for line in block.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip().format(bucket=BUCKET, **values)


def run(name, holidays=()):
    """Run one named query on Athena and return it with the same dtypes DuckDB would give."""
    dates = ", ".join(f"DATE '{d:%Y-%m-%d}'" for d in holidays)
    df = wr.athena.read_sql_query(sql(name, holidays=dates), database="citibike", workgroup="citibike",
                                  ctas_approach=False,  # plain results: no temporary tables, fewer permissions
                                  boto3_session=SESSION)
    for col in df.columns:  # pandas nullable dtypes -> numpy dtypes, so the notebook runs unchanged
        kind = str(df[col].dtype)
        if kind in ("Int64", "Float64"):
            df[col] = df[col].astype("float64" if kind == "Float64" or df[col].isna().any() else "int64")
        elif kind == "string":
            df[col] = df[col].astype(object)
    return df


def export():
    """Write data/trips.parquet as Parquet partitioned by year and month, ready to upload to S3."""
    Path("data/lake").mkdir(exist_ok=True)  # DuckDB may not create missing parent folders
    duckdb.sql("""
        COPY (SELECT *, year(started_at) AS year, month(started_at) AS month FROM 'data/trips.parquet')
        TO 'data/lake/trips' (FORMAT parquet, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE)
    """)


def tables():
    """Create the Parquet and raw CSV tables in the Glue catalog and register the month partitions."""
    for name in ["create_trips", "register_partitions", "create_trips_csv"]:
        wr.athena.start_query_execution(sql(name), database="citibike", workgroup="citibike",
                                        wait=True, boto3_session=SESSION)


def check():
    """Compare Athena's per-month counts with the local file's, and show sample timestamps."""
    local = duckdb.sql("SELECT year(started_at) AS year, month(started_at) AS month, COUNT(*) AS trips "
                       "FROM 'data/trips.parquet' GROUP BY ALL ORDER BY ALL").df()
    print("Athena:", run("check_counts"), "Local:", local, "Sample timestamps:", run("check_timestamps"), sep="\n")


def measure():
    """Print bytes scanned for the same June question on raw CSV and on Parquet, as README table rows."""
    for name in ["measure_csv", "measure_parquet", "measure_parquet_no_partition"]:
        meta = run(name).query_metadata
        print(f"| {name} | {meta['Statistics']['DataScannedInBytes'] / 1e6:,.1f} MB | {meta['QueryExecutionId']} |")


if __name__ == "__main__":
    {"export": export, "tables": tables, "check": check, "measure": measure}[sys.argv[1]]()
