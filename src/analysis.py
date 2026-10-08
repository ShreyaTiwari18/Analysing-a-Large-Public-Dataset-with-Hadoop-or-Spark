"""Reusable Spark analysis jobs for NYC Yellow Taxi data."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "outputs" / "results"
CHARTS = ROOT / "outputs" / "charts"
DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
PAYMENT_TYPES = {
    0: "Flex fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip",
}


def default_paths():
    """Full data if downloaded, otherwise the committed sample."""
    raw = ROOT / "data" / "raw" / "yellow_tripdata_2024-01.parquet"
    zones = ROOT / "data" / "raw" / "taxi_zone_lookup.csv"
    if raw.exists() and zones.exists():
        return str(raw), str(zones)
    return (str(ROOT / "data" / "sample" / "yellow_sample.parquet"),
            str(ROOT / "data" / "sample" / "taxi_zone_lookup.csv"))


def load_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def load_zones(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True)


def null_counts(df: DataFrame) -> pd.DataFrame:
    """Null count per column, as a small pandas DataFrame."""
    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
    return pd.DataFrame({"column": df.columns, "nulls": [row[c] or 0 for c in df.columns]})


def clean_trips(df: DataFrame):
    """Drop invalid rows, rule by rule. Returns (clean_df, report dict)."""
    rules = [
        ("fare_amount <= 0 (or null)", ~(F.col("fare_amount") > 0)),
        ("trip_distance <= 0 (or null)", ~(F.col("trip_distance") > 0)),
        ("passenger_count <= 0 (or null)", ~(F.col("passenger_count") > 0)),
        ("dropoff earlier than pickup (or null times)",
         ~(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))),
    ]
    report = {"rows_before": df.count()}
    cur = df
    for name, bad in rules:
        report[name] = cur.filter(bad).count()
        cur = cur.filter(~bad)
    report["rows_after"] = cur.count()
    return cur, report


def add_time_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day", F.date_format("tpep_pickup_datetime", "E"))
    )


def trips_by_hour(df):
    return df.groupBy("pickup_hour").agg(F.count("*").alias("trips")).orderBy("pickup_hour")


def trips_by_day(df) -> pd.DataFrame:
    out = df.groupBy("pickup_day").agg(F.count("*").alias("trips")).toPandas()
    out["order"] = out["pickup_day"].map({d: i for i, d in enumerate(DAY_ORDER)})
    return out.sort_values("order").drop(columns="order").reset_index(drop=True)


def top_pickup_zones(df, zones, n=10):
    z = zones.select(F.col("LocationID").alias("PULocationID"), "Zone", "Borough")
    return (
        df.groupBy("PULocationID").agg(F.count("*").alias("trips"))
        .join(z, "PULocationID", "left")
        .withColumn("Zone", F.coalesce("Zone", F.lit("Unknown")))
        .orderBy(F.desc("trips")).limit(n)
        .select("PULocationID", "Zone", "Borough", "trips")
    )


def avg_fare_distance_by_hour(df):
    return (
        df.groupBy("pickup_hour")
        .agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
             F.round(F.avg("trip_distance"), 2).alias("avg_distance"))
        .orderBy("pickup_hour")
    )


def _payment_name():
    mapping = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    return F.coalesce(mapping[F.col("payment_type").cast("int")], F.lit("Other")).alias("payment")


def payment_distribution(df):
    total = df.count()
    return (
        df.select(_payment_name()).groupBy("payment").agg(F.count("*").alias("trips"))
        .withColumn("pct", F.round(F.col("trips") * 100.0 / total, 2))
        .orderBy(F.desc("trips"))
    )


def tip_pct_by_payment(df):
    """Average tip % of fare by payment type (cash tips are not recorded, so ~0)."""
    return (
        df.select(_payment_name(), (F.col("tip_amount") / F.col("fare_amount") * 100).alias("tip_pct"))
        .groupBy("payment").agg(F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"), F.count("*").alias("trips"))
        .orderBy(F.desc("trips"))
    )


def tip_pct_by_hour(df):
    """Average tip % by hour, credit-card trips only (the only reliable tip data)."""
    return (
        df.filter(F.col("payment_type") == 1)
        .withColumn("tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100)
        .groupBy("pickup_hour").agg(F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"))
        .orderBy("pickup_hour")
    )


# ---------- Output helpers ----------
def save_csv(pdf, name):
    RESULTS.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(RESULTS / name, index=False)


def bar_chart(pdf, x, y, title, xlabel, ylabel, fname, horizontal=False, color="#3b6ea5"):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    CHARTS.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5.5))
    if horizontal:
        d = pdf.iloc[::-1]
        ax.barh(d[x].astype(str), d[y], color=color)
    else:
        ax.bar(pdf[x].astype(str), pdf[y], color=color)
        if len(pdf) > 8:
            plt.setp(ax.get_xticklabels(), rotation=0, fontsize=9)
    ax.set_title(title, fontsize=13)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(axis="x" if horizontal else "y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(CHARTS / fname, dpi=120)
    plt.close(fig)


def line_chart(pdf, x, ycols, title, xlabel, ylabel, fname):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    CHARTS.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for c, label in ycols:
        ax.plot(pdf[x], pdf[c], marker="o", label=label)
    ax.set_title(title, fontsize=13)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(0, 24, 2))
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(CHARTS / fname, dpi=120)
    plt.close(fig)


def make_charts(res):
    """Draw every chart from the dict of result DataFrames."""
    bar_chart(res["hour"], "pickup_hour", "trips", "Taxi trips by hour of day (Jan 2024)",
              "Hour of day (0-23)", "Number of trips", "trips_by_hour.png")
    bar_chart(res["day"], "pickup_day", "trips", "Taxi trips by day of week (Jan 2024)",
              "Day of week", "Number of trips", "trips_by_day.png", color="#2a9d8f")
    bar_chart(res["zones"], "Zone", "trips", "Top 10 pickup zones (Jan 2024)",
              "Number of trips", "Pickup zone", "top_pickup_zones.png", horizontal=True, color="#e76f51")
    line_chart(res["fare_dist"], "pickup_hour", [("avg_fare", "Avg fare ($)"), ("avg_distance", "Avg distance (miles)")],
               "Average fare and distance by hour of day", "Hour of day (0-23)", "Dollars / miles",
               "avg_fare_distance_by_hour.png")
    bar_chart(res["payment"], "payment", "trips", "Payment type distribution",
              "Payment type", "Number of trips", "payment_distribution.png", color="#8d6cab")
    bar_chart(res["tip_payment"], "payment", "avg_tip_pct", "Average tip % of fare by payment type",
              "Payment type", "Average tip (% of fare)", "tip_pct_by_payment.png", color="#e9a23b")
    bar_chart(res["tip_hour"], "pickup_hour", "avg_tip_pct", "Average tip % by hour (credit-card trips)",
              "Hour of day (0-23)", "Average tip (% of fare)", "tip_pct_by_hour.png", color="#e9a23b")


def run_all(spark=None, trips_path=None, zones_path=None):
    """Run every analysis, saving CSVs and charts. Returns dict of results."""
    from spark_session import get_spark
    spark = spark or get_spark()
    tp, zp = default_paths()
    trips = load_trips(spark, trips_path or tp)
    zones = load_zones(spark, zones_path or zp)

    print("Schema:")
    trips.printSchema()
    print("Row count:", trips.count())
    nulls = null_counts(trips)
    print(nulls.to_string(index=False))
    save_csv(nulls, "null_counts.csv")

    clean, report = clean_trips(trips)
    print("Cleaning report:", report)
    save_csv(pd.DataFrame(list(report.items()), columns=["step", "rows"]), "cleaning_report.csv")
    clean = add_time_features(clean).cache()

    res = {"report": report}
    res["hour"] = trips_by_hour(clean).toPandas()
    res["day"] = trips_by_day(clean)
    res["zones"] = top_pickup_zones(clean, zones).toPandas()
    res["fare_dist"] = avg_fare_distance_by_hour(clean).toPandas()
    res["payment"] = payment_distribution(clean).toPandas()
    res["tip_payment"] = tip_pct_by_payment(clean).toPandas()
    res["tip_hour"] = tip_pct_by_hour(clean).toPandas()

    for k, f in [("hour", "trips_by_hour.csv"), ("day", "trips_by_day.csv"), ("zones", "top_pickup_zones.csv"),
                 ("fare_dist", "avg_fare_distance_by_hour.csv"), ("payment", "payment_distribution.csv"),
                 ("tip_payment", "tip_pct_by_payment.csv"), ("tip_hour", "tip_pct_by_hour.csv")]:
        save_csv(res[k], f)
    make_charts(res)
    return res


if __name__ == "__main__":
    run_all()
