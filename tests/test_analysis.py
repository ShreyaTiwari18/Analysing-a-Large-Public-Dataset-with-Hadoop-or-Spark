import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import analysis as an  # noqa: E402
import wordcount_mapreduce as wc  # noqa: E402
from spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", master="local[2]")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def sample(spark):
    df = an.load_trips(spark, str(ROOT / "data" / "sample" / "yellow_sample.parquet"))
    clean, _ = an.clean_trips(df)
    return an.add_time_features(clean).cache()


@pytest.fixture(scope="session")
def zones(spark):
    return an.load_zones(spark, str(ROOT / "data" / "sample" / "taxi_zone_lookup.csv"))


def test_cleaning_removes_invalid_rows(spark):
    t = datetime
    rows = [
        (10.0, 2.0, 1, t(2024, 1, 1, 8), t(2024, 1, 1, 8, 30)),   # valid
        (0.0, 2.0, 1, t(2024, 1, 1, 8), t(2024, 1, 1, 8, 30)),    # bad fare
        (10.0, 0.0, 1, t(2024, 1, 1, 8), t(2024, 1, 1, 8, 30)),   # bad distance
        (10.0, 2.0, 0, t(2024, 1, 1, 8), t(2024, 1, 1, 8, 30)),   # bad passengers
        (10.0, 2.0, 1, t(2024, 1, 1, 9), t(2024, 1, 1, 8)),       # dropoff before pickup
    ]
    df = spark.createDataFrame(
        rows, "fare_amount double, trip_distance double, passenger_count long, "
              "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp")
    clean, report = an.clean_trips(df)
    assert clean.count() == 1
    assert report["rows_before"] == 5 and report["rows_after"] == 1
    assert report["fare_amount <= 0 (or null)"] == 1
    assert report["dropoff earlier than pickup (or null times)"] == 1


def test_trips_by_hour(sample):
    pdf = an.trips_by_hour(sample).toPandas()
    assert list(pdf.columns) == ["pickup_hour", "trips"] and len(pdf) > 0


def test_trips_by_day(sample):
    pdf = an.trips_by_day(sample)
    assert list(pdf.columns) == ["pickup_day", "trips"] and len(pdf) > 0


def test_top_zones(sample, zones):
    pdf = an.top_pickup_zones(sample, zones).toPandas()
    assert list(pdf.columns) == ["PULocationID", "Zone", "Borough", "trips"]
    assert 0 < len(pdf) <= 10


def test_fare_distance(sample):
    pdf = an.avg_fare_distance_by_hour(sample).toPandas()
    assert list(pdf.columns) == ["pickup_hour", "avg_fare", "avg_distance"] and len(pdf) > 0


def test_payment(sample):
    pdf = an.payment_distribution(sample).toPandas()
    assert list(pdf.columns) == ["payment", "trips", "pct"] and len(pdf) > 0
    assert "Credit card" in set(pdf["payment"])


def test_tips(sample):
    assert len(an.tip_pct_by_payment(sample).toPandas()) > 0
    pdf = an.tip_pct_by_hour(sample).toPandas()
    assert list(pdf.columns) == ["pickup_hour", "avg_tip_pct"] and len(pdf) > 0


def test_wordcount_implementations_match(spark):
    lines = ["a b a c b a"]
    py = wc.wordcount_python(lines)
    sp = wc.wordcount_spark(spark, lines)
    assert py == sp == {"a": 3, "b": 2, "c": 1}
