"""Download NYC TLC Yellow Taxi data (one month), zone lookup, a Gutenberg book, and build a sample."""
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
SAMPLE = ROOT / "data" / "sample"
MONTH = "2024-01"
TRIP_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{MONTH}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
BOOK_URLS = [
    "https://www.gutenberg.org/cache/epub/11/pg11.txt",
    "https://www.gutenberg.org/files/11/11-0.txt",
]


def fetch(url, dest):
    if dest.exists() and dest.stat().st_size > 0:
        print(f"exists: {dest.name}")
        return True
    try:
        with requests.get(url, stream=True, timeout=60, headers={"User-Agent": "bigdata-spark-analysis"}) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
        print(f"downloaded: {dest.name} ({dest.stat().st_size/1e6:.1f} MB)")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"failed {url}: {e}")
        dest.unlink(missing_ok=True)
        return False


def make_sample(n=1000):
    df = pd.read_parquet(RAW / f"yellow_tripdata_{MONTH}.parquet")
    # keep only January 2024 rows, then take a reproducible random sample
    df = df[(df.tpep_pickup_datetime >= f"{MONTH}-01") & (df.tpep_pickup_datetime < "2024-02-01")]
    df.sample(n=n, random_state=42).to_parquet(SAMPLE / "yellow_sample.parquet", index=False)
    zones = pd.read_csv(RAW / "taxi_zone_lookup.csv")
    zones.to_csv(SAMPLE / "taxi_zone_lookup.csv", index=False)
    print(f"sample written ({n} rows)")


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    SAMPLE.mkdir(parents=True, exist_ok=True)
    ok = fetch(TRIP_URL, RAW / f"yellow_tripdata_{MONTH}.parquet")
    ok &= fetch(ZONE_URL, RAW / "taxi_zone_lookup.csv")
    book = RAW / "alice.txt"
    if not any(fetch(u, book) for u in BOOK_URLS):
        ok = False
    if ok:
        make_sample()
    # a small book copy is public domain; keep it in sample so tests/demo work offline
    if book.exists():
        (SAMPLE / "alice.txt").write_bytes(book.read_bytes())
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
