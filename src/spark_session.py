"""Build a SparkSession with the Windows fixes (HADOOP_HOME, PYSPARK_PYTHON)."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def get_spark(app_name="bigdata-spark-analysis", master="local[*]"):
    """Return a local SparkSession; configures Hadoop/Python env on Windows."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    hadoop = ROOT / "hadoop"
    if os.name == "nt" and (hadoop / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["PATH"] = str(hadoop / "bin") + os.pathsep + os.environ["PATH"]
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
