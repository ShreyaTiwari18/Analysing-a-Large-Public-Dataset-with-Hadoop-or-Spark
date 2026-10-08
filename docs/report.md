# Report: Analysing NYC Yellow Taxi Trips with Spark

## 1. Problem statement
How does taxi demand in New York City vary by hour, weekday and location, how do fares and tips behave, and how can we process a multi-million-row public dataset with Big Data tools (Spark) while illustrating core Hadoop concepts?

## 2. The dataset through the 5 Vs
- **Volume:** 2,964,624 trips for a single month (50 MB compressed Parquet, several hundred MB in memory). The TLC archive spans years and billions of rows.
- **Velocity:** Trips are generated continuously, 24/7; TLC publishes monthly files. A production pipeline would ingest events via Kafka and Spark Structured Streaming.
- **Variety:** Structured tables (trips, zone lookup CSV). The MapReduce demo adds unstructured free text (a book).
- **Veracity:** 8.1% of rows are invalid (non-positive fares, zero distances, missing passenger counts, impossible timestamps). Some columns have ~4.7% nulls.
- **Value:** Insights for driver positioning, pricing, airport planning and tipping behaviour.

## 3. Method
**Loading:** Parquet read into a Spark DataFrame; schema, row count and null counts printed.

**Cleaning (applied in order, counts are removals at each step):**
| Rule | Rows removed |
|---|---|
| fare_amount <= 0 | 38,341 |
| trip_distance <= 0 | 56,569 |
| passenger_count <= 0 or null | 30,660 |
| dropoff earlier than pickup | 8 |
| **Total removed** | **240,827 (8.1%)** |
| **Rows remaining** | **2,723,797** |

**Analyses:** trips by hour; trips by weekday; top 10 pickup zones (join with the zone lookup); average fare and distance by hour; payment type distribution (codes mapped to names); average tip % by payment type and by hour (credit-card only).

**MapReduce demo:** word count on *Alice's Adventures in Wonderland* implemented with explicit map/shuffle/reduce phases in pure Python and with Spark's `flatMap -> map -> reduceByKey`. Both gave identical counts (2,575 distinct words, 27,427 words in total).

## 4. Results
- **Hour:** peak 18:00 with 195,918 trips; trough 04:00 with 12,783. Demand climbs from 06:00 and stays high until ~22:00.
- **Weekday:** Wed 458,785 > Tue 425,916 > Thu 397,445 > Sat 384,258 > Fri 378,569 > Mon 372,417 > Sun 306,407.
- **Zones:** JFK Airport 136,959; Upper East Side South 135,461; Midtown Center 134,967; Upper East Side North 128,098; Midtown East 101,251. LaGuardia is 9th (86,564).
- **Fare/distance:** 05:00 trips are the longest and costliest (avg $27.58, 6.21 miles), consistent with early airport runs; 18:00 is cheapest (avg $16.96, 2.87 miles).
- **Payments:** Credit card 83.47%, Cash 15.34%, Dispute 0.83%, No charge 0.36%.
- **Tips:** card trips average a 25.66% tip; cash is ~0% because cash tips are not recorded. By hour, card tips range from ~22.4% (06:00) to ~27.3% (18:00); the 03:00 value (49%) is driven by a small number of outliers.

## 5. Scaling on a real Hadoop/HDFS cluster
- **Blocks:** HDFS splits files into large blocks (128 MB by default). A multi-year taxi archive would be spread across many DataNodes, and Spark would create roughly one task per block/partition, moving compute to the data.
- **Replication:** each block is stored 3 times (default) on different nodes/racks, so losing a node does not lose data or stop jobs; the NameNode tracks block locations.
- **YARN:** the ResourceManager allocates containers on NodeManagers; our `local[*]` SparkSession would be submitted with `spark-submit --master yarn`, with executors scheduled near the data.
- **Storage format:** columnar Parquet allows column pruning and predicate pushdown, which reduces I/O at scale.
- **Ecosystem:** Hive/Spark SQL would expose the tables to analysts; Kafka + Structured Streaming would handle velocity; Oozie/Airflow would schedule the monthly job.
- The code in `src/analysis.py` uses only DataFrame APIs, so changing the input path to `hdfs://...` is essentially the only change.

## 6. Limitations and future work
- One month only; seasonal effects and trends are not visible. Extend to 12+ months.
- Running locally (single machine), so no real distribution or fault tolerance is demonstrated.
- Trips are not filtered on date range or extreme values (e.g. very long or very expensive trips); outliers remain and affect averages (e.g. 03:00 tip %).
- Tip analysis excludes cash and ignores tolls and surcharges.
- Future work: weather join, dropoff-zone flows, Spark MLlib fare prediction, streaming demo, running on a real cluster (e.g. EMR/Dataproc).

## 7. What I learned
### The 5 Vs

### Structured vs unstructured data

### HDFS

### MapReduce

### Spark and PySpark

### The Hadoop ecosystem

### Challenges and how I solved them
