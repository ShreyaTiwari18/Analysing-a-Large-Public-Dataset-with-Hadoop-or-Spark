# Guide: How to Run and How to Explain This Project

Author: Shreya Tiwari

---

## Part 1: How to run it (about 10 minutes)

**You need:** Windows/Mac/Linux, Python 3.10 or newer, Java 17, and git.

**1. Get the code**
```
git clone https://github.com/ShreyaTiwari18/Analysing-a-Large-Public-Dataset-with-Hadoop-or-Spark.git
cd Analysing-a-Large-Public-Dataset-with-Hadoop-or-Spark
```

**2. Create the environment and install packages**
```
python -m venv .venv
.venv\Scripts\activate            (Mac/Linux: source .venv/bin/activate)
pip install -r requirements.txt
```

**3. Run the four steps, in order**
| Step | Command | What you get |
|---|---|---|
| Download data | `python scripts/download_data.py` | ~50 MB of taxi trips, in `data/raw/` |
| Word-count demo | `python src/wordcount_mapreduce.py` | top 20 words + chart |
| Main analysis | `python src/analysis.py` | 7 CSVs + 7 charts in `outputs/` |
| Tests | `python -m pytest -q` | should say `8 passed` |

Optional: open `notebooks/analysis.ipynb` in VS Code or Jupyter to see the whole story with charts.

**If something goes wrong**
- A message about `winutils.exe` on Windows is only a warning. Ignore it.
- `java not found`: install Java 17 and reopen the terminal.
- Analysis runs slowly the first time: Spark is starting up, wait about a minute.
- No internet or data not downloaded: the code uses the small sample in `data/sample/`.

---

## Part 2: How to explain it simply

### The 30-second pitch
> "Every taxi ride in New York is recorded. One month alone is about 3 million rides, too big to analyse comfortably in Excel. I used **Spark**, a Big Data tool, to clean that data and answer questions like: when is it busiest, where do people get picked up, and how much do they tip? I also built a small demo of **MapReduce**, the idea behind Hadoop."

### The story in 6 steps (use these as slides)

1. **The problem.** Lots of data, too big for normal tools. Big Data tools split the work across many workers.
2. **The data.** NYC Yellow Taxi trips, January 2024: 2,964,624 rides, each with pickup time, zone, distance, fare, tip and payment type. It is public and free.
3. **Cleaning ("Veracity").** Real data is messy. About 8% of rows were wrong: zero or negative fares, zero distance, no passengers, dropoff before pickup. I removed 240,827 rows and kept 2,723,797 good ones.
4. **Analysis.** I asked six questions, and Spark answered each by grouping and counting:
   - Which hours are busiest? **6 pm** (195,918 trips). Quietest is 4 am.
   - Which day? **Wednesday** is busiest, **Sunday** quietest.
   - Where do people get picked up? **JFK Airport** first (136,959), then Upper East Side and Midtown.
   - How do fare and distance change by hour? The longest, costliest trips start around **5 am** (airport runs, about $27.58).
   - How do people pay? **83.5% card**, 15.3% cash.
   - How much do they tip? Card riders tip about **25.7%** of the fare. Cash tips are not recorded.
5. **MapReduce demo.** I counted words in *Alice in Wonderland* twice: once in plain Python and once in Spark. Both gave the same answer. (Explained below.)
6. **Quality.** 8 automatic tests check that cleaning and each calculation work, and that both word counts match.

### Explaining MapReduce with a classroom example
Imagine counting votes with 3 helpers:
- **Map:** each helper reads a pile of ballots and writes "Alice, 1" for each vote.
- **Shuffle:** all the "Alice" slips go to one table, all the "Bob" slips to another.
- **Reduce:** one person per table adds up the slips and announces the total.

That is exactly `map_phase`, `shuffle_phase` and `reduce_phase` in `src/wordcount_mapreduce.py`. Spark does the same with `flatMap -> map -> reduceByKey`, but spreads the work over many cores or machines.

### Connecting it to the course topics (a likely exam or viva question)
| Concept | One-line answer | In this project |
|---|---|---|
| **Volume** | Lots of data | 3 million rides |
| **Velocity** | Data arrives fast | Rides are generated every second; monthly files here |
| **Variety** | Different kinds of data | Tables (structured) and book text (unstructured) |
| **Veracity** | Data may be wrong | The 8% of bad rows I cleaned |
| **Value** | Useful insight | Busy hours, hotspots, tipping |
| **HDFS** | Files split into blocks, copied 3 times across machines | Not run here, but explained in `docs/report.md` |
| **MapReduce** | Map, shuffle, reduce | The word-count demo |
| **Hadoop ecosystem** | Tools that work together | Spark (processing), Parquet (storage format), YARN (job manager) |

### Honest answers to questions you may get
- **"Is this really Big Data? It ran on a laptop."** It is a mini version. 3 million rows is on the small side, but the code is the same code that would run on a real cluster, where you only change the file path to HDFS.
- **"Why Spark and not Excel or pandas?"** Excel fails at this size, and pandas keeps everything in one machine's memory. Spark scales out.
- **"Why is the 3 am tip 49%?"** A few trips with unusually large tips distort the average at an hour with few rides. I note it as a limitation.
- **"What would you do next?"** Use more months, add weather, predict fares with Spark MLlib, or try a real cluster.

### Tips for the demo
- Show the charts first (`outputs/charts/`), then explain how you got them.
- Run `python -m pytest -q` live: "8 passed" looks good and takes about a minute.
- Keep Part 2 to about 5 minutes; spend the most time on the cleaning step and MapReduce, because that is where the Big Data ideas are.
- Fill in the "What I learned" section of `docs/report.md` in your own words before presenting.
