"""MapReduce word count: pure Python (explicit phases) and Spark RDD."""
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[1]
STOP_WORDS = set(
    "the a an and or but of to in on at for with as by is was were be been are it its i you he she they we "
    "me my his her their our your this that these those not no so if then there what which who whom said "
    "had has have do did does would could should will shall can may might from up out down about into over "
    "than too very just all one them him us am s t don re ll ve d m".split()
)


def tokenize(text):
    """Lower-case the text and split it into words."""
    return re.findall(r"[a-z']+", text.lower())


def strip_gutenberg(text):
    """Remove the Project Gutenberg header/footer if present."""
    start = re.search(r"\*\*\* ?START OF.*?\*\*\*", text, re.S)
    end = re.search(r"\*\*\* ?END OF", text)
    if start and end:
        return text[start.end():end.start()]
    return text


# ---------- Pure-Python MapReduce ----------
def map_phase(lines):
    """MAP: read each line and emit a (word, 1) pair for every word we see."""
    for line in lines:
        for word in tokenize(line):
            yield (word, 1)


def shuffle_phase(pairs):
    """SHUFFLE: group all pairs that share the same word, so each word gets a list of 1s."""
    groups = defaultdict(list)
    for word, one in pairs:
        groups[word].append(one)
    return groups


def reduce_phase(groups):
    """REDUCE: add up the list of 1s for each word to get its total count."""
    return {word: sum(ones) for word, ones in groups.items()}


def wordcount_python(lines):
    return reduce_phase(shuffle_phase(map_phase(lines)))


# ---------- Spark RDD ----------
def wordcount_spark(spark, lines):
    """Same job on Spark: flatMap -> map -> reduceByKey. Returns a dict."""
    rdd = spark.sparkContext.parallelize(list(lines))
    counts = (
        rdd.flatMap(tokenize)          # one record per word
        .map(lambda w: (w, 1))         # (word, 1)
        .reduceByKey(lambda a, b: a + b)  # shuffle + sum per word
    )
    return dict(counts.collect())


def top_n(counts, n=20):
    items = [(w, c) for w, c in counts.items() if w not in STOP_WORDS and len(w) > 1]
    return sorted(items, key=lambda x: (-x[1], x[0]))[:n]


def main():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from spark_session import get_spark

    book = ROOT / "data" / "raw" / "alice.txt"
    if not book.exists():
        book = ROOT / "data" / "sample" / "alice.txt"
    lines = strip_gutenberg(book.read_text(encoding="utf-8-sig")).splitlines()

    py = wordcount_python(lines)
    spark = get_spark()
    sp = wordcount_spark(spark, lines)
    spark.stop()
    assert py == sp, "implementations disagree"
    print(f"Both implementations agree: {len(py)} distinct words, {sum(py.values())} total")

    top = top_n(py, 20)
    df = pd.DataFrame(top, columns=["word", "count"])
    print(df.to_string(index=False))
    (ROOT / "outputs" / "results").mkdir(parents=True, exist_ok=True)
    (ROOT / "outputs" / "charts").mkdir(parents=True, exist_ok=True)
    df.to_csv(ROOT / "outputs" / "results" / "wordcount_top20.csv", index=False)

    fig, ax = plt.subplots(figsize=(9, 6))
    d = df.iloc[::-1]
    ax.barh(d["word"], d["count"], color="#3b6ea5")
    ax.set_title("Top 20 words in Alice's Adventures in Wonderland (stop-words removed)")
    ax.set_xlabel("Occurrences")
    ax.set_ylabel("Word")
    fig.tight_layout()
    fig.savefig(ROOT / "outputs" / "charts" / "wordcount_top20.png", dpi=120)


if __name__ == "__main__":
    main()
