"""
Word count de ejemplo — self-contained.

Disparado por el DAG `spark_wordcount_trigger`:
    spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount.py

No depende de ningún archivo de entrada: genera su propio texto en memoria,
por lo que la tarea de Airflow siempre tiene algo que ejecutar.
"""

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("WordCount")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    lines = [
        "spark hadoop spark airflow",
        "hadoop hdfs spark etl",
        "airflow dag spark etl etl",
    ]

    counts = (
        spark.sparkContext.parallelize(lines)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: kv[1], ascending=False)
    )

    print("[INFO] Word counts:")
    for word, count in counts.collect():
        print(f"{word}\t{count}")

    spark.stop()


if __name__ == "__main__":
    main()
