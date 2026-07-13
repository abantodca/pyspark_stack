"""
Word count sobre HDFS — self-contained.

Disparado por el DAG `spark_wordcount_trigger_hdfs`:
    spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount_hdfs.py

Para no depender de que exista un archivo previo en HDFS, el script primero
escribe un texto de ejemplo en HDFS, luego lo lee y cuenta las palabras,
escribiendo el resultado de vuelta en HDFS.
"""
from pyspark.sql import SparkSession

HDFS = "hdfs://hdfs-namenode:9000"
INPUT_PATH = f"{HDFS}/wordcount/input"
OUTPUT_PATH = f"{HDFS}/wordcount/output"


def main():
    spark = SparkSession.builder \
        .appName("WordCountHDFS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    sc = spark.sparkContext

    # 1) Sembrar un texto de ejemplo en HDFS (overwrite en cada corrida)
    sample = [
        "spark hadoop spark airflow",
        "hadoop hdfs spark etl",
        "airflow dag spark etl etl",
    ]
    # Escribimos vía DataFrame para poder usar mode("overwrite")
    spark.createDataFrame([(line,) for line in sample], ["line"]) \
        .write.mode("overwrite").text(INPUT_PATH)

    # 2) Leer desde HDFS y contar
    counts = (
        sc.textFile(INPUT_PATH)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    print("[INFO] Word counts (desde HDFS):")
    for word, count in counts.sortBy(lambda kv: kv[1], ascending=False).collect():
        print(f"{word}\t{count}")

    # 3) Persistir el resultado en HDFS
    counts.map(lambda kv: f"{kv[0]}\t{kv[1]}") \
        .coalesce(1) \
        .saveAsTextFile(OUTPUT_PATH) if _path_absent(sc, OUTPUT_PATH) else None

    spark.stop()


def _path_absent(sc, path):
    """saveAsTextFile falla si el path existe; lo borramos antes vía Hadoop FS."""
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    p = hadoop.fs.Path(path)
    if fs.exists(p):
        fs.delete(p, True)
    return True


if __name__ == "__main__":
    main()
