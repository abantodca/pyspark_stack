"""wordcount para EMR Serverless — self-contained, sin master hardcodeado.

Args: 1) output_uri (opcional): s3a://.../analytics/wordcount ; si falta, solo imprime.
"""

# EMR Serverless 7.x corre PySpark con Python 3.9 por defecto (3.11 está instalado pero no es
# el intérprete salvo que fijes PYSPARK_PYTHON). El `str | None` de abajo es sintaxis 3.10+ y
# se evalúa al definir la función: sin este import el módulo revienta con TypeError ANTES de
# crear la SparkSession. Ojo, el ruff del CI usa target py312 y no lo detecta.
from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def main(output_uri: str | None) -> None:
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    lines = [
        "spark hadoop spark airflow",
        "hadoop hdfs spark etl",
        "airflow dag spark etl etl",
    ]
    counts = (
        spark.sparkContext.parallelize(lines)
        .flatMap(str.split)
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: kv[1], ascending=False)
    )
    rows = counts.collect()
    for word, count in rows:
        print(f"{word}\t{count}")
    if output_uri:
        spark.createDataFrame(rows, ["word", "count"]).write.mode("overwrite").parquet(
            output_uri
        )
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
