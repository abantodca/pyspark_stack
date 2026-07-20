# Ejemplos locales paso a paso — de lo más simple a lo avanzado

> 20 ejemplos para **copiar código a código** y entender el stack uno a uno: 5 básicos, 5 intermedios,
> 5 avanzados y 5 recontra ultra avanzados. Cada uno dice **dónde** correrlo, el **código** listo para
> pegar, **qué observar** y el **por qué** (con las trampas propias de este montaje). Datos reales del repo:
> `landing/customer_etl/` (`customers.csv`, `orders.csv`, `products.json`).
>
> Requisito: el stack arriba (`docker compose up -d`). UIs: Airflow `:8082`, Jupyter `:8888`,
> Spark `:8081`, HDFS `:9870`.

**Esquema de los datos** (lo usaremos en casi todos):

| Fuente | Columnas |
|--------|----------|
| `customers.csv` | `customer_id, customer_name, city, state, signup_date` |
| `orders.csv` | `order_id, customer_id, product_id, quantity, order_date` |
| `products.json` | `product_id, category, unit_price` — **array multilínea** (ver Ej. 7) |

**Dónde se ejecuta cada cosa:**
- **Jupyter** (`:8888`) → explorar interactivo. El driver ya apunta al cluster.
- **`docker exec spark-master spark-submit …`** → correr un `.py` en el cluster como en producción.
- **`docker exec hdfs-namenode hdfs dfs …`** → operar el sistema de archivos HDFS.

> Regla de oro del stack: **todo `spark-submit` lleva** `--conf spark.pyspark.python=python3.12
> --conf spark.pyspark.driver.python=python3.12`. Sin eso, en cuanto un executor deserializa una
> lambda, PySpark aborta con `[PYTHON_VERSION_MISMATCH]`.

---

# PASO 0 — Levantar, verificar, apagar y reanudar

Todo esto se hace desde la raíz del repo (`pyspark_stack/`).

### 0.1 Primera vez: construir y levantar

```bash
cp .env.example .env            # secretos locales + COMPOSE_PROFILES=dev (incluye Jupyter)
docker compose up -d --build    # construye imágenes y arranca todo en segundo plano (~varios min la 1ª vez)
```

### 0.2 Verificar que está sano (antes de tocar nada)

```bash
docker compose ps               # todos 'running'; airflow-init en 'exited (0)'; airflow-db 'healthy'
```

Chequeo rápido de cada pieza:

```bash
# Spark: el worker registrado con el master
docker exec spark-master curl -s localhost:8080 | grep -o 'Alive Workers.*[0-9]' | head -1
# HDFS: responde y lista la raíz
docker exec hdfs-namenode hdfs dfs -ls /
# Airflow: los 3 DAGs cargan sin error de import
docker exec airflow-scheduler airflow dags list
```

Y en el navegador: Airflow `:8082` (`admin`/`admin`), Jupyter `:8888`, Spark `:8081`, HDFS `:9870`.

> Si algún contenedor queda en `Created` con `address already in use` aunque el puerto parezca libre:
> son `docker-proxy` huérfanos. `pgrep -a docker-proxy` → `sudo kill -9 <PIDs>` → `docker compose up -d`
> ([ANALISIS_FALLOS §8](../ANALISIS_FALLOS.md)).

### 0.3 Preparar el área de ejemplos (una sola vez)

```bash
mkdir -p spark-apps/ejemplos/out        # aquí irán los .py y salidas de esta guía
```

Como `./spark-apps` está montado en los contenedores, la carpeta aparece dentro al instante.

### 0.4 Uso diario: apagar y reanudar SIN perder nada

```bash
docker compose stop             # apaga los contenedores, conserva datos y metadata (HDFS, Airflow)
docker compose start            # los vuelve a levantar tal cual estaban
# equivalente en un comando:
docker compose restart
```

### 0.5 Bajar el stack

```bash
docker compose down             # borra los contenedores PERO conserva los volúmenes (HDFS + Postgres)
docker compose up -d            # vuelve a levantar con los datos intactos
```

### 0.6 Reset total (empezar de cero)

```bash
docker compose down -v          # ⚠️ borra TAMBIÉN los volúmenes → se pierde HDFS y la BD de Airflow
docker compose build --no-cache
docker compose up -d
```

> Regla: `stop/start` para el día a día · `down` para liberar contenedores conservando datos ·
> `down -v` **solo** para empezar limpio (o tras cambiar un `Dockerfile`/versión). Detalle en
> [README](../README.md) y [ANALISIS_FALLOS §5.6](../ANALISIS_FALLOS.md).

### 0.7 Empezar la "carrera"

Con el stack verificado (0.2), arranca por el **Ejemplo 1** en Jupyter (`:8888`) y avanza en orden:
básicos (1–5) → intermedios (6–10) → avanzados (11–15). Cada ejemplo se apoya en el anterior; no
saltes el 6 (HDFS) ni el 7 (multiline JSON), que desbloquean los avanzados.

---

# NIVEL BÁSICO

## Ejemplo 1 — Primer `SparkSession` y un DataFrame en memoria

**Dónde:** Jupyter → nuevo notebook.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("ej01-hola-spark")
         .master("spark://spark-master:7077")
         .getOrCreate())

df = spark.createDataFrame(
    [("Alice", 30), ("Bob", 25), ("Cathy", 28)],
    ["name", "age"],
)
df.show()
df.printSchema()
print("filas:", df.count())
```

**Qué observar:** la tabla impresa, el schema inferido (`name: string`, `age: long`) y que en la UI
del master (`:8081`) aparece la app `ej01-hola-spark` como *running*.

**Por qué:** confirmas que el notebook-driver **se conecta al cluster real** (no a un Spark local).
`.getOrCreate()` reutiliza la sesión si ya existe: no crees varias en el mismo notebook.

---

## Ejemplo 2 — Leer un CSV y explorarlo

**Dónde:** Jupyter. Los `spark-apps` están montados en `/opt/spark-apps` dentro del contenedor.

```python
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv("file:///opt/spark-apps/landing/customer_etl/customers.csv"))

df.show(truncate=False)
df.printSchema()
df.select("customer_name", "state").show()
df.filter(df.state == "WA").show()
```

**Qué observar:** el prefijo **`file://`** (lee del FS local del contenedor, no de HDFS) y cómo
`inferSchema` detecta tipos leyendo los datos una vez de más.

**Por qué:** `header=True` usa la 1ª fila como nombres de columna; sin `inferSchema` todo sería
`string`. En jobs de producción se prefiere **schema explícito** (Ej. 15) para no pagar el escaneo
extra ni arriesgar inferencias erróneas.

---

## Ejemplo 3 — Tu primer `spark-submit` al cluster

**Dónde:** terminal del host. Crea el script y súbelo como se hace en prod.

```python
# spark-apps/ejemplos/ej03_resumen.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ej03-resumen").getOrCreate()

df = (spark.read.option("header", True).option("inferSchema", True)
      .csv("file:///opt/spark-apps/landing/customer_etl/orders.csv"))

print("[INFO] total de órdenes:", df.count())
df.groupBy("customer_id").agg(F.sum("quantity").alias("unidades")).show()

spark.stop()
```

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3.12 \
  --conf spark.pyspark.driver.python=python3.12 \
  /opt/spark-apps/ejemplos/ej03_resumen.py
```

**Qué observar:** el `.py` que creaste en el host aparece **al instante** dentro del contenedor (bind-mount),
sin reconstruir imagen. La salida sale entre los logs de Spark.

**Por qué:** este es el patrón real de ejecución (el mismo que usan los DAGs). Nota que aquí el
`SparkSession` **no fija `.master()`**: lo pasa `spark-submit` con `--master`. Es lo correcto para que
el job sea agnóstico del entorno (en prod lo fija EMR).

---

## Ejemplo 4 — Transformaciones básicas: `select`, `withColumn`, `filter`, `groupBy`

**Dónde:** Jupyter.

```python
from pyspark.sql import functions as F

orders = (spark.read.option("header", True).option("inferSchema", True)
          .csv("file:///opt/spark-apps/landing/customer_etl/orders.csv"))

(orders
 .withColumn("order_month", F.substring("order_date", 1, 7))   # 'YYYY-MM'
 .filter(F.col("quantity") >= 2)
 .groupBy("order_month")
 .agg(F.count("*").alias("n_ordenes"),
      F.sum("quantity").alias("unidades"))
 .orderBy("order_month")
 .show())
```

**Qué observar:** el encadenado *lazy* — nada se computa hasta `.show()` (una *acción*).

**Por qué:** `withColumn`/`filter`/`groupBy` son *transformaciones* (construyen el plan);
`show`/`count`/`collect`/`write` son *acciones* (lo disparan). Entender esta frontera es la base para
optimizar (Ej. 14).

---

## Ejemplo 5 — Escribir resultados y volver a leerlos (CSV y Parquet)

**Dónde:** Jupyter.

```python
resumen = (orders.groupBy("customer_id")
           .agg(F.sum("quantity").alias("unidades")))

# CSV (legible, sin tipos)
(resumen.write.mode("overwrite").option("header", True)
 .csv("file:///opt/spark-apps/ejemplos/out/resumen_csv"))

# Parquet (columnar, con tipos y compresión) — formato por defecto en data lakes
resumen.write.mode("overwrite").parquet("file:///opt/spark-apps/ejemplos/out/resumen_parquet")

# Releer parquet: el schema viaja con el archivo
spark.read.parquet("file:///opt/spark-apps/ejemplos/out/resumen_parquet").show()
```

**Qué observar:** Spark escribe **directorios con `part-*`**, no un solo archivo (un part por
partición). El parquet conserva que `unidades` es numérico; el CSV no.

**Por qué:** por eso el pipeline `customer_etl` hace `getmerge` para consolidar los `part-*` en un CSV
único (Ej. 10). `mode("overwrite")` deja re-ejecutar sin borrar a mano.

---

# NIVEL INTERMEDIO

## Ejemplo 6 — HDFS: subir datos y leerlos con `hdfs://`

**Dónde:** terminal del host (`hdfs dfs`) + un submit que lee de HDFS.

```bash
# La raíz de HDFS es de root:supergroup → operamos como root para poder crear rutas
docker exec -e HADOOP_USER_NAME=root hdfs-namenode hdfs dfs -mkdir -p /ejemplos/input
docker exec -e HADOOP_USER_NAME=root hdfs-namenode \
  hdfs dfs -put -f /opt/spark-apps/landing/customer_etl/orders.csv /ejemplos/input/
docker exec hdfs-namenode hdfs dfs -ls /ejemplos/input
```

```python
# spark-apps/ejemplos/ej06_hdfs.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ej06-hdfs").getOrCreate()

df = (spark.read.option("header", True)
      .csv("hdfs://hdfs-namenode:9000/ejemplos/input/orders.csv"))
print("[INFO] filas desde HDFS:", df.count())
spark.stop()
```
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12 \
  /opt/spark-apps/ejemplos/ej06_hdfs.py
```

**Qué observar:** la ruta HDFS lleva **FQDN completo** `hdfs://hdfs-namenode:9000/...`, y en la UI de
HDFS (`:9870` → *Utilities → Browse*) aparece `/ejemplos/input/orders.csv`.

**Por qué:** `HADOOP_USER_NAME=root` evita el clásico `Permission denied: user=airflow ... inode="/"`.
Es exactamente lo que hace `customer_etl_job_airflow.sh` antes de subir la landing.

---

## Ejemplo 7 — Joins de las 3 fuentes (+ el truco del JSON multilínea)

**Dónde:** Jupyter.

```python
from pyspark.sql import functions as F
base = "file:///opt/spark-apps/landing/customer_etl"

orders    = spark.read.option("header", True).option("inferSchema", True).csv(f"{base}/orders.csv")
customers = spark.read.option("header", True).option("inferSchema", True).csv(f"{base}/customers.csv")
# ⚠️ products.json es un ARRAY pretty-printed → SIN multiline saldría _corrupt_record
products  = spark.read.option("multiline", "true").json(f"{base}/products.json")

enriquecido = (orders
    .join(products, "product_id")
    .join(customers, "customer_id")
    .withColumn("total", F.col("quantity") * F.col("unit_price"))
    .select("order_id", "customer_name", "category", "quantity", "unit_price", "total"))

enriquecido.show(truncate=False)
```

**Qué observar:** prueba a quitar `.option("multiline","true")` y leer `products.json`: verás una
columna `_corrupt_record` y el join fallará con `cannot resolve 'product_id'`.

**Por qué:** `spark.read.json` asume **JSON Lines** (un objeto por línea). Un array con saltos de línea
necesita `multiline=true`. Es el bug crítico #1 del histórico ([ANALISIS_FALLOS §2.1](../ANALISIS_FALLOS.md)).

---

## Ejemplo 8 — Spark SQL con vistas temporales

**Dónde:** Jupyter. Mismo resultado que el Ej. 7, pero con SQL (como el job real).

```python
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")

spark.sql("""
    SELECT c.customer_name,
           COUNT(o.order_id)                    AS n_ordenes,
           SUM(o.quantity * p.unit_price)       AS total_gastado
    FROM orders o
    JOIN products  p ON o.product_id  = p.product_id
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_name
    ORDER BY total_gastado DESC
""").show()
```

**Qué observar:** DataFrame API y SQL producen el **mismo plan** (compruébalo con `.explain()`).
Elige el estilo que más claro quede; se pueden mezclar.

**Por qué:** `customer_etl_job.py` está escrito así (encadena `CREATE OR REPLACE TEMP VIEW`). SQL suele
leerse mejor para joins/agregaciones con lógica de negocio.

---

## Ejemplo 9 — Funciones de ventana (ranking y acumulado)

**Dónde:** Jupyter.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

lineas = (orders.join(products, "product_id")
          .withColumn("total", F.col("quantity") * F.col("unit_price")))

w_rank = Window.partitionBy("customer_id").orderBy(F.col("total").desc())
w_acum = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, 0)

(lineas
 .withColumn("rank_gasto", F.row_number().over(w_rank))
 .withColumn("gasto_acumulado", F.sum("total").over(w_acum))
 .select("customer_id", "order_date", "total", "rank_gasto", "gasto_acumulado")
 .orderBy("customer_id", "order_date")
 .show())
```

**Qué observar:** `row_number` reinicia por cliente (`partitionBy`); el acumulado crece dentro de cada
cliente en orden de fecha (`rowsBetween`).

**Por qué:** las *window functions* dan ranking / running totals / lag-lead sin colapsar filas (a
diferencia de `groupBy`). Son la herramienta clave para métricas por entidad.

---

## Ejemplo 10 — Job parametrizado + salida particionada + `getmerge`

**Dónde:** script + submit con argumentos + consolidación (el patrón `customer_etl`).

```python
# spark-apps/ejemplos/ej10_param.py
import sys
from pyspark.sql import SparkSession, functions as F

hdfs_in, hdfs_out = sys.argv[1], sys.argv[2]      # rutas por argumento, nada hardcodeado
spark = SparkSession.builder.appName("ej10-param").getOrCreate()

df = spark.read.option("header", True).csv(f"hdfs://hdfs-namenode:9000{hdfs_in}/orders.csv")
(df.groupBy("customer_id").agg(F.sum("quantity").alias("unidades"))
   .write.mode("overwrite").option("header", True).csv(f"hdfs://hdfs-namenode:9000{hdfs_out}"))
spark.stop()
```
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12 \
  /opt/spark-apps/ejemplos/ej10_param.py /ejemplos/input /ejemplos/output/ej10

# consolidar los part-* en un único CSV local:
docker exec -e HADOOP_USER_NAME=root hdfs-namenode \
  hdfs dfs -getmerge /ejemplos/output/ej10/part-* /opt/spark-apps/ejemplos/out/ej10.csv
docker exec hdfs-namenode cat /opt/spark-apps/ejemplos/out/ej10.csv
```

**Qué observar:** las rutas llegan por `sys.argv`, así que el mismo `.py` sirve para dev y prod
cambiando solo los argumentos.

**Por qué:** `getmerge` une los `part-*` de HDFS en un archivo. Es justo el último paso del pipeline
real para dejar `shared_output/customer_etl/loyalty_snapshot_<fecha>.csv`.

---

# NIVEL AVANZADO

## Ejemplo 11 — Job completo estilo `customer_etl` (loyalty scoring)

**Dónde:** script parametrizado que lee 3 fuentes de HDFS y escribe el snapshot. Es una versión
mínima y comentada del job de producción.

```python
# spark-apps/ejemplos/ej11_loyalty.py
import sys
from pyspark.sql import SparkSession

def main(hdfs_input, hdfs_output):
    spark = SparkSession.builder.appName("ej11-loyalty").getOrCreate()
    base = f"hdfs://hdfs-namenode:9000{hdfs_input}"

    spark.read.option("header", True).csv(f"{base}/orders.csv").createOrReplaceTempView("orders")
    spark.read.option("header", True).csv(f"{base}/customers.csv").createOrReplaceTempView("customers")
    spark.read.option("multiline", "true").json(f"{base}/products.json").createOrReplaceTempView("products")

    loyalty = spark.sql("""
        WITH enriched AS (
            SELECT o.customer_id, o.order_id, o.order_date, p.category,
                   o.quantity * p.unit_price AS total_price
            FROM orders o JOIN products p ON o.product_id = p.product_id
        ),
        metrics AS (
            SELECT customer_id,
                   COUNT(order_id)              AS total_orders,
                   SUM(total_price)             AS total_spent,
                   COUNT(DISTINCT order_date)   AS days_active,
                   COUNT(DISTINCT category)     AS categories_bought
            FROM enriched GROUP BY customer_id
        )
        SELECT c.customer_id, c.customer_name, c.city, c.state,
               m.total_orders, m.total_spent, m.days_active, m.categories_bought,
               CASE
                 WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2 THEN 'Premium'
                 WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2)  THEN 'Engaged'
                 ELSE 'Casual'
               END AS loyalty_status
        FROM metrics m JOIN customers c ON m.customer_id = c.customer_id
    """)

    loyalty.write.mode("overwrite").option("header", True).csv(f"hdfs://hdfs-namenode:9000{hdfs_output}")
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])   # <hdfs_input> <hdfs_output>
```
```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12 \
  /opt/spark-apps/ejemplos/ej11_loyalty.py /ejemplos/input /ejemplos/output/loyalty
docker exec hdfs-namenode hdfs dfs -cat /ejemplos/output/loyalty/part-* | head
```
> Antes: sube también `customers.csv` y `products.json` a `/ejemplos/input/` como en el Ej. 6.

**Qué observar:** el CTE (`WITH ... AS`) hace el mismo pipeline que las 3 vistas encadenadas del job
real, más legible. El resultado es una fila por cliente con su `loyalty_status`.

**Por qué:** este es el "producto" del stack. A partir de aquí solo falta **orquestarlo** (Ej. 12) y
parametrizar por entorno (Ej. 13).

---

## Ejemplo 12 — Orquestar tu job con un DAG de Airflow 3

**Dónde:** `dags/ej12_loyalty_dag.py` (Airflow solo carga DAGs de `./dags`).

```python
# dags/ej12_loyalty_dag.py
from datetime import datetime
# Airflow 3: DAG en el Task SDK; BashOperator en el provider 'standard'
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="ej12_loyalty",
    start_date=datetime(2025, 1, 1),
    schedule=None,             # Airflow 3: 'schedule', no 'schedule_interval'
    catchup=False,
) as dag:
    BashOperator(
        task_id="run_loyalty",
        bash_command=(
            "export HADOOP_USER_NAME=root && "
            "hdfs dfs -mkdir -p /ejemplos/input && "
            "hdfs dfs -put -f /opt/spark-apps/landing/customer_etl/*.csv  /ejemplos/input/ && "
            "hdfs dfs -put -f /opt/spark-apps/landing/customer_etl/*.json /ejemplos/input/ && "
            "spark-submit --master spark://spark-master:7077 "
            "  --conf spark.pyspark.python=python3.12 "
            "  --conf spark.pyspark.driver.python=python3.12 "
            "  /opt/spark-apps/ejemplos/ej11_loyalty.py /ejemplos/input /ejemplos/output/loyalty"
        ),
    )
```
```bash
docker exec airflow-dag-processor airflow dags reserialize      # forzar re-parseo tras crear el DAG
docker exec airflow-scheduler airflow dags unpause ej12_loyalty # arranca pausado
docker exec airflow-scheduler airflow dags trigger  ej12_loyalty
```

**Qué observar:** el DAG aparece en la UI (`:8082`). Imports de **Airflow 3**: `airflow.sdk` +
provider `standard`, y `schedule=` (no `schedule_interval=`).

**Por qué:** un `BashOperator` que hace `hdfs put` + `spark-submit` es exactamente el patrón de
`customer_etl_dag`. En prod ese Bash se cambia por un operador de EMR Serverless.

> ⚠️ Añade `set -euo pipefail` si mueves esto a un `.sh`: el shell del pipeline heredado **no** lo
> tiene y un fallo intermedio deja el DAG en `success` engañoso ([ANALISIS_FALLOS §6.2](../ANALISIS_FALLOS.md)).

---

## Ejemplo 13 — Parametrizar por entorno con Airflow `Variable` (dev/prod)

**Dónde:** DAG + Variables de Airflow (el equivalente a `env.sh`).

```bash
# definir la variable de entorno lógico:
docker exec airflow-scheduler airflow variables set loyalty_env dev
```
```python
# fragmento para dags/ej12_loyalty_dag.py
from airflow.sdk import Variable

env = Variable.get("loyalty_env", default="dev")     # Airflow 3: 'default', no 'default_var'
prefix = "/prod" if env == "prod" else ""            # dev → raíz; prod → /prod/...
hdfs_in  = f"{prefix}/ejemplos/input"
hdfs_out = f"{prefix}/ejemplos/output/loyalty"
# ...usa hdfs_in / hdfs_out en el bash_command con un f-string
```

**Qué observar:** cambiar la Variable a `prod` (`airflow variables set loyalty_env prod`) reencamina
las rutas HDFS sin tocar código.

**Por qué:** es el patrón `env.sh` (dev/prod paths) llevado a Airflow. Ojo con la ruptura de Airflow
3: `Variable.get(..., default=...)` (con `default_var=` truena con `TypeError`).

---

## Ejemplo 14 — Rendimiento: `explain`, `cache`, `broadcast`, particiones

**Dónde:** Jupyter.

```python
from pyspark.sql import functions as F

lineas = orders.join(products, "product_id")

# 1) Leer el plan físico: ¿SortMergeJoin o BroadcastHashJoin?
lineas.explain()

# 2) products es diminuto → forzar broadcast join (evita el shuffle de la tabla grande)
rapido = orders.join(F.broadcast(products), "product_id")
rapido.explain()

# 3) Reutilizas un DF caro varias veces → cachéalo (acción para materializar)
metrics = (rapido.withColumn("total", F.col("quantity") * F.col("unit_price"))
                 .groupBy("customer_id").agg(F.sum("total").alias("total")))
metrics.cache(); metrics.count()

# 4) Particiones: demasiadas = overhead; para salida pequeña, consolidar
print("particiones antes:", metrics.rdd.getNumPartitions())
metrics.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///opt/spark-apps/ejemplos/out/ej14")
```

**Qué observar:** en el `explain` del paso 2 el join pasa a `BroadcastHashJoin` (sin *Exchange* de la
tabla grande). Con `coalesce(1)` la salida es un solo `part-`.

**Por qué:** `broadcast` de la tabla chica es la optimización más rentable en un join dimensional.
`cache` evita recomputar un DF reutilizado. `coalesce` reduce particiones sin shuffle (a diferencia de
`repartition`). En este cluster de 1 worker el efecto es pequeño, pero el hábito escala a prod.

---

## Ejemplo 15 — Schema explícito, modos de lectura y Parquet particionado

**Dónde:** Jupyter. Producción-grade: no dependas de `inferSchema` ni escondas datos corruptos.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F

schema = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", StringType(),  False),
    StructField("product_id",  StringType(),  False),
    StructField("quantity",    IntegerType(), True),
    StructField("order_date",  DateType(),    True),
])

# mode=FAILFAST → aborta si una fila no encaja (en vez de tragarse el error como PERMISSIVE)
orders = (spark.read.option("header", True).option("mode", "FAILFAST")
          .schema(schema)
          .csv("file:///opt/spark-apps/landing/customer_etl/orders.csv"))
orders.printSchema()

# Escribir Parquet particionado por mes → lecturas futuras hacen partition pruning
(orders.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
       .write.mode("overwrite").partitionBy("order_month")
       .parquet("file:///opt/spark-apps/ejemplos/out/orders_parquet"))

# Al filtrar por la columna de partición, Spark solo lee ese subdirectorio:
(spark.read.parquet("file:///opt/spark-apps/ejemplos/out/orders_parquet")
      .filter(F.col("order_month") == "2025-05")
      .explain())     # busca 'PartitionFilters' en el plan
```

**Qué observar:** el directorio de salida se organiza en `order_month=2025-05/…`; el `explain` del
filtro muestra `PartitionFilters`, señal de que Spark **no lee** las demás particiones.

**Por qué:** schema explícito = lecturas más rápidas y errores tempranos; `FAILFAST` evita que datos
corruptos pasen en silencio (lo contrario del `_corrupt_record` del Ej. 7); `partitionBy` + parquet es
el layout estándar de un data lake y la base del *partition pruning* que abarata las consultas en S3
(producción).

---

# NIVEL RECONTRA ULTRA AVANZADO

> Patrones de ingeniería de datos "de verdad": streaming, cargas incrementales idempotentes,
> UDFs vectorizadas, tuning con AQE y testing automatizado. Todo corre en este mismo stack local.

## Ejemplo 16 — Structured Streaming sobre una carpeta de HDFS

**Dónde:** Jupyter (para poder parar el query con `q.stop()`), + otra terminal para "gotear" archivos.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# La fuente de archivos en streaming EXIGE schema explícito (no puede inferir sobre datos futuros)
schema = StructType([
    StructField("order_id", StringType()),   StructField("customer_id", StringType()),
    StructField("product_id", StringType()),  StructField("quantity", IntegerType()),
    StructField("order_date", StringType()),
])

stream = (spark.readStream.option("header", True).schema(schema)
          .csv("hdfs://hdfs-namenode:9000/ejemplos/stream_in"))     # vigila ESTA carpeta

agg = stream.groupBy("customer_id").agg(F.sum("quantity").alias("unidades"))

q = (agg.writeStream
     .outputMode("complete")                                        # re-emite el agregado completo
     .format("console")
     .option("checkpointLocation", "hdfs://hdfs-namenode:9000/ejemplos/chk/ej16")
     .start())
# ...deja esta celda corriendo; ejecuta el 'put' de abajo en otra terminal; luego q.stop()
```
```bash
# preparar carpeta y disparar micro-batches soltando archivos uno a uno:
docker exec -e HADOOP_USER_NAME=root hdfs-namenode hdfs dfs -mkdir -p /ejemplos/stream_in
docker exec -e HADOOP_USER_NAME=root hdfs-namenode \
  hdfs dfs -put -f /opt/spark-apps/landing/customer_etl/orders.csv /ejemplos/stream_in/lote1.csv
# vuelve a soltar otro archivo (lote2.csv, lote3.csv...) y mira cómo el agregado se actualiza solo
```

**Qué observar:** cada archivo nuevo en `stream_in/` dispara un *micro-batch* y la consola reimprime
el agregado. El `checkpointLocation` recuerda qué archivos ya procesó → si reinicias, **no** los
reprocesa.

**Por qué:** es el mismo motor DataFrame pero incremental. El checkpoint da *exactly-once* sobre la
fuente de archivos; `outputMode("complete")` es válido porque hay agregación. Para producción, la
fuente sería Kafka/Kinesis y el sink S3/Delta, pero la mecánica es idéntica.

---

## Ejemplo 17 — Carga incremental idempotente (dynamic partition overwrite + dedup)

**Dónde:** Jupyter. El problema real de todo lake: reprocesar un lote sin duplicar ni pisar lo demás.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# clave: solo sobreescribe las particiones presentes en el lote, no toda la tabla
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

TARGET = "file:///opt/spark-apps/ejemplos/out/orders_lake"

def upsert(nuevo_lote):
    # 1) quedarse con el registro MÁS RECIENTE por clave (idempotencia ante reenvíos)
    w = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())
    dedup = (nuevo_lote.withColumn("_rn", F.row_number().over(w))
                       .filter("_rn = 1").drop("_rn")
                       .withColumn("order_month", F.substring("order_date", 1, 7)))
    # 2) overwrite DINÁMICO: reemplaza solo las particiones (meses) que trae el lote
    (dedup.write.mode("overwrite").partitionBy("order_month").parquet(TARGET))

orders = (spark.read.option("header", True)
          .csv("file:///opt/spark-apps/landing/customer_etl/orders.csv"))
upsert(orders)
upsert(orders)   # correrlo 2 veces NO duplica: la partición se reemplaza, no se acumula

spark.read.parquet(TARGET).groupBy("order_month").count().show()
```

**Qué observar:** ejecutar `upsert` dos veces deja el mismo conteo (idempotente). Con
`partitionOverwriteMode=dynamic`, los meses **no** presentes en el lote quedan intactos; sin él,
`overwrite` borraría toda la tabla.

**Por qué:** es el patrón de *backfill* seguro. La ventana `row_number` colapsa reenvíos a la última
versión por clave (base de un SCD-1 / upsert sin motor transaccional). En prod con Delta/Iceberg esto
se hace con `MERGE INTO`, pero el principio —reprocesar sin efectos colaterales— es el mismo.

---

## Ejemplo 18 — Pandas UDF vectorizada (Arrow)

**Dónde:** Jupyter. Lógica Python que no existe en SQL, pero **rápida** (por lotes, no fila a fila).

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf, col

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

@pandas_udf("double")                      # recibe/devuelve pandas.Series → vectorizado con Arrow
def precio_con_descuento(precio: pd.Series, qty: pd.Series) -> pd.Series:
    base = precio * qty
    return base.where(qty < 3, base * 0.90)   # 10% de dto a partir de 3 unidades

lineas = (spark.read.option("header", True).csv("file:///opt/spark-apps/landing/customer_etl/orders.csv")
          .join(spark.read.option("multiline", "true").json(
                    "file:///opt/spark-apps/landing/customer_etl/products.json"), "product_id"))

(lineas
 .withColumn("total_neto", precio_con_descuento(col("unit_price").cast("double"),
                                                col("quantity").cast("double")))
 .select("order_id", "quantity", "unit_price", "total_neto")
 .show())
```

**Qué observar:** el UDF opera sobre `pandas.Series` completas (un batch por partición) vía Arrow, no
fila a fila. Mucho más rápido que un UDF Python clásico.

**Por qué:** aquí se cobra el peaje del stack: el Pandas UDF **solo funciona porque driver y executors
comparten Python 3.12** y ambos tienen `pyarrow` (viene con `pyspark`). Es la razón de fondo de toda
la alineación de versiones ([ANALISIS_FALLOS §5.3](../ANALISIS_FALLOS.md)). Si algún día falla con `[PYTHON_VERSION_MISMATCH]`
o `ModuleNotFoundError: pyarrow`, el problema es de imagen, no del código.

---

## Ejemplo 19 — Tuning: Adaptive Query Execution, skew y `explain("formatted")`

**Dónde:** Jupyter. Leer el plan y dejar que Spark 4 se auto-optimice.

```python
from pyspark.sql import functions as F

print("AQE activo:", spark.conf.get("spark.sql.adaptive.enabled"))   # 'true' por defecto en Spark 4

orders   = spark.read.option("header", True).csv("file:///opt/spark-apps/landing/customer_etl/orders.csv")
products = spark.read.option("multiline", "true").json("file:///opt/spark-apps/landing/customer_etl/products.json")

# Forzar SortMergeJoin (desactiva el broadcast) para ver AQE y el manejo de skew en acción:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

joined = orders.join(products, "product_id").groupBy("category").agg(F.sum("quantity").alias("u"))

joined.explain("formatted")    # busca 'AdaptiveSparkPlan isFinalPlan=...'
joined.count()                 # dispara la ejecución → AQE reescribe el plan en runtime
joined.explain("cost")         # tras ejecutar: estadísticas reales por nodo
```

**Qué observar:** en el plan aparece `AdaptiveSparkPlan`. Tras la acción, abre la UI del master
(`:8081` → tu app → pestaña **SQL**): verás el DAG con las particiones que AQE **coalescó** en runtime.

**Por qué:** AQE ajusta el nº de particiones de shuffle, convierte joins a broadcast cuando el tamaño
real lo permite y parte las particiones sesgadas (*skew*) — todo con estadísticas de ejecución, no
estimaciones. Saber leer `explain("formatted"/"cost")` y la pestaña SQL es lo que separa "corre" de
"corre eficiente".

---

## Ejemplo 20 — Testear transformaciones Spark con pytest (capstone)

**Dónde:** archivos de test + runner. La transformación se **refactoriza a función pura** y se prueba
en un Spark `local`, aislado y rápido (sin depender del cluster).

```python
# spark-apps/ejemplos/transforms.py  — lógica de negocio, sin I/O, testeable
from pyspark.sql import functions as F

def total_por_orden(orders, products):
    return (orders.join(products, "product_id")
            .withColumn("total", F.col("quantity") * F.col("unit_price"))
            .select("order_id", "total"))
```
```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # local[*] para tests: aislado y veloz, NO el cluster standalone
    s = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    yield s
    s.stop()
```
```python
# tests/test_transforms.py
import sys; sys.path.insert(0, "/opt/spark-apps/ejemplos")
from pyspark.sql import Row
from transforms import total_por_orden

def test_total_por_orden(spark):
    orders   = spark.createDataFrame([Row(order_id="O1", product_id="P1", quantity=2)])
    products = spark.createDataFrame([Row(product_id="P1", unit_price=100)])

    res = {r.order_id: r.total for r in total_por_orden(orders, products).collect()}
    assert res == {"O1": 200}
```
```bash
# correr los tests dentro del contenedor de Jupyter (tiene pyspark 4.0.3 + Python 3.12):
docker exec -w /opt jupyter-notebook python -m pytest /opt/notebooks/../spark-apps/../tests -q
# (o monta ./tests y ejecútalo donde te sea cómodo; el repo ya usa pytest — hay .pytest_cache)
```

**Qué observar:** el test crea DataFrames en memoria con `Row`, ejecuta la función pura y verifica el
resultado. No toca HDFS, Airflow ni el cluster: corre en segundos.

**Por qué:** separar **lógica** (transformaciones puras `df → df`) de **I/O y orquestación** (shell +
DAG) es lo que hace un pipeline testeable. Con esto cierras el círculo del data engineer: código que
se prueba en CI antes de desplegar, no "se ve en la UI de Airflow si funcionó". Es el antídoto al
`success` engañoso del Ej. 12.

---

## Cierre — del ejemplo al pipeline

Recorrido: leer/escribir (1–5) → HDFS, joins, SQL, ventanas, jobs parametrizados (6–10) → job
completo, orquestación, entornos, performance y layout de lake (11–15). El pipeline real del repo
(`customer_etl`) es exactamente la suma de los ejemplos 7, 8, 10, 11 y 12.

**Siguiente paso:** cuando un job funciona en local con estos patrones, el salto a producción solo
cambia dos cosas —`hdfs://…` → `s3://…` y el `--master spark://…` por EMR Serverless—; la lógica del
job queda igual. Ver [docs/02](02-produccion-aws.md) y [docs/03](03-arquitectura.md).

> Limpieza: los `spark-apps/ejemplos/out/` y las rutas `/ejemplos/...` de HDFS son scratch. Bórralos
> con `hdfs dfs -rm -r /ejemplos` y `rm -rf spark-apps/ejemplos/out` cuando termines (no los versiones).
