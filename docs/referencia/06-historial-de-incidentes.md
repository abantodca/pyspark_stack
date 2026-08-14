# Historial de incidentes del stack local

Registro de los fallos encontrados al poner a punto el stack local (HDFS + Spark standalone +
Airflow + Jupyter) y de cómo se resolvieron. Es un documento **histórico**: todos los fixes están
aplicados. Se conserva porque los mismos problemas reaparecen al reconstruir el stack desde cero.

- **Fecha del análisis:** 2026-07-12.
- **Estado:** incidentes #1 a #8 resueltos.
- **Stack resultante:** Airflow 3.2.2 · Python 3.12 · Spark 4.0.3 · JDK 17 (Temurin) · Postgres 16.

> La arquitectura de **producción** ya no se parece a esto: el cómputo Spark corre en EMR Serverless
> y el storage es S3, sin HDFS. HDFS y el Spark standalone descritos acá son el entorno de
> **desarrollo local**. Ver [02 — Producción en AWS](02-produccion-aws-terraform.md) y
> [03 — Arquitectura](03-arquitectura.md).

---

## 1. El stack en el momento del análisis

| Servicio | Imagen | Rol |
|---|---|---|
| `hdfs-namenode` / `hdfs-datanode` | `chandravenkat/hadoop-*` | Almacenamiento HDFS (`hdfs://hdfs-namenode:9000`) |
| `spark-master` / `spark-worker` | build `Dockerfile.spark` (base `apache/spark:4.0.3`) | Cluster Spark standalone (`spark://spark-master:7077`), UI en `8081` |
| `jupyter` | build `Dockerfile.jupyter` | Notebooks con PySpark 4.0.3 |
| `airflow-db` | `postgres:16` | Metadata de Airflow |
| `airflow-init` | build `Dockerfile.airflow` | One-shot: `db migrate` + `fab-db migrate` + crea el admin |
| `airflow-apiserver` | build `Dockerfile.airflow` | UI y API REST (antes `webserver`), puerto `8082` |
| `airflow-scheduler` | build `Dockerfile.airflow` | Orquestación (LocalExecutor) |
| `airflow-dag-processor` | build `Dockerfile.airflow` | Parseo de DAGs en proceso propio (nuevo en Airflow 3) |
| `airflow-triggerer` | build `Dockerfile.airflow` | Operadores deferrables |

Flujo del pipeline principal (`customer_etl`):

```
DAG → customer_etl_job_airflow.sh → env.sh → sube landing a HDFS
    → spark-submit customer_etl_job.py → getmerge del resultado a shared_output/
```

`./spark-apps`, `./dags` y `./spark-events` se montan como bind mounts dentro de los contenedores.

---

## 2. Incidentes

### #1 · Crítico — `products.json` es un array multilínea y rompe los jobs

**Dónde:** todas las lecturas de `products.json` (`customer_etl_job.py` y los jobs `sales_etl` de
entonces, ya eliminados del repositorio).

**Causa:** el archivo está *pretty-printed* como array JSON, pero `spark.read.json()` asume JSON
Lines (un objeto por línea) mientras `multiline` sea `false`, que es el valor por defecto. Spark
genera registros `_corrupt_record` y no crea las columnas `product_id`, `unit_price` ni `category`.

**Síntoma:** `AnalysisException: cannot resolve 'p.product_id' given input columns:
[_corrupt_record]` en el `JOIN` contra productos.

**Fix aplicado:** añadir la opción de lectura en el código, más robusto que reformatear los datos.

```python
df_products = spark.read.option("multiline", "true").json(products_path)
```

> El diagnóstico sigue vigente en Spark 4.0.3: el default de `multiline` no cambió.

### #2 · Crítico — los DAGs de wordcount apuntaban a scripts inexistentes

**Dónde:** `dags/spark_trigger_dag.py` y `dags/spark_trigger_hdfs_dag.py`.

**Causa:** ni `wordcount.py` ni `wordcount_hdfs.py` existían en `spark-apps/`.

**Síntoma:** `spark-submit` terminaba con `Cannot load main class` / `No such file or directory` y la
tarea quedaba en `failed`.

**Fix aplicado:** se crearon ambos scripts como *self-contained*: generan su propio texto de entrada
y no dependen de archivos previos. La variante HDFS siembra el input en `hdfs:///wordcount/input`,
lo lee y escribe en `hdfs:///wordcount/output`, borrando el output anterior para permitir
re-ejecuciones.

### #3 · Medio — ruta WSL hardcodeada fuera del contenedor

**Dónde:** `spark-apps/customer_etl/shell/customer_etl_job_airflow.sh`.

**Causa:** la rama «fuera de contenedor» hacía `source /mnt/c/pyspark_stack/.../env.sh`, una ruta de
Windows/WSL. Airflow ejecuta el script dentro del contenedor (rama `[ -f /.dockerenv ]`), así que el
flujo normal no rompía, pero sí fallaba al lanzarlo desde el host.

**Fix aplicado:** resolver la ruta relativa al propio script.

```bash
source "$(dirname "$0")/../config/env.sh" "$ENV"
```

### #4 · Medio — `eventLog` activo sin History Server

**Dónde:** `spark-events/spark-defaults.conf` y el servicio `spark-history-server`, comentado en el
Compose.

**Causa:** `spark.eventLog.enabled true` escribía eventos en `/tmp/spark-events` sin que ninguna UI
los consumiera. Quedaban además dos `.inprogress` huérfanos de corridas que nunca cerraron su log.

**Fix aplicado:** `spark.eventLog.enabled false` con un comentario sobre cómo reactivar el
historial, y borrado de los `.inprogress` huérfanos. Para volver a habilitarlo, ver
[01 §8.5](01-stack-local.md).

### #5 · Menor — fecha inexistente en el historial de versiones

`spark-apps/customer_etl/version_history.txt` declaraba `Date: 2026-02-29`; 2026 no es bisiesto.
Corregido a `2026-02-28`.

### #6 · Menor — `env.sh` no exportaba sus variables

Los `export` estaban comentados: las variables (`ENV`, `HDFS_INPUT`, `HDFS_OUTPUT`, `FINAL_CSV`,
`RUN_DATE`, `LANDING_PATH`) sobrevivían solo porque el shell hacía `source` en el mismo proceso.
Cualquier subproceso las perdía en silencio. Se descomentaron los seis `export`.

### #7 · Limpieza — `version: "3.8"` obsoleto en el Compose

Compose v2 ignora la clave `version` y emite un warning en cada comando. Se eliminó.

### #8 · Crítico — procesos `docker-proxy` huérfanos bloquean los puertos

**Dónde:** la capa Docker del host, no un archivo del repositorio.

**Síntoma:** tras `docker compose up -d`, varios contenedores quedan en `Created` y no arrancan; solo
suben los que no publican puertos en conflicto. `docker start spark-master` falla con:

```
Error response from daemon: ports are not available: exposing port TCP 0.0.0.0:8080 ...
bind: address already in use
```

…pero `ss -ltnp` reporta el puerto como libre.

**Causa:** procesos `docker-proxy` de una corrida anterior siguen ocupando 8080 y 5432, apuntando a
IPs de contenedores que ya no existen. Usan `-use-listen-fd`, por eso `ss` no los atribuye al
puerto, y sobreviven tanto a `docker compose down` como a `systemctl restart docker`.

**Diagnóstico y fix (requiere root):**

```bash
pgrep -a docker-proxy        # lista los procesos y a qué puerto/IP apuntan
sudo kill -9 <PIDs>          # solo los huérfanos identificados arriba
docker compose up -d
```

> **Prevención:** ante contenedores atascados en `Created` con `address already in use`, revisá
> `pgrep -a docker-proxy` antes de sospechar del código o del Compose.

### Resumen

| # | Severidad | Archivo(s) | Cambio |
|---|---|---|---|
| 1 | Crítico | jobs que leen `products.json` | `.option("multiline", "true")` |
| 2 | Crítico | `dags/spark_trigger*_dag.py` | Se crearon `wordcount.py` y `wordcount_hdfs.py` |
| 3 | Medio | `customer_etl_job_airflow.sh` | Ruta relativa al script en vez de `/mnt/c/...` |
| 4 | Medio | `spark-defaults.conf` | `eventLog.enabled false` y borrado de `.inprogress` |
| 5 | Menor | `version_history.txt` | `2026-02-29` → `2026-02-28` |
| 6 | Menor | `config/env.sh` | Se descomentaron los `export` |
| 7 | Limpieza | `docker-compose.yml` | Se eliminó `version: "3.8"` |
| 8 | Crítico | Docker del host | `kill -9` de los `docker-proxy` huérfanos |

---

## 3. Migración a Airflow 3.2.2

### 3.1 Qué versión y por qué

Se pasó de `apache/airflow:2.7.2-python3.8` a `apache/airflow:3.2.2-python3.12`. Se eligió 3.2.2
—último parche de la rama 3.2— por encima de 3.3.0, que llevaba seis días publicada. Airflow 2.x
quedó EOL en octubre de 2025 y ya no recibe parches de seguridad.

### 3.2 Rupturas que afectan a este stack

| Cambio | Antes (2.7) | Ahora (3.2.2) |
|---|---|---|
| Webserver → API server | `airflow webserver` | `airflow api-server` (UI + REST en FastAPI) |
| DAG processor | dentro del scheduler | proceso propio `airflow dag-processor` (obligatorio) |
| Migración de BD | `airflow db upgrade` | `airflow db migrate` |
| Tablas de auth FAB | en el core | provider `apache-airflow-providers-fab` + `airflow fab-db migrate` |
| AuthManager | FAB por defecto | hay que declarar `AIRFLOW__CORE__AUTH_MANAGER=...FabAuthManager` |
| Conexión SQLAlchemy | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| Secret key | `AIRFLOW__WEBSERVER__SECRET_KEY` | `AIRFLOW__API_AUTH__JWT_SECRET` |
| Task Execution API | — | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` apuntando al contenedor, no a localhost |
| Python mínimo | 3.8 | 3.9+ (acá 3.12) |

### 3.3 Efecto dominó: Python 3.12 → Spark 4.0.3 → Java 17

Mantener Python 3.12 —el que trae la imagen de Airflow 3— obligó a subir todo el cluster Spark:

- **PySpark 3.2.x no funciona con Python 3.11+.** Su `cloudpickle` no serializa lambdas y falla con
  `PicklingError: IndexError: tuple index out of range`.
- **Spark 4.0.3 requiere Java 17**, así que `Dockerfile.airflow` instala Temurin JDK 17 desde
  tarball (bookworm ya no trae `openjdk-11`).
- **El cluster usa las imágenes oficiales `apache/spark:4.0.3-...-java17`.** Como esa imagen está
  pensada para `spark-submit`/Kubernetes, el master y el worker se lanzan en foreground con
  `spark-class`: los scripts `sbin/start-*.sh` daemonizan y el contenedor terminaría.
- **`pyspark==4.0.3` se instala sin constraints**, en un `RUN` aparte, para que case exactamente con
  el cluster. El CLI de HDFS (Hadoop 3.4.1) bajo Java 17 necesita `HADOOP_OPTS="--add-opens ..."`.

> **Driver y executors deben compartir el mismo minor de Python.** La imagen oficial de Spark 4.0.3
> es Ubuntu 22.04 → Python 3.10, pero el driver (Airflow) es 3.12, y PySpark aborta con
> `[PYTHON_VERSION_MISMATCH]`. Por eso `Dockerfile.spark` y `Dockerfile.jupyter` instalan Python 3.12
> desde el PPA *deadsnakes*, y todos los `spark-submit` pasan
> `--conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12`.

### 3.4 Providers

`requirements.txt` se instala con el constraints file oficial de 3.2.2:

```
apache-airflow-providers-apache-spark==6.0.2
apache-airflow-providers-fab==3.6.4
```

`pyspark==4.0.3` se instala aparte y sin constraints, para casar con el cluster.

### 3.5 Nueva topología

De dos servicios (`webserver` + `scheduler`) se pasó a cinco:

```
airflow-init          # one-shot: db migrate + fab-db migrate + crea el admin
airflow-apiserver     # UI + API (8082:8080)
airflow-scheduler     # orquesta; con LocalExecutor también ejecuta las tasks
airflow-dag-processor # parsea los DAGs
airflow-triggerer     # operadores deferrables
```

La configuración repetida se factorizó en el ancla YAML `x-airflow-common`. Los `depends_on` usan
`condition: service_healthy` (Postgres) y `service_completed_successfully` (init), lo que eliminó los
bucles `while ! pg_isready` del Compose anterior.

### 3.6 Cómo se aplicó

Cambian la imagen y el esquema de la BD, así que hace falta resetear el volumen de Postgres:

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
docker compose ps          # los airflow-* en 'running'; airflow-init en 'exited (0)'
```

### 3.7 Adaptación de los DAGs

| Regla de Airflow 3 | Cambio |
|---|---|
| `schedule_interval=` | → `schedule=` |
| `from airflow import DAG` | → `from airflow.sdk import DAG` (Task SDK) |
| `from airflow.models import Variable` | → `from airflow.sdk import Variable` |
| `from airflow.operators.bash import BashOperator` | → `from airflow.providers.standard.operators.bash import BashOperator` |
| `Variable.get(..., default_var=...)` | → `Variable.get(..., default=...)` |

A tener en cuenta si los DAGs crecen: desaparecieron `execution_date` y `tomorrow_ds` (usar
`logical_date` / `data_interval_*`), las tasks ya no acceden directamente a la BD de metadata, y
SubDAGs y `SequentialExecutor` fueron eliminados. `airflow config update --fix` dentro del
contenedor ayuda a detectar configuración obsoleta.

---

## 4. Verificación end-to-end

Tras la migración se levantó el stack completo y se dispararon los tres DAGs hasta obtener salida
real.

| DAG | Estado | Salida verificada |
|---|---|---|
| `spark_wordcount_trigger` | `success` | `spark-submit` standalone, `exitCode 0` |
| `spark_wordcount_trigger_hdfs` | `success` | escribe y lee HDFS: `spark 4 · etl 3 · hadoop 2 · airflow 2 · hdfs 1 · dag 1` |
| `customer_etl_dag` | `success` | `shared_output/customer_etl/loyalty_snapshot_2026-07-12.csv` (5 clientes con `loyalty_status`) |

### 4.1 Fallos encontrados en el camino

1. **Imagen vieja de Airflow reutilizada.** Al reconstruir solo el api-server, el scheduler reusó una
   imagen 2.7.2 y entró en crash-loop pidiendo «upgrade the database». Fix: el ancla
   `x-airflow-common` fija `image: pyspark_stack-airflow:3.2.2`, así los cinco servicios comparten
   una única imagen construida una vez.
2. **`Variable.get(..., default_var=...)` inválido en Airflow 3** → `TypeError`. Fix: `default=`.
3. **`PicklingError` de cloudpickle** al ejecutar el wordcount con PySpark 3.2.1 sobre Python 3.12.
   Fix de fondo: subir todo Spark a 4.0.3 (§3.3).
4. **`[PYTHON_VERSION_MISMATCH] worker 3.10 vs driver 3.12`.** Fix: imágenes propias con Python 3.12
   y los `--conf spark.pyspark.*` en cada submit.
5. **Puerto 8080 ocupado en el host** al arrancar `spark-master`. Fix: se remapeó la UI del master a
   `8081:8080`.
6. **`Permission denied: user=airflow, access=WRITE, inode="/"` en HDFS.** La raíz de HDFS es de
   `root`. Fix: `export HADOOP_USER_NAME=root` antes de las operaciones HDFS.
7. **`IllegalArgumentException: Wrong FS: hdfs://... expected: file:///`.** `FileSystem.get(conf)`
   resolvía al FS local porque Spark no tenía `fs.defaultFS`. Fix:
   `--conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000` en el submit del DAG de HDFS.
8. **DAG en pausa → run atascado en `queued`.** Fix: `airflow dags unpause <dag_id>`.
9. **Bind mount que no reflejaba las ediciones.** El mount de `./spark-apps` es de tipo `fakeowner`
   y no seguía los reemplazos atómicos (escribir a temporal + `rename` cambia el inodo): el
   contenedor seguía viendo la versión anterior del `.sh`. Se detectó comparando inodos entre host y
   contenedor. Workaround: forzar una escritura *in-place* que preserve el inodo.

### 4.2 Comandos de diagnóstico

```bash
# estado real de una task (la UI puede marcar success si el .sh no usa `set -e`)
docker exec airflow-db psql -U airflow -t -A -c \
  "select state from task_instance where dag_id='<dag>' order by start_date desc limit 1;"

# log de la última corrida de una task
docker exec airflow-scheduler bash -lc \
  "ls -t /opt/airflow/logs/dag_id=<dag>/*/task_id=<task>/*.log | head -1 | xargs cat"

# forzar re-serialización de los DAGs tras editarlos
docker exec airflow-dag-processor airflow dags reserialize
```

> **Cuidado con los falsos positivos:** `customer_etl_job_airflow.sh` no usa `set -e`, así que
> devuelve el código del último `echo` y el DAG queda en `success` aunque el `hdfs put` o el
> `spark-submit` intermedios hayan fallado. Validá siempre la salida real —CSV no vacío,
> `hdfs dfs -cat` del output— y no solo el estado del DAG.
