# Análisis del proyecto `pyspark_stack` — Dónde falla y por qué

> Fecha: 2026-07-12
> Alcance: infraestructura Docker (Spark + HDFS + Airflow + Jupyter) y jobs PySpark de los pipelines `customer_etl` y `sales_etl`.

> **Estado:** ✅ **Todos los fixes (#1–#6) aplicados** el 2026-07-12, más limpieza del `docker-compose.yml` (#7)
> y el fallo recurrente de infraestructura (#8, `docker-proxy` huérfanos).
> Último reset limpio (2026-07-12): `down -v` + `build --no-cache` + `up -d` → los 8 contenedores `running`
> (namenode/datanode/jupyter `healthy`, worker Spark registrado, 3 DAGs sin errores de import).
>
> 🆕 **Migración a Airflow 3.2.2 (2026-07-12):** se actualizó Airflow de `2.7.2` a **`3.2.2`** (rama 3.2, el
> parche más maduro; se descartó `3.3.0` por tener solo 6 días de rodaje). Ver **§5. Migración a Airflow 3.x**.
> Esto cambia la topología: ahora hay **api-server + scheduler + dag-processor + triggerer + init** en vez de
> `webserver + scheduler`. **Requiere `down -v` + `build --no-cache`** (imagen y esquema de BD nuevos).
>
> 🆕🆕 **Stack "lo más actual" + verificación end-to-end (2026-07-12):** por decisión de mantener **Python 3.12**
> (la imagen de Airflow 3 lo trae) se tuvo que subir **todo el cluster Spark de 3.2.1 → `4.0.3`** (Python 3.11+
> es incompatible con el cloudpickle de PySpark 3.2.x). Spark 4 exige **Java 17**. Además `postgres 13 → 16`.
> **Los 3 DAGs quedaron verificados en `success` con salida real.** Ver **§5.3** (Spark 4/JDK 17), **§6**
> (historia de verificación: cloudpickle, imagen vieja, `PYTHON_VERSION_MISMATCH`, permisos HDFS, `Wrong FS`,
> des-sincronización del bind-mount).
>
> **Stack final:** Airflow **3.2.2** · Python **3.12** · Spark **4.0.3** · JDK **17 (Temurin)** · Postgres **16**.

---

## 1. Qué es el proyecto

Stack de datos local levantado con `docker-compose.yml`:

| Servicio | Imagen | Rol |
|----------|--------|-----|
| `hdfs-namenode` / `hdfs-datanode` | `chandravenkat/hadoop-*` | Almacenamiento HDFS (`hdfs://hdfs-namenode:9000`) |
| `spark-master` / `spark-worker` | build `Dockerfile.spark` (base `apache/spark:4.0.3` + Python 3.12) | Cluster Spark **4.0.3** standalone (`spark://spark-master:7077`) — UI en `8081:8080` |
| `jupyter` | build `Dockerfile.jupyter` (base `apache/spark:4.0.3` + JupyterLab + Python 3.12) | Notebooks de práctica con pyspark 4.0.3 |
| `airflow-db` (**postgres 16**) | — | Metadata de Airflow |
| `airflow-init` (one-shot) | build `Dockerfile.airflow` | Migra BD (`db migrate` + `fab-db migrate`) y crea el admin, luego sale |
| `airflow-apiserver` | build `Dockerfile.airflow` | UI + API REST (antes `webserver`) — puerto `8082:8080` |
| `airflow-scheduler` | build `Dockerfile.airflow` | Orquestación (LocalExecutor) |
| `airflow-dag-processor` | build `Dockerfile.airflow` | Parseo de DAGs en proceso propio (nuevo en Airflow 3) |
| `airflow-triggerer` | build `Dockerfile.airflow` | Operadores deferrables |

> **Airflow 3.2.2** (migrado el 2026-07-12 desde 2.7.2) — imagen base `apache/airflow:3.2.2-python3.12`.
> Ver **§5. Migración a Airflow 3.x** para el detalle de rupturas y comandos.

Flujo del pipeline principal (`customer_etl`):
`DAG (Airflow) → customer_etl_job_airflow.sh → env.sh → sube landing a HDFS → spark-submit customer_etl_job.py → getmerge del resultado a shared_output/`.

Todos los directorios `./spark-apps`, `./dags`, `./spark-events` se montan como volúmenes dentro de los contenedores.

---

## 2. Fallos detectados

### 🔴 CRÍTICO 1 — `products.json` es un array JSON multilínea → los jobs de Spark fallan  ✅ APLICADO

**Dónde:**
- `spark-apps/customer_etl/scripts/customer_etl_job.py:19`
- `spark-apps/sales_etl/scripts/sales_etl_job.py:12`
- `spark-apps/scripts/sales_etl_job.py:12`
- `spark-apps/scripts/sales_etl_old.py:10`
- `spark-apps/project1/sales_etl_job.py:27`

**Causa:** todos los `products.json` están *pretty-printed* como un array:

```json
[
  {"product_id": "P201", "category": "Books", "unit_price": 250},
  ...
]
```

Pero `spark.read.json(path)` en Spark 3.2.1 asume **JSON Lines** (un objeto por línea) cuando `multiline=false` (default). Con un array multilínea Spark genera registros `_corrupt_record` y **no crea las columnas** `product_id`, `unit_price`, `category`.

**Síntoma en tiempo de ejecución:** el `JOIN ... ON o.product_id = p.product_id` (o `s.product_id = p.product_id`) revienta con:
`AnalysisException: cannot resolve 'p.product_id' given input columns: [_corrupt_record]`.

> Nota: `orders.json` (en `spark-apps/input/orders.json`) **sí** está en formato JSONL, por eso `order_enrichment_job.py` / `practice_script.py` funcionan. El problema es exclusivo de los `products.json`.

**Fix (elige uno):**

- **A. En el código** — añadir la opción multiline:
  ```python
  df_products = spark.read.option("multiline", "true").json(products_path)
  ```
- **B. En los datos** — convertir cada `products.json` a JSONL (un objeto por línea, sin corchetes envolventes):
  ```json
  {"product_id": "P201", "category": "Books", "unit_price": 250}
  {"product_id": "P202", "category": "Electronics", "unit_price": 1200}
  ```

Recomendado: **Opción A** (menos frágil; no depende de cómo se regenere el archivo).

> **Aplicado (Opción A):** se añadió `.option("multiline", "true")` a las 5 lecturas de `products.json`
> (`customer_etl_job.py:19`, `sales_etl/scripts/sales_etl_job.py:12`, `scripts/sales_etl_job.py:12`,
> `scripts/sales_etl_old.py:10`, `project1/sales_etl_job.py:27`). Todos verificados con `py_compile`.

---

### 🔴 CRÍTICO 2 — Los DAGs `spark_wordcount_trigger` apuntan a scripts inexistentes  ✅ APLICADO

**Dónde:**
- `dags/spark_trigger_dag.py:21` → `/opt/spark-apps/wordcount.py`
- `dags/spark_trigger_hdfs_dag.py:21` → `/opt/spark-apps/wordcount_hdfs.py`

**Causa:** ni `wordcount.py` ni `wordcount_hdfs.py` existen en `spark-apps/`.

**Síntoma:** al disparar el DAG, `spark-submit` termina con
`Error: Cannot load main class ... / No such file or directory` y la tarea queda en `failed`.

**Fix:** crear los scripts `wordcount.py` / `wordcount_hdfs.py`, o eliminar/deshabilitar esos DAGs si eran solo de prueba.

> **Aplicado:** se crearon `spark-apps/wordcount.py` y `spark-apps/wordcount_hdfs.py`, ambos **self-contained**
> (generan su propio texto de entrada, así no dependen de archivos previos en el FS/HDFS). La variante HDFS
> siembra el input en `hdfs:///wordcount/input`, lo lee y escribe el resultado en `hdfs:///wordcount/output`
> (borrando el output previo para permitir re-ejecución). Verificados con `py_compile`.

---

### 🟠 MEDIO 3 — Ruta WSL hardcodeada en la rama "fuera de contenedor"

**Dónde:** `spark-apps/customer_etl/shell/customer_etl_job_airflow.sh:68`

```bash
source /mnt/c/pyspark_stack/spark-apps/customer_etl/config/env.sh "$ENV"
```

**Causa:** `/mnt/c/pyspark_stack/...` es una ruta de WSL/Windows. En este host Linux el proyecto está en `/home/carlos/Proyects/pyspark_stack`.

**Impacto:** solo afecta si el script se ejecuta **fuera** del contenedor. Airflow lo corre **dentro** (rama `[ -f /.dockerenv ]`), que usa la ruta correcta `/opt/spark-apps/...`, así que en el flujo normal no rompe — pero es una bomba de tiempo si alguien lo lanza desde el host.

**Fix:** parametrizar con una variable de entorno o ruta relativa al script:
```bash
source "$(dirname "$0")/../config/env.sh" "$ENV"
```

> **Aplicado:** `customer_etl_job_airflow.sh:68` ahora usa `source "$(dirname "$0")/../config/env.sh" "$ENV"`,
> resolviendo la ruta relativa al propio script (funciona en cualquier host y dentro del contenedor).

---

### 🟠 MEDIO 4 — `spark-history-server` deshabilitado pero `eventLog` activado  ✅ APLICADO

**Dónde:** `docker-compose.yml:64-78` (servicio comentado) + `spark-events/spark-defaults.conf`.

**Causa:** `spark-defaults.conf` tiene `spark.eventLog.enabled true` con `dir file:/tmp/spark-events`, pero el History Server está comentado. Además hay dos ficheros `*.inprogress` viejos (`app-2025051*`) que indican corridas que nunca cerraron su log.

**Impacto:** no rompe los jobs, pero:
- No hay UI de historial (puerto 18080) pese a generarse eventos.
- Si el `spark-defaults.conf` llega a montarse en Spark, `eventLog` escribe en `/tmp/spark-events`; si ese dir no existe/no tiene permisos, el job puede fallar al iniciar.

**Fix:** o reactivar el servicio `spark-history-server`, o poner `spark.eventLog.enabled false` para evitar logs huérfanos.

> **Aplicado:** se puso `spark.eventLog.enabled false` en `spark-defaults.conf` (con comentario explicando cómo
> reactivar el historial) y se borraron los `.inprogress` huérfanos (`app-20250516051249-0000`, `app-20250516051543-0001`).

---

### 🟡 MENOR 5 — Fecha inválida en el historial de versiones  ✅ APLICADO

**Dónde:** `spark-apps/customer_etl/version_history.txt:6` → `Date: 2026-02-29`.

**Causa:** 2026 no es bisiesto; el 29 de febrero no existe. Cosmético, pero denota que la fecha se escribió a mano sin validar.

**Fix:** corregir a una fecha real (p. ej. `2026-02-28`).

> **Aplicado:** `version_history.txt` corregido a `Date: 2026-02-28`.

---

### 🟡 MENOR 6 — `env.sh` no exporta variables (por diseño, pero frágil)  ✅ APLICADO

**Dónde:** `spark-apps/customer_etl/config/env.sh:24-29` (los `export` están comentados).

**Causa:** las variables (`HDFS_INPUT`, `HDFS_OUTPUT`, `FINAL_CSV`, `RUN_DATE`, `LANDING_PATH`) solo quedan disponibles porque el shell hace `source env.sh` en el mismo proceso. Funciona hoy, pero cualquier subproceso o refactor que llame a `env.sh` sin `source` perderá los valores silenciosamente.

**Impacto:** ninguno en el flujo actual; riesgo latente.

**Fix:** descomentar los `export` para robustez.

> **Aplicado:** descomentados los 6 `export` en `env.sh`.

---

### 🔴 CRÍTICO 8 — `docker-proxy` huérfanos bloquean los puertos → los contenedores quedan en `Created`  ✅ DIAGNOSTICADO

**Dónde:** capa de infraestructura Docker del host (no es un archivo del repo).

**Síntoma:** tras `docker compose up -d`, varios contenedores quedan en estado **`Created`** (no arrancan). Solo suben los que no publican puertos conflictivos (p. ej. `hdfs-namenode`, `airflow-db`). Al intentar `docker start spark-master` aparece:

```
Error response from daemon: ports are not available: exposing port TCP 0.0.0.0:8080 ...
bind: address already in use
```

pero `ss -ltnp` reporta el puerto como **libre**.

**Causa:** quedaron procesos `docker-proxy` **huérfanos** (zombies) de una corrida anterior, ocupando los puertos **8080** (spark-master) y **5432**, apuntando a IPs de contenedores que ya no existen (`172.19.0.4`, `172.19.0.7`). Usan `-use-listen-fd`, por eso `ss` no los atribuye al puerto. Sobreviven a `docker compose down` **e incluso a `sudo systemctl restart docker`**, así que el reinicio "normal" no los limpia.

**Diagnóstico:**
```bash
pgrep -a docker-proxy        # lista los procesos y a qué puerto/IP apuntan
sudo ss -ltnp | grep :8080   # puede reportar "libre" aunque el proxy lo tenga tomado
```

**Fix (requiere root):** matar los procesos huérfanos por PID y volver a levantar:
```bash
sudo kill -9 <PIDs de docker-proxy>   # p.ej. sudo kill -9 70278 70290 70389 70402
docker compose up -d
```

> **Ocurrido el 2026-07-12:** 4 `docker-proxy` (PIDs 70278/70290 → 8080, 70389/70402 → 5432) bloqueaban el arranque
> tras un `restart docker`. Se mataron manualmente, se hizo reset limpio (`down -v` + `build --no-cache` + `up -d`)
> y los 8 contenedores quedaron `running` (namenode/datanode/jupyter `healthy`, worker registrado con el master,
> 3 DAGs cargados sin errores de import).
>
> **Prevención:** ante contenedores atascados en `Created` con `address already in use`, revisar SIEMPRE
> `pgrep -a docker-proxy` antes que el código o el compose. El compose y los fixes #1–#6 no tienen que ver con esto.

---

## 3. Tabla resumen — qué cambiar y por qué

| # | Severidad | Estado | Archivo(s) | Cambio | Por qué |
|---|-----------|--------|-----------|--------|---------|
| 1 | 🔴 Crítico | ✅ | `customer_etl_job.py`, `sales_etl_job.py` (×3), `sales_etl_old.py`, `project1/sales_etl_job.py` | `spark.read.option("multiline","true").json(...)` | El array multilínea rompe la lectura y el JOIN falla con `_corrupt_record` |
| 2 | 🔴 Crítico | ✅ | `dags/spark_trigger_dag.py`, `dags/spark_trigger_hdfs_dag.py` | Creados `wordcount.py`/`wordcount_hdfs.py` (self-contained) | Apuntaban a scripts que no existían → tarea `failed` |
| 3 | 🟠 Medio | ✅ | `customer_etl_job_airflow.sh:68` | Ruta relativa `$(dirname "$0")/../config/env.sh` en vez de `/mnt/c/...` | Ruta WSL hardcodeada rompía fuera del contenedor |
| 4 | 🟠 Medio | ✅ | `spark-defaults.conf`, `spark-events/*.inprogress` | `eventLog.enabled false` + borrados los `.inprogress` huérfanos | Se generaban logs huérfanos sin UI que los consuma |
| 5 | 🟡 Menor | ✅ | `version_history.txt:6` | `2026-02-29` → `2026-02-28` | Fecha inexistente |
| 6 | 🟡 Menor | ✅ | `config/env.sh` | Descomentados los `export` | Robustez ante subprocesos |
| 7 | 🧹 Limpieza | ✅ | `docker-compose.yml` | Eliminado `version: "3.8"` obsoleto | Compose v2+ lo ignora y emite warning |
| 8 | 🔴 Crítico | ✅ | Infra Docker del host (no repo) | `sudo kill -9` de los `docker-proxy` huérfanos + reset limpio | Zombies en 8080/5432 dejaban los contenedores en `Created`; sobreviven a `restart docker` |

---

## 4. Orden recomendado para dejarlo funcionando

1. **Arreglar la lectura de `products.json`** (Crítico 1) — sin esto ningún pipeline de ventas/loyalty produce salida.
2. **Decidir sobre los DAGs wordcount** (Crítico 2) — crear los scripts o quitarlos para que Airflow no muestre tareas rojas.
3. Ajustar la ruta del shell (Medio 3) si vas a ejecutar fuera del contenedor.
4. Resolver eventLog / history server (Medio 4).
5. Limpiezas menores (5, 6) y borrar los `*.inprogress` viejos de `spark-events/`.

---

## 5. Migración a Airflow 3.x (2026-07-12)  ✅ APLICADO

### 5.1 Qué versión y por qué

| | |
|---|---|
| **Versión anterior** | `apache/airflow:2.7.2-python3.8` |
| **Versión nueva** | **`apache/airflow:3.2.2-python3.12`** |
| **Por qué 3.2.2 y no 3.3.0** | 3.3.0 salió el **6-jul-2026** (6 días de rodaje). 3.2.2 (**29-may-2026**) es el último parche de la rama 3.2 → más maduro y sin sorpresas de un `.0` recién publicado. "La más estable". |
| **Otras candidatas** | 3.1.8 (11-mar-2026, más conservadora) · 3.3.0 (la última "latest stable" según los docs). |

> Airflow 2.x quedó **EOL** (soporte terminó el 22-oct-2025), así que quedarse en 2.7.2 ya no recibe parches de seguridad.

### 5.2 Rupturas de Airflow 3 que afectan a este stack

| Cambio en Airflow 3 | Antes (2.7) | Ahora (3.2.2) |
|---|---|---|
| **Webserver → API server** | `airflow webserver` | `airflow api-server` (UI + API REST unificados en FastAPI) |
| **DAG processor separado** | dentro del scheduler | proceso propio `airflow dag-processor` (obligatorio) |
| **Migración de BD** | `airflow db upgrade` | `airflow db migrate` |
| **Tablas de auth FAB** | incluidas en el core | provider `apache-airflow-providers-fab` + `airflow fab-db migrate` |
| **AuthManager** | FAB por defecto | hay que declararlo: `AIRFLOW__CORE__AUTH_MANAGER=...FabAuthManager` (si no, el default es SimpleAuthManager y `airflow users create` no existe) |
| **Conexión SQLAlchemy** | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (se movió a la sección `[database]`) |
| **Secret key** | `AIRFLOW__WEBSERVER__SECRET_KEY` | `AIRFLOW__API_AUTH__JWT_SECRET` (el api-server firma tokens JWT) |
| **Task Execution API** | — | el scheduler habla con el api-server vía `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` (debe apuntar al **hostname del contenedor** `airflow-apiserver`, NO a `localhost`) |
| **Python mínimo** | 3.8 | 3.9+ (usamos 3.12) — por eso Airflow 3 **no arranca** con la imagen `-python3.8` |

### 5.3 Efecto dominó Python 3.12 → Spark 4.0.3 → Java 17 (el cambio de fondo)

Mantener **Python 3.12** (lo trae `apache/airflow:3.2.2-python3.12`) obligó a subir **todo** el stack Spark.
La cadena de decisiones:

- **PySpark 3.2.x no funciona con Python 3.11+.** Su `cloudpickle` no serializa lambdas y revienta con
  `PicklingError: Could not serialize object: IndexError: tuple index out of range` al ejecutar, p. ej.,
  `.reduceByKey(lambda a, b: a + b)`. → hay que subir PySpark a la serie 4.
- **Se eligió Spark 4.0.3** (el más nuevo). Spark 4 **requiere Java 17** (ya no soporta Java 11). → en
  `Dockerfile.airflow` se instala **Temurin (Adoptium) JDK 17** desde tarball (bookworm ya no trae `openjdk-11`).
- **El cluster también sube a 4.0.3.** Se usan las imágenes **oficiales** `apache/spark:4.0.3-...-java17-...`
  (las de bitnami quedaron deprecadas/tras suscripción). Como esa imagen arranca para `spark-submit`/k8s, el
  master/worker se lanzan en foreground con `spark-class` (`sbin/start-*.sh` daemonizan y el contenedor saldría).
- **`pyspark==4.0.3` se instala sin constraints** (en un `RUN` aparte) para que **case exactamente** con el
  cluster 4.0.3. El HDFS CLI (Hadoop 3.4.1) bajo Java 17 necesita `HADOOP_OPTS="--add-opens ..."` (ya fijado).

> ⚠️ **Trampa crítica — misma versión *minor* de Python en driver y executors.** La imagen oficial
> `apache/spark:4.0.3` es **Ubuntu 22.04 → Python 3.10**, pero el driver (Airflow) es **3.12**. PySpark aborta con
> `[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12`. **Fix:** se
> construyen imágenes propias (`Dockerfile.spark` para master/worker y `Dockerfile.jupyter`) que instalan
> **Python 3.12** desde el PPA *deadsnakes*, y los `spark-submit` de los DAGs pasan
> `--conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12`. Ver **§6**.

### 5.4 Providers (fijados al constraints file oficial de 3.2.2)

`requirements.txt` (instalado con `--constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt`):

```
apache-airflow-providers-apache-spark==6.0.2
apache-airflow-providers-fab==3.6.4
```

`pyspark==4.0.3` se instala aparte y **sin constraints** (debe casar con el cluster Spark 4.0.3).

### 5.5 Nueva topología de contenedores

De **2 servicios** Airflow (`webserver` + `scheduler`) se pasa a **5**:

```
airflow-init          # one-shot: db migrate + fab-db migrate + crea admin, luego sale
airflow-apiserver     # UI + API  (8082:8080)
airflow-scheduler     # orquesta (LocalExecutor → ejecuta las tasks)
airflow-dag-processor # parsea los DAGs
airflow-triggerer     # operadores deferrables
```

Se factorizó la config repetida en una ancla YAML `x-airflow-common` (build + env + volúmenes + red). Los servicios usan `depends_on` con `condition: service_healthy` (postgres) y `service_completed_successfully` (init), así que el arranque es ordenado y ya **no hacen falta los bucles `while ! pg_isready ...`** del compose anterior.

### 5.6 Cómo aplicar la actualización

Como cambian la imagen y el esquema de la BD, hay que resetear (el volumen `postgres_data` traía metadata de Airflow 2):

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
docker compose ps          # los servicios airflow-* deben quedar 'running' (init en 'exited (0)')
```

- UI: <http://localhost:8082>  · usuario `admin` / password `admin`.
- Si `airflow-init` sale con error, revisar sus logs: `docker compose logs airflow-init`.

### 5.7 Adaptación de los DAGs a Airflow 3  ✅ APLICADO

Los 3 DAGs de `dags/` se migraron a la API de Airflow 3:

| DAG | Cambios aplicados |
|---|---|
| `customer_etl_dag.py` | `schedule_interval='@daily'` → `schedule='@daily'`; imports a `from airflow.sdk import DAG, Variable`; `BashOperator` desde `airflow.providers.standard.operators.bash` |
| `spark_trigger_dag.py` | `schedule_interval=None` → `schedule=None`; `from airflow.sdk import DAG`; `BashOperator` desde el provider `standard` |
| `spark_trigger_hdfs_dag.py` | igual que el anterior |

**Reglas aplicadas (rupturas de Airflow 3):**

- `schedule_interval=` → **`schedule=`**.
- `from airflow import DAG` → **`from airflow.sdk import DAG`** (Task SDK).
- `from airflow.models import Variable` → **`from airflow.sdk import Variable`**.
- `from airflow.operators.bash import BashOperator` → **`from airflow.providers.standard.operators.bash import BashOperator`** (los operadores "clásicos" se movieron al provider `standard`, preinstalado con Airflow 3).

> **Verificado (2026-07-12):** `py_compile` OK en los 3 DAGs, y los imports (`airflow.sdk.DAG`,
> `airflow.sdk.Variable`, `providers.standard...BashOperator`) resuelven dentro de la imagen real
> `apache/airflow:3.2.2-python3.12` (`airflow 3.2.2`).

**No aplicaba a estos DAGs** (pero a tener en cuenta si crecen): contexto eliminado
(`execution_date`, `tomorrow_ds` → usar `logical_date` / `data_interval_*`), sin acceso directo a la BD de
metadata desde las tasks, y SubDAGs / SequentialExecutor eliminados. Ayuda: `airflow config update --fix`
dentro del contenedor y la guía oficial *Upgrading to Airflow 3*. Tras el primer arranque,
`docker compose logs airflow-dag-processor` muestra errores de parseo si los hubiera.

Además, en los DAGs Spark se añadieron flags al `spark-submit` (ver §6):
`--conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12` (alinear Python
driver/executor) y, en el de HDFS, `export HADOOP_USER_NAME=root` + `--conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000`.

---

## 6. Verificación end-to-end del stack final (2026-07-12)  ✅ LOS 3 DAGs EN `success`

Tras subir el stack a **Airflow 3.2.2 + Python 3.12 + Spark 4.0.3 + JDK 17 + Postgres 16**, se levantó todo
(`docker compose up -d`, 10 contenedores) y se dispararon los 3 DAGs hasta obtener salida real. La ruta hasta
el verde encadenó varios fallos; se documentan porque son **reproducibles** al reconstruir el stack.

### 6.1 Resultado final

| DAG | Estado | Salida verificada |
|---|---|---|
| `spark_wordcount_trigger` | ✅ `success` | `spark-submit` standalone, `exitCode 0` |
| `spark_wordcount_trigger_hdfs` | ✅ `success` | escribe/lee HDFS: `spark 4 · etl 3 · hadoop 2 · airflow 2 · hdfs 1 · dag 1` |
| `customer_etl_dag` | ✅ `success` | `shared_output/customer_etl/loyalty_snapshot_2026-07-12.csv` (416 B, 5 clientes con `loyalty_status`) |

### 6.2 Fallos encontrados y sus fixes (en orden de aparición)

1. **Imagen vieja de Airflow reutilizada (scheduler en crash-loop "upgrade the database... 2.7.2").**
   Al reconstruir solo `airflow-apiserver`, el servicio `airflow-scheduler` (nombre que ya existía en el compose
   2.7.2) reusó una **imagen 2.7.2 obsoleta**. **Fix:** el ancla `x-airflow-common` fija
   `image: pyspark_stack-airflow:3.2.2` → los 5 servicios comparten **una** imagen construida una vez. Se borraron
   las imágenes `pyspark_stack-airflow-*` viejas y se reconstruyó.

2. **`Variable.get(..., default_var=...)` inválido en Airflow 3.** `TypeError: unexpected keyword 'default_var'`.
   **Fix:** en `customer_etl_dag.py`, `default_var="dev"` → **`default="dev"`**.

3. **`PicklingError: IndexError: tuple index out of range` (cloudpickle) al ejecutar el wordcount.** PySpark 3.2.1
   no serializa lambdas en Python 3.12. **Fix de fondo:** subir todo Spark a **4.0.3** (ver §5.3). Esto arrastró
   JDK 17 y las imágenes propias del cluster.

4. **`[PYTHON_VERSION_MISMATCH] worker 3.10 vs driver 3.12`.** La imagen oficial `apache/spark:4.0.3` trae Python
   3.10 (Ubuntu 22.04). **Fix:** `Dockerfile.spark` + `Dockerfile.jupyter` instalan **Python 3.12** (deadsnakes) y
   fijan `PYSPARK_PYTHON=python3.12`; los `spark-submit` pasan
   `--conf spark.pyspark.python=python3.12 --conf spark.pyspark.driver.python=python3.12`
   (en *standalone client mode* el driver propaga su Python a los executors).

5. **Puerto 8080 ocupado por un proceso del host al arrancar `spark-master`.**
   `ports are not available: ... 0.0.0.0:8080: bind: address already in use`. **Fix:** se remapeó la UI del master
   a **`8081:8080`** en el `docker-compose.yml`.

6. **HDFS `Permission denied: user=airflow, access=WRITE, inode="/":root:supergroup`.** La raíz de HDFS es de
   `root`; `airflow` no puede crear `/wordcount` ni `/customer_etl`. **Fix:** `export HADOOP_USER_NAME=root` antes
   de las operaciones HDFS (en `spark_trigger_hdfs_dag.py` y en la rama in-container de `customer_etl_job_airflow.sh`).

7. **`IllegalArgumentException: Wrong FS: hdfs://... expected: file:///`.** En `wordcount_hdfs.py`, `_path_absent`
   usa `FileSystem.get(conf)` que resuelve al FS por defecto (local) porque Spark no tenía `fs.defaultFS`.
   **Fix:** `--conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000` en el `spark-submit` del DAG HDFS.

8. **DAG en pausa → run atascado en `queued`.** `spark_wordcount_trigger_hdfs` y `customer_etl_dag` arrancan
   pausados. **Fix operacional:** `airflow dags unpause <dag_id>` (o despausar en la UI).

9. **Bind-mount que no refleja los edits (quirk del entorno).** El mount de `./spark-apps` es de tipo
   `fakeowner` y **no seguía los reemplazos atómicos** (write-a-temp + rename cambia el inodo) de las ediciones;
   el contenedor seguía viendo la versión anterior del `.sh` (por eso el fix #6 "no aplicaba"). Se detectó por el
   **inodo distinto** host vs contenedor. **Workaround:** forzar una escritura *in-place* (append/truncate
   preservando inodo) para re-sincronizar. *Nota: los `.py` de `dags/` sí se re-sincronizaron vía
   `airflow dags reserialize`.*

### 6.3 Comandos útiles usados en la verificación

```bash
# estado real de una task (la UI a veces marca success aunque el .sh sin `set -e` enmascare fallos):
docker exec airflow-db psql -U airflow -t -A -c \
  "select state from task_instance where dag_id='<dag>' order by start_date desc limit 1;"

# log de la última corrida de una task:
docker exec airflow-scheduler bash -lc \
  "ls -t /opt/airflow/logs/dag_id=<dag>/*/task_id=<task>/*.log | head -1 | xargs cat"

# forzar re-serialización de DAGs tras editarlos:
docker exec airflow-dag-processor airflow dags reserialize
```

> ⚠️ **Ojo con los falsos positivos:** `customer_etl_job_airflow.sh` **no usa `set -e`**, así que devuelve el
> código del último `echo` y el DAG queda en `success` aunque el `hdfs put` o el `spark-submit` intermedios hayan
> fallado. **Siempre** validar la salida real (CSV no vacío / `hdfs dfs -cat` del output), no solo el estado del DAG.
