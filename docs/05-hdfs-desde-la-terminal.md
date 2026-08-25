# HDFS desde la terminal

> **En este documento: ejecutar, ~30 min.** Son comandos de terminal, sin scripts nuevos ni
> automatización escondida. Al terminar podés inspeccionar, buscar, subir, consultar y exportar
> datos del lakehouse con criterio para saber cuándo conviene crear una task.

Esta guía parte de un stack ya levantado con `task local:up`. Los ejemplos no suponen que exista una
fecha concreta: primero localizás una corrida y luego trabajás con esa ruta. Así siguen funcionando
mañana, en un volumen nuevo o con tus propios datos.

## 1. Confirmar el contexto antes de ejecutar

El `Taskfile.yml` usa los dos archivos Compose. Definí esta función una vez en la terminal para que
los comandos de Compose de la guía usen exactamente esa misma composición:

```bash
dc() { docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml "$@"; }
```

La imagen de HDFS contiene el cliente, pero no lo agrega al `PATH` de los procesos iniciados con
`docker exec`. Esta segunda función evita repetir una ruta interna de la imagen; no crea ningún
script ni cambia el contenedor:

```bash
hdfs() { docker exec hdfs-namenode /opt/hadoop-3.2.1/bin/hdfs "$@"; }
```

Comprobá los hechos de los que salen los comandos siguientes:

```bash
dc config --services                                      # servicios declarados
dc ps                                                      # servicios que están arriba
dc port hdfs-namenode 9870                                 # puerto real del NameNode en el host
dc exec -T hdfs-namenode env | grep CORE_CONF              # fs.defaultFS efectivo del NameNode
dc exec -T airflow-scheduler cat /opt/hadoop/etc/hadoop/core-site.xml
hdfs dfs -ls /lakehouse                                    # prueba real del cliente HDFS
```

### 1.1 Estado inicial de HDFS

Un HDFS recién inicializado contiene únicamente `/lakehouse` y `/lakehouse/landing`. El servicio
`hdfs-init` crea ambos de forma idempotente; no crea capas ni corridas. Los DAGs medallion crean
`bronze`, `silver`, `gold`, `quality` y `quarantine` bajo su proyecto y `run_date` cuando se
ejecutan. Por eso, antes del primer DAG, `hdfs dfs -ls /lakehouse` debe mostrar solo `landing`.

La reinicialización completa de HDFS elimina sus volúmenes de NameNode y DataNode; no es una
limpieza de rutas y borra toda la data del lakehouse. Conservá Postgres y los logs de Airflow salvo
que también quieras reiniciar esos componentes.

| Hecho | Consecuencia práctica |
|---|---|
| `fs.defaultFS=hdfs://hdfs-namenode:9000` | En Airflow, una ruta absoluta como `/lakehouse/...` se resuelve en HDFS. |
| `hdfs-namenode` vive en `hadoopnet` | Es un hostname interno: desde tu máquina usás `localhost:9870`; desde un contenedor, `hdfs-namenode:9000`. |
| `core-site.xml` se monta solo en Airflow | En Spark master y Jupyter usá una URI completa `hdfs://hdfs-namenode:9000/...` o configurá `fs.defaultFS`. |
| `./spark-apps` es un bind mount | Un archivo allí aparece como `/opt/spark-apps/<archivo>` en los contenedores que lo montan. |

`docker exec airflow-scheduler command -v hdfs` no devuelve nada: el cliente de línea de comandos
se ejecuta desde el NameNode. Para SQL o Parquet usás Spark en `spark-master`, Airflow o Jupyter.

## 2. URLs y WebHDFS

La fuente de verdad de puertos es el Compose efectivo:

```bash
dc ps --format 'table {{.Service}}\t{{.Ports}}'
```

| Servicio | URL | Uso |
|---|---|---|
| Airflow | http://localhost:8082 | Disparar DAGs y leer logs. |
| NameNode | http://localhost:9870 | *Utilities ▸ Browse the file system*: navegar, tamaños y réplicas. |
| Spark master | http://localhost:8081 | Workers y aplicaciones activas. |
| Spark driver de Jupyter | http://localhost:4055 | Stages y plan de una sesión de notebook abierta. |
| Jupyter | http://localhost:8888 | Exploración interactiva; solo perfil `dev`. |

`task local:urls` muestra las mismas URLs y su estado; `task local:credentials` muestra los accesos
locales.

**WebHDFS** es la API HTTP del NameNode. Desde el host sirve para metadatos:

```bash
# Siempre funciona, incluso en un HDFS recién inicializado.
curl -fsS 'http://localhost:9870/webhdfs/v1/lakehouse?op=LISTSTATUS' | python3 -m json.tool | head -20

# La capa Gold aparece después de ejecutar un DAG.
if hdfs dfs -test -d /lakehouse/gold; then
  curl -fsS 'http://localhost:9870/webhdfs/v1/lakehouse/gold?op=GETCONTENTSUMMARY'
else
  echo 'Gold aún no existe; ejecutá un DAG medallion primero.'
fi
```

`OPEN` y `CREATE` devuelven un `307` hacia el DataNode. Ese destino es interno y no está publicado
en el host, por lo que una lectura o escritura WebHDFS debe correr dentro de `hadoopnet`. Además,
esta imagen requiere identificar el usuario simple:

```bash
# Requiere que /tmp/ventas.csv exista en el NameNode; la sección 5 lo prepara.
docker exec hdfs-namenode curl -fsS -L -X PUT -T /tmp/ventas.csv \
  'http://hdfs-namenode:9870/webhdfs/v1/lakehouse/landing/daily_sales/ventas.csv?op=CREATE&overwrite=true&user.name=root'
```

## 3. Ver datos y elegir una corrida

Primero mirá qué hay realmente. No copies una fecha de ejemplo: elegí una de las que devuelva el
segundo comando. Si todavía no existe `daily_sales`, seguí las secciones 5 y 6 y volvé luego a este
punto.

```bash
hdfs dfs -ls -h /lakehouse                                  # capas actuales; en limpio, solo landing
hdfs dfs -du -h /lakehouse                                   # uso por capa
hdfs dfs -count -q -h /lakehouse                             # dirs, archivos, bytes y cuotas

# Estas tres consultas requieren que un DAG ya haya publicado Gold.
if hdfs dfs -test -d /lakehouse/gold; then
  hdfs dfs -ls -h /lakehouse/gold/daily_sales
  hdfs dfs -ls -R /lakehouse/gold | head -20
  hdfs dfs -du -h -s '/lakehouse/gold/*'
else
  echo 'Gold aún no existe; seguí las secciones 5 y 6 antes de consultar una corrida.'
fi
```

Definí variables para la corrida que vas a consultar. Reemplazá `AAAA-MM-DD` por una fecha listada
arriba o, después de ejecutar el DAG de la sección 6, por la fecha lógica de esa corrida:

```bash
PROJECT=daily_sales
RUN_DATE=AAAA-MM-DD
GOLD_DIR="/lakehouse/gold/$PROJECT/run_date=$RUN_DATE"
GOLD_URI="hdfs://hdfs-namenode:9000$GOLD_DIR"

hdfs dfs -test -e "$GOLD_DIR/_SUCCESS" && echo 'corrida lista para consultar'
hdfs dfs -stat '%y %b %r %n' "$GOLD_DIR"
```

`-du` presenta **tamaño lógico**, **espacio consumido con réplicas** y **ruta**. Por ejemplo, si el
segundo valor es tres veces el primero, esos archivos tienen factor de réplica 3.

> `-cat` y `-tail` sirven para texto. Un Parquet es binario y normalmente está comprimido con
> Snappy; leelo con Spark en la sección 7.

## 4. Buscar y diagnosticar

```bash
hdfs dfs -find /lakehouse -name '*.csv'                                      # por nombre
hdfs dfs -find /lakehouse -name '_SUCCESS' | head -20                        # salidas confirmadas
hdfs dfs -ls -R /lakehouse | awk '$1 ~ /^-/ && $5 > 1048576 {print $5, $8}'  # archivos > 1 MiB
hdfs fsck /lakehouse -files -blocks | tail -20                               # bloques y salud
hdfs dfsadmin -report | head -20                                              # capacidad y DataNodes
```

`_SUCCESS` lo escribe el committer de salida de Spark cuando termina una escritura correctamente.
Su presencia es una buena señal; su ausencia es una pista para investigar logs y archivos parciales,
no una prueba por sí sola de que el batch falló.

En este laboratorio hay un solo DataNode y los clientes escriben con réplica 3 por defecto. Por eso
`fsck` y `dfsadmin -report` muestran bloques con réplicas insuficientes aunque el filesystem esté
sano. La explicación y la corrección deliberada están en la sección 9.

## 5. Subir tu propia data

Elegí un archivo CSV local y una ruta de aterrizaje. Estos nombres se reutilizan en las secciones
siguientes:

```bash
SRC=./ventas.csv
LANDING_DIR=/lakehouse/landing/daily_sales
LANDING_FILE="$LANDING_DIR/ventas.csv"
test -f "$SRC"
```

**a) Copiar al contenedor y ejecutar `-put`.** Es el camino más directo y funciona con cualquier
ruta del host.

```bash
dc cp "$SRC" hdfs-namenode:/tmp/ventas.csv
hdfs dfs -mkdir -p "$LANDING_DIR"
hdfs dfs -put -f /tmp/ventas.csv "$LANDING_FILE"
docker exec hdfs-namenode rm -f /tmp/ventas.csv
hdfs dfs -ls -h "$LANDING_FILE"
```

**b) Sin copia intermedia, por entrada estándar.** El `-` final le indica a `-put` que lea stdin.

```bash
cat "$SRC" | docker exec -i hdfs-namenode /opt/hadoop-3.2.1/bin/hdfs \
  dfs -put -f - "$LANDING_FILE"
```

**c) Por el bind mount.** Es práctico si el archivo ya vive dentro de `spark-apps/`.

```bash
cp "$SRC" spark-apps/ventas.csv
hdfs dfs -put -f /opt/spark-apps/ventas.csv "$LANDING_FILE"
```

**d) Por WebHDFS.** Repetí el paso a), pero omití su línea de `rm` para conservar
`/tmp/ventas.csv`; luego ejecutá el comando de la sección 2. Es útil para entender la API, no más
simple que `-put` en este laboratorio.

Verificá el contenido si es texto:

```bash
hdfs dfs -stat '%y %b %r %n' "$LANDING_FILE"
hdfs dfs -cat "$LANDING_FILE" | head -5
hdfs dfs -tail "$LANDING_FILE"
```

`-put -f` reemplaza el archivo destino y `-mkdir -p` no falla si el directorio ya existe: juntos
hacen repetible la carga. Los permisos `0777` bajo `/lakehouse` son exclusivos de este laboratorio;
no representan un modelo de autorización para producción.

## 6. Hacer que un DAG lea el archivo

Cada DAG medallion tiene una variable de entorno de origen. Sin ella usa su fixture mínimo. Para
`daily_sales`, comprobá el contrato, declaralo y recreá los procesos de Airflow que leen `env_file`:

```bash
sed -n '1,/^def silver/p' dags/medallion_dags/daily_sales_medallion_dag.py

# En ops/sources.env, descomentá o agregá esta línea con la ruta que acabás de subir:
# DAILY_SALES_SOURCE_URI=hdfs://hdfs-namenode:9000/lakehouse/landing/daily_sales/ventas.csv
"${EDITOR:-vi}" ops/sources.env

dc up -d --force-recreate airflow-scheduler airflow-dag-processor
dc exec -T airflow-scheduler printenv DAILY_SALES_SOURCE_URI
```

En Airflow, dispará `medallion_daily_sales` y esperá a que finalice. Usá la fecha lógica mostrada
por esa corrida para completar `RUN_DATE` de la sección 3 y comprobá su salida:

```bash
hdfs dfs -test -e "$GOLD_DIR/_SUCCESS" && echo 'gold publicado'
```

La URI debe empezar con `hdfs://`: el driver corre en Airflow y los executors en `spark-worker`, de
modo que un `file:///home/...` de tu host no existe para el cluster. Los orígenes JSON son JSON Lines
(un objeto por línea) y las columnas son un contrato: registros inválidos van a
`/lakehouse/quarantine/<proyecto>/run_date=...`; si superan el umbral, el batch falla a propósito.

## 7. Consultar Parquet con Spark

Con `GOLD_URI` definido en la sección 3, `spark-sql` puede leer un path Parquet sin catálogo:

```bash
docker exec -w /tmp spark-master /opt/spark/bin/spark-sql --master 'local[1]' \
  --conf spark.ui.enabled=false \
  -e "select channel, sum(gross_revenue) as ingreso
      from parquet.\`$GOLD_URI\`
      group by channel"
```

`-w /tmp` evita dejar `metastore_db/` y `derby.log` en una ruta inesperada. `--master 'local[1]'`
ejecuta esta consulta de inspección dentro del contenedor, no como un job distribuido.

Cuando la exploración crece, usá PySpark. Pasar `GOLD_URI` como variable evita editar el bloque:

```bash
docker exec -e GOLD_URI="$GOLD_URI" -i airflow-scheduler python - <<'PY'
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("preview").master("local[1]")
         .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
frame = spark.read.parquet(os.environ["GOLD_URI"])
frame.printSchema()
frame.show(10, truncate=40)
spark.stop()
PY
```

En Jupyter configurá la misma URI explícita: ese contenedor no monta `core-site.xml`.

## 8. Exportar resultados

Creá un destino local explícito para no mezclar archivos generados con el código del repositorio:

```bash
EXPORT_DIR=./exports
mkdir -p "$EXPORT_DIR"
```

Para bajar una partición Parquet tal cual:

```bash
hdfs dfs -get "$GOLD_DIR" "/tmp/$PROJECT-$RUN_DATE"
dc cp "hdfs-namenode:/tmp/$PROJECT-$RUN_DATE" "$EXPORT_DIR/"
```

Para texto, `-getmerge` une los archivos. Para CSV que acabás de subir también podés escribir directo
al host; las comillas hacen que el glob lo resuelva HDFS, no tu shell:

```bash
hdfs dfs -getmerge -nl "$LANDING_DIR" /tmp/ventas_unidas.csv
dc cp hdfs-namenode:/tmp/ventas_unidas.csv "$EXPORT_DIR/ventas_unidas.csv"
hdfs dfs -cat "$LANDING_DIR/*.csv" > "$EXPORT_DIR/ventas_completo.csv"
```

`-getmerge` es solo para texto. Para llevar un Parquet como CSV, reescribilo con Spark, unilo y
copialo:

```bash
HDFS_EXPORT="/lakehouse/exports/$PROJECT/run_date=$RUN_DATE"
EXPORT_URI="hdfs://hdfs-namenode:9000$HDFS_EXPORT"

docker exec -e GOLD_URI="$GOLD_URI" -e EXPORT_URI="$EXPORT_URI" -i airflow-scheduler python - <<'PY'
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("export").master("local[1]")
         .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
frame = spark.read.parquet(os.environ["GOLD_URI"])
# coalesce(1) es aceptable para una muestra pequeña; no para tablas grandes.
frame.coalesce(1).write.mode("overwrite").option("header", True).csv(os.environ["EXPORT_URI"])
spark.stop()
PY

hdfs dfs -getmerge "$HDFS_EXPORT" /tmp/daily_sales.csv
dc cp hdfs-namenode:/tmp/daily_sales.csv "$EXPORT_DIR/daily_sales.csv"
```

Con `fs.defaultFS` apuntando al NameNode, `.csv("/tmp/export")` significa HDFS, no el disco del
contenedor. Para el disco local de un contenedor escribí `file:///tmp/export` de forma explícita.

## 9. Higiene y réplicas del laboratorio

Primero listá el objetivo; después elegí entre papelera y borrado definitivo. `-expunge` vacía la
papelera completa del usuario: no lo ejecutes como parte de una limpieza rutinaria sin revisarla.

```bash
# Sustituí la ruta por un dato desechable y comprobalo antes con -ls.
hdfs dfs -ls /lakehouse/landing/prueba.csv
hdfs dfs -rm -r /lakehouse/landing/prueba.csv                 # envía a la papelera
hdfs dfs -rm -r -skipTrash /lakehouse/landing/prueba.csv      # borra definitivamente
hdfs dfs -expunge                                              # vacía la papelera del usuario actual
```

El stack tiene un DataNode y el cliente predeterminado usa réplica 3; por eso aparecen *under
replicated blocks*. Para ajustar datos ya escritos, decidilo explícitamente y ejecutá:

```bash
hdfs dfs -setrep -w -R 1 /lakehouse
```

Para que los datos nuevos nazcan con una sola réplica, configurá `dfs.replication=1` en
`hdfs-site.xml` y hacé que llegue a **cada cliente escritor** (Airflow, Jupyter y cualquier
`spark-submit`). No lo pongas como una corrección casual en `core-site.xml`: es una propiedad HDFS y
el cliente que escribe decide el factor inicial.

## 10. Qué merece una task

Una task se gana su lugar cuando reúne varios pasos en un orden fácil de olvidar, cuando aporta
validación útil o cuando también la ejecuta CI. Un comando de una línea conviene aprenderlo y dejarlo
visible.

| Operación | ¿Task? | Motivo |
|---|---|---|
| `-ls`, `-du`, `-find`, `-cat`, `-stat` | No | Son comandos unitarios y de diagnóstico. |
| Ver data por capa y proyecto | No — sección 3 | Usa el cliente HDFS y no depende de tasks auxiliares. |
| Previsualizar Parquet | No — sección 7 | Requiere una sesión Spark configurada, pero el comando es explícito. |
| Subir un archivo | Discutible | Con stdin es una línea; el flujo con verificación tiene varios pasos. |
| Exportar Parquet a CSV | Discutible | Requiere Spark, HDFS y una copia al host. |
| Levantar, validar y probar el stack | Sí — `local:up`, `local:check`, `local:smoke` | Son operaciones repetibles y parte de la calidad del proyecto. |

Escribí un flujo a mano tres veces. Si a la tercera seguís buscando orden, escapes o rutas, ahí se
ganó su lugar en el `Taskfile.yml`.
