# El stack local, bloque por bloque

> Tramo I del stack: el entorno donde desarrollás y donde todo tiene que funcionar
> **antes** de tocar AWS. Cómo está construido el `docker-compose.yml` (HDFS + Spark +
> Jupyter + Airflow 3), el porqué de cada decisión y cómo endurecerlo sin romper la
> comodidad del desarrollo.

> [!IMPORTANT]
> **Dev y prod no son lo mismo, y la diferencia es deliberada.** Este Compose es el
> entorno de **desarrollo local**, self-contained: trae su propio HDFS y su propio
> cluster Spark. En **producción** la arquitectura es híbrida: Airflow sigue
> orquestando desde una EC2 chica, pero el cómputo Spark se delega a **EMR
> Serverless** y el storage es **S3** (`s3a://`) — **sin HDFS**. Se desarrolla acá y
> se despliega allá; el stack local no cambia. Ver
> [02](02-produccion-aws-terraform.md) y [03](03-arquitectura.md).
>
> **Qué implica en la práctica**: un job que dependa de rutas `hdfs://` escritas a
> mano funciona acá y falla allá. Parametrizá la URI base desde el principio; el
> contrato local está en [04 — DataOps local](04-dataops-local.md).

> **En este documento: LEER (~40 min) y EJECUTAR el endurecimiento de la sección 8.**
> **Salís con**: entender por qué cada servicio está donde está —no solo cómo
> levantarlo—, y con el stack endurecido lo suficiente para que sea un laboratorio y
> no una máquina abierta.

## Cómo ejecutar esta guía (contrato de copy-paste)

### Antes de empezar

Necesitás Docker Engine con Compose v2, `task` y al menos **20 GiB de RAM disponibles para Docker**.
El stack completo tiene límites de memoria que suman aproximadamente 18.25 GiB; con menos memoria, Docker puede
detener servicios. Reservá también espacio libre para imágenes, volúmenes y logs.

**EJECUTAR** desde la raíz del proyecto (la carpeta que contiene `docker-compose.yml`) para comprobar las herramientas:

```bash
docker --version
docker compose version
task --version
```

Si alguno de los tres comandos falla, instalá esa herramienta antes de continuar. No ejecutes todavía
`docker compose up`: primero necesitás crear `.env` en §8.1.

Los apartados 1–7 explican archivos que **ya existen** en el repositorio: no copies sus
fragmentos YAML al Compose ni crees archivos a partir de ellos. Los pasos ejecutables están en
§8 y §9 y usan siempre la raíz del repositorio como directorio de trabajo:
la carpeta que contiene `docker-compose.yml` y `Taskfile.yml`.

Cada instrucción que modifica algo indica una de estas acciones:

| Marca | Acción exacta |
|---|---|
| **CREAR** | Creá el archivo en la ruta indicada. Si ya existe, no ejecutes el bloque: seguí la instrucción de editar. |
| **REEMPLAZAR** | Sustituí el contenido completo del archivo indicado. Es una operación destructiva y se señala antes. |
| **EDITAR** | Abrí el archivo existente en la ruta indicada y cambiá solo la línea o bloque citado. No pegues el fragmento al final. |
| **EJECUTAR** | Pegá el bloque en una terminal ubicada en la raíz del proyecto; no crea ni edita archivos salvo que el texto lo diga. |

Antes de cada bloque, verificá dónde estás:

```bash
pwd                         # debe terminar en /pyspark_stack
test -f docker-compose.yml  # debe imprimir nada y devolver éxito
```

No uses los bloques YAML de las secciones 2–7 como archivos completos: son recortes para explicar
el Compose que ya está versionado. La única configuración local que debés crear manualmente es
`.env` en la raíz; `docker-compose.local-hardened.yml` ya existe y no se copia ni se edita en el
camino normal.

### Cómo leer los bloques de la guía

Cada bloque indica su archivo y su tipo. La regla es simple:

| Si dice | Significa |
|---|---|
| **Configuración Compose** | Es un recorte de `docker-compose.yml` en la raíz. No es código PySpark y no se pega en otro `.yml`. |
| **Configuración Dockerfile** | Es un recorte de un `Dockerfile` de la raíz. Define cómo se construye una imagen; no se ejecuta en Python. |
| **Código propio** | Es un `.py` de `dags/` o `spark-apps/`. Ese sí es código del proyecto. |
| **Comando** | Se ejecuta en una terminal desde la raíz del proyecto. |

En las secciones 2–7 todos los YAML son **Configuración Compose**. Son el mismo archivo
`docker-compose.yml` en la raíz del proyecto, mostrado por partes para poder leerlo.

### Ruta corta para usar el stack

Si tu objetivo es usarlo hoy y no estudiar cada servicio, seguí solo este orden:

1. Creá y completá `.env` en [§8.1](#81-secretos-en-un-env).
2. Ejecutá [§9.1](#91-arrancar) para levantarlo y [§9.1.1](#911-gate-confirmar-que-el-stack-completo-está-listo) para validarlo.
3. Abrí una URL de [§9.2](#92-urls). Para apagarlo sin perder datos, usá [§9.4](#94-bajar).

Las secciones 1–7 quedan como referencia para entender o diagnosticar el stack.

**Qué hacés en cada sección:**

| Sección | Qué hacés | Detalle |
|---|---|---|
| **1–2** | **Leer** (~10 min) | El mapa de los 4 subsistemas y el patrón de anclas YAML que evita repetir configuración |
| **3–6** | **Leer** (~20 min) | Un subsistema por sección: HDFS, Spark, Jupyter, Airflow. Se leen en orden: cada uno asume el anterior |
| **7** | **Leer** (~5 min) | Red, volúmenes y orden de arranque — por qué `depends_on` no alcanza |
| **8** | **Ejecutar** (~20 min) | Creás `.env` y validás el endurecimiento que ya está versionado |
| **9** | **Ejecutar** (~5 min) | Arrancás, verificás, accedés y bajás el stack |

> [!TIP]
> **Si lo que querés es *usar* el stack, no entenderlo**, este no es el documento:
> andá a [04 — DataOps local](04-dataops-local.md), que resume cómo levantar y probar
> los quince pipelines medallion. Volvé acá cuando algo no haga lo que esperabas.

## Índice

1. [Visión general](#1-visión-general)
2. [El patrón de anclas YAML](#2-el-patrón-de-anclas-yaml)
3. [Almacenamiento: HDFS](#3-almacenamiento-hdfs)
4. [Cómputo: Spark standalone](#4-cómputo-spark-standalone)
5. [Cliente interactivo: Jupyter](#5-cliente-interactivo-jupyter)
6. [Orquestación: Airflow 3](#6-orquestación-airflow-3)
7. [Redes, volúmenes y orden de arranque](#7-redes-volúmenes-y-orden-de-arranque)
8. [Endurecimiento del stack local](#8-endurecimiento-del-stack-local)
9. [Operar el stack: arrancar, acceder y bajar](#9-operar-el-stack-arrancar-acceder-y-bajar)
10. [Checklist de calidad](#10-checklist-de-calidad)

---

## 1. Visión general

> **En esta sección: LEER, ~5 min.**
> **Salís con**: el mapa de los 4 subsistemas y la regla de red que explica la mitad
> de los errores de conexión de este stack.

El Compose levanta cuatro subsistemas en una sola red de Docker (`hadoopnet`):

| Subsistema | Servicios | Rol |
|---|---|---|
| Almacenamiento | `hdfs-namenode`, `hdfs-datanode` | Sistema de archivos distribuido |
| Cómputo | `spark-master`, `spark-worker` | Cluster Spark 4.2.0 standalone |
| Interactivo | `jupyter` | Driver PySpark para trabajo exploratorio |
| Orquestación | `airflow-*` (6) + `airflow-db` | Airflow 3.2.2 + Postgres 16 |

**Los comandos del día a día están en el `Taskfile.yml` de la raíz**, versionado, para que sean los
mismos en tu máquina y en el CI. Este checkout expone únicamente tasks locales; los comandos AWS de
la guía 02 son referencia y requieren los artefactos de producción que no están en este árbol:

| Task | Qué hace |
|---|---|
| `task local:check` | Valida secretos, permisos y el Compose efectivo sin arrancar servicios |
| `task local:up` | Valida y levanta los cuatro subsistemas con el override endurecido |
| `task local:smoke` | Ejecuta Web Events end-to-end y exige evidencias en las cinco capas HDFS |
| `task local:down` | Baja el stack **conservando** los volúmenes (los datos de HDFS y Postgres siguen ahí) |
| `task local:credentials` | Muestra los accesos locales de Airflow y la URL con token de Jupyter |
| `task local:urls` | Lista las URLs locales y marca qué servicio está arriba |
| `task --list` | El catálogo completo, incluidas las tasks de producción de la [guía 02](02-produccion-aws-terraform.md) |

No son obligatorias: cuando la guía invoca Compose directamente muestra los dos archivos que lo
componen. El Taskfile es un atajo versionado para el uso diario, una vez que ya conocés el stack.

Regla base: dentro de una red de Compose, el nombre del servicio **es** el hostname DNS. Por eso
`spark://spark-master:7077` y `hdfs://hdfs-namenode:9000` resuelven solos. Nunca uses `localhost`
entre contenedores: dentro de un contenedor, `localhost` es ese mismo contenedor.

```
                         red: hadoopnet
  ┌────────────────┐   ┌──────────────┐   ┌──────────────────────────┐
  │  HDFS          │   │  Spark       │   │  Airflow 3               │
  │  namenode :9000│◄──┤ master :7077 │◄──┤ scheduler / dag-processor│
  │  datanode      │   │ worker       │   │ api-server / triggerer   │
  └────────────────┘   └──────┬───────┘   │ init (one-shot)          │
                              │           └────────────┬─────────────┘
                        ┌─────▼────┐            ┌──────▼──────┐
                        │ jupyter  │            │ postgres 16 │
                        └──────────┘            └─────────────┘
```

---

## 2. El patrón de anclas YAML

> **En esta sección: LEER, ~5 min.**
> **Salís con**: saber por qué el Compose no repite 20 líneas por servicio, y cómo
> tocar la configuración común sin editarla en cinco lugares.

Es la sección que hace legible todo lo que viene después: si no reconocés `&anchor` y
`<<: *anchor`, los bloques de las secciones 3–6 van a parecer incompletos.

**Archivo:** `docker-compose.yml`
**Tipo:** configuración Compose compartida; no es un servicio ni código propio.
**Acción:** solo leer.

```yaml
x-airflow-common: &airflow-common
  image: pyspark_stack-airflow:3.2.2
  build:
    context: .
    dockerfile: Dockerfile.airflow
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    ...
  volumes: [...]
  networks: [hadoopnet]
```

- `x-airflow-common:` — cualquier clave con prefijo `x-` es una *extension field*: Compose la ignora
  como servicio y solo sirve de plantilla.
- `&airflow-common` — define un ancla YAML: «guardá este bloque».
- `<<: *airflow-common` — el *merge* en cada servicio: «pegá acá el bloque anclado».

Los cuatro procesos persistentes de Airflow y el inicializador comparten imagen, entorno y
volúmenes. Sin este patrón habría cinco copias idénticas de unas 40 líneas.

**Por qué `image:` y `build:` juntos:** con ambos, Compose construye la imagen una vez y le asigna
ese tag; los cinco servicios principales de Airflow la reutilizan. Sin el `image:` explícito, cada
servicio podría construir la suya: cinco imágenes duplicadas de unos 7 GB, y el riesgo de que un `up` agarre
una imagen vieja.

Variables de entorno clave:

| Variable | Por qué |
|---|---|
| `AIRFLOW__CORE__EXECUTOR: LocalExecutor` | Las tasks corren como procesos locales del scheduler. No requiere Celery ni Redis; alcanza para desarrollo y cargas moderadas. |
| `AIRFLOW__CORE__AUTH_MANAGER: ...FabAuthManager` | En Airflow 3 el RBAC se movió al provider FAB. Sin esto, `airflow users create` no existe. |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | La conexión a la BD se mudó de `[core]` a `[database]` en Airflow 3. |
| `AIRFLOW__CORE__LOAD_EXAMPLES: 'False'` | No ensuciar la UI con DAGs de ejemplo. |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | Nuevo en Airflow 3: scheduler y tasks hablan con el api-server por la Task Execution API. Debe apuntar a `http://airflow-apiserver:8080/...`, nunca a localhost. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Reemplaza al viejo `WEBSERVER__SECRET_KEY`; firma los JWT que autentican a las tasks. |
| `AIRFLOW_UID: 50000` | UID del usuario `airflow` dentro de la imagen; alinea permisos con los volúmenes montados. |

Volúmenes compartidos:

**Archivo:** `docker-compose.yml`, dentro de `x-airflow-common`.
**Tipo:** configuración Compose; los directorios de la izquierda (`./dags` y
`./spark-apps`) sí contienen código propio, pero estas líneas solo los montan dentro de Airflow.

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./spark-apps:/opt/spark-apps                                        # jobs compartidos con el cluster
    - ./hadoop-config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml  # config del cliente HDFS
```

> El stack **no** monta `docker.sock`: ningún DAG usa `DockerOperator`. Montarlo daría control del
> host a los servicios de Airflow que heredan este ancla (§8.4).

---

## 3. Almacenamiento: HDFS

> **En esta sección: LEER, ~5 min.**
> **Salís con**: entender el par namenode/datanode y por qué su volumen es lo único
> del stack que no se puede recrear alegremente.

> [!NOTE]
> **HDFS es solo local.** En producción no existe: el storage es S3 (`s3a://`). Está
> acá para que puedas practicar el modelo de archivos distribuido sin pagar nada, no
> porque sea el destino. El layout obligatorio se documenta en
> [04 — DataOps local](04-dataops-local.md#contrato-obligatorio-de-almacenamiento-hdfs).

**Archivo:** `docker-compose.yml`, servicios `hdfs-namenode` y `hdfs-datanode`.
**Tipo:** configuración Compose; no es código HDFS ni hay que crear un segundo YAML.
**Acción:** solo leer.

```yaml
  hdfs-namenode:
    image: chandravenkat/hadoop-namenode@sha256:51ad92...
    environment:
      - CLUSTER_NAME=hadoop-cluster
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:9000
    ports: ["127.0.0.1:9870:9870"]
    volumes: [hdfs-nn-data:/hadoop/dfs/name, ./spark-apps:/opt/spark-apps]

  hdfs-datanode:
    image: chandravenkat/hadoop-datanode@sha256:ddf6e9...
    depends_on: [hdfs-namenode]
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:9000
      - HDFS_CONF_dfs_replication=1
    volumes: [hdfs-dn-data:/hadoop/dfs/data, ./spark-apps:/opt/spark-apps]
```

- **Namenode y datanode:** el namenode guarda metadatos (qué bloque vive dónde), el datanode guarda
  los datos. Por eso el datanode declara `depends_on: hdfs-namenode`.
- **`CORE_CONF_*` / `HDFS_CONF_*`:** las imágenes de este estilo traducen esas variables a entradas
  de `core-site.xml` y `hdfs-site.xml` durante el arranque.
- **`dfs_replication=1`:** con un solo datanode, replicar no aporta nada y genera warnings de bloques
  *under-replicated*.
- **Imágenes fijadas por `@sha256`:** pin inmutable, reproducibilidad exacta frente a un tag mutable
  como `:latest`.
- **Volúmenes nombrados:** los datos sobreviven a `docker compose down`; solo `down -v` los borra.

---

## 4. Cómputo: Spark standalone

> **En esta sección: LEER, ~5 min.**
> **Salís con**: el modelo master/worker, y de dónde sale la URL
> `spark://spark-master:7077` que van a usar todos los `spark-submit`.

> [!NOTE]
> **En producción este cluster no existe**: el cómputo se delega a EMR Serverless
> ([02 §6.4](02-produccion-aws-terraform.md#64-cómputo-spark-emr-serverless)). Lo que **sí** viaja
> es tu código: la lógica de transformación es la misma y por eso conviene mantenerla
> desacoplada del I/O mediante el runtime compartido.

**Archivo:** `docker-compose.yml`, servicios `spark-master` y `spark-worker`.
**Tipo:** configuración Compose; inicia procesos de Spark, no contiene una transformación PySpark.
**Acción:** solo leer.

```yaml
  spark-master:
    build: { context: ., dockerfile: Dockerfile.spark }
    image: pyspark_stack-spark:4.2.0
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.master.Master",
              "--host", "spark-master", "--port", "7077", "--webui-port", "8080"]
    ports: ["127.0.0.1:7077:7077", "127.0.0.1:8081:8080"]

  spark-worker:
    build: { context: ., dockerfile: Dockerfile.spark }
    image: pyspark_stack-spark:4.2.0
    depends_on: [spark-master]
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
```

La decisión no obvia es el `entrypoint`. La imagen oficial `apache/spark` está pensada para
`spark-submit` y Kubernetes, no para un cluster standalone persistente: los scripts
`sbin/start-master.sh` y `start-worker.sh` **daemonizan**, el script termina y Docker mata el
contenedor (PID 1 terminado → `Exited(0)`). La solución es arrancar la clase Java en foreground con
`spark-class`, de modo que el proceso Master/Worker sea el PID 1.

- `--host spark-master`: el master anuncia ese hostname para que el worker y los drivers lo
  encuentren; debe coincidir con el nombre del servicio.
- `8081:8080`: la UI del master corre en el `8080` interno y se publica en `8081` para evitar
  colisiones con otras herramientas; Airflow se publica por separado en el host `8082`.
- `Dockerfile.spark` instala Python 3.14 (la base trae 3.10) y fija `PYSPARK_PYTHON=python3.14`: los
  executors deben correr el mismo minor de Python que el driver o Spark aborta con
  `[PYTHON_VERSION_MISMATCH]`.
- El worker anuncia por defecto 4 cores y 3 GiB para no crear un proceso Python por cada core del
  portátil. `SPARK_WORKER_CORES` y `SPARK_WORKER_MEMORY` permiten ampliar esa capacidad desde
  `.env` sin modificar el Compose.

**Archivo relacionado:** `Dockerfile.spark`.
**Tipo:** configuración Docker propia. Define la imagen `pyspark_stack-spark:4.2.0`; no es un job
de Spark ni se ejecuta con `spark-submit`.

> `spark-history-server` está comentado en el Compose. Descomentarlo da la UI de jobs terminados
> leyendo `./spark-events` (§8.5).

---

## 5. Cliente interactivo: Jupyter

> **En esta sección: LEER, ~5 min.**
> **Salís con**: entender que Jupyter acá es un **driver de PySpark**, no un servicio
> más: se conecta al master y ejecuta en los workers.

> [!WARNING]
> **Jupyter corre bajo el perfil `dev` y no debe llegar a producción.** Sin token es
> ejecución remota de código para cualquiera que alcance el puerto — por eso
> `JUPYTER_TOKEN` está en el checklist de la sección 9 y por eso el stack de
> producción ([02 §14.1](02-produccion-aws-terraform.md#141-docker-composeprodyml--base)) no lo
> incluye.

**Archivo:** `docker-compose.yml`, servicio `jupyter`.
**Tipo:** configuración Compose; el contenido que escribas en `./notebooks` sí será código propio.
**Acción:** solo leer.

```yaml
  jupyter:
    build: { context: ., dockerfile: Dockerfile.jupyter }
    image: pyspark_stack-jupyter:4.2.0
    profiles: ["dev"]                      # solo arranca bajo el perfil dev
    ports: ["127.0.0.1:8888:8888", "127.0.0.1:4055:4040"]
    depends_on: [spark-master]
    volumes:
      - ./notebooks:/opt/notebooks
      - ./spark-apps:/opt/spark-apps
      - ./spark-events:/tmp/spark-events
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - PYSPARK_PYTHON=python3.14
      - PYSPARK_DRIVER_PYTHON=python3.14
      - JUPYTER_TOKEN=${JUPYTER_TOKEN:?define JUPYTER_TOKEN en .env}
```

- **`profiles: ["dev"]`:** Jupyter es una herramienta de desarrollo. Un `docker compose up` pelado no
  lo levanta: hace falta `COMPOSE_PROFILES=dev` en el `.env` (así viene en `.env.example`) o
  `docker compose --profile dev up`.

  > **No crees el `.env` todavía solo para leer esta sección.** La creación completa y verificable
  > está en [§8.1](#81-secretos-en-un-env). A diferencia del `.env` de producción (guía 02 §13.4),
  > este es un único archivo local: se crea una vez en la raíz del repositorio y no crece por
  > secciones.
- **`Dockerfile.jupyter` se construye sobre `apache/spark:4.2.0`**, no sobre la clásica
  `jupyter/pyspark-notebook`, que solo llega a Spark 3.5. Así el driver corre exactamente el mismo
  Spark que el cluster; encima se agregan JupyterLab y Python 3.14.
- **`4055:4040`:** la Spark UI del driver vive en el `4040` interno y se publica en `4055` para no
  chocar con otros drivers.
- **`JUPYTER_TOKEN` explícito:** Compose usa el `.env` para sustituir en el YAML, no lo inyecta en el
  proceso. Sin esta línea el token nunca llega al contenedor y JupyterLab levanta **sin
  autenticación**.

**Archivo relacionado:** `Dockerfile.jupyter`.
**Tipo:** configuración Docker propia. Construye la imagen de Jupyter; los notebooks reales están
en `notebooks/` y son los que contienen tu código.

---

## 6. Orquestación: Airflow 3

> **En esta sección: LEER, ~10 min.** Es la más densa del documento.
> **Salís con**: saber qué hace cada proceso de Airflow 3 y por qué el
> monolito `webserver`+`scheduler` de Airflow 2 ya no existe.

Importa más que las otras porque **Airflow es lo único de este Compose que sobrevive
tal cual a producción**: la EC2 corre estos mismos procesos
([02 §14.1](02-produccion-aws-terraform.md#141-docker-composeprodyml--base)). Lo que cambia allá es
lo que los rodea, no ellos.

Airflow 3 separó el viejo monolito (`webserver` + `scheduler`) en procesos independientes; todos
heredan de `*airflow-common`.

**Archivo:** `docker-compose.yml`, servicios `airflow-*`.
**Tipo:** configuración Compose; los DAGs que Airflow lee son código propio dentro de
`dags/`, pero no aparecen en este bloque.
**Acción:** solo leer.

```yaml
  airflow-init:
    <<: *airflow-common
    depends_on: { airflow-db: { condition: service_healthy } }
    command: >
      bash -c "
        airflow db migrate &&
        airflow fab-db migrate &&
        (airflow users create --username ${AIRFLOW_ADMIN_USER:-admin} ... ||
         airflow users reset-password --username ${AIRFLOW_ADMIN_USER:-admin} ...)"

  airflow-apiserver:
    <<: *airflow-common
    restart: always
    command: api-server
    ports: ["127.0.0.1:8082:8080"]
    depends_on:
      airflow-init: { condition: service_completed_successfully }

  airflow-scheduler:     { command: scheduler }
  airflow-dag-processor: { command: dag-processor }
  airflow-triggerer:     { command: triggerer }
```

| Servicio | Rol | Nota de Airflow 3 |
|---|---|---|
| `airflow-init` | Migra el esquema y crea el admin, luego sale | `db migrate` reemplaza a `db upgrade`; `fab-db migrate` crea las tablas de auth |
| `airflow-apiserver` | Sirve UI y API REST | Reemplaza a `webserver`; firma y valida los JWT |
| `airflow-scheduler` | Programa y despacha tasks | Ya no parsea DAGs |
| `airflow-dag-processor` | Parsea los `.py` de `dags/` | Proceso nuevo y separado |
| `airflow-triggerer` | Corre operadores deferrables (I/O async) | Estándar en Airflow 3 |
| `airflow-log-cleaner` | Aplica edad y tope de tamaño al volumen de logs | Servicio operativo del stack, no un proceso de Airflow |

Dependencias de arranque:

- `airflow-db: condition: service_healthy` → esperar a que Postgres pase su healthcheck
  (`pg_isready`), no solo a que el contenedor exista.
- `airflow-init: condition: service_completed_successfully` → los procesos long-running esperan a que
  la migración termine bien. Evita el clásico «la tabla no existe».

**Archivo:** `docker-compose.yml`, servicio `airflow-db`.
**Tipo:** configuración Compose de Postgres; no es SQL ni código propio.
**Acción:** solo leer.

```yaml
  airflow-db:
    image: postgres:16.14-bookworm@sha256:64154d0babcb1741988719e703419af0382b19953706149f9872fbd0f438efa8
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:?define POSTGRES_PASSWORD en .env}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
    volumes: [postgres_data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-airflow}"]
      interval: 5s
      timeout: 5s
      retries: 10
```

El healthcheck es justamente lo que habilita el `condition: service_healthy`: sin él, Compose solo
sabe que el contenedor arrancó, no que la base acepta conexiones.

---

## 7. Redes, volúmenes y orden de arranque

> **En esta sección: LEER, ~5 min.**
> **Salís con**: entender por qué `depends_on` no alcanza y qué volumen perdés si
> corrés `docker compose down -v` sin pensarlo.

> **La regla que más se olvida**: `depends_on` espera a que el contenedor **arranque**,
> no a que el servicio **esté listo**. Sin healthcheck, Airflow puede intentar migrar
> contra un Postgres que todavía no acepta conexiones, fallar, y dejarte un error que
> no menciona a Postgres por ningún lado. Es exactamente lo que resuelve la
> sección 8.2.

**Archivo:** `docker-compose.yml`, secciones finales `volumes` y `networks`.
**Tipo:** configuración Compose global; no se agrega dentro de un servicio.
**Acción:** solo leer.

```yaml
volumes:
  postgres_data:   # BD de Airflow
  hdfs-nn-data:    # metadatos de HDFS
  hdfs-dn-data:    # bloques de HDFS
  airflow_logs:    # task logs persistentes con retención acotada

networks:
  hadoopnet:       # una sola red bridge; DNS por nombre de servicio
```

- **Volúmenes nombrados:** los gestiona Docker en `/var/lib/docker/volumes`; su ciclo de vida es el
  de §3.
- **Bind mounts** (`./dags`, `./spark-apps`): carpetas del host mapeadas dentro del contenedor,
  ideales para editar código en caliente.
- **Una sola red** simplifica el DNS. En producción se podría segmentar (datos y orquestación) para
  aislar tráfico.

Orden efectivo de arranque, resuelto por `depends_on`:

```
airflow-db (healthy)
    └─► airflow-init (completa la migración)
            └─► apiserver, scheduler, dag-processor, triggerer, log-cleaner
hdfs-namenode ─► hdfs-datanode
spark-master  ─► spark-worker
spark-master  ─► jupyter   (solo bajo el perfil dev; no espera al worker)
```

Que el master esté arriba no garantiza que haya workers registrados: un notebook lanzado demasiado
pronto queda esperando executors.

---

## 8. Endurecimiento del stack local

> **En esta sección: EJECUTAR, ~20 min.** Creás el único archivo local no versionado (`.env`);
> el resto del endurecimiento ya está en archivos versionados.
> **Salís con**: secretos propios en un `.env` fuera de git, límites de memoria,
> healthchecks reales, logs persistentes pero acotados y `docker.sock` fuera del stack.

### Mapa del camino — sección 8

**Antes de empezar, el prerrequisito** es completar `.env` y ejecutar `task local:check`.

```mermaid
flowchart TD
    E1["§8.1 · Secretos en un .env<br/><i>openssl, no los defaults</i>"]
    E2["§8.2 · Override de endurecimiento<br/><i>límites, healthchecks y restart</i>"]
    E3["§8.3 · Secretos parametrizados en el base<br/><i>ya está: el Compose interpola</i>"]
    E4["§8.4 · docker.sock fuera del stack<br/><i>ya está: no se monta</i>"]
    E5["§8.5 · History server (opcional)<br/><i>para ver los jobs ya terminados</i>"]
    GATE["✅ Gate del stack local<br/>checklist de la sección 9 completo ·<br/>un DAG corre verde ·<br/>nada sensible en git"]

    E1 --> E2 --> E3 --> E4 --> GATE
    E2 -.opcional.-> E5

    style GATE fill:#d4edda,stroke:#155724
    style E5 fill:#fff3cd,stroke:#856404
```

**Reglas de esta sección:**

- **El endurecimiento de recursos va en el override versionado.** Los controles que deben estar
  siempre activos —secretos obligatorios, loopback y rotación de logs— viven en el Compose base.
- **`.env` nunca se commitea.** Está en `.gitignore`; `.env.example` es el que viaja,
  con placeholders. Un secreto commiteado sigue en la historia aunque lo borres
  después.
- **El build no recibe el repositorio completo.** `.dockerignore` sólo permite
  `requirements.txt`, que es el único archivo copiado por los Dockerfiles.
- **No existen defaults para secretos.** Si falta uno, Compose aborta antes de crear contenedores;
  `task local:check` además rechaza longitudes y valores conocidos inseguros.

> **Gotcha §8.1 — cambiar `POSTGRES_PASSWORD` con el volumen ya creado no hace nada.**
> Postgres solo aplica esas variables al **inicializar** el volumen de datos. Si ya
> levantaste el stack con la contraseña vieja, la nueva se ignora en silencio y vas a
> creer que la rotaste. Hay que recrear el volumen (perdiendo la metadata de Airflow)
> o cambiarla por SQL dentro del contenedor.

Lo que es aceptable en desarrollo pero no en producción:

| # | Problema | Riesgo | Estado |
|---|---|---|---|
| 1 | Secretos con defaults débiles | Arranque accidental con credenciales conocidas | Resuelto: obligatorios + gate local (§8.1) |
| 2 | Sin `restart` en HDFS, Spark y Jupyter | Un crash deja el servicio caído | Resuelto en override versionado (§8.2) |
| 3 | Sin healthchecks salvo en Postgres | `depends_on` no sabe si el servicio *funciona* | Resuelto en base/override (§8.2) |
| 4 | Sin límites de recursos | Un job de Spark puede comerse toda la RAM del host | Resuelto en override versionado (§8.2) |
| 5 | Jupyter sin token | Cualquiera en la red entra | Resuelto: token obligatorio y puerto loopback (§8.1) |
| 6 | Montaje de `docker.sock` | Control del host para todos los procesos de Airflow | Resuelto: no se monta (§8.4) |
| 7 | Clave `version:` obsoleta | Warning en cada comando de Compose | Resuelto: eliminada |
| 8 | Logs locales sin límite | El disco puede llenarse aunque los contenedores sigan sanos | Resuelto: rotación + retención acotada |

### Política de logs aplicada

El Compose base ya protege las dos clases de logs, sin depender del override de endurecimiento:

- `stdout/stderr` de **todos** los contenedores usa `json-file` con `max-size: 10m` y
  `max-file: 3`: cada servicio conserva aproximadamente 30 MiB como máximo.
- Los task logs de Airflow viven en el volumen nombrado `airflow_logs`, por lo que sobreviven a
  una recreación de contenedores y a `docker compose down`.
- `airflow-log-cleaner` revisa cada 15 minutos: elimina archivos de más de 30 días y, si el volumen
  aún supera 1 GiB, borra primero los más antiguos. La edad conserva contexto y el tope protege
  también ante una tormenta de logs. Los valores se parametrizan en `.env`:

```dotenv
AIRFLOW_LOCAL_LOG_RETENTION_DAYS=30
AIRFLOW_LOCAL_LOG_MAX_SIZE_MB=1024
AIRFLOW_LOG_CLEANUP_INTERVAL_MINUTES=15
```

Una poda inmediata y una comprobación de espacio se pueden ejecutar así:

```bash
docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml run --rm --no-deps airflow-log-cleaner bash /opt/pyspark-stack/ops/airflow_log_retention.sh --once
docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml exec airflow-log-cleaner du -sh /opt/airflow/logs
```

`docker compose down -v` sí elimina deliberadamente `airflow_logs`, Postgres y HDFS; no lo use
como parada rutinaria. La retención por edad limita el histórico y la rotación limita ráfagas de
los contenedores. En producción, la copia durable va a S3 y se elimina del host tras subirla
([guía 02 §14.1](02-produccion-aws-terraform.md#141-docker-composeprodyml--base)).

### 8.1 Secretos en un `.env`

**Ubicación:** `.env` (la raíz del repositorio, junto a
`docker-compose.yml`). **No** va dentro de `docs/`, `dags/` ni `scripts/`.

#### Primera instalación — CREAR `.env`

**Precondición:** no debe existir `.env`. El bloque crea el archivo desde el template, conserva
los comentarios y deja en blanco únicamente los cuatro valores secretos. Después los completás en
el mismo archivo; no crees un segundo `.env`.

```bash
test ! -e .env || { echo '.env ya existe: usá el procedimiento EDITAR de abajo'; exit 1; }
cp .env.example .env
chmod 600 .env
```

#### Completar secretos — EDITAR `.env`

Abrí **el archivo que acabás de crear** y reemplazá solo el texto vacío después de `=` en estas
cuatro líneas. Conservá los demás nombres y valores del template. Para cada valor, ejecutá el
comando indicado en una terminal, copiá su salida y pegala a la derecha del `=`.

| Línea que editás en `.env` | Ejecutá para generar el valor | Resultado esperado |
|---|---|---|
| `POSTGRES_PASSWORD=` | `openssl rand -hex 24` | 48 caracteres hexadecimales |
| `AIRFLOW_JWT_SECRET=` | `openssl rand -hex 32` | 64 caracteres hexadecimales |
| `AIRFLOW_ADMIN_PASSWORD=` | `openssl rand -hex 24` | 48 caracteres hexadecimales |
| `JUPYTER_TOKEN=` | `openssl rand -hex 32` | 64 caracteres hexadecimales |

Al terminar, las cuatro líneas tienen este aspecto (los valores mostrados son marcadores: no los
copies literalmente):

```dotenv
POSTGRES_PASSWORD=<valor-generado-para-postgres>
AIRFLOW_JWT_SECRET=<valor-generado-para-jwt>
AIRFLOW_ADMIN_PASSWORD=<valor-generado-para-admin>
JUPYTER_TOKEN=<valor-generado-para-jupyter>
```

#### Si `.env` ya existe — EDITAR, no copiar ni reemplazar

No vuelvas a ejecutar `cp .env.example .env`: sobreescribiría tu configuración local. Abrí
`.env`, completá solo los valores que estén vacíos y mantené el permiso privado:

```bash
chmod 600 .env
task local:check
```

Si `task local:check` falla, corregí la línea que indique y repetilo. No avances a §8.2 hasta que
termine correctamente.

### 8.2 Override de endurecimiento

Usá un override que Compose fusiona para añadir `restart`, healthchecks y límites de memoria. La
rotación y retención de logs ya están en el Compose base y no se duplican aquí.

**Archivo existente, no editar ni copiar:**
[`docker-compose.local-hardened.yml`](../docker-compose.local-hardened.yml). Ya está
versionado y `task local:up` siempre lo combina con el Compose base. En el camino normal de esta
guía, solo verificás que Docker pueda fusionarlo; no pegues sus servicios dentro de
`docker-compose.yml`:

```bash
task local:check
```

**Resultado esperado:** el comando termina con código 0 y no imprime errores de interpolación ni
de YAML. El arranque viene en §9.1, después de terminar las decisiones opcionales de esta sección.

> Este override endurece el **stack local completo**, útil si querés correrlo así en una sola
> máquina. No confundir con producción: el Compose de producción de la
> [guía 02 §14.1](02-produccion-aws-terraform.md) **no levanta** HDFS ni Spark —en la EC2 solo corren
> Airflow, Postgres y el monitoreo— porque el cómputo va a EMR Serverless y el storage a S3.

### 8.3 Secretos parametrizados en el Compose base

**Archivo existente, solo lectura:** `docker-compose.yml`. Ya está aplicado: el
Compose usa `${VAR:?mensaje}` para los cuatro secretos. No pegues estos fragmentos ni añadas otra
sección `environment:`; son evidencia de lo que validaste en §8.1. Sin `.env`, o con un valor
vacío, la expansión falla antes de arrancar.

**Recorte 1:** servicio `airflow-db` del mismo archivo. **Tipo:** configuración Compose.

```yaml
  airflow-db:
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:?define POSTGRES_PASSWORD en .env}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
```

**Recorte 2:** ancla `x-airflow-common` del mismo archivo. **Tipo:** configuración Compose.

```yaml
x-airflow-common: &airflow-common
  environment: &airflow-common-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:?define POSTGRES_PASSWORD en .env}@airflow-db:5432/${POSTGRES_DB:-airflow}
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:?define AIRFLOW_JWT_SECRET en .env}'
```

> Los nombres de usuario y base conservan defaults no sensibles; las contraseñas, JWT y token no.
> El Compose de producción deberá cargarlos desde SSM antes de arrancar
> ([guía 02 §13](02-produccion-aws-terraform.md)).

### 8.4 Mantener `docker.sock` fuera del stack

**No hay acción de archivo.** El Compose actual no monta el socket; verificá que no agregaste esta
línea en `docker-compose.yml` ni en `docker-compose.local-hardened.yml`:

**Tipo:** ejemplo de configuración prohibida; esta línea no debe existir en ningún archivo.

```yaml
    - /var/run/docker.sock:/var/run/docker.sock   # no agregar
```

Si algún caso futuro exige `DockerOperator`, aislalo en un ejecutor dedicado y evaluá un socket-proxy
con API limitada. No lo heredes en api-server, scheduler, triggerer y dag-processor a la vez.

### 8.5 Añadir el history-server (opcional)

Elegí esta opción solo si necesitás consultar jobs de Spark **ya terminados**. Cambia dos archivos
versionados, por lo que conviene hacerla en una rama y conservar el cambio en Git.

1. **EDITAR** `docker-compose.yml`. Buscá el bloque completo que comienza con
   `#  spark-history-server:` (cerca del servicio `spark-worker`) y quitá el `#` inicial de **cada
   una de sus líneas**, hasta antes del comentario `# Jupyter con pyspark`. No pegues un segundo
   servicio al final del archivo. Dentro de ese bloque, reemplazá la publicación
   `"18080:18080"` por `"127.0.0.1:18080:18080"` para que la UI quede limitada a tu máquina.
2. **EDITAR** `spark-events/spark-defaults.conf`. Reemplazá exactamente la línea
   existente `spark.eventLog.enabled           false` por:

   ```properties
   spark.eventLog.enabled           true
   ```

3. **EJECUTAR** desde la raíz del proyecto para reconstruir/arrancar el servicio nuevo:

   ```bash
   task local:up
   docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml ps spark-history-server
   ```

   El servicio queda disponible en <http://localhost:18080>. Si no vas a usar el History Server,
   dejá ambos archivos como están: los event logs quedarían sin consumidor.

---

## 9. Operar el stack: arrancar, acceder y bajar

> **En esta sección: EJECUTAR, ~5 min.**
> **Salís con**: el stack corriendo, las cuatro UIs abiertas con sus credenciales, y
> el comando de apagado que conserva tus datos.

**Directorio para todos los comandos de esta sección:** la raíz del proyecto. Esta sección no crea
ni edita archivos: usa el `.env` que creaste y validaste en §8.1.

### 9.1 Arrancar

**EJECUTAR:**

```bash
task local:check   # valida los cuatro secretos, el chmod 600 y el Compose combinado
task local:up      # construye lo que falte y levanta el stack en segundo plano
```

`local:up` depende de `local:check`: si falta el `.env`, un secreto es débil o el archivo no es
privado, aborta antes de tocar Docker.

La primera vez construye las tres imágenes propias y descarga las imágenes base, dependencias de
Python y el bundle del SDK de AWS; puede mover varios GiB y tardar varios minutos según tu conexión.
Las corridas siguientes reutilizan la caché y suelen ser mucho más rápidas.

Un build largo no muestra nada hasta terminar. Para verlo avanzar en vivo:

**EJECUTAR solo si necesitás ver el progreso del primer build:**

```bash
# --progress va ANTES del subcomando: en "up" es una flag desconocida
docker compose --progress plain -f docker-compose.yml -f docker-compose.local-hardened.yml up -d --build
```

Verificá el resultado:

**EJECUTAR para verificar el arranque:**

```bash
docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml ps
# los servicios persistentes; airflow-init y hdfs-init terminan con código 0
```

`airflow-init` corre las migraciones y crea el usuario admin: sale con código 0 y no vuelve a
levantar. Los cuatro procesos persistentes de Airflow y el servicio de retención de logs arrancan
recién cuando ese init termina.

### 9.1.1 Gate: confirmar que el stack completo está listo

No abras Jupyter ni ejecutes un ejemplo todavía. Con `COMPOSE_PROFILES=dev` (el valor del template),
**EJECUTAR** estos cuatro comandos, en este orden; cada uno debe terminar sin error:

```bash
docker exec spark-master curl -fsS http://localhost:8080 >/dev/null
docker exec hdfs-namenode hdfs dfs -ls /
docker exec airflow-scheduler airflow dags list
docker exec jupyter-notebook python3.14 -c 'import os; from pyspark.sql import SparkSession; spark = SparkSession.builder.master(os.environ["SPARK_MASTER"]).getOrCreate(); print(spark.range(1).count()); spark.stop()'
```

El último comando crea un trabajo mínimo: confirma que Jupyter llega al master y que Spark tiene un
worker disponible. Si dejaste `COMPOSE_PROFILES` vacío, Jupyter no arranca por diseño: omití ese
cuarto comando y no uses su URL.

Si los comandos que correspondan terminan correctamente, están listos Spark, HDFS, Airflow y Jupyter.
Después ejecutá `task local:smoke`, descrito en [04 — DataOps local](04-dataops-local.md#operación-local).
Si uno falla, no ejecutes los pipelines: revisá `docker compose -f docker-compose.yml -f
docker-compose.local-hardened.yml ps` y resolvé ese servicio primero.

### 9.2 URLs

Todo queda atado a `127.0.0.1`, nada expuesto fuera de tu máquina.

| Servicio | URL | Para qué |
|---|---|---|
| Airflow | <http://localhost:8082> | activar y monitorear los DAGs |
| Jupyter | <http://localhost:8888> | notebooks contra el clúster Spark |
| Spark Master | <http://localhost:8081> | workers registrados y apps en curso |
| HDFS NameNode | <http://localhost:9870> | explorar el filesystem y los bloques |
| Spark app UI | <http://localhost:4055> | detalle de jobs de la sesión de Jupyter |

Dos comportamientos normales que parecen fallas: el puerto **4055** no responde hasta que un
notebook crea una `SparkSession`, y el **apiserver de Airflow** tarda unos 30 segundos más que el
resto en contestar después de figurar `Up`.

### 9.3 Credenciales

Viven en el `.env`, que está fuera de git (§8.1). Para mostrarlas cuando las necesites, ejecutá:

```bash
task local:credentials
```

La tarea imprime el usuario y contraseña de Airflow, y la URL de Jupyter con su token. No compartas
su salida ni la pegues en tickets, chats o capturas.

Si necesitás leer los valores directamente desde `.env`:

```bash
grep -E '^(AIRFLOW_ADMIN_USER|AIRFLOW_ADMIN_PASSWORD)=' .env
echo "http://localhost:8888/?token=$(grep '^JUPYTER_TOKEN=' .env | cut -d= -f2)"
```

Los veinte DAGs se cargan **pausados**, que es el default de Airflow: hay que activarlos desde la UI
para que corran.

### 9.4 Bajar

Tres niveles, de menos a más destructivo:

| Qué querés | Comando | Qué pasa con los datos |
|---|---|---|
| Pausar y retomar después | `docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml stop` | intactos, los contenedores siguen existiendo |
| Liberar los contenedores | `task local:down` | intactos, los cuatro volúmenes sobreviven |
| Empezar de cero | `docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml down -v` | **se borran** los cuatro volúmenes |

`task local:down` es el apagado de todos los días: borra contenedores y red, y conserva
`postgres_data`, `hdfs-nn-data`, `hdfs-dn-data` y `airflow_logs`. Al volver a subir encontrás tus
DAGs, su historial y los datos en HDFS.

> **`down -v` es irreversible.** Te llevás puesta la base de Airflow —usuarios, historial de runs,
> conexiones—, todos los bloques de HDFS y los task logs. Es lo que hay que hacer si rotaste
> `POSTGRES_PASSWORD` en el `.env`, porque el volumen viejo conserva la contraseña anterior y
> `airflow-db` no autentica. Al levantar de nuevo, `airflow-init` recrea la base desde cero.

---

## 10. Checklist de calidad

> **En esta sección: VERIFICAR antes de pasar a producción.**
> **Salís con**: la confirmación de que el Tramo I está sano — que es el gate de
> entrada del Tramo II ([02](02-produccion-aws-terraform.md)).

- [ ] `.env` fuera de git y con secretos generados con `openssl`.
- [ ] `AIRFLOW_JWT_SECRET` único por entorno.
- [ ] `JUPYTER_TOKEN` no vacío (solo aplica en local: en producción Jupyter no corre).
- [x] `restart: unless-stopped` en todos los servicios long-running mediante el override.
- [x] Healthchecks en HDFS, Spark y Jupyter, no solo en Postgres.
- [x] Límites de memoria por servicio (`deploy.resources.limits`).
- [x] Rotación de logs Docker y retención de task logs de Airflow.
- [x] `docker.sock` fuera del stack.
- [x] Imágenes base externas pineadas por versión y `@sha256`.
- [ ] Backup de los volúmenes de Postgres y del namenode de HDFS.

> **Siguiente paso:** [03 — Arquitectura](03-arquitectura.md) para el mapa conceptual. La guía
> [02 — Producción en AWS](02-produccion-aws-terraform.md) es arquitectura objetivo y no un
> despliegue ejecutable desde este checkout.
