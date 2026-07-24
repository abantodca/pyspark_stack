# Guía profesional — Anatomía del `docker-compose.yml`

> Cómo está construido el stack (HDFS + Spark + Jupyter + Airflow 3) bloque por bloque,
> el porqué de cada decisión y un endurecimiento seguro para desarrollo local.
>
> **Edición optimizada:** distingue con claridad el Compose local del Compose de AWS, elimina
> duplicaciones y evita ocultar errores durante la inicialización de Airflow.

Índice:
1. [Visión general del stack](#1-visión-general-del-stack)
2. [El patrón de anclas YAML (`x-airflow-common`)](#2-el-patrón-de-anclas-yaml)
3. [Capa de almacenamiento: HDFS](#3-capa-de-almacenamiento-hdfs)
4. [Motor de cómputo: Spark standalone](#4-motor-de-cómputo-spark-standalone)
5. [Cliente interactivo: Jupyter](#5-cliente-interactivo-jupyter)
6. [Orquestación: Airflow 3 (5 procesos + Postgres)](#6-orquestación-airflow-3)
7. [Redes, volúmenes y orden de arranque](#7-redes-volúmenes-y-orden-de-arranque)
8. [Endurecimiento del stack local](#8-endurecimiento-del-stack-local)
9. [Checklist de calidad](#9-checklist-de-calidad)

---

## 1. Visión general del stack

El compose levanta 4 subsistemas en una sola red de Docker (`hadoopnet`):

| Subsistema | Servicios | Rol |
|---|---|---|
| Almacenamiento | `hdfs-namenode`, `hdfs-datanode` | Sistema de archivos distribuido (HDFS) |
| Cómputo | `spark-master`, `spark-worker` | Cluster Spark 4.0.3 standalone |
| Interactivo | `jupyter` | Driver PySpark para trabajo exploratorio |
| Orquestación | `airflow-*` (5) + `airflow-db` | Airflow 3 + Postgres 16 |

Regla base: dentro de una red de Compose, el nombre del servicio es el hostname DNS. Por eso
`spark://spark-master:7077` o `hdfs://hdfs-namenode:9000` resuelven solos. No uses `localhost`
entre contenedores: dentro de un contenedor, `localhost` es ese mismo contenedor.

> Dev vs prod: este compose es el entorno de DESARROLLO local, todo self-contained (HDFS +
> Spark + Jupyter + Airflow en Docker). En PRODUCCIÓN la arquitectura es híbrida: Airflow sigue
> como orquestador en una EC2 chica (`t3.large`), pero el cómputo Spark se delega a EMR
> Serverless y el storage es S3 (`s3a://`), sin HDFS. Se desarrolla acá y se despliega allá; el
> stack local no cambia. El detalle está en `02-produccion-aws-dataops-operativa-v3.md` y `03-arquitectura-mejorada.md`.

```
                         red: hadoopnet
  ┌───────────────┐   ┌──────────────┐   ┌──────────────────────────┐
  │  HDFS         │   │  Spark       │   │  Airflow 3               │
  │  namenode :9000│◄──┤ master :7077 │◄──┤ scheduler / dag-proc     │
  │  datanode     │   │ worker       │   │ api-server / triggerer    │
  └───────────────┘   └──────┬───────┘   │ init (one-shot)          │
                             │           └────────────┬─────────────┘
                        ┌────▼─────┐            ┌──────▼──────┐
                        │ jupyter  │            │ postgres 16 │
                        └──────────┘            └─────────────┘
```

---

## 2. El patrón de anclas YAML

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

Qué es y por qué:

- `x-airflow-common:` — cualquier clave con prefijo `x-` es una *extension field*: Compose la
  ignora como servicio. Sirve solo como plantilla reutilizable.
- `&airflow-common` — define un ancla YAML: "guardar este bloque en una variable".
- `<<: *airflow-common` (en cada servicio) — el *merge*: "pegá aquí todo el bloque anclado".
  Airflow 3 partió el monolito en 5 procesos que comparten imagen, env y volúmenes; sin este
  patrón habría 5 copias idénticas de ~40 líneas.

Por qué `image:` + `build:` juntos:

```yaml
  image: pyspark_stack-airflow:3.2.2   # tag fijo: los 5 servicios airflow-* reutilizan esta imagen
  build:
    context: .
    dockerfile: Dockerfile.airflow
```

Con ambos, Compose construye la imagen una vez y le asigna ese tag; los 5 servicios `airflow-*`
la reutilizan. Sin el `image:` explícito, cada servicio podría reconstruir la suya: 5 imágenes
duplicadas de ~7 GB.

Variables de entorno clave:

| Variable | Por qué |
|---|---|
| `AIRFLOW__CORE__EXECUTOR: LocalExecutor` | Las tasks se ejecutan como procesos locales en el host/contenedor del scheduler. No requiere Celery ni Redis; es adecuado para desarrollo y cargas moderadas. |
| `AIRFLOW__CORE__AUTH_MANAGER: ...FabAuthManager` | En Airflow 3 el RBAC/usuarios se movió al provider FAB. Sin esto, `airflow users create` no existe. |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | La conexión a la BD se mudó de `[core]` a `[database]` en Airflow 3. Apunta a `airflow-db` por hostname. |
| `AIRFLOW__CORE__LOAD_EXAMPLES: 'False'` | No ensuciar la UI con DAGs de ejemplo. |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | Nuevo en Airflow 3: scheduler y tasks hablan con el api-server por la Task Execution API. Debe apuntar a `http://airflow-apiserver:8080/...`, nunca a localhost. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Reemplaza al viejo `WEBSERVER__SECRET_KEY`. Firma los JWT que autentican las tasks. |
| `AIRFLOW_UID: 50000` | UID del usuario `airflow` dentro de la imagen; alinea permisos de los volúmenes montados. |

Volúmenes compartidos:

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./spark-apps:/opt/spark-apps             # los .py de Spark, compartidos con el cluster
    - ./hadoop-config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml  # config de cliente HDFS
```

> El stack no monta `docker.sock`: ninguno de los DAGs actuales usa DockerOperator. Añadirlo
> equivaldría a dar control del host a todos los procesos Airflow que heredan este anchor.

---

## 3. Capa de almacenamiento: HDFS

```yaml
# (fragmento simplificado; volúmenes completos en el compose)
  hdfs-namenode:
    image: chandravenkat/hadoop-namenode@sha256:51ad92...   # el "índice" (metadatos)
    environment:
      - CLUSTER_NAME=hadoop-cluster
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:9000
    ports: ["9870:9870"]     # UI web de HDFS
    volumes: [hdfs-nn-data:/hadoop/dfs/name]

  hdfs-datanode:
    image: chandravenkat/hadoop-datanode@sha256:ddf6e9...   # guarda los bloques reales
    depends_on: [hdfs-namenode]
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:9000
      - HDFS_CONF_dfs_replication=1
    volumes: [hdfs-dn-data:/hadoop/dfs/data]
```

El porqué:

- Namenode vs datanode: el namenode guarda metadatos (qué bloque vive dónde); el datanode guarda
  los datos. Por eso el datanode declara `depends_on: hdfs-namenode`.
- `CORE_CONF_fs_defaultFS` en ambos: las imágenes estilo `bde2020` traducen env vars
  `CORE_CONF_*` / `HDFS_CONF_*` a entradas de `core-site.xml` / `hdfs-site.xml` en el arranque.
- `dfs_replication=1`: con un único datanode, replicar no aporta y genera warnings de bloques
  under-replicated. En prod real subís esto y agregás datanodes.
- Imágenes fijadas por `@sha256:...`: pin inmutable, reproducibilidad exacta frente a un tag
  mutable como `:latest`.
- Volúmenes nombrados (`hdfs-nn-data`, `hdfs-dn-data`): los datos sobreviven a
  `docker compose down`; solo `down -v` los borra.

> En producción HDFS se reemplaza por S3 (`s3a://`); acá es el storage de trabajo para
> desarrollar y aprender.

---

## 4. Motor de cómputo: Spark standalone

```yaml
# (fragmento simplificado; volúmenes completos en el compose: ./spark-apps y ./spark-events)
  spark-master:
    build: { context: ., dockerfile: Dockerfile.spark }
    image: pyspark_stack-spark:4.0.3
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.master.Master",
              "--host", "spark-master", "--port", "7077", "--webui-port", "8080"]
    ports: ["7077:7077", "8081:8080"]

  spark-worker:
    image: pyspark_stack-spark:4.0.3
    depends_on: [spark-master]
    entrypoint: ["/opt/spark/bin/spark-class"]
    command: ["org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
```

La decisión no obvia: la imagen oficial `apache/spark` está pensada para `spark-submit` /
Kubernetes, no para un cluster standalone persistente. Los scripts `sbin/start-master.sh` /
`start-worker.sh` daemonizan: el proceso queda en segundo plano y el script termina. En Docker
eso mata el contenedor (PID 1 terminado → `Exited(0)`).

Solución: arrancar la clase Java en foreground con `spark-class`. Así el proceso Master/Worker
es el PID 1 y vive mientras viva el contenedor.

- `--host spark-master`: el master anuncia su hostname para que el worker y los drivers lo
  encuentren. Debe coincidir con el nombre del servicio.
- `ports: 8081:8080`: la UI del master corre en `8080` dentro del contenedor; se publica en
  `8081` porque `8080` ya lo usa el api-server de Airflow.
- `Dockerfile.spark` instala Python 3.12 (la base trae 3.10) y fuerza
  `PYSPARK_PYTHON=python3.12`: los executors deben correr el mismo minor de Python que el
  driver (Airflow/Jupyter, 3.12) o Spark lanza `[PYTHON_VERSION_MISMATCH]`.

> `spark-history-server` está comentado en el compose. Lee los logs de `./spark-events` para
> inspeccionar jobs terminados; queda como opcional (§8.5).

> En producción Spark corre en EMR Serverless (pago por uso, escala a cero); este Spark
> standalone es para dev local.

---

## 5. Cliente interactivo: Jupyter

```yaml
  jupyter:
    build: { context: ., dockerfile: Dockerfile.jupyter }
    image: pyspark_stack-jupyter:4.0.3
    profiles: ["dev"]        # solo arranca bajo el perfil dev (ver abajo)
    ports: ["8888:8888", "4055:4040"]
    depends_on: [spark-master]
    volumes:
      - ./notebooks:/opt/notebooks
      - ./spark-apps:/opt/spark-apps
      - ./spark-events:/tmp/spark-events
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - PYSPARK_PYTHON=python3.12
      - PYSPARK_DRIVER_PYTHON=python3.12
```

El porqué:

- `profiles: ["dev"]`: Jupyter es herramienta de desarrollo. Un `docker compose up` pelado no lo
  levanta: hace falta `COMPOSE_PROFILES=dev` en el `.env` (así viene en `.env.example`) o
  `docker compose --profile dev up`. En prod el ETL corre por Airflow y los `.ipynb` por
  papermill, sin este server.
- `Dockerfile.jupyter` construye sobre `apache/spark:4.0.3` en vez de la clásica
  `jupyter/pyspark-notebook`, que solo llega a Spark 3.5. Así el driver corre el mismo Spark
  4.0.3 que el cluster; solo se agrega JupyterLab + Python 3.12.
- `4055:4040`: la Spark UI del driver (la app del notebook) vive en `4040` interno; se publica
  en `4055` para no chocar con otros 4040.
- `SPARK_MASTER=spark://spark-master:7077`: el notebook actúa como driver contra el master
  standalone.

---

## 6. Orquestación: Airflow 3

Airflow 3 separó el viejo monolito (`webserver` + `scheduler`) en procesos independientes.
Todos heredan de `*airflow-common`:

```yaml
# (fragmento simplificado; en el compose real cada servicio long-running también declara
  airflow-init:          # one-shot: migra esquema + crea admin, luego termina
    <<: *airflow-common
    depends_on: { airflow-db: { condition: service_healthy } }
    command: >
      bash -euc '
        airflow db migrate
        airflow fab-db migrate
        airflow users list | grep -q admin ||
          airflow users create --username admin ... --password "$${AIRFLOW_ADMIN_PASSWORD}"
      '

  airflow-apiserver:     # UI + REST API (antes 'webserver'). Puerto 8080 interno
    <<: *airflow-common
    restart: always
    command: api-server
    ports: ["8082:8080"]
    depends_on:
      airflow-init: { condition: service_completed_successfully }

  airflow-scheduler:     # decide qué corre y cuándo
    command: scheduler
  airflow-dag-processor: # NUEVO en A3: parsea los DAGs en su propio proceso
    command: dag-processor
  airflow-triggerer:     # ejecuta operadores deferrables (async)
    command: triggerer
```

El rol de cada proceso:

| Servicio | Rol | Nota Airflow 3 |
|---|---|---|
| `airflow-init` | Migra el esquema y crea el admin, luego sale | `db migrate` reemplaza a `db upgrade`; `fab-db migrate` crea las tablas de auth (`ab_user`, `ab_role`…) |
| `airflow-apiserver` | Sirve UI + API REST | Reemplaza a `webserver`; es quien firma/valida los JWT |
| `airflow-scheduler` | Programa y despacha tasks | Ya no parsea DAGs |
| `airflow-dag-processor` | Parsea los `.py` de `dags/` | Proceso nuevo y separado en A3 |
| `airflow-triggerer` | Corre `deferrable operators` (I/O async) | Estándar en A3 |

Dependencias de arranque (`depends_on` con condiciones):

- `airflow-db: condition: service_healthy` → esperar a que Postgres pase su healthcheck
  (`pg_isready`), no solo a que el contenedor exista.
- `airflow-init: condition: service_completed_successfully` → los procesos long-running esperan
  a que la migración termine con éxito. Evita el clásico "tabla no existe".

Postgres 16 (`airflow-db`):

```yaml
  airflow-db:
    image: postgres:16
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
    volumes: [postgres_data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-airflow}"]
      interval: 5s
      timeout: 5s
      retries: 10
```

El healthcheck es lo que habilita el `condition: service_healthy` de arriba: sin él, Compose
solo sabe "el contenedor arrancó", no "la BD acepta conexiones".

---

## 7. Redes, volúmenes y orden de arranque

```yaml
volumes:
  postgres_data:   # BD de Airflow
  hdfs-nn-data:    # metadatos HDFS
  hdfs-dn-data:    # bloques HDFS

networks:
  hadoopnet:       # una sola red bridge; DNS por nombre de servicio
```

- Volúmenes nombrados: gestionados por Docker (viven en `/var/lib/docker/volumes`); su ciclo de
  vida ya se explicó en §3.
- Bind mounts (`./dags`, `./spark-apps`): carpetas del host mapeadas dentro; ideales para código
  que editás en caliente.
- Una sola red simplifica el DNS. En prod podrías segmentar (data / orchestration) para aislar
  tráfico.

Orden efectivo de arranque (resuelto por `depends_on`):

```
airflow-db (healthy)
    └─► airflow-init (completa migración)
            └─► apiserver, scheduler, dag-processor, triggerer
hdfs-namenode ─► hdfs-datanode
spark-master  ─► spark-worker
spark-master  ─► jupyter   (solo bajo el perfil dev; no espera al worker)
```

`jupyter` depende solo de `spark-master` (no del worker) y únicamente arranca con el perfil
`dev` activo (§5). Que el master esté arriba no garantiza que haya workers registrados: un
notebook lanzado demasiado pronto queda esperando executors.

---

## 8. Endurecimiento del stack local

Los problemas del compose actual, aceptables en dev pero no en producción:

| # | Problema | Riesgo |
|---|---|---|
| 1 | Secretos con defaults débiles (`${POSTGRES_PASSWORD:-airflow}`, JWT `change-me-in-prod`, admin/admin) | Sin un `.env` con valores fuertes, quedan las credenciales por defecto |
| 2 | Sin `restart` en HDFS/Spark/Jupyter | Un crash deja el servicio caído |
| 3 | Sin healthchecks salvo Postgres | `depends_on` no sabe si el servicio *funciona* |
| 4 | Sin límites de recursos | Un Spark job puede comerse toda la RAM del host |
| 5 | `docker.sock` | Ya resuelto: no se monta en el Compose actual |
| 6 | Clave `version:` en el compose — obsoleta en la Compose Specification (Compose v2 la ignora con un warning). Ya resuelto: se eliminó; el compose actual no la lleva | Warning ruidoso y falsa sensación de "pin" que no controla nada |
| 7 | Jupyter sin token | Cualquiera en la red entra |

### 8.1. Secretos en un `.env` (ya soportado por el compose)

El compose base ya lee los secretos por interpolación (ver 8.3); solo falta darle valores
fuertes. Copiá la plantilla del repo y reemplazá los defaults:

```bash
cp .env.example .env   # .env ya está en .gitignore; no commitear
```

```dotenv
# .env  — valores de producción
COMPOSE_PROFILES=dev   # "dev" levanta Jupyter; en prod dejalo vacío o quitá la línea
POSTGRES_USER=airflow
POSTGRES_PASSWORD=cambia-esto-por-algo-fuerte
POSTGRES_DB=airflow
AIRFLOW_JWT_SECRET=genera-uno-con-openssl-rand-hex-32
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=cambia-esto-tambien
JUPYTER_TOKEN=pon-un-token-largo
GRAFANA_ADMIN_PASSWORD=cambia-esto-tambien
```

Generá secretos de verdad:

```bash
openssl rand -hex 32   # para AIRFLOW_JWT_SECRET
```

### 8.2. Override para endurecer el stack local

En vez de tocar el compose base, usá un override que Compose fusiona automáticamente: el dev
queda intacto y se añade el endurecimiento (restart, logging, healthchecks, límites). Guardalo
como `docker-compose.local-hardened.yml` y levántalo con:

```bash
docker compose -f docker-compose.yml -f docker-compose.local-hardened.yml up -d
```

> Ojo con "prod": el snippet de abajo es un endurecimiento genérico del stack local completo
> (útil si querés correrlo hardened en una sola caja). La producción real (AWS) es híbrida y NO
> coincide con esto: el override de PROD de `02-produccion-aws-dataops-operativa-v3.md` **no levanta**
> `hdfs-namenode`/`hdfs-datanode` ni `spark-master`/`spark-worker` — esos servicios son solo
> dev/local. En la caja de producción (EC2 `t3.large`) solo corren Airflow + Postgres +
> monitoreo; el cómputo Spark se delega a EMR Serverless y el storage es S3. Tomá las secciones
> `hdfs-*`/`spark-*` de este snippet como referencia para el stack local, no para la nube.

```yaml
# docker-compose.local-hardened.yml — límites y healthchecks para el laboratorio local
x-restart: &restart-policy
  restart: unless-stopped

x-logging: &default-logging          # rota logs para no llenar el disco
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }

services:
  hdfs-namenode:
    <<: [*restart-policy, *default-logging]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 15s
      timeout: 5s
      retries: 5
    deploy:
      resources:
        limits: { memory: 2g }

  hdfs-datanode:
    <<: [*restart-policy, *default-logging]
    deploy:
      resources:
        limits: { memory: 2g }

  spark-master:
    <<: [*restart-policy, *default-logging]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 15s
      timeout: 5s
      retries: 5
    deploy:
      resources:
        limits: { memory: 2g }

  spark-worker:
    <<: [*restart-policy, *default-logging]
    deploy:
      resources:
        limits: { memory: 4g }        # tope duro del contenedor (cgroup)
    # stack LOCAL: en AWS, Spark ni HDFS existen en `docker-compose.prod.yml` (docs/02 §14.1)

  # (jupyter no aparece acá: está bajo el perfil `dev`. En AWS, `docker-compose.prod.yml`

  airflow-db:
    <<: [*restart-policy, *default-logging]

  airflow-apiserver:   { <<: *default-logging }
  airflow-scheduler:   { <<: *default-logging }
  airflow-dag-processor: { <<: *default-logging }
  airflow-triggerer:   { <<: *default-logging }
```

### 8.3. Secretos parametrizados en el compose base (ya aplicado)

El compose base ya usa interpolaciones `${VAR:-default}`: sin `.env` corre con defaults de dev;
con `.env` (o el `.env` generado desde SSM, ver la guía de producción §13.1) toma los valores
reales. Fragmentos del compose actual:

```yaml
# docker-compose.yml  (fragmentos, tal como está)
  airflow-db:
    image: postgres:16
    environment:
      - POSTGRES_USER=${POSTGRES_USER:-airflow}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
      - POSTGRES_DB=${POSTGRES_DB:-airflow}
```

```yaml
x-airflow-common: &airflow-common
  environment: &airflow-common-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@airflow-db:5432/${POSTGRES_DB:-airflow}
    AIRFLOW__API_AUTH__JWT_SECRET: '${AIRFLOW_JWT_SECRET:-change-me-in-prod}'
```

Y el `airflow-init` usando las vars:

```yaml
  airflow-init:
    <<: *airflow-common
    command: >
      bash -euc '
        airflow db migrate
        airflow fab-db migrate
        airflow users list | grep -q "$${AIRFLOW_ADMIN_USER:-admin}" ||
          airflow users create --username "$${AIRFLOW_ADMIN_USER:-admin}" --firstname Admin \
            --lastname User --role Admin --email admin@example.com \
            --password "$${AIRFLOW_ADMIN_PASSWORD:-admin}"
      '
```

> Los defaults existen para el entorno local. El Compose de AWS no debe aceptar defaults para
> secretos: carga valores desde SSM antes de arrancar.

### 8.4. Mantener `docker.sock` fuera del stack

El Compose actual no monta el socket. No agregues esta línea al `x-airflow-common`:

```yaml
    - /var/run/docker.sock:/var/run/docker.sock   # no agregar
```

Si en el futuro un caso exige DockerOperator, aislalo en un ejecutor dedicado y evaluá un
socket-proxy con API limitada; no lo heredes en api-server, scheduler, triggerer y dag-processor.

### 8.5. Añadir el history-server (opcional, ya casi listo)

Descomentá el bloque de `spark-history-server` en el compose y arrancalo; leerá
`./spark-events` para darte la UI de jobs terminados en `:18080`. Recordá también poner
`spark.eventLog.enabled true` en `spark-events/spark-defaults.conf` (hoy está en `false`
porque sin History Server los event logs quedaban huérfanos); el detalle está en la
guía de producción §13.6.

---

## 9. Checklist de calidad

Antes de considerar el stack "listo":

- [ ] `.env` fuera de git (`.gitignore`) y con secretos generados con `openssl`.
- [ ] `AIRFLOW_JWT_SECRET` único por entorno.
- [ ] Jupyter con `JUPYTER_TOKEN` no vacío (solo aplica local: en prod no corre).
- [ ] `restart: unless-stopped` en todos los servicios long-running.
- [ ] Healthchecks en HDFS, Spark y Jupyter (no solo Postgres).
- [ ] Límites de memoria por servicio (`deploy.resources.limits`).
- [ ] Rotación de logs (`max-size`, `max-file`).
- [ ] `docker.sock` quitado o detrás de un proxy.
- [ ] Imágenes pineadas por tag inmutable o `@sha256`.
- [ ] Volúmenes con backup (Postgres + HDFS namenode).

> Siguiente paso: ver `02-produccion-aws-dataops-operativa-v3.md` — guía única de producción (un solo camino: EC2
> self-managed + S3 + Lambda/EventBridge), Terraform, estado remoto en S3, monitoreo,
> CI/CD y automatización de costo con auto start/stop. La arquitectura está en `03-arquitectura-mejorada.md`.

