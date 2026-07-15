# pyspark_stack

Plataforma de datos reproducible: **Airflow 3 + Spark 4 + HDFS + Jupyter** orquestada con Docker
Compose, más **guías expertas para llevarla a producción en AWS** (Terraform, monitoreo con
Prometheus/Grafana, CI/CD y secretos gestionados).

```
Airflow 3.2 (5 procesos) + Postgres  →  orquestación
Spark 4.0.3 (master + worker)        →  cómputo
HDFS (namenode + datanode)           →  almacenamiento
Jupyter (PySpark 4)                  →  notebooks
```

## Estructura

| Ruta | Qué es |
|------|--------|
| `docker-compose.yml` | Stack completo (Airflow + Spark + HDFS + Jupyter) |
| `Dockerfile.*` | Imágenes: airflow, spark, jupyter, history |
| `dags/` | DAGs de Airflow |
| `spark-apps/` | Jobs PySpark (customer_etl, sales_etl, …) |
| `notebooks/` | Notebooks de Jupyter |
| `hadoop-config/` | `core-site.xml` (cliente HDFS) |
| `docs/` | **Guías** (ver abajo) |

## Guías

- **[docs/01 — Docker Compose explicado](docs/01-docker-compose-explicado.md)** — anatomía del stack,
  bloque por bloque, y refactor de producción.
- **[docs/02 — Producción en AWS](docs/02-produccion-aws.md)** — la guía grande (**un solo camino**:
  EC2 self-managed + S3 + Lambda/EventBridge): Terraform, auto start/stop, data lake S3 (s3a),
  disparo de DAGs vía SSM, **CI/CD (GitHub Actions + OIDC)**, notebooks (papermill), **monitoreo
  (Prometheus + Grafana + Alertmanager + Loki)**, **secretos con SSM/Secrets Manager** y hardening.
  Todo copy-paste, y **cada recurso Terraform con su equivalente manual en la consola AWS** (desplegables 🖱️).
- **[docs/03 — Arquitectura](docs/03-arquitectura.md)** — diagramas (ASCII + Mermaid) y flujos del
  mismo camino (sin MWAA/EMR/Glue).

## Arranque rápido (local)

Requisitos: Docker + Docker Compose. Recomendado 16 GB+ de RAM para el stack completo.

```bash
# 1) secretos locales (además activa el perfil "dev" → Jupyter, ver .env.example)
cp .env.example .env

# 2) Stack completo (con COMPOSE_PROFILES=dev del .env, incluye Jupyter)
docker compose up -d --build
```

| UI | URL |
|----|-----|
| Airflow | http://localhost:8082 |
| Jupyter | http://localhost:8888 |
| Spark master | http://localhost:8081 |
| HDFS | http://localhost:9870 |

> **Jupyter es dev-only.** Vive en el perfil `dev` de Compose: es una herramienta para
> explorar/depurar antes de promover el código a un DAG. En producción no arranca (el ETL corre
> por Airflow y los notebooks por papermill, sin necesitar este server). Copiar `.env.example`
> deja `COMPOSE_PROFILES=dev`, así que `docker compose up` lo incluye; sin esa variable, levantalo
> puntualmente con `docker compose --profile dev up -d jupyter`.

### Máquina chica (~8 GB)

Solo Spark + Jupyter (sin HDFS/Airflow). El compose `docker-compose.dev.yml` está listo para copiar
en la [guía de producción](docs/02-produccion-aws.md):

```bash
docker compose -f docker-compose.dev.yml up -d --build
```

## Producción en AWS

Ver **[docs/02](docs/02-produccion-aws.md)**: EC2 self-managed + S3 + Lambda/EventBridge (un solo
camino), Terraform con estado remoto, monitoreo, CI/CD y secretos en AWS. Todo el código es
copy-paste dentro de la guía.

## Seguridad

- Los secretos del `docker-compose.yml` están parametrizados (`${VAR:-default}`): en local usan
  defaults; en producción se sobreescriben vía `.env` o **AWS SSM / Secrets Manager** (ver la guía de producción).
- `.env`, estados de Terraform y `alertmanager.yml` están en `.gitignore` — nunca subir secretos reales.
