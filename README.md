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
- **[docs/02 — Producción en AWS](docs/02-produccion-aws.md)** — la guía grande: Terraform
  (EC2 / serverless), auto start/stop, **CI/CD (GitHub Actions + OIDC)**, notebooks (papermill),
  **monitoreo (Prometheus + Grafana + Alertmanager + Loki)**, **secretos con SSM/Secrets Manager**
  y hardening. Todo copy-paste.
- **[docs/03 — Arquitectura](docs/03-arquitectura.md)** — diagramas (ASCII + Mermaid) y flujos.

## Arranque rápido (local)

Requisitos: Docker + Docker Compose. Recomendado 16 GB+ de RAM para el stack completo.

```bash
# 1) (opcional) secretos locales
cp .env.example .env

# 2) Stack completo
docker compose up -d --build
```

| UI | URL |
|----|-----|
| Airflow | http://localhost:8082 |
| Jupyter | http://localhost:8888 |
| Spark master | http://localhost:8081 |
| HDFS | http://localhost:9870 |

### Máquina chica (~8 GB)

Solo Spark + Jupyter (sin HDFS/Airflow). El compose está en la guía 02 §14.2:

```bash
docker compose -f docker-compose.dev.yml up -d --build   # crealo desde docs/02 §14.2
```

## Producción en AWS

Ver **[docs/02](docs/02-produccion-aws.md)**: EC2 self-managed (o serverless), Terraform con
estado remoto, monitoreo, CI/CD y secretos en AWS. Todo el código es copy-paste dentro de la guía.

## Seguridad

- Los secretos del `docker-compose.yml` están parametrizados (`${VAR:-default}`): en local usan
  defaults; en producción se sobreescriben vía `.env` o **AWS SSM / Secrets Manager** (docs/02 §13.1).
- `.env`, estados de Terraform y `alertmanager.yml` están en `.gitignore` — nunca subir secretos reales.
