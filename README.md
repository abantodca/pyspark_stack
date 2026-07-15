# pyspark_stack

Plataforma de datos local y reproducible: Airflow 3.2 + Spark 4.0.3 + HDFS + Jupyter, orquestada con Docker Compose. Incluye guías para llevarla a producción en AWS.

```
Airflow 3.2 (5 procesos) + Postgres  →  orquestación
Spark 4.0.3 (master + worker)        →  cómputo
HDFS (namenode + datanode)           →  almacenamiento
Jupyter (PySpark 4)                  →  notebooks (solo dev)
```

## Arranque rápido

Requisitos: Docker + Docker Compose. Recomendado 16 GB+ de RAM para el stack completo.

```bash
# 1) Secretos locales (además activa el perfil "dev" → Jupyter, ver .env.example)
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

Jupyter es dev-only: vive en el perfil `dev` de Compose y en producción no arranca. Copiar `.env.example` deja `COMPOSE_PROFILES=dev`, así que `docker compose up` lo incluye; sin esa variable, se levanta con `docker compose --profile dev up -d jupyter`.

### Modo dev-lite (~8 GB de RAM)

Solo Spark + Jupyter, sin HDFS ni Airflow. El archivo `docker-compose.dev.yml` no viene en el repo: hay que crearlo copiándolo desde la [guía de producción](docs/02-produccion-aws.md), §14.2.

```bash
# Paso previo: crear docker-compose.dev.yml desde docs/02 §14.2
docker compose -f docker-compose.dev.yml up -d --build
```

## Documentación

- [docs/01 — Docker Compose explicado](docs/01-docker-compose-explicado.md): anatomía del stack, bloque por bloque, y refactor de producción.
- [docs/02 — Producción en AWS](docs/02-produccion-aws.md): un solo camino (EC2 self-managed + S3 + Lambda/EventBridge) con Terraform, CI/CD, monitoreo, secretos y hardening. Todo el código es copy-paste.
- [docs/03 — Arquitectura](docs/03-arquitectura.md): diagramas y flujos del mismo camino.
- [ANALISIS_FALLOS.md](ANALISIS_FALLOS.md): registro histórico de fallos del stack y sus fixes.

## Seguridad

- Los secretos del `docker-compose.yml` están parametrizados (`${VAR:-default}`): en local usan defaults; en producción se sobreescriben vía `.env` o AWS SSM / Secrets Manager (ver docs/02).
- `.env`, estados de Terraform y `alertmanager.yml` están en `.gitignore`. Nunca subir secretos reales.
