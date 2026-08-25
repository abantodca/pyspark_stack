# pyspark_stack

Plataforma de datos local y reproducible: **Airflow 3.2 + Spark 4.2.0 + HDFS + Jupyter**, orquestada
con Docker Compose, más las guías para llevarla a producción en AWS.

El ciclo es: se desarrolla en local con el stack completo y se despliega en una arquitectura
**híbrida** — Airflow sigue orquestando desde una EC2 chica (`t3.large`), el cómputo Spark pasa a
EMR Serverless y el data lake a S3, sin HDFS.

```
Airflow 3.2 (5 procesos) + Postgres 16  →  orquestación
Spark 4.2.0 (master + worker)           →  cómputo
HDFS (namenode + datanode)              →  almacenamiento
Jupyter (PySpark 4)                     →  notebooks (solo dev)
```

## Arranque rápido

Requisitos: Docker y Docker Compose. Reservá al menos 20 GiB de RAM para Docker: los límites del
stack completo suman aproximadamente 18.25 GiB.

```bash
cp .env.example .env
chmod 600 .env
# Generá y pegá cuatro valores distintos con `openssl rand -hex 32`.
task local:up
task local:smoke
```

`task local:up` rechaza secretos vacíos/débiles, valida el Compose efectivo y usa el override local
con límites, healthchecks y política de reinicio.

| UI | URL |
|---|---|
| Airflow | http://localhost:8082 |
| Jupyter | http://localhost:8888 |
| Spark master | http://localhost:8081 |
| HDFS | http://localhost:9870 |

Jupyter vive en el perfil `dev` de Compose y en producción no arranca. Copiar `.env.example` deja
`COMPOSE_PROFILES=dev`, así que `docker compose up` lo incluye; sin esa variable se levanta con
`docker compose --profile dev up -d jupyter`.

## Contenido del repositorio

```
dags/
  medallion_dags/  15 proyectos end-to-end Source → Bronze → Silver → Gold
  medallion/       runtime, almacenamiento y contratos compartidos
spark-apps/      ubicación para jobs Spark externos; los 15 DAGs locales son autocontenidos
notebooks/       bind mount para notebooks locales
hadoop-config/   core-site.xml del cliente HDFS
spark-events/    configuración de eventLog de Spark
ops/             utilidades operativas y sources.env: el origen de datos de cada proyecto
Taskfile.yml     comandos repetibles del stack local (task --list)
docs/            cinco guías, decisiones ADR y referencia de arquitectura AWS
```

Convención inequívoca: los 15 pipelines end-to-end usan
`dags/medallion_dags/<dominio>_medallion_dag.py`.

Este checkout implementa únicamente el stack local. La guía de AWS describe una arquitectura
objetivo, pero los módulos Terraform, scripts, Compose productivo, workflows y jobs de EMR no están
presentes: no hay un despliegue de producción ejecutable desde este árbol.

## Documentación

- [Índice y estado de implementación](docs/README.md): qué está implementado, qué falta probar y qué es roadmap.
- [ADR](docs/adr/README.md): las ocho decisiones estructurales, con sus alternativas descartadas.
- [01 — Stack local](docs/01-stack-local.md): anatomía del `docker-compose.yml`, bloque por bloque.
- [02 — Producción en AWS con Terraform](docs/02-produccion-aws-terraform.md): la guía completa de despliegue y
  operación, en un solo documento — fundamentos y costo (§1–§4), el núcleo EC2 (§5), data lake y EMR Serverless
  (§6–§7), operación y diagnóstico (§8–§10), CI/CD, secretos y runbook (§11–§15), y la evolución (§16–§22).
- [03 — Arquitectura](docs/03-arquitectura.md): componentes, flujos, seguridad y costos.
- [04 — DataOps local](docs/04-dataops-local.md): 15 ETL medallion end-to-end.
- [05 — HDFS desde la terminal](docs/05-hdfs-desde-la-terminal.md): ver, buscar, subir, consultar y
  exportar data del lakehouse a mano, con los comandos crudos.

## Seguridad

- Los cuatro secretos locales son obligatorios; `task local:check` rechaza valores débiles y exige
  modo `0600` en `.env`. El Compose productivo deberá cargarlos desde AWS SSM.
- `.env`, los estados de Terraform y las claves están en `.gitignore`; `.dockerignore` evita que
  entren al contexto de build. Nunca subas secretos reales.
