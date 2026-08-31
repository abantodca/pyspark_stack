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

Requisitos: Docker y Docker Compose. El stack completo tiene límites que suman aproximadamente
**11.1 GiB** (son techos, no memoria reservada).

Primero seguí [la sección 0 de la guía local](docs/01-stack-local.md#0-construcción-incremental-del-entorno):
crea Dockerfiles, Compose, Taskfile y el template `.env.example`. Después completá los secretos en
[§8.1](docs/01-stack-local.md#81-secretos-en-un-env) y levantá el stack con [§9.1](docs/01-stack-local.md#91-arrancar).

El comando `task local:up`, creado por esa guía, rechaza secretos vacíos/débiles, valida el Compose
efectivo y usa el override local con límites, healthchecks y política de reinicio.

Con el stack arriba, el siguiente paso es escribir los pipelines:
[docs/06-medallion-desde-cero.md](docs/06-medallion-desde-cero.md). `task local:smoke`
funciona recién cuando escribiste el proyecto Web Events (§16 de esa guía).

| UI | URL |
|---|---|
| Airflow | http://localhost:8082 |
| Jupyter | http://localhost:8888 |
| Spark master | http://localhost:8081 |
| HDFS | http://localhost:9870 |

Jupyter vive en el perfil `dev` de Compose y en producción no arranca. El template activa ese perfil
para conservar el laboratorio completo. Las capacidades del worker, la concurrencia de Airflow y los
límites de memoria se escalan desde `.env` sin editar YAML.

## Contenido del repositorio

```
docs/            guías, incluido el bootstrap completo del stack local
dags/            vacío: lo escribís vos siguiendo docs/06-medallion-desde-cero.md
notebooks/       ubicación para notebooks locales
spark-apps/      ubicación para jobs Spark externos
```

Convención inequívoca: los 15 pipelines end-to-end usan
`dags/medallion_dags/<dominio>_medallion_dag.py`.

> [!IMPORTANT]
> **El código de los pipelines no se versiona: está en la guía 06.** `dags/` arranca
> vacío y lo llenás copiando, en orden, los quince proyectos de
> [docs/06-medallion-desde-cero.md](docs/06-medallion-desde-cero.md) — que además explica
> por qué cada línea está donde está. `task local:gate` verifica que estén los quince.

Este checkout contiene la guía para construir el stack local, no una copia prearmada de sus
archivos. La guía de AWS describe una arquitectura objetivo; los módulos Terraform, scripts,
Compose productivo, workflows y jobs de EMR no están presentes: no hay un despliegue de producción
ejecutable desde este árbol.

## Documentación

- [Índice y estado de implementación](docs/README.md): qué está implementado, qué falta probar y qué es roadmap.
- [ADR](docs/adr/README.md): las decisiones estructurales y sus alternativas descartadas.
- [01 — Stack local](docs/01-stack-local.md): anatomía del `docker-compose.yml`, bloque por bloque.
- [02 — Producción en AWS con Terraform](docs/02-produccion-aws-terraform.md): la guía completa de despliegue y
  operación, en un solo documento — fundamentos y costo (§1–§4), el núcleo EC2 (§5), data lake y EMR Serverless
  (§6–§7), operación y diagnóstico (§8–§10), CI/CD, secretos y runbook (§11–§15), y la evolución (§16–§22).
- [03 — Arquitectura](docs/03-arquitectura.md): componentes, flujos, seguridad y costos.
- [06 — Medallion desde cero](docs/06-medallion-desde-cero.md): **el taller**. Los 15 proyectos
  en copy-paste incremental, de un DAG de 45 líneas a reconciliación multi-fuente, con la
  metodología para escribir el decimosexto solo.

## Seguridad

- Los cuatro secretos locales son obligatorios; `task local:check` rechaza valores débiles y exige
  modo `0600` en `.env`. El Compose productivo deberá cargarlos desde AWS SSM.
- `.env`, los estados de Terraform y las claves están en `.gitignore`; `.dockerignore` evita que
  entren al contexto de build. Nunca subas secretos reales.
