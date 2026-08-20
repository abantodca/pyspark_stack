# pyspark_stack

Plataforma de datos local y reproducible: **Airflow 3.2 + Spark 4.0.3 + HDFS + Jupyter**, orquestada
con Docker Compose, más las guías para llevarla a producción en AWS.

El ciclo es: se desarrolla en local con el stack completo y se despliega en una arquitectura
**híbrida** — Airflow sigue orquestando desde una EC2 chica (`t3.large`), el cómputo Spark pasa a
EMR Serverless y el data lake a S3, sin HDFS.

```
Airflow 3.2 (5 procesos) + Postgres 16  →  orquestación
Spark 4.0.3 (master + worker)           →  cómputo
HDFS (namenode + datanode)              →  almacenamiento
Jupyter (PySpark 4)                     →  notebooks (solo dev)
```

## Arranque rápido

Requisitos: Docker y Docker Compose. Recomendado 16 GB de RAM para el stack completo.

```bash
cp .env.example .env
chmod 600 .env
# Generá y pegá cuatro valores distintos con `openssl rand -hex 32`.
task local:up
```

`task local:up` rechaza secretos vacíos/débiles, valida el Compose efectivo y usa el override local
con límites, healthchecks y política de reinicio. `task test` ejecuta los contratos y las
transformaciones en la misma imagen de Airflow que usa el pipeline.

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
dags/            DAGs de Airflow del stack local
spark-apps/      jobs PySpark, shell del pipeline customer_etl y datos de landing
notebooks/       notebooks de práctica y del pipeline
tests/           controles de integridad de los DAGs
hadoop-config/   core-site.xml del cliente HDFS
spark-events/    configuración de eventLog de Spark
scripts/         utilidades: prod-env.sh (contexto de producción) y validadores de las guías
Taskfile.yml     los comandos repetidos: infra, deploy, validadores (task --list)
docs/            documentación: cuatro guías centrales, adr/ (decisiones) y referencia/
```

El repositorio contiene el proyecto local y una **composición Terraform parcial** (`network` y
`orchestrator`) que puede validarse sin credenciales. El resto de la infraestructura de producción,
Lambdas, Compose productivo, workflows y jobs de EMR siguen siendo guía de implementación: no se
consideran desplegables ni probados de punta a punta.

La excepción es `scripts/prod-env.sh`: sí se versiona, porque corre en **tu** máquina. Convierte los
outputs de Terraform en variables de entorno, y es lo que hace que cada comando de las guías se
copie y funcione tal cual, sin editar IDs, IPs ni nombres de bucket
([contrato completo](docs/02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-léalo-antes-de-copiar-cualquier-comando)).

## Documentación

- [Índice y estado de implementación](docs/README.md): qué está implementado, qué falta probar y qué es roadmap.
- [ADR](docs/adr/README.md): las ocho decisiones estructurales, con sus alternativas descartadas.
- [01 — Stack local](docs/01-stack-local.md): anatomía del `docker-compose.yml`, bloque por bloque.
- [02 — Producción en AWS con Terraform](docs/02-produccion-aws-terraform.md): la guía completa de despliegue y
  operación, en un solo documento — fundamentos y costo (§1–§4), el núcleo EC2 (§5), data lake y EMR Serverless
  (§6–§7), operación y diagnóstico (§8–§10), CI/CD, secretos y runbook (§11–§15), y la evolución (§16–§22).
- [03 — Arquitectura](docs/03-arquitectura.md): componentes, flujos, seguridad y costos.
- [04 — Ejemplos locales](docs/04-ejemplos-locales.md): 21 ejercicios progresivos sobre este stack.

Material de consulta en [`docs/referencia/`](docs/referencia):

- [02b — Producción en AWS por consola](docs/referencia/02b-produccion-aws-consola.md): el mismo camino, sin IaC.
- [05 — Production readiness](docs/referencia/05-production-readiness.md): controles previos al primer despliegue.
- [06 — Historial de incidentes](docs/referencia/06-historial-de-incidentes.md): fallos del stack local y sus fixes.
- [07 — Secuencia de ejecución](docs/referencia/07-secuencia-de-ejecucion.md): por qué cada comando va donde va, cuáles
  dejan de hacer falta al avanzar, y en qué se diferencia la secuencia de lo que haría un operador.
- [Gobierno y operaciones de datos](docs/referencia/08-gobierno-operaciones-datos.md): ownership,
  contratos, calidad, SLO, incidentes, retención y gate para datos reales.

## Seguridad

- Los cuatro secretos locales son obligatorios; `task local:check` rechaza valores débiles y exige
  modo `0600` en `.env`. El Compose productivo deberá cargarlos desde AWS SSM.
- `.env`, los estados de Terraform y las claves están en `.gitignore`; `.dockerignore` evita que
  entren al contexto de build. Nunca subas secretos reales.
