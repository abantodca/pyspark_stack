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
cp .env.example .env          # secretos locales; activa el perfil "dev" (Jupyter)
docker compose up -d --build
```

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
docs/            documentación: stack local, arquitectura y producción en AWS
```

Este repositorio contiene **solo el proyecto local**. La infraestructura de producción —Terraform,
Compose de producción, Lambdas, workflows de CI/CD y jobs de EMR Serverless— no se versiona acá: se
crea paso a paso siguiendo [docs/02](docs/02-produccion-aws-terraform.md), que incluye el contenido
completo de cada archivo.

La excepción es `scripts/prod-env.sh`: sí se versiona, porque corre en **tu** máquina. Convierte los
outputs de Terraform en variables de entorno, y es lo que hace que cada comando de las guías se
copie y funcione tal cual, sin editar IDs, IPs ni nombres de bucket
([contrato completo](docs/02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-leelo-antes-de-copiar-cualquier-comando)).

## Documentación

- [Índice y estado de implementación](docs/README.md): qué está implementado, qué falta probar y qué es roadmap.
- [01 — Stack local](docs/01-stack-local.md): anatomía del `docker-compose.yml`, bloque por bloque.
- [02 — Producción en AWS con Terraform](docs/02-produccion-aws-terraform.md): guía completa de despliegue y operación.
- [02b — Producción en AWS por consola](docs/02b-produccion-aws-consola.md): el mismo camino, sin IaC.
- [03 — Arquitectura](docs/03-arquitectura.md): componentes, flujos, seguridad y costos.
- [04 — Ejemplos locales](docs/04-ejemplos-locales.md): 21 ejercicios progresivos sobre este stack.
- [05 — Production readiness](docs/05-production-readiness.md): controles previos al primer despliegue.
- [06 — Historial de incidentes](docs/06-historial-de-incidentes.md): fallos del stack local y sus fixes.

## Seguridad

- Los secretos locales tienen defaults deliberadamente débiles: sirven para desarrollo, nunca para
  producción. El Compose de producción exige valores explícitos, cargados desde AWS SSM.
- `.env`, los estados de Terraform y las claves están en `.gitignore`. Nunca subas secretos reales.
