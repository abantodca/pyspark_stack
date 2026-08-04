# Documentación de `pyspark_stack`

Esta carpeta separa lo que ya funciona de la arquitectura objetivo. Un componente se considera
**implementado** solo cuando existe como código versionado y está cubierto por una validación
repetible. Lo marcado como **roadmap** no forma parte todavía del runbook de producción.

| Documento | Propósito | Estado |
|---|---|---|
| [01 — Stack local](01-stack-local.md) | Anatomía del Compose y de los contenedores | Implementado |
| [02 — Producción con Terraform](02-produccion-aws-terraform.md) | Arquitectura objetivo y runbook IaC | Guía completa; sin desplegar |
| [02b — Producción por consola](02b-produccion-aws-consola.md) | El mismo camino, sin IaC | Referencia; sin desplegar |
| [03 — Arquitectura](03-arquitectura.md) | Vista lógica, seguridad y evolución | Implementado + roadmap |
| [04 — Ejemplos locales](04-ejemplos-locales.md) | Tutorial progresivo de 21 ejercicios | Implementado |
| [05 — Production readiness](05-production-readiness.md) | Controles previos al primer despliegue | Implementado |
| [06 — Historial de incidentes](06-historial-de-incidentes.md) | Fallos del stack local y sus fixes | Histórico |

## Qué contiene el repositorio

El repositorio versiona **únicamente el proyecto local**: Compose, Dockerfiles, DAGs, jobs PySpark,
notebooks y tests. Todo lo de producción se crea siguiendo la guía 02 (o la 02b), que trae el
contenido íntegro de cada archivo.

| Capacidad | Dónde vive |
|---|---|
| Spark, HDFS, Jupyter y Airflow en local | Repositorio — implementado |
| Terraform de EC2, S3, EMR Serverless, IAM y automatización | Guía 02 §4–§7 |
| DAG de Airflow contra EMR Serverless | Guía 02 §10.2 |
| Jobs Spark para EMR Serverless | Guía 02 §6.4 |
| Compose de producción y carga de secretos desde SSM | Guía 02 §13.4 y §14.1 |
| Validación en CI y despliegue con OIDC | Guía 02 §11 |
| Observabilidad Prometheus/Grafana/Loki | Guía 02 §12 y §14.2 — roadmap |
| Tablas Iceberg | Guía 02 §16 — roadmap; el job de referencia escribe Parquet |
| dbt, Great Expectations y OpenLineage | Guía 02 §19–§22 — roadmap |

## Regla de mantenimiento

Los comandos, políticas y configuraciones ejecutables viven en sus archivos canónicos. La
documentación explica decisiones y enlaza esos archivos; no mantiene una segunda copia que pueda
divergir. Cada cambio de arquitectura debe actualizar esta matriz.
