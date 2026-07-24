# Documentación de `pyspark_stack`

Esta carpeta separa el estado implementado de la arquitectura objetivo. Un componente solo se
considera **implementado** cuando existe como código versionado y está cubierto por una validación
repetible. Los bloques marcados como **roadmap** no son todavía pasos del runbook de producción.

| Documento | Propósito | Estado |
|---|---|---|
| [01 — Docker Compose](01-docker-compose-explicado-mejorado.md) | Stack local y contenedores | Implementado |
| [02 — AWS con Terraform](02-produccion-aws-dataops-operativa-v3.md) | Arquitectura objetivo y runbook IaC | Parcial; no desplegado |
| [02b — AWS por consola](02b-produccion-aws-consola-mejorado.md) | Alternativa manual | Referencia; no desplegada |
| [03 — Arquitectura](03-arquitectura-mejorada.md) | Vista lógica, seguridad y evolución | Implementado + roadmap |
| [04 — Ejemplos locales](04-ejemplos-local-paso-a-paso-mejorado.md) | Tutorial progresivo | Implementado como guía |
| [05 — Production readiness](05-production-readiness-checklist.md) | Gates antes del primer despliegue | Implementado |

## Estado de capacidades

| Capacidad | Estado en el repositorio |
|---|---|
| Spark/HDFS/Jupyter/Airflow local | Implementado |
| Terraform EC2, S3, EMR Serverless, IAM y automatización | Implementado, pendiente de despliegue |
| DAG Airflow → EMR Serverless | Implementado, pendiente de prueba integrada |
| Secretos desde SSM | Script implementado, pendiente de prueba integrada |
| Validación CI | Implementada; pendiente de primera ejecución en GitHub |
| Observabilidad Prometheus/Grafana/Loki | Roadmap |
| Tablas Iceberg | Roadmap; el job actual escribe Parquet |
| dbt y Great Expectations | Roadmap |
| OpenLineage | Roadmap |
| CD hacia AWS | Roadmap; deliberadamente no habilitado |

## Regla de mantenimiento

Los comandos, políticas y configuraciones ejecutables deben vivir en sus archivos canónicos.
La documentación explica decisiones y enlaza esos archivos; no debe mantener una segunda copia
que pueda divergir. Cada cambio de arquitectura debe actualizar esta matriz.
