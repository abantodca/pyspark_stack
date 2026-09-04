# pyspark_stack

Repositorio **exclusivamente documental** para construir una plataforma de datos con Airflow,
Spark, HDFS y Jupyter en local, y su arquitectura de producción en AWS con Terraform.

Este checkout no contiene una instalación materializada. Los Dockerfiles, archivos Compose,
Taskfiles, DAGs, jobs PySpark, scripts y módulos Terraform se crean siguiendo las guías; no se
versionan en la raíz ni en carpetas de runtime. Así, la documentación es la única fuente de verdad
y no puede divergir de una copia de código generada previamente.

## Recorrido recomendado

1. [01 — Stack local](docs/01-stack-local.md): crea y explica el entorno reproducible local.
2. [06 — Medallion desde cero](docs/06-medallion-desde-cero.md): crea el runtime y los quince
   pipelines, con ejercicios incrementales.
3. [03 — Arquitectura](docs/03-arquitectura.md): fija límites, responsabilidades y criterios de
   evolución para producción.
4. [02 — Producción en AWS con Terraform](docs/02-produccion-aws-terraform.md): materializa y opera
   la arquitectura de referencia, sección por sección.

La guía 02 puede crear recursos facturables. Que sus bloques estén completos no significa que la
infraestructura esté desplegada o validada en una cuenta AWS: deben ejecutarse sus checkpoints y el
gate de datos reales.

## Contenido versionado

```text
README.md       punto de entrada
docs/           guías y decisiones de arquitectura (ADR)
```

Los archivos generados al practicar las guías pertenecen al workspace del lector, no a la fuente
documental. Antes de conservar un cambio, comprobá que actualizaste la guía dueña del bloque y no
solo su copia materializada.

## Seguridad

Nunca versiones `.env`, estados o planes de Terraform, claves, secretos, datos de ejecución,
artefactos empaquetados ni archivos generados por Docker, Spark o Airflow. Las guías incluyen los
controles y archivos de exclusión que deben crearse antes de materializar cada entorno.
