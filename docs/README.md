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
| Contexto de producción en variables de entorno (`scripts/prod-env.sh`) | Repositorio — implementado; contrato en guía 02 §3.1 |
| Terraform de EC2, S3, EMR Serverless, IAM y automatización | Guía 02 §4–§7 |
| DAG de Airflow contra EMR Serverless | Guía 02 §9.4 |
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

Los comandos de las guías **no llevan valores escritos a mano**: leen variables de entorno que
produce Terraform (contrato en la [guía 02 §3.1](02-produccion-aws-terraform.md#31-contrato-de-variables-de-entorno-leelo-antes-de-copiar-cualquier-comando)).
Dos validadores lo mantienen sano y corren sin credenciales AWS:

```bash
python3 scripts/check-doc-links.py    # enlaces, anclas y referencias §
python3 scripts/check-doc-env.py      # contrato de variables y bloques de comandos
```

`check-doc-links.py` comprueba que los enlaces relativos apunten a archivos que existen, que las
anclas `#seccion` correspondan a un encabezado real (con las reglas de slug de GitHub) y que cada
`§X.Y` exista — sea local o cruzada (`guía 02 §13.4`, `docs/01 §8.5`). Las guías se renumeran al
editarlas y una referencia rota es invisible leyendo de corrido.

`check-doc-env.py` verifica que ninguna variable se use antes de la sección que la crea (las guías
son incrementales), que todo `$VAR` exista como output o venga de `prod-env.sh`, que los bloques
` ``` ` estén balanceados, que cada bloque bash parsee con `bash -n`, que ningún comando lleve un
nombre de recurso, un account id o una ruta SSH escritos a mano, y que el `prod-env.sh` embebido en
la guía no derive del archivo del repo. Al agregar un recurso: declarás su `output` en la sección
que lo crea y usás la variable en MAYÚSCULAS — el cargador no se toca.
