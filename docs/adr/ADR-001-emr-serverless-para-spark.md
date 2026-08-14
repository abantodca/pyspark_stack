# ADR-001 — El cómputo Spark vive fuera de la EC2, en EMR Serverless

**Estado:** accepted · 2026-08-12

## Contexto

El stack local corre Spark en contenedores junto a Airflow ([guía 01](../01-stack-local.md)). Al
promover a AWS había que decidir dónde corre Spark en producción, sabiendo que el perfil de uso es
**a ráfagas**: unas pocas corridas por día, minutos cada una, y el resto del tiempo idle.

La regla económica del proyecto es que almacenar es barato y constante, y computar es lo que cuesta
y solo cuando corrés. Una EC2 dimensionada para Spark se paga completa las 24 horas aunque compute
diez minutos.

## Decisión

**Spark corre en EMR Serverless**, con su propio rol de ejecución, y la EC2 queda como orquestador:
solo Airflow + Postgres + monitoreo. Airflow lanza cada job con `EmrServerlessStartJobOperator` y lo
poolea con `EmrServerlessJobSensor` ([guía 02 §9.4](../02-produccion-aws-terraform.md#94-dag-de-referencia-para-emr-serverless));
nunca corre `spark-submit` local.

Consecuencia directa: la EC2 baja a `t3.large` burstable, que es lo correcto para una carga que pasa
la mayor parte del tiempo idle.

## Consecuencias

**Se gana:**

- La app escala a cero entre corridas: sin job, no hay factura de cómputo.
- El techo de gasto es explícito (`maximum_capacity`, 16 vCPU / 64 GB) y no depende de que nadie se
  olvide de apagar un cluster.
- Los permisos de Spark quedan separados de los de Airflow: el job tiene su propio rol, acotado a
  los dos buckets del lake ([guía 02 §6.4](../02-produccion-aws-terraform.md#64-cómputo-spark-emr-serverless)).

**Se pierde:**

- **Cold start de 1–2 minutos** en la primera corrida tras un período idle. Es el costo único de
  escalar a cero, no una degradación sostenida.
- El código deja de ser idéntico al local: los entrypoints EMR no usan `.master()` y la config viaja
  por-job en `sparkSubmitParameters`, no en un `spark-defaults.conf`.
- La Spark UI vive en la consola de EMR, no en la EC2: se diagnostica distinto.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Spark en la misma EC2 (como en local) | Obliga a dimensionar la EC2 para el pico y pagarla 24×7. Es exactamente lo que la regla económica evita |
| EMR on EC2 (cluster clásico) | Hay que gestionar el cluster, y escalar a cero implica crearlo y destruirlo en cada corrida |
| AWS Glue | Menos control sobre la versión de Spark y la config del job; el código deja de parecerse al local |
| Fargate con Spark propio | Habría que construir y mantener la imagen, el scheduler y el shuffle service: reimplementar EMR |

## Dónde vive

Guía 02 [guía 02 §6.4](../02-produccion-aws-terraform.md#64-cómputo-spark-emr-serverless) (módulo `emr`),
[guía 02 §9.4](../02-produccion-aws-terraform.md#94-dag-de-referencia-para-emr-serverless) (el DAG) y
[guía 02 §17](../02-produccion-aws-terraform.md#17-qué-motor-usar-para-cada-tarea) (cuándo usar cada motor).
