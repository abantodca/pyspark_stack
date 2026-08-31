# Architecture Decision Records

Decisiones estructurales de este stack, con su contexto y sus consecuencias. Cada una está
referenciada desde la guía que la implementa ([02](../02-produccion-aws-terraform.md)) y desde
[03 — Arquitectura](../03-arquitectura.md).

**Cambiar una decisión ratificada acá implica un ADR nuevo que la supersede, no un parche local.**
Si una sección de la guía contradice un ADR, el que está mal es uno de los dos: arreglalos juntos.

| # | Decisión | Estado |
|---|---|---|
| [001](ADR-001-emr-serverless-para-spark.md) | El cómputo Spark vive fuera de la EC2, en EMR Serverless | accepted |
| [002](ADR-002-plano-de-control-single-node.md) | El plano de control es single-node, sin alta disponibilidad | accepted |
| [003](ADR-003-disparo-por-ssm-no-api-http.md) | Los DAGs se disparan por SSM, nunca exponiendo la API de Airflow | accepted |
| [004](ADR-004-backend-s3-con-use-lockfile.md) | El lock del state lo hace S3 con `use_lockfile`, sin DynamoDB | accepted |
| [005](ADR-005-composicion-envs-modules.md) | La infra es una composición `envs/prod` + `modules/*`, no un módulo raíz plano | accepted |
| [006](ADR-006-el-modulo-que-crea-otorga.md) | El módulo que crea el recurso es el que otorga el acceso a él | accepted |
| [007](ADR-007-contrato-de-variables-por-output.md) | Ningún comando lleva valores escritos a mano: salen de `terraform output` | accepted |
| [008](ADR-008-apagado-job-aware.md) | El apagado automático es *job-aware*: no corta DAGs en vuelo | accepted |
| [009](ADR-009-arquitectura-por-etapas-y-gates-de-produccion.md) | Plano de datos serverless y plano de control por etapas | accepted |

Los ADR 001–008 se registraron el **2026-08-12**, en la reorganización que refundió la guía 02 en
un solo documento y pasó su Terraform a composición por módulos. ADR-009, del **2026-08-31**, fija
los gates de datos reales y el criterio para evolucionar el plano de control.
