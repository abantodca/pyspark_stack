# ADR-003 — Los DAGs se disparan por SSM, nunca exponiendo la API de Airflow

**Estado:** accepted · 2026-08-12

## Contexto

Hace falta disparar DAGs desde fuera de la EC2: por cron (EventBridge Scheduler) y por evento
(`ObjectCreated` en S3). La forma directa sería llamar a la API REST de Airflow por HTTP, lo que
obliga a exponer un puerto y a gestionar credenciales de API.

El diseño de red del stack es que **la única puerta abierta a Internet** es, opcionalmente, HTTPS
restringido a tu `/32`. Todo lo demás va por túnel SSH.

## Decisión

Una Lambda `trigger-airflow` ejecuta `airflow dags trigger` **dentro** de la instancia vía
`ssm:SendCommand`. No se abre ningún puerto nuevo, no hay token de API que rotar, y el permiso está
acotado por ARN a esa instancia y al documento `AWS-RunShellScript`.

La misma Lambda arranca la EC2 si el evento la encuentra apagada, lo que hace compatible el disparo
por evento con el apagado automático de [ADR-008](ADR-008-apagado-job-aware.md).

## Consecuencias

**Se gana:**

- Superficie de ataque sin cambios: disparar DAGs no agrega ni un puerto.
- La autorización es IAM, no una credencial de aplicación: auditable en CloudTrail y revocable sin
  tocar Airflow.

**Se pierde:**

- **Latencia**: `SendCommand` es asíncrono y hay que poolear `GetCommandInvocation`. No sirve para
  disparos interactivos de baja latencia.
- Depende del **agente SSM**: si está `ConnectionLost`, el disparo falla con un `AccessDenied` que
  parece de permisos y no lo es. Es el primer paso del diagnóstico de guía 02 §8.6.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Exponer la API REST de Airflow | Abre un puerto a Internet y agrega credenciales de API que hay que rotar y auditar aparte |
| API Gateway + Lambda contra la API de Airflow | Misma exposición, con una capa más y sin resolver la credencial |
| Airflow deferrable sensors poleando S3 | Obliga a tener el scheduler encendido 24×7: choca con el apagado automático |

## Dónde vive

Guía 02 [guía 02 §7.1](../02-produccion-aws-terraform.md#71-lambda-que-dispara-los-dags-vía-ssm) (módulo
`triggers`), [guía 02 §7.3](../02-produccion-aws-terraform.md#73-disparo-por-evento-archivo-nuevo-en-s3-vía-sqs)
y el catálogo de diagnóstico de [guía 02 §8.6](../02-produccion-aws-terraform.md#86-diagnóstico-rápido).
