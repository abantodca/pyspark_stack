# ADR-009 — Plano de datos serverless y plano de control por etapas

**Estado:** accepted · 2026-08-31

## Contexto

El diseño inicial mezcla dos preguntas distintas: cómo procesar datos de forma eficiente y cuánto
coste/resiliencia necesita el orquestador. S3, EMR Serverless y Lambda pueden escalar y
persistir fuera de la EC2; Airflow y Postgres no. Tratar todo como un único “stack productivo”
oculta el punto único de fallo y permite activar datos reales sin calidad, alertas ni recuperación
probada.

## Decisión

Se separa la arquitectura en un plano de datos serverless y un plano de control con tres etapas:

1. **Etapa A:** EC2 única para Airflow/Postgres; S3 + EMR Serverless + Lambda de ciclo de vida;
   CloudWatch/SNS para señal crítica. Es el diseño implementado por la guía 02.
2. **Etapa B:** mismo plano de datos, pero MWAA o Airflow HA gestionado cuando RTO/RPO o la
   operación multi-equipo no toleren el single-node.
3. **Etapa C:** cuentas separadas, Lake Formation/KMS CMK, catálogo/lineage central y backups
   cross-account para datos regulados o multi-dominio.

La etapa A solo se autoriza con datos reales después de calidad/promoción, alarmas externas,
límites de coste, parcheo con dueño y restore test registrado. Si se agrega ingesta por eventos,
también exige reintentos, DLQ y replay antes de activarla.

## Consecuencias

**Se gana:**

- El coste de cómputo sigue al uso; no se introduce HA ni NAT por anticipación.
- El SPOF del control plane queda visible y tiene criterio objetivo de salida.
- Calidad, operación, coste y recuperación dejan de ser extensiones “nice to have”.

**Se pierde:**

- La etapa A no promete disponibilidad continua de Airflow.
- Hay gates y evidencia que preparar antes de activar eventos y datos reales.
- Las etapas B/C elevan coste y requieren nuevo diseño, no un cambio aislado de Terraform.

## Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Convertir todo a MWAA/servicios HA desde el inicio | Incrementa coste fijo sin demostrar que el SLO lo requiere |
| Mantener una sola EC2 y llamarla “HA” con más contenedores | No elimina el dominio de fallo de host, EBS ni Postgres |
| Retrasar calidad, alertas, límites de coste y restore test hasta después | Convierte el primer incidente o sobrecoste en el mecanismo de aprendizaje |

## Dónde vive

[03 — Arquitectura](../03-arquitectura.md#2-decisión-por-etapas) y guía 02
[§1.2](../02-produccion-aws-terraform.md#12-gate-de-producción-qué-falta-y-qué-no-se-negocia),
[§10.10.3](../02-produccion-aws-terraform.md#10103-recuperación) y
[§11](../02-produccion-aws-terraform.md#11-observabilidad-prometheus-grafana-y-loki).
