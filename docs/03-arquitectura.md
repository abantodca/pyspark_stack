# Arquitectura de producción — decisiones y límites

> **En este documento: LEER, ~10 min.** No hay comandos, tamaños, precios ni recetas.
> **Salís con**: el alcance de cada plano, los compromisos aceptados y el criterio para cambiar
> de arquitectura.

La arquitectura responde **por qué y hasta dónde**; la [guía 02](02-produccion-aws-terraform.md)
responde **cómo materializar y operar**. Los [ADR](adr/README.md) son el registro normativo de cada
decisión: un cambio de arquitectura requiere un ADR que la sustituya y su actualización en la guía.

> [!IMPORTANT]
> Esta es una referencia de diseño. El checkout implementa el stack local; los artefactos de AWS
> descritos por la guía 02 no están presentes ni validados end-to-end. El estado efectivo vive en la
> [matriz de documentación](README.md).

## 1. Alcance y fronteras

El diseño separa aquello que persiste y escala del proceso que lo coordina:

```mermaid
flowchart LR
    subgraph control[Plano de control: reemplazable]
        airflow[Airflow + metadata]
    end
    subgraph data[Plano de datos: durable y elástico]
        storage[S3 + catálogo]
        compute[EMR Serverless]
    end
    subgraph guardrails[Controles transversales]
        access[Identidad y acceso]
        signal[Alertas, coste y recuperación]
        quality[Calidad, contrato y trazabilidad]
    end

    airflow --> compute --> storage
    access --- control
    access --- data
    signal --- control
    signal --- data
    quality --- data
```

### Vista de componentes y flujos de referencia

La vista anterior explica los límites. Esta segunda vista muestra cómo se relacionan los componentes
principales del diseño de referencia; no es inventario de recursos desplegados ni receta de
implementación. Para el estado efectivo, prevalece la [matriz de documentación](README.md).

```mermaid
flowchart TD
    operator[Operador o desarrollador]
    producer[Productor de datos]
    ci[CI/CD con identidad de servicio]
    alerts[Alertas, coste y recuperación fuera del host]

    subgraph aws[Entorno AWS de referencia]
        subgraph triggers[Disparo y automatización]
            scheduler[Planificador]
            queue[SQS y DLQ]
            trigger[Lambda de disparo]
            ssm[Canal privado de administración]
            lifecycle[Automatización de encendido y apagado]
        end

        subgraph control_plane[Plano de control - reemplazable en etapa A]
            airflow[Airflow]
            metadata[(Metadata de Airflow)]
            local_signal[Señales locales auxiliares]
            airflow --- metadata
        end

        emr[EMR Serverless - Spark]
        catalog[Catálogo de datos]

        subgraph lake[Data lake durable]
            raw[raw]
            curated[curated]
            analytics[analytics]
        end
        artifacts[Artefactos y logs remotos]
    end

    operator -->|administración privada| ssm
    ssm -->|dispara DAGs sin API pública| airflow
    producer -->|publica lote| raw
    raw -->|evento de datos| queue
    scheduler -->|ejecución programada| trigger
    queue -->|evento recuperable| trigger
    trigger -->|orden privada| ssm
    scheduler -->|ventana operativa| lifecycle
    lifecycle -->|gestiona el host del control plane| control_plane
    airflow -->|lanza job| emr
    emr <-->|metadatos de tablas| catalog
    emr -->|lee y escribe| lake
    airflow -->|logs de tareas| artifacts
    emr -->|logs de ejecución| artifacts
    ci -->|publica artefactos| artifacts
    artifacts -->|entrega controlada| ssm
    control_plane -->|salud y fallos| alerts
    emr -->|estado y coste| alerts
    queue -->|mensajes no procesados| alerts
    local_signal -.->|complementa, no sustituye| alerts
```

Lectura del flujo: un lote entra a `raw`, su evento queda protegido por una cola y el disparo llega
a Airflow por un canal privado. Airflow delega el cómputo en EMR Serverless, que usa el catálogo y
lee/escribe en el data lake. Ni el dato durable ni las señales críticas dependen del host del plano
de control.

| Frontera | Decisión | Consecuencia deliberada |
|---|---|---|
| Plano de control | Airflow y su metadata comienzan juntos en un único host reemplazable | Hay un SPOF aceptado en etapa A; no se resuelve añadiendo contenedores al mismo host |
| Plano de datos | Almacenamiento durable y cómputo Spark bajo demanda quedan fuera del host de Airflow | El coste de Spark sigue al uso y la caída del orquestador no implica pérdida de datos |
| Disparo y acceso | La automatización y administración no dependen de exponer la API de Airflow | La integración usa identidades AWS y canales de administración privados |
| Señal crítica | Las alertas que deben sobrevivir al host viven fuera de él | Las herramientas de observabilidad locales son auxiliares, no un sustituto |

## 2. Decisión por etapas

| Etapa | Se usa cuando | Decisión | Límite que permanece |
|---|---|---|---|
| **A · Base de coste controlado** | Un equipo, batch con volumen bajo o irregular y RTO de horas | Plano de control single-node; plano de datos serverless | No promete alta disponibilidad del orquestador |
| **B · Control plane resiliente** | El RTO/RPO, varios equipos u operación fuera de horario ya no toleran el SPOF | Migrar Airflow/metadata a una opción HA gestionada | Calidad, gobierno y recuperación siguen siendo responsabilidad del producto de datos |
| **C · Multi-dominio o regulada** | PII sensible, auditoría, acceso granular o dominios múltiples | Separar cuentas y reforzar gobierno, claves y backups | Exige un modelo operativo; no se obtiene al activar un servicio aislado |

El volumen por sí solo no obliga a avanzar de etapa. El disparador es el riesgo aceptado: SLO,
RTO/RPO, clasificación de datos, número de operadores y coste de una interrupción.

## 3. Invariantes de diseño

- El cómputo Spark no comparte el host del orquestador.
- El almacenamiento de datos no depende de discos efímeros del plano de control.
- Una identidad tiene permisos mínimos para una responsabilidad; no se distribuyen access keys.
- Todo disparo es recuperable e idempotente antes de procesar datos reales.
- Ningún dataset se promueve sin owner, contrato, validaciones y un procedimiento de reversión.
- Un backup no cuenta como recuperación hasta que se prueba contra el RPO/RTO acordado.
- Un presupuesto, una alerta y un responsable son parte del sistema, no anexos operativos.

Estas invariantes no prescriben herramientas adicionales. Si una implementación las incumple, se
debe justificar explícitamente en un ADR y en el gate de producción.

## 4. Decisiones ratificadas

| Decisión | Por qué existe | Registro canónico |
|---|---|---|
| Spark fuera del control plane | Evitar cómputo ocioso y aislar recursos del orquestador | [ADR-001](adr/ADR-001-emr-serverless-para-spark.md) |
| Control plane single-node en A | Coste y complejidad proporcionales al SLO inicial | [ADR-002](adr/ADR-002-plano-de-control-single-node.md) |
| Disparo privado, sin API HTTP de Airflow | Reducir superficie de exposición y usar identidad de servicio | [ADR-003](adr/ADR-003-disparo-por-ssm-no-api-http.md) |
| State de Terraform con lock nativo de S3 | Estado único y bloqueo sin servicio adicional | [ADR-004](adr/ADR-004-backend-s3-con-use-lockfile.md) |
| Infraestructura compuesta por entorno y módulos | Límites claros entre componentes y contratos explícitos | [ADR-005](adr/ADR-005-composicion-envs-modules.md) |
| El creador de un recurso concede su acceso | Reducir dependencias cruzadas y permisos implícitos | [ADR-006](adr/ADR-006-el-modulo-que-crea-otorga.md) |
| Contexto operativo desde outputs | Eliminar IDs, IPs y nombres copiados a mano | [ADR-007](adr/ADR-007-contrato-de-variables-por-output.md) |
| Apagado consciente de jobs | No sacrificar ejecuciones para ahorrar coste | [ADR-008](adr/ADR-008-apagado-job-aware.md) |
| Evolución por etapas y gates | Hacer explícito cuándo A deja de ser suficiente | [ADR-009](adr/ADR-009-arquitectura-por-etapas-y-gates-de-produccion.md) |

## 5. Condiciones para datos reales

La etapa A no se autoriza con un despliegue exitoso. Antes de activar eventos o datos reales debe
existir evidencia de los siguientes resultados:

| Resultado | Pregunta que debe poder responderse |
|---|---|
| Propiedad y clasificación | ¿Quién responde por cada dataset, su PII, retención y consumidores? |
| Calidad y promoción | ¿Qué impide que un lote inválido llegue a datos curados y cómo se revierte? |
| Recuperación | ¿Se probó restaurar dentro del RPO/RTO aceptado? |
| Operación | ¿Un fallo llega a un responsable y puede reprocesarse de forma segura? |
| Coste | ¿Qué límite, señal y owner detienen o investigan un incremento anómalo? |
| Seguridad | ¿El acceso, los cambios de control y el ciclo de parches tienen dueño y evidencia? |

El detalle de controles y la evidencia requerida están en la guía 02: [gate de producción (§1.2)](02-produccion-aws-terraform.md#12-gate-de-producción-qué-falta-y-qué-no-se-negocia), [runbook (§15)](02-produccion-aws-terraform.md#15-runbook-de-puesta-en-producción), [gobierno (§18)](02-produccion-aws-terraform.md#18-gobierno-resiliencia-y-costos), [calidad (§20)](02-produccion-aws-terraform.md#20-calidad-de-datos) y [recuperación (§21.3)](02-produccion-aws-terraform.md#213-recuperación).

## 6. Dónde vive cada detalle

| Necesidad | Fuente de verdad |
|---|---|
| Arquitectura, límites, criterios de evolución y trade-offs | Este documento y los [ADR](adr/README.md) |
| Terraform, comandos, variables, despliegue, operación y runbook | [Guía 02](02-produccion-aws-terraform.md) |
| Estado de implementación frente a diseño | [Índice de documentación](README.md) |
| Diseño y operación del entorno reproducible local | [Stack local](01-stack-local.md) y [DataOps local](04-dataops-local.md) |

Si una decisión cambia, actualizá primero el ADR, después este documento y por último la guía de
implementación en el mismo cambio. Si solo cambian comandos, archivos o procedimientos, la
arquitectura no debe repetirlos: actualizá exclusivamente la guía 02.
