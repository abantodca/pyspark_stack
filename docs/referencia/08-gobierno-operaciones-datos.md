# Gobierno y operaciones de datos

> **Estado:** estándar operativo del proyecto. Los controles locales están implementados; los
> controles AWS son condiciones de entrada y no prueban que exista un despliegue.

Este documento define quién decide, qué evidencia debe producir un lote y cuándo puede publicarse.
No agrega servicios ni costo cloud: usa contratos en código, pruebas, logs estructurados, metadatos
de Airflow y los controles AWS ya contemplados en la arquitectura. Catálogo avanzado, lineage o una
herramienta especializada requieren una decisión posterior con costo y beneficio medidos.

## Principios no negociables

- Un dataset sin owner, steward, clasificación y consumidor declarado no entra a producción.
- Un lote no se publica sólo porque el job terminó: debe aprobar contrato, calidad y reconciliación.
- La entrada de un run es inmutable e identificable; una ruta mutable no es evidencia suficiente.
- Reintentar el mismo lote produce el mismo resultado o una nueva versión explícita, nunca duplicados.
- Todo control deja evidencia vinculada a `dag_id`, `run_id`, fecha lógica y versión del artefacto.
- Una excepción tiene responsable, riesgo, compensación y fecha de vencimiento.

## Responsabilidad y segregación

| Rol | Accountable por | No debe hacer en solitario |
|---|---|---|
| Data Product Owner | propósito, consumidores, semántica, frescura y aceptación del dato | aprobar su propia excepción de calidad crítica |
| Data Steward | clasificación, glosario, claves, reglas de calidad y retención | cambiar reglas de negocio sin aprobación del owner |
| Data Platform Owner | runtime, IAM, costos, disponibilidad y recuperación | alterar semántica del producto de datos |
| DataOps Operator | ejecución, triage, reintentos, evidencia y comunicación | saltar el gate o corregir datos fuente en silencio |
| Security/Privacy | acceso, tratamiento de datos personales y respuesta a exposición | aceptar riesgo de negocio en nombre del owner |

En este repositorio los nombres concretos están **por designar**. Eso no bloquea el laboratorio con
datos sintéticos, pero sí bloquea incorporar datos reales o autorizar un primer despliegue.

## Registro mínimo de datasets

| Dataset | Clave / grano | Clasificación preliminar | Reglas mínimas | Estado |
|---|---|---|---|---|
| `orders` | una fila por `order_id` | confidencial; comportamiento de cliente | clave única, cantidad positiva, fecha válida, referencias existentes | contrato local implementado |
| `products` | una fila por `product_id` | interna | clave única, precio no negativo, categoría no nula | contrato local implementado |
| `customers` | una fila por `customer_id` | personal/confidencial si deja de ser sintético | clave única; nombre, ciudad y estado protegidos; fecha válida | contrato local implementado |
| `customer_loyalty` | una fila por cliente activo y fecha lógica | perfil derivado personal/confidencial | clave única, gasto no negativo, estado en dominio, reconciliación de clientes activos | gate local implementado |

Antes de usar datos reales, el steward debe completar propietario, sistema fuente, consumidores,
base de tratamiento, residencia, retención aprobada y procedimiento de supresión. La guía técnica
propone retenciones para controlar costo; no reemplaza esa aprobación legal y de negocio.

## Contrato de entrada y versionado

El contrato de un lote contiene como mínimo: nombre y versión del esquema, fecha lógica, objetos
exactos, tamaño, checksum, versión del objeto cuando exista, productor y timestamp. El consumidor
rechaza campos obligatorios ausentes, objetos inesperados y cambios incompatibles.

Compatibilidad:

- agregar un campo nullable puede ser compatible si ningún consumidor exige esquema exacto;
- renombrar, eliminar, cambiar tipo, clave o significado exige una versión mayor y plan de migración;
- una nueva versión se prueba contra productores y consumidores antes de promoverla;
- el esquema y la lógica que generaron una salida deben poder asociarse a un commit o digest.

La guía AWS describe manifests, pero su job de referencia todavía lee rutas fijas. Por tanto, el
flujo event-driven **no está autorizado para datos reales** hasta que el job consuma exactamente los
objetos y versiones validados por el manifest. Ese cambio de despliegue queda a cargo del usuario.

## Ciclo de vida y publicación

```text
landing/raw inmutable → validación estructural → staging por run
                    → calidad + reconciliación → promoción atómica a curated
                    → modelos analytics → consumidores
                         ↘ rechazo/cuarentena + incidente
```

`raw` conserva evidencia del productor; `staging` es reemplazable y no se ofrece a consumidores;
`curated` sólo recibe lotes aprobados; `analytics` añade semántica de consumo. Un fallo conserva el
lote y sus métricas, no publica parcialmente y no se “arregla” editando el resultado a mano.

## Gate de calidad

El gate local ejecuta esquemas explícitos, parsing estricto, no nulos, unicidad, rangos, dominios,
integridad referencial y reconciliación entre clientes activos y salida. `task test` demuestra las
reglas de negocio y escenarios de rechazo sin levantar servicios externos.

Para producción, antes de promover un lote se exige además:

- volumen y frescura comparados con una línea base aprobada;
- checksum/conteo de entrada y salida registrados;
- reglas por campo con severidad `block`, `warn` o `observe`;
- quarantine para fallos del productor y retry sólo para fallos transitorios;
- aprobación explícita para cualquier override, nunca un parámetro oculto del DAG.

Great Expectations no es requisito: SQL, PySpark y pytest bastan si producen la misma evidencia. Se
adopta una herramienta adicional sólo si reduce carga operativa neta.

## SLI, SLO y recuperación

Estos son objetivos de ingeniería propuestos; owner y negocio deben ratificarlos antes de medirlos
como compromiso:

| Indicador | Objetivo inicial | Evidencia |
|---|---|---|
| Frescura | lote diario publicado antes de fecha lógica + 2 h en al menos 95% de ejecuciones | timestamps de Airflow y dataset |
| Calidad de publicación | 100% de lotes publicados aprobaron reglas `block` | métricas `[QUALITY]` ligadas al run |
| Completitud | filas de salida = clientes activos reconciliados | métricas de entrada/salida |
| Recuperación | RPO máximo 24 h y RTO objetivo 2 h | restore drill y cronología del incidente |
| Detección | todo fallo terminal o mensaje en DLQ genera una señal accionable | prueba de alerta y acuse |

Un porcentaje sin volumen, ventana y fuente de medición no es un SLO. Los meses sin ejecución no se
consideran éxito. Un cambio de objetivo se registra como decisión, no se reescribe retroactivamente.

## Operación e incidentes

| Severidad | Ejemplo | Respuesta |
|---|---|---|
| SEV-1 | exposición de datos, corrupción publicada o pérdida irreversible | detener publicación, preservar evidencia, avisar a Security/owner y activar recuperación |
| SEV-2 | lote crítico vencido, DLQ creciendo o restauración necesaria | asignar operador, contener, recuperar y comunicar ETA |
| SEV-3 | degradación sin incumplir frescura, warning de calidad o capacidad | corregir en horario operativo y seguir tendencia |

El registro mínimo incluye impacto, inicio/detección, run y objetos afectados, decisiones, timeline,
causa, recuperación, validación posterior y acciones con dueño/fecha. Un rerun requiere confirmar que
la causa fue corregida, que la entrada no cambió y que la escritura es idempotente. Nunca se vacía
una DLQ ni se borra staging antes de preservar la evidencia necesaria.

## Cambio, acceso y evidencia

- Pull request, revisión y tests verdes para lógica, contratos, DAGs y documentación.
- Artefactos productivos inmutables por commit/digest; `latest` y rutas sobrescribibles son sólo lab.
- Acceso por mínimo privilegio, identidad individual y credenciales temporales; revisión periódica.
- Restore drill en un entorno sin datos reales antes del go-live y después de cambios de backup.
- Evidencia de release: plan completo, artefactos, resultados de tests, aprobación y rollback probado.
- El uso de `terraform -target` termina al construir módulos; una promoción usa y revisa plan completo.

## Gate para datos reales

No se autoriza producción hasta que todas estas condiciones tengan evidencia:

- [ ] owner, steward, operador y contacto de seguridad con nombres concretos;
- [ ] clasificación, propósito, consumidores y retención aprobados para cada dataset;
- [ ] contrato versionado y manifest enlazado a objetos inmutables exactos;
- [ ] staging, quality gate y promoción sin publicación parcial;
- [ ] SLO ratificados, alertas probadas y runbook con escalamiento;
- [ ] backup y restauración demostrados dentro de RPO/RTO;
- [ ] artefactos inmutables, dependencias escaneadas y rollback ensayado;
- [ ] costo esperado y límites de capacidad revisados sin añadir servicios no aprobados.

Hasta entonces, el repositorio es un laboratorio local endurecido y una arquitectura de referencia,
no una plataforma certificada para datos reales.
