# DataOps local: los 15 pipelines medallion

> **En este documento: CONSULTAR, ~5 min.** Es la referencia de operación de los
> pipelines. El código y su explicación están en la
> [guía 06](06-medallion-desde-cero.md); acá va cómo se corren y dónde escriben.

> [!IMPORTANT]
> **`dags/` arranca vacío.** El código de los 15 proyectos no se versiona: se escribe
> siguiendo la [guía 06](06-medallion-desde-cero.md), que lo entrega completo y en orden,
> explicando cada decisión. `task local:gate` verifica que estén los quince.

Cada DAG en `dags/medallion_dags/<dominio>_medallion_dag.py` es un pipeline autónomo:
Source → Bronze → Silver → Gold. Airflow orquesta tres tareas PySpark, Spark ejecuta el
cómputo y HDFS conserva datos, métricas de calidad y cuarentenas.

La infraestructura común vive en `dags/medallion/runtime.py`:

- `LakehouseConfig` valida nombres, fechas, capas y la URI física.
- `SparkSessionFactory` configura las sesiones contra HDFS y Spark standalone.
- `HdfsLakehouseStorage` construye destinos idempotentes y escribe Parquet Snappy.
- `QualityGate` publica métricas y detiene batches que incumplen el umbral.

```text
fuente → Bronze → Silver → Gold
                  ├→ quality
                  └→ quarantine
```

## Operación local

```bash
task local:check     # secretos, estructura y Compose efectivo
task local:up
task local:gate      # ¿están escritos los 15 proyectos?
task local:smoke     # Web Events de punta a punta contra Spark y HDFS reales
```

`task local:smoke` ejecuta `medallion_web_events` contra Spark y HDFS reales, así que
requiere tener escrito ese proyecto ([guía 06 §16](06-medallion-desde-cero.md)). Para
operar HDFS, subir una fuente propia, consultar Parquet o exportar resultados, seguí la
[guía 05](05-hdfs-desde-la-terminal.md).

Las salidas se escriben en:

```text
hdfs://hdfs-namenode:9000/lakehouse/<bronze|silver|gold|quality|quarantine>/<proyecto>/run_date=<YYYY-MM-DD>
```

## Pipelines disponibles

| DAG | Dominio Gold | Guía 06 |
|---|---|---|
| `medallion_customer_360` | clientes y lifetime value por segmento | §14 |
| `medallion_daily_sales` | ingresos, unidades y ticket medio por canal | §15 |
| `medallion_web_events` | eventos, sesiones y usuarios por hora | §16 |
| `medallion_product_catalog` | surtido y precios por categoría | §17 |
| `medallion_inventory_snapshot` | disponibilidad y reposición por SKU | §18 |
| `medallion_support_tickets` | backlog, resolución y SLA | §19 |
| `medallion_payment_reconciliation` | montos conciliados y diferencias | §20 |
| `medallion_supplier_performance` | fill rate, puntualidad y demora | §21 |
| `medallion_marketing_attribution` | ingreso atribuido por canal y campaña | §22 |
| `medallion_fraud_signals` | exposición y alertas por riesgo | §23 |
| `medallion_demand_forecasting` | forecast y propuesta de reposición | §24 |
| `medallion_customer_churn_features` | scores de churn y MRR por riesgo | §25 |
| `medallion_order_fulfillment_otif` | scorecard OTIF y excepciones | §26 |
| `medallion_aml_transaction_monitoring` | alertas AML y controles | §27 |
| `medallion_subscription_revenue` | MRR/ARR y movimientos de revenue | §28 |

Cada DAG incluye fixtures mínimos para poder correr en local. Para usar una fuente propia,
definí las variables `*_SOURCE_URI` apropiadas en `ops/sources.env`, recreá los servicios
de Airflow y usá una URI `hdfs://hdfs-namenode:9000/...`; el procedimiento está en la
guía 05.

## Contrato de ejecución

El Compose inyecta:

```text
LAKEHOUSE_ROOT=hdfs://hdfs-namenode:9000/lakehouse
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
SPARK_MASTER=spark://spark-master:7077
PYSPARK_PYTHON=python3.14
PYSPARK_DRIVER_PYTHON=python3.14
PYTHONPATH=/opt/airflow/dags:/opt/spark-apps/projects
```

`hdfs-init` crea `/lakehouse` antes de `airflow-init` y abre permisos solamente para este
laboratorio. No representa un modelo de autorización para producción.
