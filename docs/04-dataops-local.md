# DataOps local: 15 pipelines medallion

Cada DAG en `dags/medallion_dags/<dominio>_medallion_dag.py` es un pipeline autónomo:
Source → Bronze → Silver → Gold. Airflow orquesta tres tareas PySpark, Spark ejecuta el cómputo y
HDFS conserva datos, métricas de calidad y cuarentenas.

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
task local:check
task local:up
task local:smoke
```

`task local:smoke` ejecuta `medallion_web_events` contra Spark y HDFS reales. Para operar HDFS,
subir una fuente propia, consultar Parquet o exportar resultados, seguí la
[guía 05](05-hdfs-desde-la-terminal.md).

Las salidas se escriben en:

```text
hdfs://hdfs-namenode:9000/lakehouse/<bronze|silver|gold|quality|quarantine>/<proyecto>/run_date=<YYYY-MM-DD>
```

## Pipelines disponibles

| DAG | Dominio Gold |
|---|---|
| `medallion_customer_360` | clientes y lifetime value por segmento |
| `medallion_daily_sales` | ingresos, unidades y ticket medio por canal |
| `medallion_inventory_snapshot` | disponibilidad y reposición por SKU |
| `medallion_payment_reconciliation` | montos conciliados y diferencias |
| `medallion_web_events` | eventos, sesiones y usuarios por hora |
| `medallion_marketing_attribution` | ingreso atribuido por canal y campaña |
| `medallion_supplier_performance` | fill rate, puntualidad y demora |
| `medallion_support_tickets` | backlog, resolución y SLA |
| `medallion_fraud_signals` | exposición y alertas por riesgo |
| `medallion_product_catalog` | surtido y precios por categoría |
| `medallion_customer_churn_features` | scores de churn y MRR por riesgo |
| `medallion_demand_forecasting` | forecast y propuesta de reposición |
| `medallion_aml_transaction_monitoring` | alertas AML y controles |
| `medallion_order_fulfillment_otif` | scorecard OTIF y excepciones |
| `medallion_subscription_revenue` | MRR/ARR y movimientos de revenue |

Cada DAG incluye fixtures mínimos para poder correr en local. Para usar una fuente propia, definí
las variables `*_SOURCE_URI` apropiadas en `ops/sources.env`, recreá los servicios de Airflow y
usá una URI `hdfs://hdfs-namenode:9000/...`; el procedimiento está en la guía 05.

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
