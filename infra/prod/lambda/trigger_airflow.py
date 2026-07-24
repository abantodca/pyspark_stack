import os
import csv
import json
import hashlib
import urllib.parse
import boto3

ssm = boto3.client("ssm")
ec2 = boto3.client("ec2")
s3 = boto3.client("s3")

INSTANCE_ID = os.environ["INSTANCE_ID"]
DEFAULT_DAG = os.environ.get("DEFAULT_DAG", "customer_etl_emr")

# Contrato mínimo por archivo: columnas/keys requeridas. Lo que no está acá no se valida (pasa
# igual) — ampliá a medida que sumes fuentes. Esto es un gate BARATO (mira solo el header/las
# primeras keys); la validación de contenido de verdad es Great Expectations (§20), después del ETL.
CONTRACTS = {
    "orders.csv": {"order_id", "customer_id", "product_id", "quantity", "order_date"},
    "customers.csv": {"customer_id", "customer_name", "city", "state", "signup_date"},
    "products.json": {"product_id", "category", "unit_price"},
}


class ContractViolation(Exception):
    pass


def _peek_columns(bucket, key):
    """Lee los primeros ~2 KB del objeto (Range GET, NO descarga el archivo entero) y devuelve
    sus columnas. CSV: el header. JSON: las keys del primer registro (soporta array u objeto).
    """
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-2047")["Body"].read()
    head = body.decode("utf-8", errors="replace")
    if key.endswith(".csv"):
        return set(next(csv.reader([head.splitlines()[0]])))
    if key.endswith(".json"):
        # products.json es un array multilínea (ver docs/04 Ej. 7): el Range GET puede cortar a
        # mitad de objeto. "Mejor esfuerzo": si no parsea con la muestra, NO bloqueamos — un falso
        # negativo acá es preferible a un falso positivo que frena un archivo válido.
        try:
            data = json.loads(head)
        except json.JSONDecodeError:
            return None
        first = data[0] if isinstance(data, list) and data else data
        return set(first.keys()) if isinstance(first, dict) else None
    return None


def _validar_contrato(bucket, key):
    esperado = CONTRACTS.get(key.rsplit("/", 1)[-1])
    if esperado is None:
        return
    columnas = _peek_columns(bucket, key)
    if columnas is None:
        return
    faltan = esperado - columnas
    if faltan:
        raise ContractViolation(
            f"{key}: faltan columnas {sorted(faltan)} (esperadas {sorted(esperado)})"
        )


def _ec2_lista(instance_id):
    """True si la instancia está running Y el agente SSM está Online. Si está stopped, dispara el
    start (idempotente) y devuelve False: NO esperamos adentro de la Lambda con un sleep — eso solo
    quema tiempo de ejecución sin ganar nada. El caller propaga el estado "todavía no" para que el
    transporte reintente en unos minutos."""
    state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0][
        "Instances"
    ][0]["State"]["Name"]
    if state == "stopped":
        ec2.start_instances(InstanceIds=[instance_id])
        return False
    if state != "running":  # pending, stopping, shutting-down
        return False
    infos = ssm.describe_instance_information(
        Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
    )["InstanceInformationList"]
    return bool(infos) and infos[0]["PingStatus"] == "Online"


def _disparar_dag(dag, conf, run_id=None):
    trigger = f"airflow dags trigger {dag}"
    if run_id:
        # Determinístico (derivado de bucket+key): si SQS reintenta un mensaje que YA disparó el
        # DAG con éxito (SendCommand es fire-and-forget, la Lambda no confirma el resultado antes
        # de retornar), `airflow dags trigger` con el MISMO --run-id falla en vez de crear un
        # segundo dagrun para el mismo archivo. Auditoría §1.3: sin esto, el retry (que es lo que
        # nos da la resiliencia de §7.3) podía convertirse en un doble-procesamiento silencioso.
        trigger += f" --run-id '{run_id}'"
    if conf:
        trigger += f" --conf '{json.dumps(conf)}'"
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Comment=f"trigger airflow dag {dag}",
        Parameters={"commands": [f"docker exec airflow-scheduler {trigger}"]},
    )
    return resp["Command"]["CommandId"]


def handler(event, context):
    """Dos formas de entrada:
    - Cron (EventBridge Scheduler, invocación async directa): {"dag": "customer_etl_emr"}.
    - Evento S3 (vía la cola SQS primaria, §7.3): {"Records": [{"body": "<S3 event JSON>"}]}.
    """
    bucket = key = run_id = None
    if "Records" in event and event["Records"] and "body" in event["Records"][0]:
        # batch_size=1 (§7.3): un mensaje SQS = un evento S3 = una invocación.
        rec = json.loads(event["Records"][0]["body"])["Records"][0]["s3"]
        key = urllib.parse.unquote_plus(
            rec["object"]["key"]
        )  # S3 codifica espacios/especiales
        bucket = rec["bucket"]["name"]
        dag, conf = DEFAULT_DAG, {"bucket": bucket, "key": key}
        # run_id determinístico por archivo (auditoría §1.3): un reintento del MISMO objeto
        # produce el MISMO run_id, así que un doble-trigger no crea un doble dagrun. El cron
        # (rama de abajo) no lo necesita tanto: dispara una vez al día, y su propio retry async
        # de Lambda es 1-2 intentos en minutos, no una cola que puede reintentar 5 veces.
        run_id = "s3-" + hashlib.sha256(f"{bucket}/{key}".encode()).hexdigest()[:16]
    else:
        dag, conf = event.get("dag", DEFAULT_DAG), {}

    try:
        if bucket and key:
            _validar_contrato(
                bucket, key
            )  # ContractViolation: NO reintentar, no tiene sentido
    except ContractViolation as e:
        print(
            f"RECHAZADO por contrato de datos: {e}"
        )  # log-based metric filter si querés alertar
        return {"status": "rejected", "reason": str(e)}

    if not _ec2_lista(INSTANCE_ID):
        # Se propaga sin capturar: dispara el retry de SQS (evento S3) o el retry async de Lambda
        # (cron) — a los pocos minutos la EC2 ya debería estar arriba y este mismo intento pasa.
        raise RuntimeError(
            f"EC2 {INSTANCE_ID} no está lista todavía (arrancando); reintentar"
        )

    return {"dag": dag, "conf": conf, "commandId": _disparar_dag(dag, conf, run_id)}
