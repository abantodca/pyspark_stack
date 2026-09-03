import os
import time

import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
TARGET_INSTANCE_ID = os.environ["INSTANCE_ID"]


def _dags_activos(instance_id):
    """Cuenta los DAG runs en estado 'running' DENTRO de la EC2, vía SSM SendCommand.
    Guardia anti-corte: si hay alguno, NO apagamos (otro DAG sigue corriendo). Ante cualquier
    duda (comando fallido, salida no numérica) es conservador y devuelve >0 → no apagar."""
    # Airflow 3: contamos los DAG runs 'running' consultando la metadata DB desde el scheduler.
    # (Alternativas equivalentes: `airflow jobs check --job-type SchedulerJob` para salud del
    #  scheduler, o `airflow dags list-runs --state running` filtrando por DAG.)
    py = (
        "from airflow.models.dagrun import DagRun;"
        "from airflow.utils.state import DagRunState;"
        "print(len(DagRun.find(state=DagRunState.RUNNING)))"
    )
    cmd = f'docker exec airflow-scheduler python -c "{py}"'
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment="startstop: chequeo de DAG runs activos",
        Parameters={"commands": [cmd]},
    )
    cid = resp["Command"]["CommandId"]
    inv = {"Status": "Pending"}
    for _ in range(20):  # espera hasta ~40s a que el comando termine
        time.sleep(2)
        inv = ssm.get_command_invocation(CommandId=cid, InstanceId=instance_id)
        if inv["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
            break
    if inv["Status"] != "Success":
        return 1  # no pudimos verificar → conservador: no apagar
    try:
        return int(inv["StandardOutputContent"].strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 1


def handler(event, context):
    """Prende o apaga únicamente la EC2 recibida por INSTANCE_ID.
    event = {"action": "start"} | {"action": "stop"} | {"action": "stop", "force": true}
    El stop es JOB-AWARE: no apaga si hay DAG runs corriendo (sección 10.3).
    Con force=true apaga igual; se reserva para una intervención manual de emergencia."""
    action = event.get("action", "stop")

    # El DAG invoca de forma asíncrona mientras su última task aún figura running. La espera
    # acotada permite que Airflow confirme SUCCESS antes de que la guarda consulte los DAG runs.
    delay = min(max(int(event.get("delay_seconds", 0)), 0), 60)
    if action == "stop" and delay:
        time.sleep(delay)

    # Esta Lambda pertenece a una instancia concreta. Filtrar solo por un tag compartido podría
    # apagar otro stack de la misma cuenta.
    resp = ec2.describe_instances(InstanceIds=[TARGET_INSTANCE_ID])
    wanted_state = "stopped" if action == "start" else "running"
    ids = [
        i["InstanceId"]
        for r in resp["Reservations"]
        for i in r["Instances"]
        if i["State"]["Name"] == wanted_state
    ]
    if not ids:
        return {"msg": "instancia sin transición pendiente", "action": action}

    if action == "start":
        ec2.start_instances(InstanceIds=ids)
    else:
        # --- GUARDIA ANTI-CORTE: no apagar si algún DAG sigue corriendo (sección 10.3) ---
        # La task request_safe_stop del DAG (sección 6.6) invoca esta Lambda al terminar (trigger_rule=all_done);
        # con varios DAGs en vuelo, solo el ÚLTIMO en terminar la deja apagar.
        #
        # force=True saltea el guard y se conserva únicamente para una intervención manual de
        # emergencia. El schedule normal no lo envía: un control de costo no debe interrumpir un
        # DAG legítimo ni dejar a Airflow sin registrar correctamente el estado final.
        if event.get("force"):
            ec2.stop_instances(InstanceIds=ids)
            return {"action": action, "instances": ids, "forced": True}

        # Evaluar todas las instancias encontradas. El diseño normal tiene una sola, pero esta
        # iteración evita detener otras instancias etiquetadas si el stack se amplía o se duplica.
        blocked = {}
        safe_to_stop = []
        for instance_id in ids:
            activos = _dags_activos(instance_id)
            if activos > 0:
                blocked[instance_id] = activos
            else:
                safe_to_stop.append(instance_id)

        if safe_to_stop:
            ec2.stop_instances(InstanceIds=safe_to_stop)
        if blocked:
            return {
                "action": action,
                "stopped": safe_to_stop,
                "blocked": blocked,
                "msg": "hay DAG runs activos o el estado no pudo verificarse",
            }

    return {"action": action, "instances": ids}
