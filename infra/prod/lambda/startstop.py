import os
import time
import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")


def _dags_activos(instance_id):
    """Cuenta los DAG runs en estado 'running' DENTRO de la EC2, vía SSM SendCommand.
    Guardia anti-corte: si hay alguno, NO apagamos (otro DAG sigue corriendo). Ante cualquier
    duda (comando fallido, salida no numérica) es conservador y devuelve >0 → no apagar.
    """
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
    """Prende o apaga las EC2 marcadas con el tag AutoStartStop=true.
    event = {"action": "start"} | {"action": "stop"} | {"action": "stop", "force": true}
    El stop es JOB-AWARE: no apaga si hay DAG runs corriendo (§10.3).
    Con force=true apaga igual: es el cierre duro de las 22:00 (§7.2)."""
    action = event.get("action", "stop")
    tag_key = os.environ.get("TAG_KEY", "AutoStartStop")
    tag_val = os.environ.get("TAG_VALUE", "true")

    # Solo estados accionables: start sobre "stopping" lanza IncorrectInstanceState.
    states = ["stopped"] if action == "start" else ["running"]
    resp = ec2.describe_instances(
        Filters=[
            {"Name": f"tag:{tag_key}", "Values": [tag_val]},
            {"Name": "instance-state-name", "Values": states},
        ]
    )
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    if not ids:
        return {"msg": "no instances tagged", "action": action}

    if action == "start":
        ec2.start_instances(InstanceIds=ids)
    else:
        # --- GUARDIA ANTI-CORTE: no apagar si algún DAG sigue corriendo (§10.3) ---
        # La task trigger_stop del DAG invoca esta Lambda al terminar (trigger_rule=all_done);
        # con varios DAGs en vuelo, solo el ÚLTIMO en terminar la deja apagar.
        #
        # force=True SALTEA el guard, y solo lo manda el cron de cierre de las 22:00 (§7.2).
        # Sin él, un DAG colgado —que sigue en 'running' para siempre— o un agente SSM caído
        # (_dags_activos devuelve 1 por precaución) dejaban la EC2 prendida indefinidamente:
        # el respaldo de costo no existía justo en el caso que decía cubrir.
        # Contrapartida asumida: un job legítimo que cruce las 22:00 UTC se corta. Si tenés
        # jobs largos, corré el cron más tarde (var.stop_cron) en vez de sacar el force.
        if event.get("force"):
            ec2.stop_instances(InstanceIds=ids)
            return {"action": action, "instances": ids, "forced": True}

        activos = _dags_activos(ids[0])  # un solo nodo pyspark-stack-node
        if activos > 0:
            return {"msg": f"{activos} DAG run(s) activos, no apago", "instances": ids}
        ec2.stop_instances(InstanceIds=ids)

    return {"action": action, "instances": ids}
