from __future__ import annotations
import os
import glob
import pandas as pd
from datetime import datetime
from jinja2 import Template

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import get_current_context

# === Rutas dentro del contenedor (mapeadas al host) ===
INBOX_DIR = "/opt/airflow/data/inbox"
OUTBOX_DIR = "/opt/airflow/data/outbox"

# Patrón del archivo que esperas a diario:
#   - Por fecha de ejecución del DAG: "ventas_{{ ds_nodash }}.csv"
#   - O cualquier CSV: "*.csv"
FILE_PATTERN = "ventas_{{ ds_nodash }}.csv"  # ajusta si tu archivo tiene otro nombre


def _rendered_glob_pattern() -> str:
    """Renderiza el patrón Jinja con el contexto del DAG y compone la ruta completa."""
    ctx = get_current_context()
    pattern = Template(FILE_PATTERN).render(**ctx)
    return os.path.join(INBOX_DIR, pattern)


def wait_for_file_callable() -> bool:
    """Devuelve True cuando existe al menos un archivo que cumple el patrón."""
    pattern = _rendered_glob_pattern()
    return len(glob.glob(pattern)) > 0


def get_first_match_path() -> str:
    """Obtiene la ruta del primer archivo que coincide con el patrón y la retorna (XCom)."""
    pattern = _rendered_glob_pattern()
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No se encontró archivo con patrón: {pattern}")
    return matches[0]  # Airflow guarda el return_value en XCom


def transform_file():
    """Ejemplo de transformación: normaliza columnas, castea 'monto' y agrega timestamp."""
    ctx = get_current_context()
    ti = ctx["ti"]
    src_path = ti.xcom_pull(task_ids="get_input_file", key="return_value")

    df = pd.read_csv(src_path)

    # Normaliza columnas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ejemplo de lógica: forzar numérico en 'monto' si existe
    if "monto" in df.columns:
        df["monto"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)

    df["processed_at"] = datetime.utcnow().isoformat()

    # Construye nombre de salida
    base = os.path.basename(src_path)
    name, ext = os.path.splitext(base)
    out_path = os.path.join(OUTBOX_DIR, f"{name}_processed{ext}")

    # Escribe resultado
    os.makedirs(OUTBOX_DIR, exist_ok=True)
    df.to_csv(out_path, index=False)

    # Archiva el original
    archived_dir = os.path.join(INBOX_DIR, "archivados")
    os.makedirs(archived_dir, exist_ok=True)
    os.replace(src_path, os.path.join(archived_dir, base))

    return out_path


with DAG(
    dag_id="file_sensor_pipeline",
    description="Espera un archivo desde el host, lo transforma y deja la salida en outbox.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # ✅ reemplaza days_ago
    schedule="@daily",                                    # ✅ forma moderna
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "analytics", "retries": 0},
    tags=["files", "sensors"],
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=wait_for_file_callable,
        poke_interval=30,      # revisa cada 30s
        timeout=60 * 60 * 6,   # espera hasta 6 horas
        mode="poke",           # usa 'reschedule' si quieres liberar el worker mientras espera
    )

    get_input_file = PythonOperator(
        task_id="get_input_file",
        python_callable=get_first_match_path,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_file,
    )

    wait_for_file >> get_input_file >> transform
