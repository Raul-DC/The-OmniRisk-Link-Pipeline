from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# =========================================================
# DEFAULT ARGS
# =========================================================

default_args = {
    "owner": "omnirisk",

    # ❌ EVITAMOS DEPENDENCIA ENTRE RUNS (rompe demos fácilmente)
    "depends_on_past": False,

    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="omnirisk_pipeline",

    default_args=default_args,

    # ✔ más estable para demos que days_ago + catchup completo
    start_date=days_ago(1),

    schedule="@daily",

    # ❌ EVITAMOS EXPLOSIÓN DE 7 EJECUCIONES SI ALGO FALLA
    catchup=False,

    max_active_runs=1,
    concurrency=1,

    tags=["omnirisk", "tprm", "risk-pipeline"],
) as dag:


    # =====================================================
    # 1. ENRICH VENDORS (TPRM + API resiliente)
    # =====================================================
    enrich_vendors = BashOperator(
        task_id="enrich_vendors",
        bash_command="""
        set -euo pipefail

        echo "[INFO] START enrich_vendors | ds={{ ds }} | run_id={{ run_id }}"

        cd /opt/airflow/work

        export EXEC_DATE="{{ ds }}"

        python etl/enrich_vendors.py "$EXEC_DATE"

        echo "[INFO] DONE enrich_vendors | ds={{ ds }} | run_id={{ run_id }}"
        """,
    )


    # =====================================================
    # 2. SPARK PROCESSING (BIG DATA)
    # =====================================================
    process_transactions = BashOperator(
        task_id="process_transactions",
        bash_command="""
        set -euo pipefail

        echo "[INFO] START process_transactions | ds={{ ds }} | run_id={{ run_id }}"

        docker exec spark bash -c "
            cd /workspace && \
            spark-submit spark/process_transactions.py --process-date {{ ds }}
        "

        echo "[INFO] DONE process_transactions | ds={{ ds }} | run_id={{ run_id }}"
        """,
    )


    # =====================================================
    # 3. GRAPH EXPORT (NEO4J + NETWORKX)
    # =====================================================
    graph_export = BashOperator(
        task_id="graph_export",
        bash_command="""
        set -euo pipefail

        echo "[INFO] START graph_export | ds={{ ds }} | run_id={{ run_id }}"

        cd /opt/airflow/work

        export EXEC_DATE="{{ ds }}"

        python neo4j/graph_export.py "$EXEC_DATE"

        echo "[INFO] DONE graph_export | ds={{ ds }} | run_id={{ run_id }}"
        """,
    )


    # =====================================================
    # DEPENDENCIAS
    # =====================================================
    enrich_vendors >> process_transactions >> graph_export
