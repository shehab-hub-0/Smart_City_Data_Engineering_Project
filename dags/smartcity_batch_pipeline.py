from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

_PYPATH = (
    "export PYTHONPATH=$PYTHONPATH"
    ":/opt/airflow/scripts"
    ":/opt/airflow/scripts/data"
    " && "
)

with DAG(
    "smartcity_batch_pipeline",
    default_args=_default_args,
    description="Generate CSV and Database tables, then push everything to MinIO Landing Zone.",
    schedule=None,
    catchup=False,
    tags=["smartcity", "batch", "generator"],
) as dag:

    batch_start = EmptyOperator(task_id="start")

    # Task 1: Generate CSV Data
    gen_csv = BashOperator(
        task_id="generate_csv",
        bash_command=f"set +e\n{_PYPATH} python3 /opt/airflow/scripts/data/producers/main.py --mode csv",
        execution_timeout=timedelta(minutes=5),
    )

    # Task 2: Generate DB Data
    gen_db = BashOperator(
        task_id="generate_db",
        bash_command=f"set +e\n{_PYPATH} python3 /opt/airflow/scripts/data/producers/main.py --mode db",
        execution_timeout=timedelta(minutes=5),
    )

    # Task 3: Send generated data to Kafka Batch topic
    send_to_kafka = BashOperator(
        task_id="batch_to_kafka",
        bash_command=f"set +e\n{_PYPATH} python3 /opt/airflow/scripts/data/producers/batch_to_kafka.py",
        execution_timeout=timedelta(minutes=5),
    )

    batch_end = EmptyOperator(task_id="end")

    # Run CSV and DB generation in parallel, then send to Kafka
    batch_start >> [gen_csv, gen_db] >> send_to_kafka >> batch_end
