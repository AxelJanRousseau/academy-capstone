import datetime as dt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from conveyor.operators import ConveyorSparkSubmitOperatorV2
role = "axeljan_project-{{ macros.conveyor.env() }}"


dag = DAG(
    dag_id="Docker_flow",
    description="deploy docker to aws",
    default_args={"owner": "Airflow"},
    schedule_interval='@once',
    start_date=dt.datetime(year=2022, month=9, day=7),
    catchup=False
)


submit_batch_job =  ConveyorSparkSubmitOperatorV2(
    dag=dag,
    spark_main_version=3,
    task_id="the-task-id",
    num_executors=1,
    driver_instance_type="mx.small",
    executor_instance_type="mx.small",
    aws_role=role,
    application='local:///usr/src/app/script.py',
    application_args=[
        "--environment", "{{ macros.conveyor.env() }}",
        "--snapshotDates", "2019-11-06",
    ]
)

all_done = DummyOperator(task_id='All_done', dag=dag)

submit_batch_job >> all_done

