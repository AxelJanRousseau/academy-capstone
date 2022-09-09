import datetime as dt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(
    dag_id="Docker_flow",
    description="deploy docker to aws",
    default_args={"owner": "Airflow"},
    schedule_interval="/10 * * * *",
    start_date=dt.datetime(year=2022, month=9, day=9),
)

submit_batch_job = BatchOperator(
    task_id='submit_batch_job',
    job_name='axeljan_rousseau_airflow_docker_job',
    job_queue='academy-capstone-summer-2022-job-queue',
    job_definition='axeljan_rousseau_docker_app',
    dag=dag
)

all_done = DummyOperator(task_id='All_done', dag=dag)

submit_batch_job >> all_done

