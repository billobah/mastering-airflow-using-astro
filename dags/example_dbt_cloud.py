from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

dbt_account_id = Variable.get('DBT_ACCOUNT_ID')
dbt_job_id = Variable.get('DBT_JOB_COMPUTE_DAILY_PRODUCT_REVENUE_ID')
with DAG(
    dag_id="compute_daily_product_revenue",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": dbt_account_id},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    ingest = EmptyOperator(task_id="ingest_retail")
    notify = EmptyOperator(task_id="notify")

    transform_daily_product_revenue = DbtCloudRunJobOperator(
        task_id="transform_daily_product_revenue",
        job_id=dbt_job_id,
        check_interval=10,
        timeout=300,
    )
    
    ingest >> transform_daily_product_revenue >> notify