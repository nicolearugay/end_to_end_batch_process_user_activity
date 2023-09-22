from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner': 'airflow'
}

with DAG('batch_processing_dag',
  start_date=days_ago(2),
  schedule_interval=None,
  default_args=default_args
) as dag:

  opr_run_batch_processing = DatabricksRunNowOperator(
    task_id='run_batch_processing',
    databricks_conn_id='databricks_default',
    job_id=1234
    json={
      "notebook_params": {
        "file_location": "/file/path",
        "file_type": "json"
      }
    }
  )
