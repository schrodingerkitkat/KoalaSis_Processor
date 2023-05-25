from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import test_pipeline

# Define the default arguments for the DAG
# These are the settings that will be applied to every task in the DAG,
# unless explicitly overridden by a task's own arguments.
default_args = {
    'owner': 'airflow',  # The owner of the task, typically the team or individual responsible for the pipeline
    'depends_on_past': False,  # Do not wait for previous executions to complete successfully before running this task
    'email_on_failure': False,  # Do not send an email when the task fails
    'email_on_retry': False,  # Do not send an email when the task is retried
    'retries': 1,  # The number of times a task should be retried before it's considered failed
    'retry_delay': timedelta(minutes=5),  # The time to wait between retries
}

# Instantiate the DAG object
dag = DAG(
    'koala_sis_pipeline',  # A unique identifier for the DAG
    default_args=default_args,
    description='A pipeline to download, transform, and validate data from KoalaSis',  # A human-readable description of the pipeline's purpose
    schedule=timedelta(days=1),  # How often the pipeline should run (once a day in this case)
    start_date=datetime(2023, 5, 1),  # The first date the pipeline will execute
    catchup=False,  # Do not backfill pipeline runs for dates before the start_date
)

# Define the pipeline tasks
# Each task represents a distinct unit of work in the pipeline

# Task 1: Download data from KoalaSis to a CSV file
download_data_task = PythonOperator(
    task_id='download_data',  # A unique identifier for this task within the DAG
    python_callable=test_pipeline.download_data_to_csv,  # The function to call to execute this task
    dag=dag,  # Associate this task with the DAG we defined earlier
)

# Task 2: Transform and conform the data to meet specific requirements
conform_data_task = PythonOperator(
    task_id='conform_data',  # A unique identifier for this task within the DAG
    python_callable=test_pipeline.conform_data,  # The function to call to execute this task
    dag=dag,  # Associate this task with the DAG we defined earlier
)

# Set task dependencies
# This defines the order in which tasks will be executed within the pipeline
download_data_task >> conform_data_task  # Run the conform_data_task after the download_data_task completes successfully

