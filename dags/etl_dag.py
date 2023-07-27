from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email


from settings.params import DAGS_DEFAULT_ARGS



from utils import (
    extract_wb_data, 
    transform_wb_data, 
    load_api_data_to_s3, 
    extract_api_data, 
    transform_api_data, 
    load_wb_data_to_s3
    )
from settings.params import API_URL


def failure_function(context):
    dag_run = context.get('dag_run')
    msg = "The Airflow task has failed. Please check the logs for more information."
    subject = f"DAG {dag_run} Failed"
    send_email(to='diopbamba86@gmail.com', subject=subject, html_content=msg)


def success_function(context):
    dag_run = context.get('dag_run')
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(to='diopbamba86@gmail.com', subject=subject, html_content=msg)


with DAG(
    "EPT_DATA_ENG_ETL_V0.1.0",
    default_args=DAGS_DEFAULT_ARGS,
    start_date=datetime.utcnow(),
    schedule="@daily",
    catchup=False
) as dag:
    

    extract_wb_data = PythonOperator(
        task_id="extract_wb_data",
        python_callable=extract_wb_data,
        provide_context=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )
    transform_wb_data = PythonOperator(
        task_id="transform_wb_data",
        python_callable=transform_wb_data,
        provide_context=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )
    load_wb_data_to_s3 = PythonOperator(
        task_id="load_wb_data_to_s3",
        python_callable=load_wb_data_to_s3,
        email_on_failure=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )
    extract_api_data = PythonOperator(
        task_id="extract_api_data",
        python_callable=extract_api_data,
        op_args=[API_URL],
        provide_context=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )
    transform_api_data = PythonOperator(
        task_id="transform_api_data",
        python_callable=transform_api_data,
        provide_context=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )
    load_api_data_to_s3 = PythonOperator(
        task_id="load_data_api_to_s3",
        python_callable=load_api_data_to_s3,
        email_on_failure=True,
        on_failure_callback=failure_function,
        on_success_callback=success_function
    )


    extract_wb_data >> transform_wb_data >> load_wb_data_to_s3
    extract_api_data >> transform_api_data >> load_api_data_to_s3


