from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PyhtonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta 

# DAG arguments
default_args = {
    "owner": "SD",
    "start_date": days_ago(0),
    "email": ["ibm@ibm.com"],
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    dag_id="process_web_log",
    default_args=default_args,
    description="Airflow ETL to extract IP addresses from a webserver logfile",
    schedule_interval=timedelta(days=1),
)

print(default_args)

# TASK 1 : Extract data
extract_data = BashOperator(
    task_id="extract_data",
    bash_command="cut -d' ' -f1 $AIRFLOW_HOME/dags/capstone/accesslog.txt > extracted_data.txt",
    dag=dag,
)

# TASK 2 : Transform data
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="grep -v '198.46.149.143' extracted_data.txt > transformed_data.txt ",
    dag=dag,
)

# TASK 3 : Load data
load_data = BashOperator(
    task_id="load_data",
    bash_command="tar -cf transformed_data.txt weblog.tar",
    dag=dag,
)

extract_data >> transform_data >> load_data
