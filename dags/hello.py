from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Igor',
    'depends_on_past': False,
    'email': ['igor.enenberg@develeap.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='An example DAG',
    schedule=None,  # to run on-demand or timedelta(days=1), cron (*/2 * * * *), etc.
    start_date=days_ago(2),
    catchup=False,
    tags=['example1'],
)

say_hello = BashOperator(
    task_id='say_hello',
    bash_command="echo hello",
    dag=dag
)

say_goodbye = BashOperator(
    task_id='say_goodbye',
    bash_command="echo goodbye",
    dag=dag
)

say_hello >> say_goodbye
