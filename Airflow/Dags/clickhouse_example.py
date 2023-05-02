from datetime import datetime #fgdsfgdfgcxcxvcxv
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow import DAG 
from airflow.decorators import task

default_args = {
	'owner': 'Kazantsev',
	'email': ['Kazantsev_M_P@itgri.ru'],
	'email_on_failure': False,
	'catchup': False
}


def sample_usage():
    click_sql = ClickHouseHook(clickhouse_conn_id="clickhouse")

    df = click_sql.get_pandas_df("SHOW TABLES")
    print(df.count())
    print(df)

    
with DAG(
	default_args=default_args,
	dag_id='clickhouse_example',
	tags=['examples'],
	start_date= datetime(2021, 1, 1),
	schedule_interval=None
) as dag:
    some_db_tsk = PythonOperator(task_id="some_db_tsk", python_callable=sample_usage)