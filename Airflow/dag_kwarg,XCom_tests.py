"""
Lesson 3
"""
from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
#from airfow.models import XCom, TaskInstanse

DEFAULT_ARGS = {
    'start_date': datetime(2023, 1, 1),
    'owner': 'ds',
    'email': ['sokolartemy@gmail.ru'],
    'email_on_failure': True,
    'retries': 4,
    'sla': timedelta(hours=1),
    'poke_interval': 300
}

with DAG("a-sokolov-6-ais",
         #schedule_interval='@dayly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ais','xcom']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def func_jinja():
        #date_param = kwargs['tomorow_ds']
        #lk = []
        #lv = []
        #for k,v in kwargs:
        #    lk.append(k)
        #    lv.append(v)
        #logging.info("kwargs:", lk, lv )
        sss = "test_jinja_tomorrow_ds: {{ tomorrow_ds }} , {{ ds }},  {{ ts }}"
        logging.info('logging_s:' , sss)
        logging.info(f'''logging_s2: {{ tomorrow_ds }} , {{ ds }}''')


    def func_xcom_push(**kwargs):
        logging.info("1 line done ")

        dtt = kwargs['tomorrow_ds']
        s4 = 'config_jinja_contains: {{ conf }}'
        kwargs['ti'].xcom_push(key = 'XCom_key1', value = s4)
        kwargs['ti'].xcom_push(key='XCom_key2', value='v2')
        logging.info("last line", dtt)
        logging.info(f"last line2 {dtt}")

    def func_kwargs(**kwargs):
        logging.info("1 line done ")
        logging.info(f"func_kwargs done, kwargs: {kwargs}")

    def func_xcom_pull(**kwargs):
        logging.info("1 line done ")
        #st = ti.xcom_pull(task_ids = )
        sss = 'extra' + str(kwargs['ti'].xcom_pull(task_ids='task_xcom_push', key='XCom_key2'))
        logging.info(kwargs['ti'].xcom_pull(task_ids='task_xcom_push', key='XCom_key2'))
        logging.info(f'should be written extra+ v2:  { sss} ')
        #logging.info(kwargs['templates_dict']['implicit'])


    task_jinja = PythonOperator(
        task_id='task_jinja',
        python_callable=func_jinja
    )

    task_xcom_push = PythonOperator(
        task_id='task_xcom_push',
        python_callable=func_xcom_push
    )

    task_kwarg = PythonOperator(
        task_id='task_kwarg',
        python_callable=func_kwargs
    )

    task_xcom_pull = PythonOperator(
        task_id='task_xcom_pull',
        python_callable=func_xcom_pull

    )

    dummy >> [task_xcom_push, task_jinja, task_kwarg]
    task_xcom_push >> task_xcom_pull