"""
Тестовая витрина для стажеров ГПБ
"""

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG 
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import logging
import os
import pathlib

default_args = {
	'owner': 'Sokolov',
	'start_date': datetime(2023, 4, 20, 0, 0),
    'end_date': datetime(2023, 4, 28, 0, 0), #datetime(),
	'email': ['sokolartemy@gmail.com'],
	#'email_on_failure': ['sokolartemy@gmail.com'],
	'catchup': False
}
   
with DAG(
	default_args=default_args,
	dag_id='ais_dag_postgresql',
	tags=['examples'],
	start_date= datetime(2023, 4, 21),
	schedule_interval=timedelta(days=1)
) as dag:

	pg_hook = PostgresHook(
		postgres_conn_id='postgres'
		, schema='postgres'
	)
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()

	def drop_tbl(**kwargs):

		sql_drop = '''
		drop table if exists postgres.public.gpb_intern_test; 
		commit; '''
		cursor.execute(sql_drop)

		today_date = kwargs['ds']
		logging.info(f'''Удалена таблица за дату: {today_date}. ''')

	def create_tbl(**kwargs):
		#Создаем таблицу и sequence
		sql_create = '''
		create table if not exists postgres.public.gpb_intern_test
			(id  int, name varchar(32), subject varchar(32), mark int); 
			commit; 
		CREATE SEQUENCE if not exists mysequence2
		INCREMENT 1
		START 4'''
		cursor.execute(sql_create)

	def insert_from_file(**kwargs):
		# Запуск sql скрипта из файла
		current_dir = str(os.getcwd())
		kwargs['ti'].xcom_push(value=current_dir, key='current_dir')
		fspath = os.fspath(current_dir)
		kwargs['ti'].xcom_push(value=fspath, key='fspath')

		# Файл в действительности записывается, но в директории не виден. При этом из него можно считывать данные
		with open(pathlib.Path.cwd() / 'sokolov' / 'f.txt', 'w') as opened_file:
			opened_file.write(''' insert into postgres.public.gpb_intern_test(id, name, subject, mark) 
			values (nextval('mysequence2'),  'Maxim', 'Airflow', 5)
			, (nextval('mysequence2'),  'Maxim', 'Git', 4)
			, (nextval('mysequence2'),  'Sveta', 'Hadoop', 4)
			, (nextval('mysequence2'),  'Ivan', 'Python', 5);
			 commit;
			  ''')

		with open(pathlib.Path.cwd() / 'sokolov' / 'f.txt', 'r') as opened_file:
			sql_from_file = opened_file.read()
		cursor.execute(sql_from_file)

	def insert_from_airflow(**kwargs):
		# Запуск sql скрипта напрямую из airflow
		sql_from_airflow = ''' insert into postgres.public.gpb_intern_test(id, name, subject, mark) 
		values (nextval('mysequence2'),  'Elon', 'Rocket launch', 3);
		 commit; '''
		cursor.execute(sql_from_airflow)

	def best_student(**kwargs):
		# Вопрос: В чем опасность использования row_number() ?
		sql_best = '''
		with cte1 as (
		select name, avg(mark)
		, row_number() over(
							order by avg(mark) desc
							) as rn
		from postgres.public.gpb_intern_test
		group by name			
		)
		select name 
		from cte1
		where rn = 1
		'''
		cursor.execute(sql_best)
		query_res2 = cursor.fetchall()
		query_res2 = str(query_res2[0][0])

		#Отправляем переменную в XCom (необязательно)
		kwargs['ti'].xcom_push(value=query_res2, key='query_res2')

		sql_create_best = '''
		create table if not exists postgres.public.gpb_intern_test_best
			(id  int, name varchar(32), date_time timestamp) ;
			commit; 
		CREATE SEQUENCE if not exists sequence_best
		INCREMENT 1
		START 3'''
		cursor.execute(sql_create_best)
		date_param = kwargs['ds']
		sql_insert_best = '''insert into postgres.public.gpb_intern_test_best(id, name, date_time) 
					values (nextval('sequence_best'),  {0}, now()) ;
					 commit; '''.format("'" + query_res2 + "'")
		cursor.execute(sql_insert_best)

# Определение Tasks (Задач - вершин дага)
	task_drop_tbl = PythonOperator(
		task_id='task_drop_tbl',
		python_callable=drop_tbl,
		do_xcom_push=True
		)

	task_create_tbl = PythonOperator(
		task_id='task_create_tbl',
		python_callable=create_tbl,
		do_xcom_push=True
		)

	task_insert_from_file = PythonOperator(
		task_id='task_insert_from_file',
		python_callable=insert_from_file,
		do_xcom_push=True
		)
	task_insert_from_airflow = PythonOperator(
		task_id='task_insert_from_airflow',
		python_callable=insert_from_airflow,
		do_xcom_push=True
		)
	task_best_student = PythonOperator(
		task_id='task_best_student',
		python_callable=best_student,
		do_xcom_push=True
		)

# Определение ацикличного графа
	task_drop_tbl >> task_create_tbl >> [task_insert_from_file, task_insert_from_airflow] >> task_best_student
