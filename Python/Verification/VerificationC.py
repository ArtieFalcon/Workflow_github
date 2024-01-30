#TODO: run spark session through try-exept-finally 
#TODO: revision imports
#TODO: add Docstrings to class abd mthds

#DONT FORGET To STOP SESSION
import sys
import os
sys.path.insert(0, '/usr/hdp/current/***’)
sys.path.insert(0, '/usr/hdp/current/***’)
#from datetime import datetime, date, timedelta
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, struct, regexp_replace, when, expr, col, lower
#import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark import HiveContext
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.6'
os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client/"

#Дополнительный импорт
import pandas as pd
import numpy as np
import datetime
import warnings
warnings.filterwarnings("ignore")
from IPython.core.display import display, HTML
#display(HTML("<style>.container { width:98%; }</style>"))
pd.options.display.float_format = '{:,.6f}'.format
pd.set_option('display.max_columns', None)
from IPython.display import Markdown, display

def printmd(string, color=None):
    colorstr = "<span style='color:{}'>{}</span>".format(color, string)
    display(Markdown(colorstr))
    
class VerificationClass(): 
    def __init__(self):
        # За какой период происходит сравнение
        self.period = '' #'2021-03-01 00:00:00'
        # По сколько записей за раз запрос будет извлекать из таблицы (500 или менее)
        #self.rows_amount = 100
        # Каково максимальное расхождение в параметрах (например, для пустых 2%, для всех остальных – 30%)
        self.null_diff = 0.05
        self.other_diff = 0.3
        #self.null_diff_desc = f' {self.null_diff*100} %'
        self.html_str = '''
        <head>
        <style>
        .button {
          border: none;
          color: black;
          padding: 5px 22px;
          text-align: center;
          text-decoration: none;
          display: inline-block;
          font-size: 14px;
          margin: 4px 2px;
          cursor: pointer;
          width:230px;
        }

        .button1 {background-color: #f3f5bc;} 
        .button2 {background-color: #eff294;}
        .button3 {background-color: #dade5f;} 
        </style>
        </head>
        <body>
        '''
        
        
    def empty(self):
        
        display(HTML('''<button onclick="alert('Максимальное расхождение в пустых значениях = 0.05 \n в иных - 30%')">Параметры отклонений</button>'''))
        print('self.null_diff = ', self.null_diff)
        
        
        html_str2 ='''
        <button class="button button1" onclick="alert('паааа! {price}')" >Параметры отклонений</button>
        <button class="button button2" onclick="alert('паааа! {price}')">Параметры Spark сессии</button>
        <button class="button button3" onclick="alert('паааа! {price}')">Сравниваемые объекты</button>
        '''.format(price = ddd)
        display(HTML(self.html_str+html_str2))


    def RunSpark(self):
        spark_application_name = '***'
        spark_queue = '***'
        
        html_str2 ='''
        <button class="button button1" 
        onclick="alert('spark_application_name: {spark_application_name}. spark_queue: {spark_queue}.')">
        Параметры Spark сессии</button>
        '''.format(spark_application_name = spark_application_name, spark_queue = spark_queue)
        display(HTML(self.html_str+html_str2))
        
        spark =  (SparkSession.builder
                 .master('yarn')
                 .appName(spark_application_name)
                 .config('spark.yarn.queue', spark_queue)
                 .config('spark.driver.cores', '8')
                 .config('spark.driver.memory', '16g')
                 .config('spark.executor.memory', '16g')
                 .config('spark.driver.maxResultSize', '8g')
                 .config('spark.executor.cores', '2') #
                 .config("spark.executor.instances", '4')
                 .config('spark.sql.warehouse.dir', 'file://{}/***'.format(os.path.expanduser('~')))
                 .config('spark.dynamicAllocation.enabled', 'true')
                 .config('spark.shuffle.service.enabled', 'true')
                 .enableHiveSupport()
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setCheckpointDir("/tmp/***_checkpoints")
        
        return spark
    
    def Verify(self, spark, sample_table, sample_date_field, test_table, test_date_field, report_date_field, rows_amount):
        html_str3 ='''
        <button class="button button2" 
        onclick="alert('максимальное отклонение пустых значений: {f1}. Иных отклонений: {f2}.')">
        Параметры отклонений</button>
        <button class="button button3" 
        onclick="alert('Таблица-Эталон: {etalon}. Тестируемая таблица: {test_tbl}.')">
        Сравниваемые объекты</button>
        '''.format(f1 = self.null_diff, f2 = self.other_diff, etalon = sample_table, test_tbl = test_table)
        display(HTML(self.html_str+html_str3))
    
        def get_description(spark, table_nm):
            df_structure = spark.sql("Describe " + table_nm).collect()
            df_structure = pd.DataFrame(df_structure, columns = ['column_name', 'data_type', 'comment'])
            df_structure = df_structure[['column_name','data_type']]
            return df_structure
    
        try:
            # Получаем описание обеих таблиц
            df_sample_description = get_description(spark, sample_table)
            df_test_description = get_description(spark, test_table)

            df_description = pd.merge(df_sample_description, df_test_description, on = ['column_name'], suffixes = ['_sample', '_test'])
            df_description_diff = df_description[df_description['data_type_sample'] != df_description['data_type_test']]
            if df_sample_description.shape[0] != df_description.shape[0] and df_test_description.shape[0] != df_description.shape[0]:
                printmd(f'WARN: Наименования столбцов таблиц {sample_table} и {test_table} не совпадают!', color="red")
            else:
                printmd(f'Наименования столбцов таблиц {sample_table} и {test_table} совпадают', color="green")
            print('Столбцов с одинаковым наименованием:', df_description.shape[0], '\n', list(df_description['column_name']))
            print('\n Столбцов с одинаковым наименованием, но различающимися типами данных: ', df_description_diff.shape[0])
            if df_description_diff.shape[0] != 0:
                print('Разные типы для следующих столбцов: \n', df_description_diff)
                print('Тип данных различается для полей:')
                for index, field in enumerate(df_description_diff['column_name'], 1):
                    print('\t' + str(index) + '. ' + field)
            no_col_in_test = df_sample_description[df_sample_description['column_name'].isin(df_description['column_name']) == False]['column_name']
            print('Отсутствуют в test-таблице\n', no_col_in_test, ':', no_col_in_test.count(), 'столбцов')
            no_col_in_sample = df_test_description[df_test_description['column_name'].isin(df_description['column_name']) == False]['column_name']
            print('\n Отсутствуют в sample-таблице\n',no_col_in_sample,':', no_col_in_sample.count(), 'столбцов')

            #return get_description(spark, sample_table)

            #Статистические показатели
            def get_num_stats(spark, table_name, rows_amount):
                # Извлечение структуры
                df_structure = spark.sql(f"Describe {table_name}").collect()
                df_structure = pd.DataFrame(df_structure, columns = ['column_name', 'data_type', 'comment'])
                # Отберём только числовые поля
                df_structure_num = df_structure[df_structure['data_type'].isin(['string', 'timestamp', 'date']) == False].reset_index(drop = True)
                df_structure_num = df_structure_num[df_structure_num['column_name'].str.contains("#") == False].reset_index(drop = True)
                #print(df_structure_num)
                print('Расчёт показателей по числовым полям таблицы ' + \
                          table_name + ' (число столбцов: ' + str(df_structure_num.shape[0]) + ')')
                # Расчёт по числовым полям
                result = pd.DataFrame()
                columns = ['report_date', 'column_name', 'rows_amount', 'not_nulls_amount', 'minimum_value',\
                           'maximum_value', 'average_value', 'summ_value']
                if df_structure_num.shape[0] > 0:
                # Запускаем подсчёт по каждым rows_amount (500 или менее) столбцам, чтобы хватало памяти для расчёта
                    for array in np.split(df_structure_num.index.values, [rows_amount * (i + 1) for i in range(int(len(df_structure_num.index) / rows_amount))]):
                        if len(array) > 0:
                            query = ""
                            start = datetime.datetime.now() #.strftime("%Y-%m-%d %H:%M:%S")
                            print('Начало рассчета: ' + str(start) ) # + ' (' + str(array[0] + 1) + ')')
                            for index, row in df_structure_num[df_structure_num.index.isin(array)].iterrows():
                                query += "\nSelect " + f'{report_date_field}' + " report_date,"
                                query += "\n\t'" + row['column_name'] + "' column_name,"
                                query += "\n\tCount(*) rows_amount,"
                                query += "\n\tCount(" + row['column_name'] + ") not_nulls_amount,"
                                #query += "\n\tCount(Distinct Cast(" + row['column_name'] + " as Decimal(38,6))) distinct_amount,"
                                query += "\n\tMin(Cast(" + row['column_name'] + " as Decimal(38,6))) minimum_value,"
                                query += "\n\tMax(Cast(" + row['column_name'] + " as Decimal(38,6))) maximum_value,"
                                query += "\n\tAvg(Cast(" + row['column_name'] + " as Decimal(38,6))) average_value,"
                                query += "\n\tSum(Cast(" + row['column_name'] + " as Decimal(38,6))) summ_value"
                                query += "\nFrom " + table_name + f"\nWhere {sample_date_field} = {report_date_field}"
                                query += "\nGroup By " + f'{report_date_field}' + f", '{row['column_name']}'\n Union All"
                            query = query[:-10]
                            #print(query)
                            result_df = spark.sql(query).collect()
                result = pd.concat([result, pd.DataFrame(result_df, columns = columns)])
                finish = datetime.datetime.now() #.strftime("%Y-%m-%d %H:%M:%S")
                print('Конец рассчета:  ' + str(finish) + f' (рассчитано за {finish-start})') # ' (' + str(array[0] + 1) + ')' )
                result['nulls_amount'] = result['rows_amount'] - result['not_nulls_amount']
                result['minimum_value'] = result['minimum_value'].astype('float64')
                result['maximum_value'] = result['maximum_value'].astype('float64')
                result['average_value'] = result['average_value'].astype('float64')
                result['summ_value'] = result['summ_value'].astype('float64')
                result = pd.merge(result, df_structure[['column_name', 'data_type']])
                result = result[['column_name', 'data_type', 'nulls_amount', \
                'minimum_value', 'maximum_value', 'average_value', 'summ_value']]
                return result

            #get_num_stats(spark, table_name = sample_table, rows_amount = rows_amount)

            #Статстика для НЕчисловых полей
            def get_str_stats(spark, table_name, rows_amount):
                # Извлечение структуры 
                df_structure = spark.sql(f"Describe {table_name}").collect()
                df_structure = pd.DataFrame(df_structure, columns = ['column_name', 'data_type', 'comment'])
                # Отберём нечисловые поля
                df_structure_str = df_structure[df_structure['data_type'].isin(['string', 'timestamp', 'date']) == True].reset_index(drop = True)
                df_structure_str = df_structure_str[df_structure_str['column_name'].str.contains("#") == False].reset_index(drop = True)
                print('Расчёт показателей по текстовым полям таблицы ' + \
                          table_name + ' (число столбцов: ' + str(df_structure_str.shape[0]) + ')')
                result = pd.DataFrame()
                columns = ['report_date', 'column_name', 'rows_amount', 'not_nulls_amount', 'distinct_amount']
                if df_structure_str.shape[0] > 0:
                # Запускаем подсчёт по каждым rows_amount (500 или менее) столбцам, чтобы хватало памяти для расчёта
                    for array in np.split(df_structure_str.index.values, [rows_amount * (i + 1) for i in range(int(len(df_structure_str.index) / rows_amount))]):
                        if len(array) > 0:
                            query = ""
                            start = datetime.datetime.now() #.strftime("%Y-%m-%d %H:%M:%S")
                            print('Начало рассчета: ' + str(start)) # + ' (' + str(array[0] + 1) + ')')
                            for index, row in df_structure_str[df_structure_str.index.isin(array)].iterrows():
                                query += "\nSelect " + f'{report_date_field}' + " report_date,"
                                query += "\n\t'" + row['column_name'] + "' column_name,"
                                query += "\n\tCount(*) rows_amount,"
                                query += "\n\tCount(" + row['column_name'] + ") not_nulls_amount,"
                                query += "\n\tCount(distinct " + row['column_name'] + ") distinct_amount"
                                query += "\nFrom " + table_name + f"\nWhere {sample_date_field} = {report_date_field}"
                                query += "\nGroup By " + f'{report_date_field}' + f", '{row['column_name']}'\n Union All"
                            query = query[:-10]
                            #print(query)
                            result_df = spark.sql(query).collect()
                result = pd.concat([result, pd.DataFrame(result_df, columns = columns)])
                finish = datetime.datetime.now() #.strftime("%Y-%m-%d %H:%M:%S")
                print('Конец рассчета:  ' + str(finish) + f' (рассчитано за {finish-start})') # ' (' + str(array[0] + 1) + ')' )
                result['nulls_amount'] = result['rows_amount'] - result['not_nulls_amount']
                result = pd.merge(result, df_structure[['column_name', 'data_type']])
                result = result[['column_name', 'data_type', 'nulls_amount', 'distinct_amount']]
                return result

                    # Расчёт для таблицы-эталона
            df_sample_num_stats = get_num_stats(spark, table_name = sample_table, rows_amount = rows_amount) 
            df_sample_str_stats = get_str_stats(spark, table_name = sample_table, rows_amount = rows_amount)
            df_sample_stats = pd.concat([df_sample_num_stats, df_sample_str_stats], sort = False)
            #df_sample_stats
            # Расчёт для таблицы-теста
            df_test_num_stats = get_num_stats(spark, table_name = test_table, rows_amount = rows_amount)
            df_test_str_stats = get_str_stats(spark, table_name = test_table, rows_amount = rows_amount)
            df_test_stats = pd.concat([df_test_num_stats, df_test_str_stats], sort = False)
           # df_test_stats
            #print('log1')

            # Объединим расчёты по column_name и report_date
            df_result = pd.merge(df_sample_stats, df_test_stats, on = ['column_name'], suffixes = ['_sample', '_test'])
            # Расчитаем соотношения полей
            df_result['null_ratio'] = df_result['nulls_amount_test'] / df_result['nulls_amount_sample']
            df_result['unique_ratio'] = df_result['distinct_amount_test'] / df_result['distinct_amount_sample']
            df_result['min_ratio'] = df_result['minimum_value_test'] / df_result['minimum_value_sample']
            df_result['max_ratio'] = df_result['maximum_value_test'] / df_result['maximum_value_sample']
            df_result['avg_ratio'] = df_result['average_value_test'] / df_result['average_value_sample']
            df_result['summ_ratio'] = round(df_result['summ_value_test'] / df_result['summ_value_sample'], 15)
            df_result.fillna(1, inplace = True)
            #display(df_result)
            # Присвоение короткие названия столбцам
            new_columns = ['column_name', 'data_type_sample', 'null_sample', 'min_sample','max_sample', 'avg_sample',
                 'summ_sample', 'unique_sample', 'data_type_test', 'null_test', 'min_test', 'max_test', 'avg_test', 'summ_test',
             'unique_test', 'null_ratio', 'unique_ratio', 'min_ratio', 'max_ratio', 'avg_ratio', 'summ_ratio']
            df_result.columns = new_columns
            # Переставим столбцы местами для удобного восприятия статистических показателей
            df_result = df_result[['column_name', 'data_type_sample', 'null_sample', 'null_test', 'null_ratio', 'unique_sample', 'unique_test',
            'unique_ratio', 'min_sample', 'min_test', 'min_ratio', 'max_sample', 'max_test', 'max_ratio', 'avg_sample', 'avg_test',
            'avg_ratio',  'summ_sample', 'summ_test', 'summ_ratio']]
            #getcontext().prec = 10 # numbers after zero
            df_result['null_ratio'] = df_result['null_ratio'] #.apply(lambda x: Decimal(x))
            df_result['unique_ratio'] = df_result['unique_ratio'] #.apply(lambda x: Decimal(x))
            df_result['min_ratio'] = df_result['min_ratio'] #.apply(lambda x: Decimal(x))
            df_result['max_ratio'] = df_result['max_ratio'] #.apply(lambda x: Decimal(x))
            df_result['avg_ratio'] = df_result['avg_ratio'] #.apply(lambda x: Decimal(x))
            df_result['summ_ratio'] = df_result['summ_ratio'] #.apply(lambda x: Decimal(x))
            #df_result['summ_ratio'] = pd.to_numeric(df_result['summ_ratio']) #.div(10**9)
            #display(df_result)
            

            value_ratio = 1	
            df_result_not_1=df_result.loc[(df_result['null_ratio'] != value_ratio) | \
                                          (df_result['unique_ratio'] != value_ratio) |\
                                          (df_result['min_ratio'] != value_ratio) |\
                                          (df_result["max_ratio"] != value_ratio) |\
                                          (df_result['avg_ratio'] !=value_ratio) |\
                                          (df_result['summ_ratio'] !=value_ratio)]
            # условие и переформатирование необходимо так как 1.000000000026 в dataframe выглядит как 1.000000 
            # не видно что расхождения есть. Делаем их видимыми, изменяя precision на 12
            if df_result_not_1.shape[0] != 0:
                printmd(f'WARN: Расхождения обнаружены:', color="white")
                df_result_not_1['null_ratio'] = df_result_not_1['null_ratio'].map('{:,.12f}'.format)
                df_result_not_1['unique_ratio'] = df_result_not_1['unique_ratio'].map('{:,.12f}'.format)
                df_result_not_1['min_ratio'] = df_result_not_1['min_ratio'].map('{:,.12f}'.format)
                df_result_not_1['max_ratio'] = df_result_not_1['max_ratio'].map('{:,.12f}'.format)
                df_result_not_1['avg_ratio'] = df_result_not_1['avg_ratio'].map('{:,.12f}'.format)
                df_result_not_1['summ_ratio'] = df_result_not_1['summ_ratio'].map('{:,.12f}'.format)
                display(df_result_not_1)

                conditions_statement='''abs(1 - null_ratio.astype('float')) > ''' + str(self.null_diff) + \
                ''' | abs(1 - unique_ratio.astype('float')) > ''' + str(self.other_diff) + \
                ''' | abs(1 - min_ratio.astype('float')) > ''' + str(self.other_diff) + \
                ''' | abs(1 - max_ratio.astype('float')) > ''' + str(self.other_diff) + \
                ''' | abs(1 - avg_ratio.astype('float')) > ''' + str(self.other_diff) + \
                ''' | abs(1 - summ_ratio) > ''' + str(self.other_diff)

                df_result_for_save = df_result.query(conditions_statement)
                if df_result_for_save.shape[0]==0:
                    print('\n')
                    printmd(f'WARN: Однако, расхождения в пределах допустимых значений null_diff={self.null_diff} и other_diff={self.other_diff}', color="green")
                    #printmd(f'OK: Расхождения в {sample_table} и {test_table} отсутсвуют!', color="green")
                    html_ok = '''
                    <style> .box {  margin: 10px;  padding: 12px;  font-size: 18px;  width:640px;  border-radius: 18px;}
                    .ok {  color: black;  background: cornsilk;  border: 1px solid darkgreen;} </style>
                    <div class="box ok">  &#9432;  Расхождения в пределах допустимых значений. </div>
                    '''
                    display(HTML(html_ok))
                else:
                    printmd(f'WARN: А также данные расхождения вне допустимых null_diff={self.null_diff} и other_diff={self.other_diff}', color="red")
                    html_warn = '''
                    <style> .box {  margin: 10px;  padding: 12px;  font-size: 18px;  width:640px;  border-radius: 18px;}
                    .ok {  color: black;  background: #ffa442;  border: 1px solid darkgreen;} </style>
                    <div class="box ok">  &#9888;  Сверка не пройдена! </div>
                    '''
                    display(HTML(html_warn))
                    display(df_result_for_save)
            else:
                printmd(f'OK: Расхождения в {sample_table} и {test_table} отсутсвуют!', color="green")
                html_ok = '''
                <style> .box {  margin: 10px;  padding: 12px;  font-size: 18px;  width:640px;  border-radius: 18px;}
                .ok {  color: black;  background: #6ff2a2;  border: 1px solid darkgreen;} </style>
                <div class="box ok">  &#9432;  Сверка пройдена успешно! </div>
                '''
                display(HTML(html_ok))
                display(df_result)
            
        #else: 
           # pass
        finally:
            spark.stop()    
            print('=== SPARK STOPPED ===')
        
