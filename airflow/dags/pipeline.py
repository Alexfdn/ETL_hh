# import pendulum
import requests
from datetime import timedelta, datetime
import json
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow import DAG
import psycopg2
# from parsing_hh.main import get_vacancy

# Получение объекта Connection с помощью метода BaseHook.get_connection

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    "start_date": datetime(2023, 11, 1),
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    dag_id = 'etl_hh',
    default_args = default_args,
    schedule_interval="@daily",
    catchup=False
    )


def get_conn_credentials(conn_id)-> BaseHook.get_connection: 

    """
    Function returns dictionary with connection credentials

    :param conn_id: str with airflow connection id
    :return: Connection
    """
    conn = BaseHook.get_connection(conn_id)
    return conn

pg_conn = get_conn_credentials('my_db_conn')
# Извлекаем параметры соединения с базой данных
pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema


conn = psycopg2.connect(
    host=pg_hostname,
    port=pg_port,
    user=pg_username,
    password=pg_pass,
    database=pg_db
)
conn.autocommit = True

filter = ['NAME:("Data Engineer" OR "Дата Инженер" OR "Инженер данных"\
          OR "Аналитик данных" OR "Data Analyst"\
          OR "Data scientist" OR "Исследователь данных")']

def get_hh(filter, pg, period):
    url = 'https://api.hh.ru/vacancies'
    params = {'text': filter, 'per_page': 100, 'page': pg, 'period': period} 
    r = requests.get(url, params=params)
    get_json = r.json()
    items = get_json['items']
    found = get_json['found']
    data = r.content.decode()
    pages = get_json['pages']
    return int(pages), data

def check_db_result():
    cursor = conn.cursor()
    sql = '''SELECT id FROM settings.setup LIMIT 1'''
    cursor.execute(sql)
    answer = cursor.fetchall()
    return answer

# @run_time
def get_vacancy():
    raw_vacancies = []
    if not check_db_result():
        period = 30
    else:
        period = 1
    
    for page in range(0, get_hh(filter, 0, period)[0] + 1):
        page_dict = json.loads(get_hh(filter, page, period)[1])

        for vacancy in page_dict.get('items'):
            raw_vacancies.append(vacancy)
    
    vacancies = set()
    for raw in raw_vacancies:
         vacancies.add((
            int(raw['id']), 
            raw['name'],
            raw['published_at'],
            bool(True if raw['archived'] == 'true' else False),                                                           #is_archive
            bool(True if raw['type']['id'] == 'open' else False),
            (raw['employer']['id'] if 'id' in raw['employer'] else None),                                             #employer_id
            (raw['employer']['name'] if 'name' in raw['employer'] else None),
            bool(raw['employer']['accredited_it_employer'] if 'accredited_it_employer' in raw['employer'] else None),
            (raw['experience']['id'] if 'id' in raw['experience'] else None),                                         #experience_id
            (raw['experience']['name'] if 'name' in raw['experience'] else None),
            raw['area']['id'] if 'id' in raw['experience'] else None,
            raw['area']['name'] if 'name' in raw['experience'] else None,
            (raw['salary']['from'] if raw['salary'] is not None else None),
            (raw['salary']['to'] if raw['salary'] is not None else None),
            (raw['salary']['currency'] if raw['salary'] is not None else None),
            bool(raw['salary']['gross'] if raw['salary'] is not None else None)))
    return vacancies


def load_data():
    cursor = conn.cursor()
    data = get_vacancy()
    for row in data:
        sql = '''INSERT INTO staging.vacancy
        (id,
        vacancy_name,
        published_at,
        is_archive,
        is_open,
        employer_id,
        employer_name,
        is_accredited_it_employer,
        experience_id,
        experience_name,
        area_id,
        area_name,
        salary_from,
        salary_to,
        salary_currency,
        is_gross
        )
        VALUES(
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(sql, row)
        conn.commit()


t_start = EmptyOperator(task_id='Start',
                        dag=dag)

task_truncate_staging = PostgresOperator(
    task_id = 'truncate_staging',
    postgres_conn_id='my_db_conn',
    sql = '''TRUNCATE staging.vacancy''',
    dag=dag
)

task_load_staging = PythonOperator(
        task_id = 'load_staging',
        python_callable = load_data,
        dag=dag
    )

task_load_settings = PostgresOperator(
    task_id = 'load_settings_setup',
    postgres_conn_id='my_db_conn',
    sql = '''INSERT INTO settings.setup (date_start)
            VALUES(now())''',
    dag=dag
)

task_load_core_dim_area = PostgresOperator(
    task_id = 'load_core_dim_area',
    postgres_conn_id='my_db_conn',
    sql = '''
        MERGE INTO core.dim_area AS trg
        USING(
            SELECT DISTINCT 
                area_id,
                area_name
            FROM staging.vacancy) AS src
        ON trg.area_id = src.area_id
        WHEN NOT MATCHED
        THEN INSERT(
            area_id,
            name)
        VALUES(
            src.area_id,
            src.area_name)
        WHEN MATCHED 
            AND (COALESCE(trg.name, '') <> COALESCE(src.area_name, '') 
            OR trg.area_id <> src.area_id )
        THEN UPDATE
        SET 
            area_id = src.area_id, 
            name = src.area_name''',
    dag=dag
)

task_load_core_dim_employer = PostgresOperator(
    task_id = 'load_core_dim_employer',
    postgres_conn_id='my_db_conn',
    sql = '''
        MERGE INTO core.dim_employer AS trg
        USING(
            SELECT DISTINCT 
                employer_id,
                employer_name
            FROM staging.vacancy) AS src
        ON trg.employer_id = src.employer_id
        WHEN NOT MATCHED
        THEN INSERT(
            employer_id,
            name)
        VALUES(
            src.employer_id,
            src.employer_name)
        WHEN MATCHED
                AND (COALESCE(trg.name, '') <> COALESCE(src.employer_name, '')
                OR trg.employer_id <> src.employer_id)
        THEN UPDATE
        SET 
            employer_id = src.employer_id, 
            name = src.employer_name''',
    dag=dag
)

task_load_core_dim_experience = PostgresOperator(
    task_id = 'load_core_dim_experience',
    postgres_conn_id='my_db_conn',
    sql = '''
        MERGE INTO core.dim_experience AS trg
        USING(
            SELECT DISTINCT 
                experience_id,
                experience_name
            FROM staging.vacancy) AS src
        ON trg.experience_id = src.experience_id
        WHEN NOT MATCHED
        THEN INSERT(
            experience_id,
            name)
        VALUES(
            src.experience_id,
            src.experience_name)
        WHEN MATCHED
                AND (COALESCE(trg.name, '') <> COALESCE(src.experience_name, '')
                OR COALESCE(trg.experience_id,'') <> COALESCE(src.experience_id,''))
        THEN UPDATE
        SET 
            experience_id = src.experience_id, 
            name = src.experience_name''',
    dag=dag
)

task_load_core_dim_currency = PostgresOperator(
    task_id = 'load_core_dim_currency',
    postgres_conn_id = 'my_db_conn',
    sql = '''
        MERGE INTO core.dim_currency AS trg
        USING(
            SELECT DISTINCT 
                salary_currency,
                CASE
                    WHEN salary_currency = 'RUR'
                    THEN 1
                    WHEN salary_currency = 'USD'
                    THEN 90
                    WHEN salary_currency = 'KZT'
                    THEN 0.2
                    WHEN salary_currency = 'BYR'
                    THEN 28.7
                END AS to_rub
            FROM staging.vacancy) AS src
        ON trg.currency_id = src.salary_currency
        WHEN NOT MATCHED
        THEN INSERT(
            currency_id, 
            to_rub
            )
        VALUES(
            src.salary_currency,
            src.to_rub
            )
        WHEN MATCHED AND trg.to_rub <> src.to_rub
        THEN UPDATE
        SET 
            currency_id = src.salary_currency, 
            to_rub = src.to_rub''',
    dag=dag
)

task_load_core_fact_vacancy = PostgresOperator(
    task_id = 'load_core_fact_vacancy',
    postgres_conn_id = 'my_db_conn',
    sql = '''
        MERGE INTO core.fact_vacancy AS trg
        USING(
            SELECT DISTINCT
                    id AS vacancy_id,
                    CASE 
                        WHEN vacancy_name ILIKE '%data engineer%' 
                            OR vacancy_name ILIKE '%data инженер%'
                            OR vacancy_name ILIKE '%data-инженер%'
                            OR vacancy_name ILIKE '%дата инженер%'
                            OR vacancy_name ILIKE '%инженер данных%'
                            OR vacancy_name ILIKE '%дата-инженер%'
                        THEN 'Инженер данных'
                        WHEN vacancy_name ILIKE '%аналитик данных%' 
                            OR vacancy_name ILIKE '%data analyst%'
                            OR vacancy_name ILIKE '%data аналитик%'
                        THEN 'Аналитик данных'
                        WHEN vacancy_name ILIKE '%data scientist%'
                            OR vacancy_name ILIKE '%исследователь данных%'
                        THEN 'Data scientist'
                    END AS vacancy_name,
                    CASE
                        WHEN vacancy_name LIKE ('%Стажер%')
                        THEN 'Intern'
                        WHEN vacancy_name LIKE ('%Младший%')
                            OR vacancy_name LIKE ('%Junior%')
                        THEN 'Junior'
                        WHEN vacancy_name LIKE ('%Старший%')
                            OR vacancy_name LIKE ('%Middle%')
                        THEN 'Middle'
                        WHEN vacancy_name LIKE ('%Ведущий%')
                            OR vacancy_name LIKE ('%Senior%')
                        THEN 'Senior'
                        WHEN vacancy_name LIKE ('%Руководитель%')
                            OR vacancy_name LIKE ('%Team Lead%')
                        THEN 'Team Lead'
                    END AS grade,
                    CASE
                        WHEN experience_id = 'noExperience'
                        THEN 'Intern'
                        WHEN experience_id = 'between1And3'
                        THEN 'Junior'
                        WHEN experience_id = 'between3And6'
                        THEN 'Middle'
                        WHEN experience_id = 'moreThan6'
                        THEN 'Senior'
                    END AS grade_experience,
                    published_at,
                    is_archive,
                    is_open,
                    employer_id ,
                    experience_id,
                    area_id,
                    salary_from,
                    salary_to,
                    is_gross,
                    salary_currency
                FROM staging.vacancy) AS src
        ON trg.vacancy_id = src.vacancy_id
        WHEN NOT MATCHED
        THEN INSERT(
            vacancy_id,
            vacancy_name,
            grade,
            grade_experience,
            published_at,
            is_archive,
            is_open,
            employer_id,
            experience_id,
            area_id,
            salary_from,
            salary_to,
            is_gross,
            salary_currency)
        VALUES(
            src.vacancy_id,
            src.vacancy_name,
            src.grade,
            src.grade_experience,
            src.published_at,
            src.is_archive,
            src.is_open,
            src.employer_id,
            src.experience_id,
            src.area_id,
            src.salary_from,
            src.salary_to,
            src.is_gross,
            src.salary_currency)
        WHEN MATCHED
        THEN UPDATE
        SET 
            vacancy_id = src.vacancy_id,
            vacancy_name = src.vacancy_name,
            grade = src.grade,
            grade_experience = src.grade_experience,
            published_at = src.published_at,
            is_archive = src.is_archive,
            is_open = src.is_open,
            employer_id = src.employer_id,
            experience_id = src.experience_id,
            area_id = src.area_id,
            salary_from = src.salary_from,
            salary_to = src.salary_to,
            is_gross = src.is_gross,
            salary_currency = src.salary_currency''',
        dag=dag
)
t_start>>task_truncate_staging>>task_load_staging>>task_load_settings>>\
task_load_core_dim_area>>task_load_core_dim_employer>>\
task_load_core_dim_experience>>task_load_core_dim_currency>>\
task_load_core_fact_vacancy


