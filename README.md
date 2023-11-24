##ЦЕЛЬ ПРОЕКТА

Создание ETL-процесса формирования витрин данных для анализа вакансий доступных на сайте hh.ru

#Используемый стек

python
airflow
postgresql
docker compose

#Реализация

Для развертывания проекта используется docker compose

Для оркестрации ETL-процесса используется Apache Airflow

Для получения данных из API hh.ru и последующей загрузки в базу данных используется python-скрипт

Для формирования хранилища данных используются PL/pgSQL-скрипты, вызываемые Airflow

Для формироваяни необходимых витрин данных используются табличные функции на языке PL/pgSQL

#Запуск

Airflow
login: airflow
password: airflow

Postgres connection
server: host.docker.internal
port: 5430

database: test
login: postgres
password: password

В бд запустить скрипт по созданию dwh в бд test