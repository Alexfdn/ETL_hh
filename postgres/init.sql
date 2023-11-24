CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS settings;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS data_mart;

CREATE TABLE IF NOT EXISTS settings.setup(
    id serial,
    date_start timestamp
);


--STAGING
CREATE TABLE IF NOT EXISTS staging.vacancy( 
	id int,
	vacancy_name varchar(255),
	published_at timestamp,
	is_archive bool,
	is_open bool,
	employer_id int,
	employer_name varchar(255),
	is_accredited_it_employer bool,
	experience_id varchar(255),
	experience_name varchar(255),
	area_id int,
	area_name varchar(255),
	salary_from decimal,
	salary_to decimal,
	salary_currency varchar(10),
	is_gross bool,
	type_id int
);

--CORE

CREATE TABLE IF NOT EXISTS core.dim_area(
	id serial,
	area_id int,
	name varchar(255),
	UNIQUE(area_id)
);

CREATE TABLE IF NOT EXISTS core.dim_employer(
	id serial,
	employer_id int,
	name varchar(255),
	is_accredited_it_employer bool,
	UNIQUE(employer_id)
);

CREATE TABLE IF NOT EXISTS core.dim_experience(
	id serial,
	experience_id varchar(255),
	name varchar(255),
	UNIQUE(experience_id)
);

CREATE TABLE IF NOT EXISTS core.dim_currency(
	id serial,
	currency_id varchar(10),
	to_rub NUMERIC,
	UNIQUE(currency_id)
);

CREATE TABLE IF NOT EXISTS core.fact_vacancy(
	id serial,
	vacancy_id int,
	vacancy_name varchar(255),
	grade varchar(20), 
	grade_experience varchar(20),
	published_at timestamp,
	is_archive bool,
	is_open bool,
	employer_id int REFERENCES core.dim_employer(employer_id) ON DELETE SET NULL,
	experience_id varchar(255) REFERENCES core.dim_experience(experience_id) ON DELETE SET NULL,
	area_id int REFERENCES core.dim_area(area_id) ON DELETE SET NULL,
	salary_from decimal,
	salary_to decimal,
	is_gross bool,
	salary_currency varchar(10) REFERENCES core.dim_currency(currency_id) ON DELETE SET NULL
);


--DATA MART

CREATE OR REPLACE VIEW data_mart.de_region
AS(
	WITH
grade_salary AS(
	SELECT 
		area_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			area_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Инженер данных'
	) AS vac
	GROUP BY area_id
	--, salary_currency

)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_area.area_id,
	dim_area.name AS area_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
				
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_area AS dim_area
	ON vacancy.area_id = dim_area.area_id

LEFT JOIN grade_salary
	ON grade_salary.area_id = vacancy.area_id

WHERE vacancy.vacancy_name = 'Инженер данных'
	
GROUP BY
	dim_area.area_id,
	dim_area.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
);

CREATE OR REPLACE VIEW data_mart.da_region
AS(
	WITH
grade_salary AS(
	SELECT 
		area_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			area_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Аналитик данных'
	) AS vac
	GROUP BY area_id
)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_area.area_id,
	dim_area.name AS area_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
				
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_area AS dim_area
	ON vacancy.area_id = dim_area.area_id

LEFT JOIN grade_salary
	ON grade_salary.area_id = vacancy.area_id

WHERE vacancy.vacancy_name = 'Аналитик данных'
	
GROUP BY
	dim_area.area_id,
	dim_area.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
);

CREATE OR REPLACE VIEW data_mart.ds_region
AS(
	WITH
grade_salary AS(
	SELECT 
		area_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			area_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Data scientist'
	) AS vac
	GROUP BY area_id

)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_area.area_id,
	dim_area.name AS area_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
				
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_area AS dim_area
	ON vacancy.area_id = dim_area.area_id

LEFT JOIN grade_salary
	ON grade_salary.area_id = vacancy.area_id

WHERE vacancy.vacancy_name = 'Data scientist'
	
GROUP BY
	dim_area.area_id,
	dim_area.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
);



CREATE OR REPLACE VIEW data_mart.de_employer AS(
	WITH
grade_salary AS(
	SELECT 
		employer_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			employer_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Инженер данных'
	) AS vac
	GROUP BY employer_id

)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_employer.employer_id,
	dim_employer.name AS employer_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
			
		
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_employer AS dim_employer
	ON vacancy.employer_id = dim_employer.employer_id

LEFT JOIN grade_salary
	ON grade_salary.employer_id = vacancy.employer_id

WHERE vacancy.vacancy_name = 'Инженер данных'
	
GROUP BY
	dim_employer.employer_id,
	dim_employer.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
	
);


CREATE OR REPLACE VIEW data_mart.da_employer AS(
	WITH
grade_salary AS(
	SELECT 
		employer_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			employer_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Аналитик данных'
	) AS vac
	GROUP BY employer_id

)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_employer.employer_id,
	dim_employer.name AS employer_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
			
		
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_employer AS dim_employer
	ON vacancy.employer_id = dim_employer.employer_id

LEFT JOIN grade_salary
	ON grade_salary.employer_id = vacancy.employer_id

WHERE vacancy.vacancy_name = 'Аналитик данных'
	
GROUP BY
	dim_employer.employer_id,
	dim_employer.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
	
);


CREATE OR REPLACE VIEW data_mart.ds_employer AS(
	WITH
grade_salary AS(
	SELECT 
		employer_id,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			employer_id,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
		WHERE vacancy.vacancy_name = 'Data scientist'
	) AS vac
	GROUP BY employer_id

)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	dim_employer.employer_id,
	dim_employer.name AS employer_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
			
		
FROM core.fact_vacancy AS vacancy

LEFT JOIN core.dim_employer AS dim_employer
	ON vacancy.employer_id = dim_employer.employer_id

LEFT JOIN grade_salary
	ON grade_salary.employer_id = vacancy.employer_id

WHERE vacancy.vacancy_name = 'Data scientist'
	
GROUP BY
	dim_employer.employer_id,
	dim_employer.name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
	
);


	
CREATE OR REPLACE VIEW data_mart.vacancies AS(
WITH
grade_salary AS(
	SELECT 
		vacancy_name,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Intern'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_intern,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Junior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_jun,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Middle'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_middle,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Senior'
				THEN COALESCE(salary_to, salary_from) 
			END),2) AS avg_sal_senior,
		ROUND(AVG(
			CASE	
				WHEN grade = 'Team Lead'
				THEN COALESCE(salary_to, salary_from)  
			END),2) AS avg_sal_lead,
		MIN(
			CASE
				WHEN grade = 'Intern'
				THEN salary_from
			END) AS min_sal_intern,
		MIN(
			CASE
				WHEN grade = 'Junior'
				THEN salary_from
			END) AS min_sal_jun,
		MIN(
			CASE
				WHEN grade = 'Middle'
				THEN salary_from
			END) AS min_sal_middle,
		MIN(
			CASE
				WHEN grade = 'Senior'
				THEN salary_from
			END) AS min_sal_senior,
		MIN(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_from
			END) AS min_sal_lead,
		
		MAX(
			CASE
				WHEN grade = 'Intern'
				THEN salary_to
			END) AS max_sal_intern,
		MAX(
			CASE
				WHEN grade = 'Junior'
				THEN salary_to
			END) AS max_sal_jun,
		MAX(
			CASE
				WHEN grade = 'Middle'
				THEN salary_to
			END) AS max_sal_middle,
		MAX(
			CASE
				WHEN grade = 'Senior'
				THEN salary_to
			END) AS max_sal_senior,
		MAX(
			CASE
				WHEN grade = 'Team Lead'
				THEN salary_to
			END) AS max_sal_lead,
		MIN(salary_from) AS min_salary,
		MAX(salary_to) AS max_salary
		
	FROM( 
		SELECT
			vacancy.id,
			vacancy_name,
			COALESCE (vacancy.grade, vacancy.grade_experience) AS grade,
			salary_from * to_rub AS salary_from,
			salary_to * to_rub AS salary_to
		FROM core.fact_vacancy AS vacancy
		LEFT JOIN core.dim_currency AS currency
			ON vacancy.salary_currency = currency.currency_id
	) AS vac
	GROUP BY vacancy_name
)
	
SELECT 
	ROW_NUMBER() OVER() AS id,
	vacancy.vacancy_name,
	SUM ( 
		CASE
			WHEN vacancy.is_open = TRUE 
			THEN 1
			ELSE 0
		END) AS cnt_vacancy,
	SUM (
		CASE
			WHEN vacancy.salary_from IS NOT NULL 
				OR vacancy.salary_to IS NOT NULL
			THEN 1
			ELSE 0
		END
		) AS cnt_vac_salary,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Intern'
				THEN 1
			END) AS cnt_intern,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Junior'
				THEN 1
			END) AS cnt_junior,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Middle'
				THEN 1
			END) AS cnt_middle,
		SUM(
			CASE
				WHEN COALESCE (vacancy.grade, vacancy.grade_experience) = 'Senior'
				THEN 1
			END) AS cnt_senior,
		SUM(
			CASE
				WHEN vacancy.grade = 'Team Lead'
				THEN 1
			END) AS cnt_lead,
			avg_sal_intern,
			avg_sal_jun,
			avg_sal_middle,
			avg_sal_senior,
			avg_sal_lead,
			min_sal_intern,
			min_sal_jun,
			min_sal_middle,
			min_sal_senior,
			min_sal_lead,
			max_sal_intern,
			max_sal_jun,
			max_sal_middle,
			max_sal_senior,
			max_sal_lead,
			min_salary,
			max_salary,
			ROUND(avg_sal_intern / max_salary,2) AS ratio_sal_intern,
			ROUND(avg_sal_jun / max_salary,2) AS ratio_sal_jun,
			ROUND(avg_sal_middle / max_salary,2) AS ratio_sal_middle,
			ROUND(avg_sal_senior / max_salary,2) AS ratio_sal_senior,
			ROUND(avg_sal_lead / max_salary,2) AS ratio_sal_lead
			
		
FROM core.fact_vacancy AS vacancy

LEFT JOIN grade_salary
	ON grade_salary.vacancy_name = vacancy.vacancy_name
	
GROUP BY
	vacancy.vacancy_name,
	avg_sal_intern,
	avg_sal_jun,
	avg_sal_middle,
	avg_sal_senior,
	avg_sal_lead,
	min_sal_intern,
	min_sal_jun,
	min_sal_middle,
	min_sal_senior,
	min_sal_lead,
	max_sal_intern,
	max_sal_jun,
	max_sal_middle,
	max_sal_senior,
	max_sal_lead,
	min_salary,
	max_salary
);	
