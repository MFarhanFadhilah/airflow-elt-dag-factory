{% set definition = ti.xcom_pull(task_ids="infer_schema", key="definition") %}
{% set datatype = ti.xcom_pull(task_ids="infer_schema", key="datatype") %}
{% set date_columns = datatype.date %}
{% set numeric_columns = datatype.numeric %}

SELECT
{% for col in definition %}
{% if col in date_columns %}
    CASE 
        WHEN {{ col }} IS NOT NULL AND {{ col }} <> "" THEN 
            COALESCE(
                SAFE.PARSE_DATE("%m/%d/%Y", {{ col }}), 
                SAFE.PARSE_DATE("%m/%d/%y", {{ col }})
            )
        ELSE NULL 
    END AS {{ col }},
{% elif col in numeric_columns %}
    CASE 
        WHEN {{ col }} IS NOT NULL AND {{ col }} <> "" AND RIGHT({{ col }}, 1) = "%" 
            THEN CAST(REPLACE(REPLACE({{ col }}, "%", ""), ",", "") AS FLOAT64) / 100
        WHEN {{ col }} IS NOT NULL AND {{ col }} <> "" 
            THEN CAST(REPLACE({{ col }}, ",", "") AS FLOAT64) 
        ELSE NULL 
    END AS {{ col }},
{% else %}
    CASE 
        WHEN {{ col }} IS NOT NULL AND {{ col }} <> "" THEN {{ col }}
        ELSE NULL 
    END AS {{ col }},
{% endif %}
{% endfor %}
    load_datetime,
    job_date,
    job_id,
    row
FROM `{{ params.project_id }}.{{ params.source_table }}`
WHERE job_id = "{{ run_id }}"
  AND job_date = PARSE_DATE("%Y%m%d", "{{ ds_nodash }}")