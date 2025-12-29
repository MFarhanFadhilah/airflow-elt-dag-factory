{% set definition = ti.xcom_pull(task_ids="infer_schema", key="definition") %}
{% set datatype = ti.xcom_pull(task_ids="infer_schema", key="datatype") %}
{% set date_columns = datatype.date %}
{% set numeric_columns = datatype.numeric %}

SELECT
{% for col in definition %}
    TRIM(CAST({{ col }} AS STRING)) {{ col }},
{% endfor %}
    CURRENT_DATETIME("Asia/Jakarta") load_datetime,
    PARSE_DATE("%Y%m%d", "{{ ds_nodash }}") job_date,
    "{{ run_id }}" job_id,
    CONCAT(
    {% for col in definition %}
        COALESCE(CAST({{ col }} AS STRING), ""){% if not loop.last %}, "{{ params.field_delimiter }}",{% endif %}
    {% endfor %}
    ) AS row
FROM `{{ params.project_id }}.{{ params.source_table }}`
WHERE 1=1
{% for col in date_columns %}
    AND REGEXP_CONTAINS(COALESCE({{ col }}, "12/31/2999"), r"^(\d{1,2}-[A-Za-z]{3}-\d{4}|\d{1,2}/\d{1,2}/(\d{2}|\d{4}))$")
{% endfor %}
{% for col in numeric_columns %}
    AND REGEXP_CONTAINS(COALESCE({{ col }},"0"), r"^\d+(\.\d+)?%?$")
{% endfor %}