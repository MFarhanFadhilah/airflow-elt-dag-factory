{% set definition = ti.xcom_pull(task_ids="infer_schema", key="definition") %}
{% set datatype = ti.xcom_pull(task_ids="infer_schema", key="datatype") %}
{% set date_columns = datatype.date %}
{% set numeric_columns = datatype.numeric %}

{% if date_columns or numeric_columns %}
SELECT
    CONCAT(
    {% for col in definition %}
        COALESCE(CAST({{ col }} AS STRING), ""){% if not loop.last %}, "{{ params.field_delimiter }}",{% endif %}
    {% endfor %}
    ) row,
    "DATATYPE MISMATCH" rej_reason,
    ARRAY_TO_STRING([
    {% if date_columns %}
    {% for col in date_columns %}
        IF(NOT REGEXP_CONTAINS(COALESCE({{ col }},"12/31/2999"), r"^(\d{1,2}-[A-Za-z]{3}-\d{4}|\d{1,2}/\d{1,2}/(\d{2}|\d{4}))$"), "{{ col }}: " || COALESCE({{ col }}, "NULL"), NULL)
        {% if not loop.last or numeric_columns %},{% endif %}
    {% endfor %}
    {% endif %}
    
    {% if numeric_columns %}
    {% for col in numeric_columns %}
        IF(NOT REGEXP_CONTAINS(COALESCE({{ col }},"0"), r"^[0-9,]+(\.[0-9]+)?%?$"), "{{ col }}: " || COALESCE({{ col }}, "NULL"), NULL)
        {% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}
    ], "{{ params.field_delimiter }}") rej_detail,
    CURRENT_DATETIME("Asia/Jakarta") load_datetime,
    PARSE_DATE("%Y%m%d", "{{ ds_nodash }}") job_date,
    "{{ run_id }}" job_id,
FROM `{{ params.project_id }}.{{ params.source_table }}`
WHERE 
{% if date_columns %}
(
{% for col in date_columns %}
    NOT REGEXP_CONTAINS(COALESCE({{ col }}, "12/31/2999"), r"^(\d{1,2}-[A-Za-z]{3}-\d{4}|\d{1,2}/\d{1,2}/(\d{2}|\d{4}))$")
    {% if not loop.last %}OR{% endif %}
{% endfor %}
)
{% endif %}
{% if date_columns and numeric_columns %} OR {% endif %}
{% if numeric_columns %}
(
    {% for col in numeric_columns %}
    NOT REGEXP_CONTAINS(COALESCE({{ col }},"0"), r"^[0-9,]+(\.[0-9]+)?%?$")
    {% if not loop.last %}OR{% endif %}
    {% endfor %}
)
{% endif %}
{% endif %}