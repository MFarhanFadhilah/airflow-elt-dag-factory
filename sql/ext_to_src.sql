{% set definition = ti.xcom_pull(task_ids="infer_schema", key="definition") %}
{% set datatype = ti.xcom_pull(task_ids="infer_schema", key="datatype") %}
{% set date_columns = datatype.date %}
{% set numeric_columns = datatype.numeric %}

SELECT
{% for col in definition %}
    TRIM(CAST({{ col }} AS STRING)) {{ col }},
{% endfor %}
    CURRENT_DATETIME("Asia/Jakarta") load_datetime,
    PARSE_DATE("%Y%m%d", substr(cast({{ job_id_bq(data_interval_end) }} as string),1,8)) job_date,
    {{ job_id_bq(data_interval_end) }} job_id,
    "Varion Google Sheets" path_filename,
    CONCAT(
    {% for col in definition %}
        COALESCE(CAST({{ col }} AS STRING), ""){% if not loop.last %}, "{{ params.field_delimiter }}",{% endif %}
    {% endfor %}
    ) AS row
FROM `{{ params.project_id }}.{{ params.source_dataset }}.{{ params.source_table }}`
WHERE CONCAT(
    {% for col in date_columns %}
        COALESCE({{ col }},""){% if not loop.last %},{% endif %}
    {% endfor %}
) <> ""
{% for col in date_columns %}
    AND REGEXP_CONTAINS(COALESCE({{ col }}, "12/31/2999"), r"^(\d{1,2}-[A-Za-z]{3}-\d{4}|\d{1,2}/\d{1,2}/(\d{2}|\d{4}))$")
{% endfor %}
{% for col in numeric_columns %}
    AND REGEXP_CONTAINS(COALESCE({{ col }},"0"), r"^\d+(\.\d+)?%?$")
{% endfor %}