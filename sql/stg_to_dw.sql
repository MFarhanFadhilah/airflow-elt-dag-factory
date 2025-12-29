{% set definition = ti.xcom_pull(task_ids="infer_schema", key="definition") %}

SELECT
{% for col in definition %}
    {{ col }},
{% endfor %}
    load_datetime,
    job_date,
    job_id
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                {% for col in params.primary_key %}
                CAST({{ col }} AS STRING){% if not loop.last %}, {% endif %}
                {% endfor %} 
            ORDER BY job_id DESC
        ) rownum
    FROM `{{ params.project_id }}.{{ params.source_dataset }}.{{ params.source_table }}`
    WHERE job_id = {{ job_id_bq(data_interval_end) }}
        AND job_date = PARSE_DATE("%Y%m%d", substr(cast({{ job_id_bq(data_interval_end) }} as string),1,8))
)
WHERE rownum = 1