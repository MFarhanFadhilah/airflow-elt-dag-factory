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
    FROM `{{ params.project_id }}.{{ params.source_table }}`
    WHERE job_id = "{{ run_id }}"
        AND job_date = PARSE_DATE("%Y%m%d", "{{ ds_nodash }}")
)
WHERE rownum = 1