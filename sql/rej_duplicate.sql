SELECT
    row,
    "DUPLICATE RECORD" rej_reason,
    CONCAT(
        {% for col in params.primary_key %}
        "{{ col }}: ", COALESCE(CAST({{ col }} AS STRING), "NULL")
        {% if not loop.last %}, "{{ params.field_delimiter }}",{% endif %}
        {% endfor %}
    ) rej_detail,
    CURRENT_DATETIME("Asia/Jakarta") load_datetime,
    PARSE_DATE("%Y%m%d", substr(cast({{ job_id_bq(data_interval_end) }} as string),1,8)) job_date,
    {{ job_id_bq(data_interval_end) }} job_id,
    path_filename
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
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
WHERE rownum > 1