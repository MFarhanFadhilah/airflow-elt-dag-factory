import yaml
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from personal_project.airflow_elt_dag_factory.operators.gsheet_to_bq import GSheetToBQOperator
from personal_project.airflow_elt_dag_factory.operators.infer_schema import InferBQSchemaOperator
from personal_project.airflow_elt_dag_factory.operators.cleanup_xcom import CleanupXComOperator

CONFIG_DIR = Path(__file__).resolve().parent / "dag_config"

def create_dag(config: dict) -> DAG:
    def bq_insert_task(task_id, params, destination, write_disposition):
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": f"{{% include 'sql/{task_id}.sql' %}}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["project_id"],
                        "datasetId": destination.split(".")[0],
                        "tableId": destination.split(".")[1],
                    },
                    "writeDisposition": write_disposition,
                }
            },
            params=params,
        )

    with DAG(
        dag_id=config["job_name"],
        start_date=datetime(2023, 1, 1),
        schedule_interval=config["schedule"],
        catchup=False,
        tags=["gsheet", "elt"],
    ) as dag:
        
        gsheet_to_bq = GSheetToBQOperator(
            task_id="gsheet_to_bq",
            project_id=config["project_id"],
            gsheet_id=config["gsheet"]["gsheet_id"],
            range=config["gsheet"]["range"],
            gcs_bucket=config["gcs"]["bucket"],
            gcs_folder=config["gcs"]["folder"],
            bq_dataset=config["bq_tables"]["tmp"].split(".")[0],
            bq_table=config["bq_tables"]["tmp"].split(".")[1],
        )

        infer_schema = InferBQSchemaOperator(
            task_id="infer_schema",
            project_id=config["project_id"],
            tmp_table=config["bq_tables"]["tmp"],
        )

        ext_to_src = bq_insert_task(
            "ext_to_src",
            {
                "project_id": config["project_id"],
                "source_table": config["bq_tables"]["tmp"],
            },
            config["bq_tables"]["src"],
            "WRITE_TRUNCATE",
        )

        rej_datatype = bq_insert_task(
            "rej_datatype",
            {
                "project_id": config["project_id"],
                "source_table": config["bq_tables"]["tmp"],
            },
            config["bq_tables"]["rej"],
            "WRITE_APPEND",
        )

        src_to_stg = bq_insert_task(
            "src_to_stg",
            {
                "project_id": config["project_id"],
                "source_table": config["bq_tables"]["src"],
            },
            config["bq_tables"]["stg"],
            "WRITE_TRUNCATE",
        )

        rej_duplicate = bq_insert_task(
            "rej_duplicate",
            {
                "project_id": config["project_id"],
                "source_table": config["bq_tables"]["stg"],
                "primary_key": config["bq_schema"]["primary_key"],
            },
            config["bq_tables"]["rej"],
            "WRITE_APPEND",
        )

        stg_to_dw = bq_insert_task(
            "stg_to_dw",
            {
                "project_id": config["project_id"],
                "source_table": config["bq_tables"]["stg"],
                "primary_key": config["bq_schema"]["primary_key"],
            },
            config["bq_tables"]["dw"],
            "WRITE_TRUNCATE",
        )

        cleanup_xcom = CleanupXComOperator(
            task_id="cleanup_xcom",
        )

        # Dependencies
        (
            gsheet_to_bq
            >> infer_schema
            >> [ext_to_src, rej_datatype]
            >> src_to_stg
            >> [rej_duplicate, stg_to_dw]
            >> cleanup_xcom
        )

    return dag

for cfg in CONFIG_DIR.glob("*.yaml"):
    with open(cfg) as f:
        config = yaml.safe_load(f)

    dag = create_dag(config)
    globals()[dag.dag_id] = dag
