import re
import csv
from google.cloud import bigquery
from tempfile import NamedTemporaryFile
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class GSheetToBQOperator(BaseOperator):
    template_fields = ("gsheet_id", "range", "gcs_folder")

    def __init__(
        self,
        *,
        project_id,
        gsheet_id,
        range,
        gcs_bucket,
        gcs_folder,
        bq_dataset,
        bq_table,
        delimiter="|",
        sheets_conn_id="google_sheets_sa",
        gcs_conn_id="google_cloud_default",
        bq_conn_id="google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.gsheet_id = gsheet_id
        self.range = range
        self.gcs_bucket = gcs_bucket
        self.gcs_folder = gcs_folder
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.delimiter = delimiter
        self.sheets_conn_id = sheets_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.bq_conn_id = bq_conn_id

    def _sanitize(self, name):
        name = re.sub(r"[^a-zA-Z0-9_]", "_", str(name))
        return re.sub(r"_+", "_", name).strip("_").lower() or "col"

    def execute(self, context):
        sheets = GSheetsHook(gcp_conn_id=self.sheets_conn_id)
        gcs = GCSHook(gcp_conn_id=self.gcs_conn_id)
        bq = BigQueryHook(
            gcp_conn_id=self.bq_conn_id,
            use_legacy_sql=False,
        ).get_client(project_id=self.project_id)

        self.log.info("Reading Google Sheet %s", self.gsheet_id)
        data = sheets.get_values(self.gsheet_id, self.range)
        if not data:
            raise AirflowException("No data found in Google Sheet")

        raw_headers, body = data[0], data[1:]

        seen, headers = {}, []
        for h in raw_headers:
            clean = self._sanitize(h)
            seen[clean] = seen.get(clean, 0) + 1
            headers.append(f"{clean}_{seen[clean]-1}" if seen[clean] > 1 else clean)

        rows = [headers]
        for r in body:
            padded = r + [""] * (len(headers) - len(r))
            rows.append([str(v).replace("\n", " ").strip() for v in padded])

        object_name = f"{self.gcs_folder}/{context['ds_nodash']}.csv"

        self.log.info("Uploading CSV to gs://%s/%s", self.gcs_bucket, object_name)
        with NamedTemporaryFile("w+", newline="", encoding="utf-8") as tmp:
            writer = csv.writer(tmp, delimiter=self.delimiter)
            writer.writerows(rows)
            tmp.flush()
            gcs.upload(self.gcs_bucket, object_name, tmp.name)

        schema = [bigquery.SchemaField(c, "STRING") for c in headers]
        self.log.info("Loading data into BigQuery %s.%s", self.bq_dataset, self.bq_table)
        load_job = bq.load_table_from_uri(
            f"gs://{self.gcs_bucket}/{object_name}",
            f"{self.project_id}.{self.bq_dataset}.{self.bq_table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format="CSV",
                field_delimiter=self.delimiter,
                write_disposition="WRITE_TRUNCATE",
                allow_jagged_rows=True,
            ),
        )
        load_job.result()

        # Delete file
        gcs.delete(self.gcs_bucket, object_name)
