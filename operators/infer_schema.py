import re
from collections import Counter
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
INT_REGEX = re.compile(r"^\d+$")
FLOAT_REGEX = re.compile(r"^\d+(\.\d+)?$")
PERCENT_REGEX = re.compile(r"^\d+(\.\d+)?%$")


class InferBQSchemaOperator(BaseOperator):
    """
    Infers column definitions and datatype groups from TMP table.
    Pushes results to XCom for SQL templating.
    """

    def __init__(
        self,
        *,
        project_id: str,
        tmp_table: str,
        sample_limit: int = 1000,
        bq_conn_id="google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.tmp_table = tmp_table
        self.sample_limit = sample_limit
        self.bq_conn_id = bq_conn_id

    def _infer_column_type(self, values):
        counts = Counter()

        for v in values:
            if v is None or v == "":
                continue
            v = str(v).strip()

            if DATE_REGEX.match(v):
                counts["DATE"] += 1
            elif PERCENT_REGEX.match(v):
                counts["FLOAT"] += 1
            elif INT_REGEX.match(v):
                counts["INT64"] += 1
            elif FLOAT_REGEX.match(v):
                counts["FLOAT"] += 1
            else:
                counts["STRING"] += 1

        return counts.most_common(1)[0][0] if counts else "STRING"

    def execute(self, context):
        bq = BigQueryHook(
            gcp_conn_id=self.bq_conn_id,
            use_legacy_sql=False,
        ).get_client(project_id=self.project_id)

        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.tmp_table}`
            WHERE RAND() <= 0.5
            LIMIT {self.sample_limit}
        """
        df = bq.query(query).to_dataframe()

        # Create metadata
        definition = list(df.columns)

        date_columns = []
        numeric_columns = []

        for col in df.columns:
            inferred = self._infer_column_type(df[col].tolist())

            if inferred == "DATE":
                date_columns.append(col)
            elif inferred in ("INT64", "FLOAT"):
                numeric_columns.append(col)

        datatype = {
            "date": date_columns,
            "numeric": numeric_columns,
        }

        # Push to XCOM
        ti = context["ti"]

        ti.xcom_push(key="definition", value=definition)
        ti.xcom_push(key="datatype", value=datatype)

        self.log.info("Inferred definition: %s", definition)
        self.log.info("Inferred datatype groups: %s", datatype)

        return {
            "definition": definition,
            "datatype": datatype,
        }
