import yaml
import gspread
import argparse
from pathlib import Path
from google.oauth2.service_account import Credentials

PROJECT_DIR = Path(__file__).resolve().parent.parent
KEY_DIR = PROJECT_DIR / "key"
DAG_CONFIG_DIR = PROJECT_DIR / "dag_config"

def parse_csv(val):
    return [v.strip() for v in val.split(",")] if val else []


def get_service_account_file() -> Path:
    key_files = list(KEY_DIR.glob("*.json"))

    if not key_files:
        raise FileNotFoundError("No service account JSON found in key/")
    if len(key_files) > 1:
        raise RuntimeError("Multiple service account JSON files found in key/")

    return key_files[0]

def generate_yaml(project_id: str, gsheet_id: str, job_name: str):
    creds = Credentials.from_service_account_file(
        get_service_account_file(),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(gsheet_id).worksheet("pipeline_control")
    rows = sheet.get_all_records()

    # Find target job
    matched = [r for r in rows if r.get("job_name") == job_name]

    if not matched:
        raise ValueError(f"Job '{job_name}' not found in pipeline_control")

    row = matched[0]
    status = str(row.get("status", "")).upper()

    if status != "OK":
        raise RuntimeError(
            f"Job '{job_name}' has status '{status}'. "
            f"YAML generation is only allowed when status = 'OK'."
        )

    DAG_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Build YAML config
    config = {
        "project_id": project_id,
        "job_name": row["job_name"],
        "schedule": row["schedule"],
        "dag_params": {
            "field_delimiter": row["field_delimiter"],
        },
        "gsheet": {
            "gsheet_id": row["gsheet_id"],
            "range": row["range"],
        },
        "gcs": {
            "bucket": row["gcs_bucket"],
            "folder": row["gcs_folder"],
        },
        "bq_tables": {
            "tmp": row["tmp_table"],
            "src": row["src_table"],
            "stg": row["stg_table"],
            "dw": row["dw_table"],
            "rej": row["rej_table"],
        },
        "bq_schema": {
            "primary_key": parse_csv(row.get("primary_keys")),
        },
    }

    output_file = DAG_CONFIG_DIR / f"{job_name}.yaml"

    with output_file.open("w") as f:
        yaml.dump(config, f, sort_keys=False)

    print(f"YAML generated successfully: {output_file}")

# CLI entrypoint
def main():
    parser = argparse.ArgumentParser(
        description="Generate Airflow DAG YAML from Google Sheets control plane"
    )

    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--gsheet-id",
        required=True,
        help="Google Sheets ID containing pipeline_control",
    )
    parser.add_argument(
        "--job-name",
        required=True,
        help="Job name to generate YAML for",
    )

    args = parser.parse_args()

    generate_yaml(
        project_id=args.project_id,
        gsheet_id=args.gsheet_id,
        job_name=args.job_name,
    )


if __name__ == "__main__":
    main()
