import os
import logging
from google.cloud import bigquery
from data_agent.constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES




# ------------------------------
# 0️⃣ Helper: Sanitize user input for logs
# ------------------------------
def sanitize_for_log(value: str) -> str:
    """Sanitize string to prevent log injection."""
    if not isinstance(value, str):
        value = str(value)
    # Replace newlines and carriage returns
    return value.replace("\n", "\\n").replace("\r", "\\r")


# ------------------------------
# 1️⃣ Setup general/public logger
# ------------------------------
public_logger = logging.getLogger("public")
public_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
public_logger.addHandler(console_handler)

# ------------------------------
# 2️⃣ Setup secure logger for internal errors
# ------------------------------
# Ensure log folder exists
secure_log_dir = "C:\\tmp"
os.makedirs(secure_log_dir, exist_ok=True)
secure_log_file = os.path.join(secure_log_dir, "secure_app.log")

secure_logger = logging.getLogger("secure")
secure_logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(secure_log_file)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)
secure_logger.addHandler(file_handler)

# ------------------------------
# 3️⃣ BigQuery utility functions
# ------------------------------

def get_table_description(table_name: str) -> str:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
        table = client.get_table(table_id)
        return table.description if table.description else ""
    except Exception as e:
        public_logger.error(f"Error fetching table description for {sanitize_for_log(table_name)}. Please contact support.")
        secure_logger.error(f"Failed to fetch table description for {sanitize_for_log(table_name)}.\nException: {sanitize_for_log(e)}", exc_info=True)
        return ""


def get_table_ddl_strings() -> list[dict]:
    all_table_ddls = []
    client = bigquery.Client(project=PROJECT_ID)

    base_query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name, 
            table_type,
            creation_time,
            ddl
        FROM
            `{PROJECT_ID}.{DATASET_NAME}.INFORMATION_SCHEMA.TABLES`
        WHERE
            table_type = 'BASE TABLE'
    """

    if TABLE_NAMES:
        formatted_table_names = ", ".join([f"'{name}'" for name in TABLE_NAMES])
        base_query += f" AND table_name IN ({formatted_table_names})"
    base_query += " ORDER BY table_name;"

    try:
        query_job = client.query(base_query)
        results = query_job.result()
        for row in results:
            if row.ddl:
                all_table_ddls.append({
                    "table_catalog": row.table_catalog,
                    "table_schema": row.table_schema,
                    "table_name": row.table_name,
                    "table_type": row.table_type,
                    "creation_time": row.creation_time,
                    "ddl": row.ddl
                })
        return all_table_ddls

    except Exception as e:
        public_logger.error("Failed to fetch DDL strings. Please contact support.")
        secure_logger.error(f"Failed to fetch DDL strings.\nException: {sanitize_for_log(e)}", exc_info=True)
        return []


def get_total_rows(table_name: str) -> int:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.{table_name}`"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row[0]
    except Exception as e:
        public_logger.error(f"Failed to fetch total rows for {sanitize_for_log(table_name)}.")
        secure_logger.error(f"Error fetching total rows for {sanitize_for_log(table_name)}.\nException: {e}", exc_info=True)
        return 0


def get_total_column_count() -> int:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT count(*) as total_columns FROM `{PROJECT_ID}.{DATASET_NAME}.INFORMATION_SCHEMA.COLUMNS`"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row.total_columns
    except Exception as e:
        public_logger.error("Failed to fetch total column count.")
        secure_logger.error(f"Error fetching total column count.\nException: {sanitize_for_log(e)}", exc_info=True)
        return 0


def fetch_sample_data_for_single_table(table_name: str, num_rows: int = 3) -> list[dict]:
    if not PROJECT_ID or not DATASET_NAME:
        public_logger.error("PROJECT_ID and DATASET_NAME must be configured to fetch sample data.")
        return []

    try:
        client = bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        public_logger.error(f"Failed to create BigQuery client for project {PROJECT_ID}.")
        secure_logger.error(f"BigQuery client creation failed for project {PROJECT_ID}.\nException: {e}", exc_info=True)
        return []

    full_table_name = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    try:
        public_logger.info(f"Fetching sample data for table: {full_table_name}")
        table_reference = bigquery.table.TableReference.from_string(full_table_name, default_project=PROJECT_ID)
        rows_iterator = client.list_rows(table_reference, max_results=num_rows)
        table_sample_rows = [dict(row.items()) for row in rows_iterator]
        if not table_sample_rows:
            public_logger.info(f"No sample data found for table '{full_table_name}'.")
        return table_sample_rows
    except Exception as e:
        public_logger.error(f"Error fetching sample data for {sanitize_for_log(full_table_name)}.")
        secure_logger.error(f"Error fetching sample data for {sanitize_for_log(full_table_name)}.\nException: {sanitize_for_log(e)}", exc_info=True)
        return []