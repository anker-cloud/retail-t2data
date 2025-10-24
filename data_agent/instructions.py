import os
import datetime
import logging
import json
import yaml
import time
import tempfile
import google.generativeai as genai

# GCP Imports
from google.cloud import storage

# Import your project's modules
from .utils import (
    fetch_table_entry_metadata,
    fetch_bigquery_data_profiles,
    fetch_sample_data_for_tables,
    log_startup_kpis
)
from .constants import MODEL, GCS_BUCKET_FOR_DEBUGGING

# ------------------------------
#  Public logger
# ------------------------------
public_logger = logging.getLogger("public")
public_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_formatter)
public_logger.addHandler(console_handler)

# ------------------------------
#  Secure logger
# ------------------------------
secure_log_dir = "C:\\tmp"
os.makedirs(secure_log_dir, exist_ok=True)
secure_log_file = os.path.join(secure_log_dir, "secure_instructions.log")

secure_logger = logging.getLogger("secure")
secure_logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(secure_log_file)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
secure_logger.addHandler(file_handler)


def json_serial_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _log_prompt_for_debugging(prompt_content: str):
    """Logs the entire prompt as a structured JSON payload for Cloud Logging."""
    try:
        log_payload = {
            "severity": "INFO",
            "message": "Complete agent instructions for debugging. Expand the jsonPayload to view.",
            "full_prompt": prompt_content
        }
        print(json.dumps(log_payload))
        public_logger.info("Logged prompt for debugging.")
    except Exception as e:
        public_logger.warning(f"Could not create structured debug prompt: {e}")
        secure_logger.error(f"Structured debug prompt creation failed:\nException: {e}", exc_info=True)


def _save_instructions_for_debugging(prompt_content: str):
    """
    Saves the final generated prompt to a file for debugging.
    - GCS if running on Cloud Run
    - Local temp directory otherwise
    """
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"prompt_{timestamp}.txt"
        is_cloud_run = os.environ.get('K_SERVICE')

        if is_cloud_run:
            # Save to GCS
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_FOR_DEBUGGING)
            blob = bucket.blob(filename)
            blob.upload_from_string(prompt_content)
            public_logger.info(f"Saved full prompt to GCS: gs://{GCS_BUCKET_FOR_DEBUGGING}/{filename}")
        else:
            temp_dir = tempfile.gettempdir()
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(prompt_content)
            public_logger.info(f"Saved full prompt locally: {file_path}")

    except Exception as e:
        public_logger.warning("Could not save prompt for debugging; continuing execution.")
        secure_logger.error(f"Saving prompt for debugging failed:\nException: {e}", exc_info=True)


def _build_master_instructions() -> str:
    """Fetches, formats, combines all context for the agent and logs KPIs."""
    start_time = time.time()
    public_logger.info("Building master agent instructions... (runs once at startup)")

    try:
        # Fetch dynamic data
        table_metadata = fetch_table_entry_metadata()
        data_profiles = fetch_bigquery_data_profiles()
        samples =[]
        if not data_profiles:
            public_logger.info("Data profiles not found. Fetching sample data as a fallback.")
            samples = fetch_sample_data_for_tables()

        # Convert to JSON strings
        table_metadata_str = json.dumps(table_metadata, indent=2, default=json_serial_default)
        data_profiles_str = json.dumps(data_profiles, indent=2, default=json_serial_default)
        samples_str = json.dumps(samples, indent=2, default=json_serial_default)

        # Load static YAML template
        script_dir = os.path.dirname(__file__)
        yaml_file_path = os.path.join(script_dir, 'instructions.yaml')
        with open(yaml_file_path, 'r', encoding='utf-8') as f:
            instructions_yaml = yaml.safe_load(f)

        instruction_template = "\n---\n".join(instructions_yaml.values())

        # Inject dynamic data
        final_prompt = instruction_template.format(
            table_metadata=table_metadata_str,
            data_profiles=data_profiles_str,
            samples=samples_str
        )

        # Log and save for debugging
        public_logger.info("--- START: FINAL POPULATED AGENT INSTRUCTIONS ---")
        _log_prompt_for_debugging(final_prompt)
        public_logger.info("--- END: FINAL POPULATED AGENT INSTRUCTIONS ---")
        _save_instructions_for_debugging(final_prompt)

        # KPI: Token count
        try:
            model_for_token_count = genai.GenerativeModel(MODEL)
            token_count = model_for_token_count.count_tokens(final_prompt).total_tokens
        except Exception as e:
            token_count = 0
            public_logger.warning(f"Could not calculate token count.")
            secure_logger.error(f"Token count calculation failed:\nException: {e}", exc_info=True)

        total_load_time = time.time() - start_time
        log_startup_kpis(
            metadata=table_metadata,
            profiles=data_profiles,
            token_count=token_count,
            load_time=total_load_time
        )

        public_logger.info(f"Caching complete. Final prompt length: {len(final_prompt)} characters.")

        return final_prompt

    except Exception as e:
        public_logger.error("Failed to build master instructions. Returning empty string.")
        secure_logger.error(f"Master instruction build failed:\nException: {e}", exc_info=True)
        return ""


# Module-level cache
CACHED_INSTRUCTIONS = _build_master_instructions()


def return_instructions_bigquery() -> str:
    """Returns the pre-cached master instructions instantly."""
    public_logger.info(f"Returning CACHED_INSTRUCTIONS. Length: {len(CACHED_INSTRUCTIONS)} characters.")
    return CACHED_INSTRUCTIONS