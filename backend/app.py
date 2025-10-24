import os
import logging
import sys
import functools
import time
import sqlalchemy
import collections
import uuid
from flask import Flask, send_from_directory, abort, jsonify, request, current_app
from dotenv import load_dotenv
import json
import pprint
import re

# ------------------------------
# Utility: sanitize user input before logging
# ------------------------------
def sanitize_for_log(value: str) -> str:
    """Remove control characters like newlines to prevent log injection."""
    if not isinstance(value, str):
        value = str(value)
    # remove newline, carriage return, and other non-printable characters
    return re.sub(r'[\x00-\x1f\x7f]', '?', value)

# ------------------------------
# Setup public logger (console)
# ------------------------------
public_logger = logging.getLogger("public")
public_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(console_formatter)
public_logger.addHandler(console_handler)

# ------------------------------
# Setup secure logger (file)
# ------------------------------
secure_log_dir = "C:\\tmp"
os.makedirs(secure_log_dir, exist_ok=True)
secure_log_file = os.path.join(secure_log_dir, "secure_app.log")

secure_logger = logging.getLogger("secure")
secure_logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(secure_log_file)
file_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)
secure_logger.addHandler(file_handler)

# --- Import other modules after logging is set up ---
from backend.utils import (
    get_table_description,
    get_table_ddl_strings,
    get_total_rows,
    get_total_column_count,
    fetch_sample_data_for_single_table
)

try:
    from data_agent.agent import root_agent
    from google.adk.runners import Runner
    from google.adk.sessions.database_session_service import DatabaseSessionService
    from google.adk.sessions.in_memory_session_service import InMemorySessionService
    from google.genai import types as genai_types
except ImportError as e:
    public_logger.critical("A critical module could not be imported. The app cannot start.")
    secure_logger.critical(f"Module import failed:\nException: {e}", exc_info=True)
    root_agent = Runner = DatabaseSessionService = InMemorySessionService = genai_types = None

# Load .env
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

# ------------------------------
# Flask App Factory
# ------------------------------
def create_app():
    app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')

    def cache(timeout=3600):
        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                cache_key = request.url
                if cache_key in wrapper.cache:
                    result, timestamp = wrapper.cache[cache_key]
                    if (time.time() - timestamp) < timeout:
                        public_logger.debug(f"Returning cached result for {sanitize_for_log(cache_key)}")
                        return result
                result = f(*args, **kwargs)
                wrapper.cache[cache_key] = (result, time.time())
                return result
            wrapper.cache = {}
            return wrapper
        return decorator

    APP_NAME = "data_agent_chatbot"

    if all([Runner, InMemorySessionService, root_agent]):
        try:
            db_url = "sqlite:///./my_agent_data.db"
            engine = sqlalchemy.create_engine(db_url)
            engine.connect()
            public_logger.info("Database connection successful.")
            session_service = DatabaseSessionService(db_url=db_url)
        except Exception as e:
            public_logger.warning("Failed to connect to the database, falling back to in-memory session.")
            secure_logger.warning(f"Database connection failed:\nException: {e}", exc_info=True)
            session_service = InMemorySessionService()

        try:
            runner = Runner(app_name=APP_NAME, agent=root_agent, session_service=session_service)
            app.runner = runner
            app.session_service = session_service
            app.genai_types = genai_types
            public_logger.info("ADK Runner initialized successfully.")
        except Exception as e:
            public_logger.critical("Could not initialize ADK Runner.")
            secure_logger.critical(f"Runner initialization failed:\nException: {e}", exc_info=True)
            app.runner = None
    else:
        public_logger.critical("ADK Runner could not be initialized due to missing components.")
        app.runner = None

    frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
    if not os.path.isdir(frontend_build_path):
        public_logger.warning(f"React build directory not found at {frontend_build_path}.")
    app.config['FRONTEND_BUILD_DIR'] = frontend_build_path

    # ------------------------------
    # 💥 HSTS Header Fix
    # ------------------------------
    @app.after_request
    def add_hsts_header(response):
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        return response

    # ------------------------------
    # API Routes
    # ------------------------------
    @app.route("/api/login", methods=["POST"])
    def login():
        session_service = current_app.session_service
        runner = current_app.runner
        if not all([session_service, runner]):
            return jsonify({"error": "Session service not initialized."}), 500

        req_data = request.get_json()
        user_id = sanitize_for_log(req_data.get('user_id'))
        if not user_id:
            return jsonify({"error": "user_id is required"}), 400

        try:
            session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
            public_logger.info(f"New session created for user '{user_id}' with session_id: {sanitize_for_log(session.id)}")
            return jsonify({"session_id": session.id, "user_id": user_id}), 200
        except Exception as e:
            public_logger.error(f"Failed to create session for user '{user_id}'.")
            secure_logger.error(f"Session creation failed for user '{user_id}':\nException: {e}", exc_info=True)
            return jsonify({"error": "Could not create session."}), 500

    @app.route("/api/logout", methods=["POST"])
    def logout():
        req_data = request.get_json()
        user_id = sanitize_for_log(req_data.get('user_id'))
        session_id = sanitize_for_log(req_data.get('session_id'))
        public_logger.info(f"User '{user_id}' logged out of session '{session_id}'.")
        return jsonify({"message": "Logout successful"}), 200

    @app.route("/api/chat", methods=["POST"])
    async def chat_handler():
        kpi_data = collections.defaultdict(lambda: "N/A")
        runner = current_app.runner
        session_service = current_app.session_service
        genai_types = current_app.genai_types
        if not all([runner, session_service, genai_types]):
            return jsonify({"error": "Chat components not initialized on the server."}), 500

        session_id = None
        user_id = None
        try:
            req_data = request.get_json()
            user_id = sanitize_for_log(req_data.get('user_id'))
            session_id = sanitize_for_log(req_data.get('session_id'))
            message_text = sanitize_for_log(req_data.get('message', {}).get('message'))

            if not all([user_id, session_id, message_text]):
                public_logger.warning("Chat request failed due to missing user_id, session_id, or message.")
                return jsonify({"error": "user_id, session_id, and message are required"}), 400

            public_logger.info(f"[CHAT_START] user '{user_id}', session '{session_id}': {message_text}")
            kpi_data.update({"user_id": user_id, "session_id": session_id, "question": message_text})

            final_response_parts, llm_response_text = [], ""
            kpi_data["llm_round_trips"] = 0

            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=genai_types.Content(parts=[genai_types.Part(text=message_text)], role='user')
            ):
                if event.error_code:
                    final_response_parts.append({"role": "assistant", "content": f"I'm sorry, I encountered a technical issue...{event.error_code}"})
                    kpi_data["agent_error"] = event.error_code
                    break

                if hasattr(event, 'content') and event.content:
                    if event.content.role == 'model':
                        kpi_data["llm_round_trips"] += 1
                        text_in_this_turn = ""
                        sql_in_this_turn = None

                        for part in event.content.parts:
                            if hasattr(part, 'text') and part.text:
                                text_in_this_turn += part.text
                            if hasattr(part, 'function_call') and part.function_call:
                                raw_sql = part.function_call.args.get('sql_query')
                                if raw_sql:
                                    sql_in_this_turn = raw_sql
                                    kpi_data["generated_sql"] = sql_in_this_turn

                        if text_in_this_turn:
                            llm_response_text += text_in_this_turn

                        if sql_in_this_turn:
                            formatted_sql = f"```sql\n{sql_in_this_turn}\n```"
                            final_response_parts.append({"role": "model", "content": formatted_sql})
                        elif text_in_this_turn:
                            final_response_parts.append({"role": "model", "content": text_in_this_turn})

            kpi_data["clarification_asked"] = True if kpi_data["generated_sql"] == "N/A" and llm_response_text else False
            public_logger.info(f"[CHAT_END] user '{user_id}': {final_response_parts}")
            return jsonify({"session_id": session_id, "messages": final_response_parts}), 200

        except Exception as e:
            kpi_data["server_error"] = str(e)
            public_logger.error(f"Error during chat processing: {str(e)}")
            secure_logger.error(f"Chat handler error:\nException: {e}", exc_info=True)
            return jsonify({"session_id": session_id or "", "messages": [], "error": "Internal server error"}), 500

    @app.route("/api/tables", methods=["GET"])
    @cache(timeout=3600)
    def list_tables():
        try:
            tables = get_table_ddl_strings()
            num_tables, total_rows, table_names = len(tables), 0, []
            for table in tables:
                table_name = table["table_name"]
                table_names.append(table_name)
                total_rows += get_total_rows(table_name)
            total_columns = get_total_column_count()
            public_logger.info(f"Listed tables: {sanitize_for_log(','.join(table_names))}")
            return jsonify({"tables": table_names, "num_tables": num_tables, "total_columns": total_columns, "total_rows": total_rows}), 200
        except Exception as e:
            public_logger.error(f"Error listing tables: {str(e)}")
            secure_logger.error(f"List tables failed:\nException: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route("/api/table_data", methods=["GET"])
    @cache(timeout=3600)
    def get_table_data():
        table_name = sanitize_for_log(request.args.get("table_name"))
        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        try:
            sample_rows = fetch_sample_data_for_single_table(table_name=table_name)
            description = get_table_description(table_name)
            public_logger.info(f"Fetched data for table: {table_name}")
            return jsonify({"data": sample_rows, "description": description}), 200
        except Exception as e:
            public_logger.error(f"Error getting table data for {table_name}: {str(e)}")
            secure_logger.error(f"Get table data failed for {table_name}:\nException: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route("/api/code", methods=["GET"])
    def get_code_file():
        filepath = sanitize_for_log(request.args.get("filepath"))
        if not filepath:
            return jsonify({"error": "Filepath is required"}), 400

        allowed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_agent")
        safe_filename = os.path.basename(filepath)
        abs_filepath = os.path.join(allowed_dir, safe_filename)
        if not abs_filepath.startswith(allowed_dir):
            return jsonify({"error": "Invalid filepath"}), 400

        try:
            with open(abs_filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            public_logger.info(f"Read code file: {safe_filename}")
            return jsonify({"content": content}), 200
        except FileNotFoundError:
            return jsonify({"error": "File not found"}), 404
        except Exception as e:
            public_logger.error(f"Error reading code file {safe_filename}: {str(e)}")
            secure_logger.error(f"Read code file failed for {safe_filename}:\nException: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route("/api/test_query", methods=["GET"])
    async def test_query():
        user_id = sanitize_for_log(request.args.get("user_id"))
        question = sanitize_for_log(request.args.get("question"))
        if not all([user_id, question]): 
            return jsonify({"error": "Both 'user_id' and 'question' required"}), 400

        runner, genai_types, session_service = current_app.runner, current_app.genai_types, current_app.session_service
        if not all([runner, genai_types, session_service]): 
            return jsonify({"error": "Chat components not initialized"}), 500

        try:
            temp_session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
            public_logger.debug(f"[TEST_ENDPOINT] Created temporary session_id: {sanitize_for_log(temp_session.id)}")
            new_message = genai_types.Content(parts=[genai_types.Part(text=question)], role='user')

            generated_sql, agent_error, llm_response = None, None, ""
            async for event in runner.run_async(user_id=user_id, session_id=temp_session.id, new_message=new_message):
                if event.error_code:
                    agent_error = {"code": event.error_code, "message": event.error_message}
                    break
                if hasattr(event, 'content') and event.content:
                    for part in event.content.parts:
                        if hasattr(part, 'function_call') and part.function_call:
                            raw_sql = part.function_call.args.get('sql_query')
                            if raw_sql: generated_sql = raw_sql
                        if hasattr(part, 'text') and part.text: llm_response += part.text
                if generated_sql or agent_error: break

            if agent_error: return jsonify({"status": "AgentError", "error": agent_error}), 400
            if not generated_sql and llm_response:
                return jsonify({"status": "ClarificationNeeded", "clarification_question": llm_response.strip()}), 200
            return jsonify({"status": "Success", "generated_sql": generated_sql or "No SQL generated."}), 200

        except Exception as e:
            public_logger.error(f"Error in test_query endpoint: {e}")
            secure_logger.error(f"Test query failed:\nException: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_react_app(path):
        build_dir = current_app.config.get('FRONTEND_BUILD_DIR')
        if not build_dir: return abort(404, description="React build directory not found.")
        if path != "" and os.path.exists(os.path.join(build_dir, path)):
            return send_from_directory(build_dir, path)
        else:
            return send_from_directory(build_dir, 'index.html')

    return app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    public_logger.info(f"Starting Flask server on http://0.0.0.0:{port} (debug={debug_mode})")
    app.run(debug=debug_mode, host='0.0.0.0', port=port)