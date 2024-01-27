import os
import sqlite3

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from werkzeug.security import check_password_hash, generate_password_hash

from util.cloud_run_upscale import cloud_run_upscale, get_resources_limits
from util.system_metric import get_all_metrics, get_metric

load_dotenv()
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

app.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET_KEY")
jwt = JWTManager(app)

DATABASE_PATH = os.environ.get("DATABASE_PATH", "./db/database.db")


def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/api/login", methods=["POST"])
def login():
    if type(request.json) is not dict:
        return jsonify({"msg": "Bad request"}), 400
    print(request.json)
    username = request.json.get("username", None)
    password = request.json.get("password", None)

    conn = get_db_connection()
    user = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()

    if user is None or not check_password_hash(user["password_hash"], password):
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token), 200


@app.route("/api/register_user", methods=["POST"])
@jwt_required()
def register_user():
    if type(request.json) is not dict:
        return jsonify({"msg": "Bad request"}), 400
    username = request.json.get("username")
    password = request.json.get("password")

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    password_hash = generate_password_hash(password)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"error": "Username already exists"}), 409
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify({"message": "User created successfully"}), 201


@app.route("/api/revoke_user", methods=["POST"])
@jwt_required()
def revoke_user():
    if type(request.json) is not dict:
        return jsonify({"msg": "Bad request"}), 400
    username = request.json.get("username")

    if not username:
        return jsonify({"error": "Username is required"}), 400

    if username == "admin":
        return jsonify({"error": "Cannot revoke admin"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    user = cursor.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    if user is None:
        conn.close()
        return jsonify({"error": "User not found"}), 404

    try:
        cursor.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify({"message": "User revoked successfully"}), 200


@app.route("/api/list_users", methods=["POST"])
def list_users():
    conn = get_db_connection()
    cursor = conn.cursor()

    users = cursor.execute("SELECT username FROM users").fetchall()
    user_list = [user["username"] for user in users]

    conn.close()
    return jsonify(user_list)


@app.route("/")
def check_page():
    return "The server is running!"


@app.route("/api/system_metric", methods=["POST"])
@jwt_required()
def get_system_metric_api():
    """
    Retrieves specified metrics for a Google Cloud Run service over a specified time range.

    This function queries Google Cloud Monitoring for metrics related to a Cloud Run service. It supports various
    metrics like request counts, latencies, instance counts, CPU and memory utilization, etc. The time range for
    the query can be specified in days, hours, and minutes.

    Args:
        metric (str): The specific metric to retrieve. Valid options include 'request_count', 'request_latencies',
                      'instance_count', 'CPU_utilization', 'memory_utilization', 'startup_latency'.
        days (int): The number of days to go back in time for the metric data.
        hours (int): The number of hours to go back in time for the metric data.
        minutes (int): The number of minutes to go back in time for the metric data.

    Returns:
        str: The JSON representation of the metric data. If an error occurs, the JSON representation of the error
             message is returned instead.
    """
    request_body: dict[str, str] = request.json  # type: ignore
    metric = request_body["metric"]
    days = int(request_body.get("days", 0))
    hours = int(request_body.get("hours", 0))
    minutes = int(request_body.get("minutes", 0))
    try:
        df = get_metric(metric, days, hours, minutes)
        return jsonify(df.to_json()), 200
    except Exception as e:
        print(f"An error occurred while getting the metric '{metric}':\n{e}")
        return jsonify(f"An error occurred while getting the metric '{metric}':\n{e}"), 500


@app.route("/api/all_system_metric", methods=["POST"])
@jwt_required()
async def get_all_system_metric_api():
    """
    Retrieves specified metrics for a Google Cloud Run service over a specified time range.

    This function queries Google Cloud Monitoring for metrics related to a Cloud Run service. It supports various
    metrics like request counts, latencies, instance counts, CPU and memory utilization, etc. The time range for
    the query can be specified in days, hours, and minutes.

    Args:
        metric (str): The specific metric to retrieve. Valid options include 'request_count', 'request_latencies',
                      'instance_count', 'CPU_utilization', 'memory_utilization', 'startup_latency'.
        days (int): The number of days to go back in time for the metric data.
        hours (int): The number of hours to go back in time for the metric data.
        minutes (int): The number of minutes to go back in time for the metric data.

    Returns:
        str: The JSON representation of the metric data. If an error occurs, the JSON representation of the error
             message is returned instead.
    """
    request_body: dict[str, str] = request.json  # type: ignore
    days = int(request_body.get("days", 0))
    hours = int(request_body.get("hours", 0))
    minutes = int(request_body.get("minutes", 0))
    try:
        return jsonify(await get_all_metrics(days, hours, minutes)), 200
    except Exception as e:
        print(f"An error occurred while getting the metrics:\n{e}")
        return jsonify(f"An error occurred while getting the metrics:\n{e}"), 500


@app.route("/api/cloud_run_upscale", methods=["POST"])
@jwt_required()
def cloud_run_upscale_api():
    """
    Upscales the specified Google Cloud Run service by updating its memory and/or CPU limits.

    This function updates the resource limits of a Cloud Run service based on the provided memory and CPU limits.
    It retrieves the service configuration from environment variables, and then applies the requested changes.
    At least one of 'memory_limit' or 'cpu_limit' must be provided to update the service.

    Args:
        memory_limit (str | None): The new memory limit for the Cloud Run service.
                                   This should be a string like '512Mi' or '1Gi'. If None, memory limit is not updated.
        cpu_limit (str | None): The new CPU limit for the Cloud Run service.
                                This should be a string representing the number of CPUs, e.g., '1', '2'. If None,
                                CPU limit is not updated.

    Raises:
        ValueError: If neither memory_limit nor cpu_limit is provided.

    Returns:
        int: The HTTP status code of the operation. 200 if successful, 500 otherwise.
    """
    request_body: dict[str, str] = request.json  # type: ignore
    memory_limit = request_body.get("memory_limit", None)
    cpu_limit = request_body.get("cpu_limit", None)
    state_code = cloud_run_upscale(memory_limit, cpu_limit)

    if state_code == 200:
        return jsonify("Cloud Run service successfully upscaled."), 200
    return jsonify("Cloud Run service could not be upscaled."), 500


@app.route("/api/get_resources_limits", methods=["POST"])
def get_resources_limits_api():
    """
    Retrieves the current memory and CPU limits of the Google Cloud Run service.

    Returns:
        str: The JSON representation of the resource limits. If an error occurs, the JSON representation of the error
             message is returned instead.
    """
    try:
        result = get_resources_limits()
        return jsonify(result), 200
    except Exception as e:
        return jsonify(f"An error occurred while getting the resource limits:\n{e}"), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, host="0.0.0.0", port=port)
