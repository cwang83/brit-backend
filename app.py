import dynamodb
import time

from flask import Flask, request, jsonify
from flask_cors import CORS

from typing import List, Dict, Any, Tuple, Optional

app = Flask(__name__)
# CORS(app)


def get_user(username: str) -> Optional[Dict[str, str]]:
    return dynamodb.user_get(username)


def put_user(username: str, password: str):
    dynamodb.user_put(username, password)


def get_all_user_items_records(username: str) -> List[Dict[str, Any]]:
    all_user_items_records = dynamodb.user_items_get_all(username)
    return all_user_items_records


def put_user_items_record(username: str, timestamp_ms: int, user_items: List[Dict[str, Any]]):
    dynamodb.user_items_put(username, timestamp_ms, user_items)


def try_authenticate_user(username: str, password: str, true_user_pass: Dict[str, str]) -> Tuple[str, str]:
    if not true_user_pass:
        return False, f"{username} does not exist"

    true_password = true_user_pass["password"]
    if password != true_password:
        return False, "wrong password"
    else:
        return True, "logged in"


def try_add_user(username: str, password: str, true_user_pass: Dict[str, str]) -> Tuple[str, str]:
    if true_user_pass:
        return False, f"{username} already exists"

    put_user(username, password)
    return True, f"{username} added"


def find_last_user_items(all_user_items_records: List[Dict[str, str]]):
    if not all_user_items_records:
        return []

    last_user_items_desc = sorted(all_user_items_records, key=lambda x: x["timestamp"], reverse=True)
    return last_user_items_desc[0]["items"]


def sum_user_items_costs(user_items: List[Dict[str, Any]]) -> float:
    if not user_items:
        return 0.0

    total_cost = sum([float(item["price"]) for item in user_items])
    return total_cost


@app.route("/signup", methods=["POST"])
def signup():
    user_pass = request.json
    username = user_pass.get("username", "").strip()
    password = user_pass.get("password", "").strip()

    resp = dict()
    if not username or not password:
        resp["success"] = False
        resp["message"] = "username or password is blank"
    else:
        success, message = try_add_user(username, password, get_user(username))
        resp["success"] = success
        resp["message"] = message

    return jsonify(resp)


@app.route("/login", methods=["POST"])
def login():
    user_pass = request.json
    username = user_pass.get("username", "").strip()
    password = user_pass.get("password", "").strip()

    resp = dict()
    if not username or not password:
        resp["success"] = False
        resp["message"] = "username or password is blank"
    else:
        success, message = try_authenticate_user(username, password, get_user(username))
        resp["success"] = success
        resp["message"] = message

    return jsonify(resp)


@app.route("/items/<user>", methods=["GET", "POST"])
def items(user: str):
    last_user_items = []
    if request.method == "POST":
        user_items = request.json
        timestamp_ms = int(time.time_ns() / 1_000_000)
        put_user_items_record(user, timestamp_ms, user_items)
        last_user_items = find_last_user_items(get_all_user_items_records(user))
    else:
        last_user_items = find_last_user_items(get_all_user_items_records(user))

    return jsonify(last_user_items)


@app.route("/summary/<user>", methods=["GET"])
def summary(user: str):
    resp = dict()
    last_user_items = find_last_user_items(get_all_user_items_records(user))
    total_cost = sum_user_items_costs(last_user_items)
    resp["total_cost"] = total_cost

    return jsonify(resp)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
