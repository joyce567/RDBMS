from flask import Flask, request, jsonify
from mini_rdbms import MiniRDBMS

app = Flask(__name__)
db = MiniRDBMS()

db.create_table(
    "users",
    {"id": "INT", "name": "TEXT", "email": "TEXT"},
    primary_key="id",
    unique_keys=["email"]
)

@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    db.insert("users", data)
    return jsonify({"status": "created"})

@app.route("/users", methods=["GET"])
def get_users():
    return jsonify(db.select("users"))

@app.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    db.update("users", request.json, ("id", user_id))
    return jsonify({"status": "updated"})

@app.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    db.delete("users", ("id", user_id))
    return jsonify({"status": "deleted"})

if __name__ == "__main__":
    app.run(debug=True)
