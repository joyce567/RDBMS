from mini_rdbms import MiniRDBMS

db = MiniRDBMS()

print("MiniRDBMS REPL. Type 'exit' to quit.")

while True:
    cmd = input("db> ").strip()
    if cmd == "exit":
        break

    try:
        if cmd.startswith("CREATE TABLE"):
            db.create_table(
                "users",
                {"id": "INT", "name": "TEXT", "email": "TEXT"},
                primary_key="id",
                unique_keys=["email"]
            )
            print("Table created.")

        elif cmd.startswith("INSERT"):
            db.insert("users", {"id": 1, "name": "Alice", "email": "a@mail.com"})
            print("Row inserted.")

        elif cmd.startswith("SELECT"):
            rows = db.select("users")
            for r in rows:
                print(r)

    except Exception as e:
        print("Error:", e)
