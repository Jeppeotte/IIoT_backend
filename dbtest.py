import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "admin",
    "dbname": "telemetry"
}

try:
    client = psycopg2.connect(**DB_CONFIG)
    print("Succesfully connected to TimescaleDB")

except Exception as e:
    print(f" DB connection error: {e}")

try:
    with client.cursor() as cursor:
        query = "SELECT * FROM group1"
        cursor.execute(query)

except psycopg2.OperationalError:
    print("Lost connection to DB")
