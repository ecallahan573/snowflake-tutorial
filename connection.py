import tomllib
from pathlib import Path
import snowflake.connector

config = tomllib.loads(Path("config.toml").read_text())["snowflake"]

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    role=config["role"],
    database=config["database"],
    warehouse=config["warehouse"]
)

cur = conn.cursor()
try:
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()[0]
    print(f"Connected to Snowflake version: {version}")
finally:
    cur.close()
    conn.close()
