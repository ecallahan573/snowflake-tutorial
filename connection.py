import tomllib
from pathlib import Path
import snowflake.connector

config = tomllib.loads(Path("config.toml").read_text())["snowflake"]

conn = snowflake.connector.connect(**config)

cur = conn.cursor()
try:
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()[0]
    print(f"Connected to Snowflake version: {version}")
finally:
    cur.close()
    conn.close()
