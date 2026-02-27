# Snowflake for Data Managers — Handout

Your reference for every concept, SQL statement, and Python snippet from the training session.
Keep this — it's designed to be useful long after the session ends.

---

## 1. Snowflake in 60 seconds

Snowflake is a cloud data warehouse. Unlike a traditional database server you install and manage, Snowflake separates **storage** (where your data lives) from **compute** (the processing power that runs your queries). You can scale compute up/down independently of storage.

**Key concepts for data managers:**

- **Database → Schema → Table/View** — same hierarchy you know from PostgreSQL or SQL Server
- **Warehouse** — a named compute cluster. Think of it as a worker pool. It auto-suspends when idle (no cost) and auto-resumes when you run a query.
- **Role** — your identity for permissions. Always check your active role: `SELECT CURRENT_ROLE();`
- **Stage** — a landing zone for files you want to load (internal or external like S3)
- **Snowsight** — the web UI where you write SQL, manage objects, and monitor queries

**Our schema pattern at Montrose:**
```
DATABASE
├── RAW          ← data as received (CSV drops, API payloads, sensor dumps)
├── TRANSFORM    ← cleaned, typed, joined, business logic applied
└── ANALYTICS    ← reporting-ready views for BI tools and consumers
```

---

## 2. Connecting to Snowflake

### From the browser (Snowsight)

Navigate to your org's Snowflake URL. Use the role selector (top-left) to set your role. Use the database/schema selector or `USE` statements in a worksheet.

### Local Python setup (virtual environment)

If you want to run Python examples from VS Code/terminal, set up a project virtual environment first:

```bash
# make a new folder for this class
mkdir snowflake-training
cd snowflake-training

# locate your python - any one of these will work
brew list --versions python@3.10 python@3.11 python@3.12
# you should get something like
# `python@3.11 3.11.10`

# now find the binary
which -a python3.11 && python3.11 --version
# and you should get something like
# /opt/homebrew/bin/python3.11
# Python 3.11.10

# so i have "python3.11"

# require Python 3.10+ (python3.11 shown here)
python3.11 --version

# create a virtual environment for this class
python3.11 -m venv .venv

# set up env - edit with your user/pass, role and db - note username is without the @montrose part
cat > .venv/.env <<'EOF'
SNOWFLAKE_ACCOUNT=ETLXDPW-SB05598

SNOWFLAKE_USER=your_montrose_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=TRAINING_your_montrose_username_ADMIN
SNOWFLAKE_DATABASE=TRAINING_your_montrose_username_DB

SNOWFLAKE_WAREHOUSE=CTEH_DATA_WH
SNOWFLAKE_SCHEMA=RAW
EOF
chmod 600 .venv/.env
echo ".venv/.env created"

# activate virtual environment
source .venv/bin/activate

# load new env - replace with your location
set -a
source /Users/anton/code/snowflake/training/.venv/.env
set +a

# install requirements
# should show 3.10+ (for example 3.11.x)
python --version   
python -m pip install --upgrade pip
pip install snowflake-connector-python "snowflake-snowpark-python[pandas]" pandas
```

Verify packages:

```bash
python -c "import snowflake.connector; from snowflake.snowpark import Session; import pandas as pd; print('OK')"
```

When done with class:

```bash
deactivate
```

### From Python — snowflake-connector-python

This is the standard Python connector. It gives you a DB-API cursor, and you can fetch results into pandas DataFrames.

```python
import os
from getpass import getpass
import snowflake.connector
import pandas as pd

user = (os.getenv("SNOWFLAKE_USER") or input("Snowflake user: ")).strip().upper()

conn = snowflake.connector.connect(
    account="ETLXDPW-SB05598",
    user = user,
    role = f"TRAINING_{user}_ADMIN",
    password=os.getenv("SNOWFLAKE_PASSWORD") or getpass("Snowflake password: "),
    warehouse="CTEH_DATA_WH",
    database=f"TRAINING_{user}_DB",
    schema="RAW",
)
cur = conn.cursor()

# Run a query and get a pandas DataFrame
cur.execute("SELECT * FROM monitoring_sites")
df = cur.fetch_pandas_all()
print(df.head())

# don't close these yet - we can use them later - this keeps your authenticated session open. 
# conn.close() to logout
#cur.close()
#conn.close()
```

### From Python — Snowpark (Python-native DataFrames in Snowflake)

Snowpark lets you write pandas-style code that executes **inside Snowflake** — data never leaves the cloud.

Where to run this (current Snowsight behavior):

1. Open Snowsight and sign in (`https://app.snowflake.com/etlxdpw/sb05598`).
2. Use one of these execution paths:
   - `Notebooks` (recommended, if enabled): create a Python notebook and run cells there.
   - Local Python (VS Code/terminal): run the script from your virtualenv.
3. Do not use Workspaces Python files for execution; they are currently authoring-only and show "Python files cannot currently be run in Workspaces."

```python
import os
from getpass import getpass
from snowflake.snowpark import Session

user = (os.getenv("SNOWFLAKE_USER") or input("Snowflake user: ")).strip().upper()

connection_params = {
    "account": "ETLXDPW-SB05598",
    "user": user,
    "role": f"TRAINING_{user}_ADMIN",
    # Prompts in terminal without echoing characters on screen
    "password": os.getenv("SNOWFLAKE_PASSWORD") or getpass("Snowflake password: "),
    "warehouse": "CTEH_DATA_WH",
    "database": f"TRAINING_{user}_DB",
    "schema": "RAW"
}
session = Session.builder.configs(connection_params).create()

# Query a table — returns a Snowpark DataFrame (lazy, runs in Snowflake)
df = session.table("monitoring_sites")
df.show()

# Convert to pandas when you need data locally
pdf = df.to_pandas()

# logout
#session.close()
```
---

## 3. Building the training database

We'll create a small environmental monitoring dataset: monitoring sites with coordinates, sampling events, and lab results.

### Create the database and schemas (Snowflake SQL)

```sql
USE ROLE SYSADMIN;  -- or your admin role

CREATE DATABASE IF NOT EXISTS TRAINING_DB;
USE DATABASE TRAINING_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS TRANSFORM;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
```

### Create and populate tables

```sql
USE SCHEMA RAW;

-- Monitoring sites with geographic coordinates
CREATE OR REPLACE TABLE monitoring_sites (
    site_id       STRING,
    site_name     STRING,
    latitude      NUMBER(10,6),
    longitude     NUMBER(10,6),
    site_type     STRING,
    county        STRING,
    state         STRING
);

INSERT INTO monitoring_sites VALUES
    ('MS-001', 'Bishop Creek Upstream',    37.3635, -118.3942, 'SURFACE_WATER', 'Inyo', 'CA'),
    ('MS-002', 'Bishop Creek Downstream',  37.3621, -118.3887, 'SURFACE_WATER', 'Inyo', 'CA'),
    ('MS-003', 'Owens River North',        37.3890, -118.4201, 'SURFACE_WATER', 'Inyo', 'CA'),
    ('MS-004', 'Pine Creek Well MW-1',     37.3745, -118.4055, 'GROUNDWATER',   'Inyo', 'CA'),
    ('MS-005', 'Tungsten Hills Soil',      37.3810, -118.3760, 'SOIL',          'Inyo', 'CA'),
    ('MS-006', 'Volcanic Tableland Air',   37.4920, -118.3680, 'AIR',           'Mono', 'CA'),
    ('MS-007', 'Hot Creek Seep',           37.6610, -118.8290, 'SURFACE_WATER', 'Mono', 'CA'),
    ('MS-008', 'Mammoth Well GW-3',        37.6485, -118.9720, 'GROUNDWATER',   'Mono', 'CA');

-- Sampling events
CREATE OR REPLACE TABLE sampling_events (
    event_id      STRING,
    site_id       STRING,
    collected_ts  STRING,       -- raw string as received from field
    sampler_name  STRING,
    matrix        STRING,
    depth_ft      STRING
);

INSERT INTO sampling_events VALUES
    ('EV-1001', 'MS-001', '2026-01-15 08:30:00', 'J. Rivera',  'WATER', NULL),
    ('EV-1002', 'MS-002', '2026-01-15 09:15:00', 'J. Rivera',  'WATER', NULL),
    ('EV-1003', 'MS-003', '2026-01-15 10:45:00', 'K. Slone',   'WATER', NULL),
    ('EV-1004', 'MS-004', '2026-01-16 07:00:00', 'K. Slone',   'WATER', '45'),
    ('EV-1005', 'MS-005', '2026-01-16 11:30:00', 'M. Chen',    'SOIL',  '2'),
    ('EV-1006', 'MS-001', '2026-02-15 08:00:00', 'J. Rivera',  'WATER', NULL),
    ('EV-1007', 'MS-002', '2026-02-15 09:00:00', 'J. Rivera',  'WATER', NULL),
    ('EV-1008', 'MS-004', '2026-02-16 07:30:00', 'K. Slone',   'WATER', '45'),
    ('EV-1009', 'MS-006', '2026-02-10 06:00:00', 'A. Park',    'AIR',   NULL),
    ('EV-1010', 'MS-007', '2026-02-12 14:00:00', 'M. Chen',    'WATER', NULL);

-- Lab results
CREATE OR REPLACE TABLE lab_results (
    result_id     STRING,
    event_id      STRING,
    analyte       STRING,
    cas_number    STRING,
    result_value  STRING,
    result_unit   STRING,
    detect_flag   STRING,
    method        STRING
);

INSERT INTO lab_results VALUES
    ('LR-001', 'EV-1001', 'ARSENIC',        '7440-38-2', '0.012',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-002', 'EV-1001', 'BENZENE',        '71-43-2',   '0.003',  'MG/L',  'N', 'EPA 8260'),
    ('LR-003', 'EV-1002', 'ARSENIC',        '7440-38-2', '0.045',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-004', 'EV-1002', 'BENZENE',        '71-43-2',   '0.008',  'MG/L',  'Y', 'EPA 8260'),
    ('LR-005', 'EV-1003', 'ARSENIC',        '7440-38-2', '0.009',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-006', 'EV-1004', 'ARSENIC',        '7440-38-2', '0.022',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-007', 'EV-1004', 'TRICHLOROETHENE','79-01-6',   '0.0038', 'MG/L',  'N', 'EPA 8260'),
    ('LR-008', 'EV-1005', 'ARSENIC',        '7440-38-2', '8.5',    'MG/KG', 'Y', 'EPA 6010'),
    ('LR-009', 'EV-1005', 'LEAD',           '7439-92-1', '42.0',   'MG/KG', 'Y', 'EPA 6010'),
    ('LR-010', 'EV-1006', 'ARSENIC',        '7440-38-2', '0.015',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-011', 'EV-1007', 'ARSENIC',        '7440-38-2', '0.052',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-012', 'EV-1007', 'BENZENE',        '71-43-2',   '0.006',  'MG/L',  'Y', 'EPA 8260'),
    ('LR-013', 'EV-1008', 'ARSENIC',        '7440-38-2', '0.028',  'MG/L',  'Y', 'EPA 200.8'),
    ('LR-014', 'EV-1009', 'BENZENE',        '71-43-2',   '0.0004', 'MG/M3', 'N', 'TO-15'),
    ('LR-015', 'EV-1010', 'ARSENIC',        '7440-38-2', '0.031',  'MG/L',  'Y', 'EPA 200.8');
```

---

## 4. Side-by-side: Python way vs Snowflake SQL

For every operation below, we show three approaches:
1. **Local pandas** — what you'd write today on your laptop
2. **snowflake-connector-python** — the bridge (run SQL from Python, get DataFrames back)
3. **Snowpark** — Python-native DataFrames that execute inside Snowflake

---

### Operation 1: Read a table

**Snowflake SQL**
```sql
SELECT * FROM RAW.monitoring_sites;
```

**Local pandas** (pull from Snowflake first, then analyze locally)
```python
query = "SELECT * FROM RAW.monitoring_sites"
cur.execute(query)
df = cur.fetch_pandas_all()
print(df.head())
```

**snowflake-connector-python**
```python
cur.execute("SELECT * FROM RAW.monitoring_sites")
df = cur.fetch_pandas_all()
print(df.head())
```

**Snowpark**
```python
df = session.table("RAW.monitoring_sites")
df.show()
```

---

### Operation 2: Filter rows

**Snowflake SQL**
```sql
SELECT * FROM RAW.monitoring_sites
WHERE site_type = 'GROUNDWATER';
```

**Local pandas**
```python
gw = df[df['site_type'] == 'GROUNDWATER']
```

**snowflake-connector-python**
```python
cur.execute("SELECT * FROM RAW.monitoring_sites WHERE site_type = 'GROUNDWATER'")
gw = cur.fetch_pandas_all()
print(df.head())

```

**Snowpark**
```python
from snowflake.snowpark.functions import col
gw = session.table("RAW.monitoring_sites").filter(col("SITE_TYPE") == "GROUNDWATER")
gw.show()
```

---

### Operation 3: Join tables

**Snowflake SQL**
```sql
SELECT
    s.site_name,
    e.event_id,
    e.collected_ts,
    l.analyte,
    l.result_value,
    l.result_unit
FROM RAW.monitoring_sites s
JOIN RAW.sampling_events e ON s.site_id = e.site_id
JOIN RAW.lab_results l     ON e.event_id = l.event_id
ORDER BY s.site_name, e.collected_ts;
```

**Local pandas**
```python
sites = pd.read_csv("monitoring_sites.csv")
events = pd.read_csv("sampling_events.csv")
labs = pd.read_csv("lab_results.csv")

merged = (sites
    .merge(events, on='site_id')
    .merge(labs, on='event_id')
    [['site_name','event_id','collected_ts','analyte','result_value','result_unit']]
    .sort_values(['site_name','collected_ts']))
```

**snowflake-connector-python**
```python
sql = """
SELECT s.site_name, e.event_id, e.collected_ts,
       l.analyte, l.result_value, l.result_unit
FROM RAW.monitoring_sites s
JOIN RAW.sampling_events e ON s.site_id = e.site_id
JOIN RAW.lab_results l ON e.event_id = l.event_id
ORDER BY s.site_name, e.collected_ts
"""
cur.execute(sql)
df = cur.fetch_pandas_all()
```

**Snowpark**
```python
sites = session.table("RAW.monitoring_sites")
events = session.table("RAW.sampling_events")
labs = session.table("RAW.lab_results")

merged = (sites
    .join(events, "SITE_ID")
    .join(labs, "EVENT_ID")
    .select("SITE_NAME", "EVENT_ID", "COLLECTED_TS",
            "ANALYTE", "RESULT_VALUE", "RESULT_UNIT")
    .sort("SITE_NAME", "COLLECTED_TS"))
merged.show()
```

---

### Operation 4: Aggregate / Group By

**Snowflake SQL**
```sql
SELECT
    s.site_name,
    l.analyte,
    COUNT(*)              AS sample_count,
    AVG(l.result_value::NUMBER(12,6)) AS avg_result,
    MAX(l.result_value::NUMBER(12,6)) AS max_result
FROM RAW.monitoring_sites s
JOIN RAW.sampling_events e ON s.site_id = e.site_id
JOIN RAW.lab_results l     ON e.event_id = l.event_id
GROUP BY s.site_name, l.analyte
ORDER BY s.site_name, l.analyte;
```

**Local pandas**
```python
merged['result_value'] = pd.to_numeric(merged['result_value'])
summary = (merged
    .groupby(['site_name', 'analyte'])['result_value']
    .agg(['count', 'mean', 'max'])
    .reset_index())
```

**snowflake-connector-python**
```python
cur.execute("""
    SELECT s.site_name, l.analyte,
           COUNT(*) AS sample_count,
           AVG(l.result_value::NUMBER(12,6)) AS avg_result,
           MAX(l.result_value::NUMBER(12,6)) AS max_result
    FROM RAW.monitoring_sites s
    JOIN RAW.sampling_events e ON s.site_id = e.site_id
    JOIN RAW.lab_results l ON e.event_id = l.event_id
    GROUP BY s.site_name, l.analyte
    ORDER BY s.site_name, l.analyte
""")
summary = cur.fetch_pandas_all()
print(summary.head())
```

**Snowpark**
```python
from snowflake.snowpark.functions import col, count, avg, max as max_

merged = (sites.join(events, "SITE_ID").join(labs, "EVENT_ID"))

summary = (merged
    .group_by("SITE_NAME", "ANALYTE")
    .agg(
        count("*").alias("SAMPLE_COUNT"),
        avg(col("RESULT_VALUE").cast("NUMBER(12,6)")).alias("AVG_RESULT"),
        max_(col("RESULT_VALUE").cast("NUMBER(12,6)")).alias("MAX_RESULT")
    )
    .sort("SITE_NAME", "ANALYTE"))
summary.show()
```

---

### Operation 5: Create a TRANSFORM table (the pipeline pattern)

**Snowflake SQL**
```sql
CREATE OR REPLACE TABLE TRANSFORM.sample_results AS
SELECT
    s.site_id,
    s.site_name,
    s.latitude,
    s.longitude,
    s.site_type,
    e.event_id,
    TO_TIMESTAMP_NTZ(e.collected_ts)    AS collected_ts,
    UPPER(e.matrix)                      AS matrix,
    TRY_TO_NUMBER(e.depth_ft)           AS depth_ft,
    l.result_id,
    UPPER(l.analyte)                     AS analyte,
    l.cas_number,
    l.result_value::NUMBER(12,6)         AS result_value,
    UPPER(l.result_unit)                 AS result_unit,
    l.detect_flag = 'Y'                  AS is_detect,
    l.method
FROM RAW.monitoring_sites s
JOIN RAW.sampling_events e ON s.site_id = e.site_id
JOIN RAW.lab_results l     ON e.event_id = l.event_id;
```

**Snowpark equivalent**
```python
from snowflake.snowpark.functions import col, upper, to_timestamp_ntz
from snowflake.snowpark.types import DecimalType

result = (sites
    .join(events, "SITE_ID")
    .join(labs, "EVENT_ID")
    .select(
        col("SITE_ID"), col("SITE_NAME"),
        col("LATITUDE"), col("LONGITUDE"), col("SITE_TYPE"),
        col("EVENT_ID"),
        to_timestamp_ntz(col("COLLECTED_TS")).alias("COLLECTED_TS"),
        upper(col("MATRIX")).alias("MATRIX"),
        col("RESULT_ID"),
        upper(col("ANALYTE")).alias("ANALYTE"),
        col("CAS_NUMBER"),
        col("RESULT_VALUE").cast(DecimalType(12,6)).alias("RESULT_VALUE"),
        upper(col("RESULT_UNIT")).alias("RESULT_UNIT"),
        (col("DETECT_FLAG") == "Y").alias("IS_DETECT"),
        col("METHOD")
    ))

# Write to TRANSFORM schema
result.write.mode("overwrite").save_as_table("TRANSFORM.sample_results")
```

---

### Operation 6: Create an ANALYTICS view with screening-level flags

**Snowflake SQL**
```sql
CREATE OR REPLACE VIEW ANALYTICS.v_exceedances AS
SELECT
    *,
    CASE
        WHEN analyte = 'ARSENIC' AND result_unit = 'MG/L'
             AND result_value > 0.010
             THEN TRUE                               -- MCL = 0.010 mg/L
        WHEN analyte = 'BENZENE' AND result_unit = 'MG/L'
             AND result_value > 0.005
             THEN TRUE                               -- MCL = 0.005 mg/L
        WHEN analyte = 'LEAD' AND result_unit = 'MG/KG'
             AND result_value > 400.0
             THEN FALSE                              -- EPA RSL soil = 400 mg/kg
        ELSE FALSE
    END AS exceeds_screening
FROM TRANSFORM.sample_results;

-- Query it
SELECT site_name, analyte, result_value, result_unit, exceeds_screening
FROM ANALYTICS.v_exceedances
WHERE exceeds_screening = TRUE
ORDER BY site_name, analyte;
```

---

## 5. Geospatial in Snowflake

Snowflake has built-in geospatial types and functions. If you've used PostGIS, ArcPy, or geopandas, the concepts map directly.

### Concept mapping

| You know this (GIS/Python) | Snowflake equivalent |
|---|---|
| Shapely Point / geopandas GeoDataFrame | `GEOGRAPHY` or `GEOMETRY` type |
| `ST_Distance()` in PostGIS | `ST_DISTANCE()` in Snowflake |
| `ST_Within()`, `ST_Contains()` | Same names in Snowflake |
| EPSG:4326 (WGS 84 lat/lon) | `GEOGRAPHY` type (default is WGS 84) |
| Projected CRS (e.g., State Plane) | `GEOMETRY` type with `ST_TRANSFORM()` |
| H3 hexagonal grids | Built-in `H3_*` functions |
| Spatial join in geopandas | `ST_DWITHIN()`, `ST_CONTAINS()` in SQL joins |

### Add a GEOGRAPHY column to our sites

```sql
ALTER TABLE RAW.monitoring_sites ADD COLUMN geog GEOGRAPHY;

UPDATE RAW.monitoring_sites
SET geog = ST_MAKEPOINT(longitude, latitude);

-- Verify
SELECT site_id, site_name, geog, ST_ASGEOJSON(geog) AS geojson
FROM RAW.monitoring_sites;
```

### Distance between two sites

```sql
SELECT
    a.site_name AS site_a,
    b.site_name AS site_b,
    ROUND(ST_DISTANCE(a.geog, b.geog), 1) AS distance_meters
FROM RAW.monitoring_sites a, RAW.monitoring_sites b
WHERE a.site_id < b.site_id
ORDER BY distance_meters;
```

### Find all sites within 5 km of a point

```sql
SELECT site_name, site_type,
       ROUND(ST_DISTANCE(geog, ST_MAKEPOINT(-118.3942, 37.3635)), 0) AS dist_m
FROM RAW.monitoring_sites
WHERE ST_DWITHIN(geog, ST_MAKEPOINT(-118.3942, 37.3635), 5000)
ORDER BY dist_m;
```

### H3 hexagonal indexing

H3 assigns every point on Earth to a hexagonal cell at a given resolution (0=coarsest, 15=finest). Useful for spatial aggregation, heatmaps, and fast spatial joins.

```sql
-- Assign each site to an H3 cell at resolution 7 (~5 km² hexagons)
SELECT
    site_name,
    H3_POINT_TO_CELL(geog, 7)          AS h3_index,
    H3_CELL_TO_BOUNDARY(
        H3_POINT_TO_CELL(geog, 7)
    )                                    AS hex_boundary
FROM RAW.monitoring_sites;

-- Aggregate results by H3 cell
SELECT
    H3_POINT_TO_CELL(s.geog, 7)         AS h3_cell,
    COUNT(DISTINCT s.site_id)            AS site_count,
    COUNT(*)                              AS result_count,
    AVG(t.result_value)                  AS avg_result
FROM RAW.monitoring_sites s
JOIN TRANSFORM.sample_results t ON s.site_id = t.site_id
WHERE t.analyte = 'ARSENIC'
GROUP BY h3_cell;
```

### Geospatial from Python (geopandas + connector)

```python
import geopandas as gpd
from shapely.geometry import Point

# Fetch sites with coordinates via connector
cur.execute("SELECT site_id, site_name, latitude, longitude FROM RAW.monitoring_sites")
df = cur.fetch_pandas_all()

# Build a GeoDataFrame locally
gdf = gpd.GeoDataFrame(
    df,
    geometry=[Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])],
    crs="EPSG:4326"
)

# Now you can use all geopandas spatial ops locally
# Buffer, spatial joins, plotting, export to shapefile/GeoJSON, etc.
print(gdf.head())
```

---

## 6. Loading data from files (stages)

In practice, you'll load data from CSV/JSON/Parquet files, not INSERT statements.

### Internal stage (files uploaded to Snowflake)

```sql
-- Create a stage in your RAW schema
CREATE OR REPLACE STAGE RAW.file_stage;

-- Upload a file (from Snowsight: use the "Put" button in the stage browser)
-- Or from SnowSQL CLI:
-- PUT file:///local/path/sites.csv @RAW.file_stage;

-- Load from stage into table
COPY INTO RAW.monitoring_sites
FROM @RAW.file_stage/sites.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

### From Python — write a pandas DataFrame directly

```python
from snowflake.connector.pandas_tools import write_pandas

# Assuming df is your pandas DataFrame
success, nchunks, nrows, _ = write_pandas(
    conn, df, 'MONITORING_SITES', schema='RAW'
)
print(f"Loaded {nrows} rows in {nchunks} chunks")
```

### From Snowpark — write DataFrame to table

```python
# session.write_pandas() uploads a local pandas DataFrame to Snowflake
snowpark_df = session.write_pandas(df, "MONITORING_SITES", schema="RAW", auto_create_table=True)
```

---

## 7. Time Travel (bonus — unique to Snowflake)

Snowflake keeps historical versions of your data. You can query data as it existed in the past.

```sql
-- See what the table looked like 10 minutes ago
SELECT * FROM TRANSFORM.sample_results AT(OFFSET => -600);

-- See what it looked like before a specific query changed it
SELECT * FROM TRANSFORM.sample_results BEFORE(STATEMENT => '<query_id>');

-- Accidentally dropped a table? Undrop it.
DROP TABLE TRANSFORM.sample_results;
UNDROP TABLE TRANSFORM.sample_results;  -- it's back
```

---

## 8. Quick reference: Snowflake SQL for pandas users

| pandas | Snowflake SQL |
|--------|--------------|
| `df.head(10)` | `SELECT * FROM table LIMIT 10;` |
| `df.shape[0]` | `SELECT COUNT(*) FROM table;` |
| `df.describe()` | `SELECT MIN(col), MAX(col), AVG(col), STDDEV(col) FROM table;` |
| `df.dtypes` | `DESCRIBE TABLE table_name;` |
| `df.columns` | `SHOW COLUMNS IN TABLE table_name;` |
| `df[df['col'] > 5]` | `SELECT * FROM table WHERE col > 5;` |
| `df.groupby('col').agg(...)` | `SELECT col, AGG(...) FROM table GROUP BY col;` |
| `df.sort_values('col')` | `SELECT * FROM table ORDER BY col;` |
| `df.merge(df2, on='key')` | `SELECT * FROM t1 JOIN t2 ON t1.key = t2.key;` |
| `df.to_csv('out.csv')` | `COPY INTO @stage FROM table FILE_FORMAT=(TYPE='CSV');` |
| `pd.read_csv('file.csv')` | `COPY INTO table FROM @stage FILE_FORMAT=(TYPE='CSV');` |

---

## 9. Package reference

```bash
# The connector (run SQL from Python, fetch into pandas)
pip install snowflake-connector-python

# Snowpark (Python-native DataFrames that execute in Snowflake)
pip install snowflake-snowpark-python

# Geospatial (local analysis alongside Snowflake)
pip install geopandas shapely
```

**When to use which:**
- **snowflake-connector-python** — you want to run SQL and get results into pandas. Most common for scripting, ETL, and integration. This is your workhorse.
- **Snowpark** — you want to write pandas-style transformations that run server-side in Snowflake. Great for large datasets that shouldn't leave the cloud. Also needed for writing UDFs and stored procedures in Python.
- **Both together** — totally normal. Use the connector for quick queries and data loading, Snowpark for heavy transformations.

---

## 10. Resources

- Snowflake SQL Reference: https://docs.snowflake.com/en/sql-reference
- Snowpark Python Guide: https://docs.snowflake.com/en/developer-guide/snowpark/python/index
- Geospatial Functions: https://docs.snowflake.com/en/sql-reference/functions-geospatial
- snowflake-connector-python: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
- H3 in Snowflake: https://docs.snowflake.com/en/sql-reference/functions/h3_point_to_cell

---

*Montrose Environmental — Snowflake Training for Data Managers*
