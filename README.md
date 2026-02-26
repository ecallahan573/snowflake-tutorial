# snowflake
## Environment

1.  Requires Python ≥3.10, <3.14
2.  Set up a python virtual environment using the method of your choice or follow the below instructions for Poetry. A requirements file is stored the git repo along with the pyproject.toml file
3.  Install Poetry

```
# Install Poetry
	# Linux
	curl -sSL https://install.python-poetry.org | python3 -
	# Windows Powershell. Windows users must eadd %APPDATA%\pypoetry\venv\Scripts\poetry to their paths.
	(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
	
# Clone github repo
git clone https://github.com/ecallahan573/snowflake-tutorial.git

# Create virtual environment
poetry install --no-root

# Test the environment for connectivity
poetry run python connection.py
```

## Connect

1.  Edit the config.toml file provided in the repo. Special characters in your password may cause rejection of login.

```
[snowflake]
account = "ETLXDPW-SB05598"
user = "" # e.g. TTESTERSON
password = ""
role = "TRAINING_{USER}_ADMIN" # e.g. TRAINING_TTESTERSON_ADMIN
warehouse = "CTEH_DATA_WH"
database = "TRAINING_{USER}_DB" # e.g. TRAINING_TTESTERSON_DB
schema = "RAW"
```

2\. Test the connection

```
poetry run python connection.py
```