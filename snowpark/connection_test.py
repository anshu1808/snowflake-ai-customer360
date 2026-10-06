import os

from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

session = Session.builder.configs(connection_parameters).create()

print("Connected to Snowflake")
print("Current database:", session.get_current_database())
print("Current schema:", session.get_current_schema())
print("Current warehouse:", session.get_current_warehouse())

session.close()