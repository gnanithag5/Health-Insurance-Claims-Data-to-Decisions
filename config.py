import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),
}

AWS_CONFIG = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "bucket": os.getenv("AWS_BUCKET"),
    "prefix": os.getenv("AWS_PREFIX", "")
}
