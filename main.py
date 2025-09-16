import subprocess
import sys
import logging
import snowflake.connector
from config import SNOWFLAKE_CONFIG
from config import AWS_CONFIG

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def run_command(command: str):
    """Run shell command (dbt)."""
    full_cmd = f"{command} --profiles-dir ."
    logging.info(f"Running command: {full_cmd}")
    result = subprocess.run(full_cmd, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed: {full_cmd}")
        sys.exit(result.returncode)

def run_sql_file(sql_file: str):
    """Execute a SQL file in Snowflake."""
    logging.info(f"Executing SQL file: {sql_file}")
    with open(sql_file, "r") as f:
        sql_script = f.read()

    sql_script = sql_script.format(
        AWS_ACCESS_KEY_ID=AWS_CONFIG["access_key"],
        AWS_SECRET_ACCESS_KEY=AWS_CONFIG["secret_key"],
        AWS_BUCKET=AWS_CONFIG["bucket"],
        AWS_PREFIX=AWS_CONFIG["prefix"]
    )

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        role=SNOWFLAKE_CONFIG["role"],
        database=SNOWFLAKE_CONFIG["database"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )
    cs = conn.cursor()
    try:
        # split by semicolon but skip empty parts
        for statement in sql_script.split(";"):
            stmt = statement.strip()
            if stmt:
                logging.info(f"Running statement:\n{stmt[:80]}...")
                cs.execute(stmt)
        logging.info(f"Successfully executed: {sql_file}")
    finally:
        cs.close()
        conn.close()

def simulate_changes():
    """Run the simulate_changes.sql script to mutate data."""
    run_sql_file("scripts/simulate_changes.sql")
    logging.info("Simulated changes applied.")

def main():
    logging.info("Healthcare Claims ELT Pipeline Starting...")

    # 1. Load raw data from S3 → Snowflake
    run_sql_file("Data_Loading_S3/load_data_from_S3.sql")

    # 2. Load seeds (lookups)
    run_command("dbt seed")

    # 3. Run staging
    run_command("dbt run --select staging")

    # 4. Run intermediate
    run_command("dbt run --select intermediate")

    # 5. Run marts
    run_command("dbt run --select marts")

    # 6. Run snapshots (baseline SCD)
    run_command("dbt snapshot")

    # 7. Simulate changes + rerun snapshots
    simulate_changes()
    run_command("dbt snapshot")

    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
