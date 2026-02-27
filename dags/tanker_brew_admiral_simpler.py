import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from pathlib import Path
# Imports moved inside functions to allow faster DAG parsing
# from capstone_tanker_brew_admiral.snowpark.load_to_snowflake import upload_to_snowflake
# from capstone_tanker_brew_admiral.snowpark.ingest_sanctions import ingest_sanctions
# from include.eczachly.snowflake_queries import get_snowpark_session
# --- Configuration ---
# Add project root to path
DAG_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
# Note: Adjust logic depending on where this file sits. 
# If in dags/eczachly, root is ../.. 
# If in capstone/dags, root is ../..
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAG_FILE_DIR))

if PROJECT_ROOT not in sys.path:
    airflow_home = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
    if airflow_home not in sys.path:
        sys.path.append(airflow_home)

# Import moved to ingestion_wrapper
# try:
#     from capstone_tanker_brew_admiral.dk_ingestion import run_dk_ingestion
# except ImportError as e:
#     print(f"Failed to import ingestion script: {e}")
#     run_dk_ingestion = None

# Cosmos Imports
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

# --- Global Paths ---
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
DBT_PROJECT_PATH = os.path.join(AIRFLOW_HOME, 'capstone_tanker_brew_admiral', 'dbt_project')
DBT_EXECUTABLE_PATH = os.path.join(AIRFLOW_HOME, 'dbt_venv/bin/dbt')

# --- Global Wrappers ---

def ingestion_wrapper(**context):
    from capstone_tanker_brew_admiral.dk_ingestion import run_dk_ingestion

    execution_date = context['ds']
    return run_dk_ingestion(execution_date=execution_date)

def upload_snowflake_wrapper(**context):
    # --- A. Sanctions Ingestion (Once) ---
    from capstone_tanker_brew_admiral.snowpark.ingest_sanctions import ingest_sanctions
    from include.eczachly.snowflake_queries import get_snowpark_session

    print("--- [Sanctions] Starting Ingestion ---")
    session = get_snowpark_session()
    sanctions_path = os.path.join(PROJECT_ROOT, 'include', 'sanctions.csv')
    ingest_sanctions(session, local_path=sanctions_path)
    session.close()
    print("--- [Sanctions] Ingestion Complete ---")

    local_csv_path = context['task_instance'].xcom_pull(task_ids='download_and_filter_dk_ais_data')
    
    if not local_csv_path or local_csv_path == 'None':
        print("No CSV produced from Ingestion. Skipping upload.")
        # If sanctions succeeded, we still return False regarding AIS?
        return False
        
    execution_date = context['ds']
    print(f"Uploading {local_csv_path} to Snowflake...")

    from capstone_tanker_brew_admiral.snowpark.load_to_snowflake import upload_to_snowflake
    success = upload_to_snowflake(Path(local_csv_path), "DK_AIS_BRONZE", execution_date)
    if not success:
        raise Exception("Snowflake Upload Failed")
        
    return local_csv_path

def alert_wrapper(**context):
    execution_date = context['ds']
    from capstone_tanker_brew_admiral.alerting.send_telegram_alerts import check_and_alert
    check_and_alert(execution_date)

@dag(
    description='Capstone: Tanker Brew Admiral (Simplified)',
    default_args={
        "owner": "nicolassteel",
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(minutes=60),
    },
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['capstone', 'tanker_brew', 'ingestion', 'nicolassteel'],
)
def tanker_brew_admiral_simpler():
    
    # --- 1. Ingestion ---
    # --- 1. Ingestion ---
    download_and_filter_dk_ais_data = PythonOperator(
        task_id='download_and_filter_dk_ais_data',
        python_callable=ingestion_wrapper,
    )

    # --- 2. Upload to Snowflake (Includes Sanctions) ---
    upload_to_snowflake = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload_snowflake_wrapper,
    )

    # --- Cosmos Configuration ---
    profile_config = ProfileConfig(
        profile_name="tanker_brew_profile",
        target_name="dev",
        profiles_yml_filepath=os.path.join(DBT_PROJECT_PATH, 'profiles.yml'),
    )

    # --- 3. Silver Layer ---
    step3_silver_layer = DbtTaskGroup(
        group_id="step3_silver_layer",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            select=["stg_dk_ais", "int_ais_trajectories"],#, "dim_vessels_snapshot"],
        ),
        operator_args={"vars": {'target_date': '{{ ds }}'}},
    )

    # --- 4. Gold Layer ---
    step4_gold_layer = DbtTaskGroup(
        group_id="step4_gold_layer",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            # Select Gold models (include upstream staging for WAP)
            select=["fct_daily_vessel_activity"],
        ),
        operator_args={"vars": {'target_date': '{{ ds }}'}},
    )

    # --- 5. Alerting ---
    send_telegram_alerts = PythonOperator(
        task_id='send_telegram_alerts',
        python_callable=alert_wrapper,
        trigger_rule="all_success"
    )

    download_and_filter_dk_ais_data >> upload_to_snowflake >> step3_silver_layer >> step4_gold_layer >> send_telegram_alerts


tanker_brew_admiral_simpler()
