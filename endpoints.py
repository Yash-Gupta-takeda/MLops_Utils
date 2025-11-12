import os
import json
import mlflow
import pickle
import configparser
from mlflow.tracking import MlflowClient
from run_mlops_utility_notebook import ModelMonitoringSDK  # adjust import path if needed
from data_drift import DataDriftSDK
from data_quality import DataQualitySDK


def get_full_table_name(config, env, table_key):
    catalog = config.get('DEFAULT', env, fallback="")
    table = config.get('DEFAULT', table_key, fallback="")
    return f"{catalog}.{table}" if catalog and table else ""


# -------------------------
# Load configuration
# -------------------------
config = configparser.ConfigParser()
config.read('config.ini')


env = config.get('DEFAULT', 'env', fallback='LOCAL')
experiment_name = config.get('DEFAULT', 'EXPERIMENT_NAME', fallback='Default_Experiment')

unity_catalog_table = config.get('DEFAULT', 'MODEL_MONITORING_METRICS', fallback="C:/python scripts/MLops_utils/data/training/model_monitoring_metrics.csv")
training_table = config.get('DEFAULT', 'training_table', fallback="C:/python scripts/MLops_utils/data/training/testing.csv")
inference_table = config.get('DEFAULT', 'inference_table', fallback="C:/python scripts/MLops_utils/data/training/scoring.csv")
data_drift_table = config.get('DEFAULT', 'data_drift_table', fallback="C:/python scripts/MLops_utils/data/training/drift_metrics.csv")
model_owner = config.get('DEFAULT', 'model_owner', fallback="unknown@takeda.com")
model_type = config.get('DEFAULT', 'MODEL_TYPE', fallback="Classification")
target_column = config.get('DEFAULT', 'TARGET_COLUMN', fallback="price")
model_name = config.get('DEFAULT', 'MODEL_NAME', fallback="price")
# Optional: if you want to extract specific columns from your datasets
columns_to_extract = None
# Local fallback pickle model path
local_pickle_model = config.get('DEFAULT', 'LOCAL_PICKLE_MODEL', fallback="C:/python scripts/MLops_utils/iris_model.pkl")

INPUT_TABLE = config.get('DEFAULT', 'training_table', fallback="")
OUTPUT_TABLE = config.get('DEFAULT', 'SCORING_DATA_PATH', fallback="")
MODEL_NAME = config.get('DEFAULT', 'MODEL_NAME', fallback="")
USER_EMAIL = config.get('DEFAULT', 'USER_EMAIL', fallback="")

# -------------------------
# Initialize DataDriftSDK (MLflow first)
# -------------------------
# Initialize SDK
sdk = DataDriftSDK(
    drift_output_path=data_drift_table,   # can also be parquet or delta path
    env="dev",
    config_path=target_column
)

# Run drift detection
metrics_df = sdk.run(
    training_source=training_table,
    scoring_source=inference_table,
    model_name=model_name,
    user_email="ds@example.com",
    target_column="target"
)

# -------------------------
# Initialize DataQualitySDK (MLflow first)
# -------------------------
# Local CSV
dq_sdk = DataQualitySDK(OUTPUT_TABLE, platform="local")
dq_sdk.run(INPUT_TABLE, MODEL_NAME, USER_EMAIL)

# -------------------------
# Initialize SDK (MLflow first)
# -------------------------
sdk = ModelMonitoringSDK(
    unity_catalog_table=unity_catalog_table,
    env=env,
    model_source="pickle",  # will switch dynamically if fallback used
    model_path=local_pickle_model
)

# # -------------------------
# # Get MLflow experiment and runs
# # -------------------------
# experiment = mlflow.get_experiment_by_name(experiment_name)
# if experiment is None:
#     raise ValueError(f"Experiment '{experiment_name}' not found in MLflow.")

# experiment_id = experiment.experiment_id
# runs = mlflow.search_runs(experiment_ids=[experiment_id])
# run_ids = runs['run_id'].tolist()

# print(f"Found {len(run_ids)} runs for experiment '{experiment_name}'.")


# # -------------------------
# # Iterate through MLflow runs
# # -------------------------
# for run_id in run_ids:
#     run = mlflow.get_run(run_id)
#     run_name = run.data.tags.get("mlflow.runName", f"Run_{run_id}")

#     # Extract logged model path from MLflow
#     log_model_history = json.loads(run.data.tags.get('mlflow.log-model.history', '[]'))
#     if not log_model_history:
#         print(f"Skipping run {run_id} — no logged model history.")
#         continue

#     artifact_path = log_model_history[0].get('artifact_path', '')
#     model_uri = f"runs:/{run_id}/{artifact_path}"
#     print(f"\n▶ Processing Run ID: {run_id} — Model URI: {model_uri}")

#     model_loaded = False

#     # Try MLflow model first
#     try:
#         model = mlflow.pyfunc.load_model(model_uri)
#         print(f"✅ Successfully loaded MLflow model for run {run_id}.")
#         model_loaded = True

#     except Exception as e:
#         print(f"⚠️ MLflow model load failed for run {run_id}: {e}")
#         print(f"→ Falling back to local pickle model: {local_pickle_model}")

#         if os.path.exists(local_pickle_model):
#             sdk.model_source = "pickle"
#             sdk.model_path = local_pickle_model
#             model_loaded = True
#         else:
#             print(f"Pickle model not found at {local_pickle_model}. Skipping run {run_id}.")
#             continue

#     # Proceed only if a model (MLflow or pickle) is available
#     if model_loaded:

try:
    sdk.run(
                run_id= None,
                model_uri= None,
                model_name='hcp_model',
                model_type=model_type,
                model_owner=model_owner,
                training_table=training_table,
                inference_table=inference_table,
                data_drift_table=data_drift_table,
                target_column=target_column,
                columns_to_extract=columns_to_extract
            )
    # print(f"✅ Monitoring completed for run {model_name} ({sdk.model_source.upper()} model).")

except Exception as e:
    print(f"Error running monitoring for {model_name}: {e}")
