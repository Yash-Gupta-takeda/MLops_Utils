import os
import json
import logging
import configparser
from datetime import datetime, timezone
from pytz import timezone as pytz_timezone
from typing import Optional
import pandas as pd

from evidently import Report
from evidently.presets import DataDriftPreset
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataDriftSDK:
    """
    A platform-agnostic Python SDK to compute and export data drift metrics (using Evidently).
    Supports CSV or SQL database exports. Preserves Databricks/UnityCatalog logic, but decoupled.
    """

    def __init__(
        self,
        drift_output_path: str,
        env: str = "dev",
        config_path: str = "config.ini",
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the SDK.

        Args:
            drift_output_path (str): Path to store drift results (CSV, parquet, or DB connection string).
            env (str): Environment name (dev/test/prod).
            config_path (str): Path to the configuration file.
            spark (SparkSession, optional): Existing Spark session or None.
        """
        self.drift_output_path = drift_output_path
        self.env = env
        self.config = configparser.ConfigParser()

        read_files = self.config.read(config_path)
        if not read_files:
            logger.warning(f"No configuration file found at '{config_path}'. Using defaults.")

        # Initialize Spark if available
        try:
            self.spark = spark or SparkSession.builder.appName("DataDriftSDK").getOrCreate()
            logger.info("SparkSession initialized successfully.")
        except Exception as e:
            logger.warning(f"Spark unavailable: {e}")
            self.spark = None

    def load_data(self, source: str) -> pd.DataFrame:
        """
        Load data from CSV, parquet, or SQL table.

        Args:
            source (str): File path or SQL table name.

        Returns:
            pd.DataFrame: Loaded data.
        """
        try:
            if source.endswith(".csv"):
                logger.info(f"Loading CSV data from {source}")
                return pd.read_csv(source)
            elif source.endswith(".parquet"):
                logger.info(f"Loading Parquet data from {source}")
                return pd.read_parquet(source)
            elif self.spark:
                logger.info(f"Loading data from Spark SQL table: {source}")
                spark_df = self.spark.sql(f"SELECT * FROM {source}")
                return spark_df.toPandas()
            else:
                raise ValueError(f"Unsupported source format: {source}")
        except Exception as e:
            logger.error(f"Error loading data from {source}: {e}")
            raise

    def convert_timestamp_to_string(self, df: pd.DataFrame, fmt: str = "%Y-%m-%d %H:%M:%S") -> pd.DataFrame:
        df_copy = df.copy()
        timestamp_columns = df_copy.select_dtypes(include=["datetime64", "datetime64[ns]"]).columns
        for col in timestamp_columns:
            df_copy[col] = df_copy[col].dt.strftime(fmt)
        return df_copy

    def calculate_drift_metrics(self, training_data: pd.DataFrame, scoring_data: pd.DataFrame,
                                model_name: str, training_table: str, scoring_table: str,
                                user_email: str, target_column: str) -> pd.DataFrame:
        """
        Calculate data drift metrics using Evidently’s DataDriftPreset, handling both default
        and custom parameters. Returns a pandas DataFrame with columns:
        ["TIMESTAMP", "MODEL_NAME", "FEATURE_NAME", "DATA_DRIFT", "STAT_TEST", "DRIFT_SCORE",
        "TYPE", "REF_MEAN", "SCORE_MEAN", "THRESHOLD", "TRAINING_DATA", "SCORING_DATA",
        "FRANCHISE", "BRAND", "RANK", "USERMAIL"]
        """
        try:
            logger.info("Calculating data drift metrics (default & custom support).")

            # timestamp for all rows (UTC)
            from datetime import datetime, timezone
            timestamp = datetime.now(timezone.utc)

            # If env indicates prod-like, keep localized UTC timestamp (preserve original intent)
            if getattr(self, "env", "").lower() in ("main", "prod", "prd"):
                from datetime import datetime
                from pytz import timezone as _pytz_timezone
                utc = _pytz_timezone("UTC")
                timestamp = utc.localize(datetime.now())

            # ensure datetime columns converted to strings (keeps behavior intact)
            training_data = self.convert_timestamp_to_string(training_data)
            scoring_data = self.convert_timestamp_to_string(scoring_data)

            # Dropping columns that have null values in entire column (same as original)
            null_columns = [col for col in training_data.columns if training_data[col].isnull().all()]
            if null_columns:
                logger.info(f"Dropping all-null columns: {null_columns}")
            training_data = training_data.drop(columns=null_columns)
            scoring_data = scoring_data.drop(columns=null_columns)

            # Dropping target column if specified
            if target_column:
                logger.info(f"Dropping target column '{target_column}' from both datasets.")
                if target_column in training_data.columns:
                    training_data = training_data.drop(columns=[target_column])
                if target_column in scoring_data.columns:
                    scoring_data = scoring_data.drop(columns=[target_column])

            # Build preset_kwargs from self.config (exact same keys/names as original)
            preset_kwargs = {}

            # dataset-level "drift_share"
            drift_share_cfg = self.config.get("DEFAULT", "DRIFT_SHARE", fallback=None)
            if drift_share_cfg and drift_share_cfg.strip() != "":
                try:
                    preset_kwargs["drift_share"] = float(drift_share_cfg)
                except ValueError:
                    logger.warning(f"Ignoring invalid DRIFT_SHARE='{drift_share_cfg}'")

            # global param "method" override
            method_cfg = self.config.get("DEFAULT", "METHOD", fallback=None)
            if method_cfg:
                preset_kwargs["method"] = method_cfg.strip()

            # categorical override: cat_method, cat_threshold
            cat_method_cfg = self.config.get("DEFAULT", "CAT_METHOD", fallback=None)
            if cat_method_cfg:
                preset_kwargs["cat_method"] = cat_method_cfg.strip()
            cat_threshold_cfg = self.config.get("DEFAULT", "CAT_THRESHOLD", fallback=None)
            if cat_threshold_cfg:
                try:
                    preset_kwargs["cat_threshold"] = float(cat_threshold_cfg)
                except ValueError:
                    logger.warning(f"Ignoring invalid CAT_THRESHOLD='{cat_threshold_cfg}'")

            # numerical override: num_method, num_threshold
            num_method_cfg = self.config.get("DEFAULT", "NUM_METHOD", fallback=None)
            if num_method_cfg:
                preset_kwargs["num_method"] = num_method_cfg.strip()
            num_threshold_cfg = self.config.get("DEFAULT", "NUM_THRESHOLD", fallback=None)
            if num_threshold_cfg:
                try:
                    preset_kwargs["num_threshold"] = float(num_threshold_cfg)
                except ValueError:
                    logger.warning(f"Ignoring invalid NUM_THRESHOLD='{num_threshold_cfg}'")

            # per-column method override(JSON)
            per_col_method_cfg = self.config.get("DEFAULT", "PER_COLUMN_METHOD", fallback=None)
            if per_col_method_cfg and per_col_method_cfg.strip() != "":
                try:
                    preset_kwargs["per_column_method"] = json.loads(per_col_method_cfg)
                except json.JSONDecodeError:
                    logger.warning(f"Ignoring invalid JSON in PER_COLUMN_METHOD='{per_col_method_cfg}'")

            # per-column threshold(JSON)
            per_col_threshold_cfg = self.config.get("DEFAULT", "PER_COLUMN_THRESHOLD", fallback=None)
            if per_col_threshold_cfg and per_col_threshold_cfg.strip() != "":
                try:
                    tmp = json.loads(per_col_threshold_cfg)
                    preset_kwargs["per_column_threshold"] = {k: float(v) for k, v in tmp.items()}
                except (json.JSONDecodeError, ValueError):
                    logger.warning(f"Ignoring invalid JSON in PER_COLUMN_THRESHOLD='{per_col_threshold_cfg}'")

            if preset_kwargs:
                logger.info(f"DataDriftPreset parameters: {preset_kwargs}")
            else:
                logger.info("No custom DataDriftPreset parameters found; using Evidently defaults.")

            # Run Evidently DataDriftPreset (preserving original usage)
            drift_report = Report(metrics=[DataDriftPreset(**preset_kwargs)])
            drift_report = drift_report.run(reference_data=training_data, current_data=scoring_data)
            drift_results = drift_report.dict()

            # Extract dataset-level drift share from metric "DriftedColumnsCount(...)"
            dataset_share = None
            for m in drift_results.get("metrics", []):
                metric_id = m.get("metric_id", "")
                if metric_id.startswith("DriftedColumnsCount("):
                    val = m.get("value", {})
                    dataset_share = float(val.get("share", 0.0))
                    break

            if dataset_share is None:
                logger.warning("No 'DriftedColumnsCount(...)' entry found; defaulting dataset_share=0.0")
                dataset_share = 0.0

            # Decide dataset-level drift flag:
            if "drift_share" in preset_kwargs:
                dataset_drift_flag = (dataset_share >= preset_kwargs["drift_share"])
            else:
                dataset_drift_flag = (dataset_share > 0.0)

            dataset_drift_metrics = {
                "TIMESTAMP":     timestamp,
                "MODEL_NAME":    model_name,
                "FEATURE_NAME":  "overall",
                "DATA_DRIFT":    dataset_drift_flag,
                "STAT_TEST":     self.config.get("DEFAULT", "DEFAULT_STAT_TEST", fallback="N/A"),
                "DRIFT_SCORE":   dataset_share,
                "TYPE":          self.config.get("DEFAULT", "DATASET_TYPE", fallback="Dataset"),
                "REF_MEAN":      None,
                "SCORE_MEAN":    None,
                "THRESHOLD":     preset_kwargs.get("drift_share", None),
                "TRAINING_DATA": training_table,
                "SCORING_DATA":  scoring_table,
                "FRANCHISE":     self.config.get("DEFAULT", "FRANCHISE", fallback="N/A"),
                "BRAND":         self.config.get("DEFAULT", "BRAND", fallback="N/A"),
                "RANK":          0,
                "USERMAIL":      self.config.get("DEFAULT", "USER_EMAIL", fallback=""),
            }

            metrics = [dataset_drift_metrics]

            # --- Full definitions of methods (restored and expanded) ---
            # Methods that return p-values (smaller => stronger evidence of difference). For these,
            # treat drift as True when p_value <= threshold (if threshold provided).
            P_VALUE_METHODS = {
                # classic distributional p-value tests
                "ks",              # Kolmogorov-Smirnov (numeric)
                "chisquare",       # Chi-square (categorical)
                "chi-square",
                "chi_square",
                "t_test",          # Student's t-test (numeric)
                "mannw",           # Mann-Whitney U (a.k.a. Mann-Whitney-Wilcoxon)
                "mannwhitney",
                "mann_whitney",
                "wilcoxon",
                "anderson",        # Anderson-Darling (gives stat + critical values; here we treat as p-like)
                "fisher_exact",    # Fisher's exact (categorical, contingency tables)
                "fisher-exact",
                "cramer_von_mises",
                "cramer_von_mises_stat",
                "empirical_mmd",   # empirical maximum mean discrepancy (some implementations return p-value)
                "tvd",             # total variation distance sometimes reported as test with associated p
                # aliases / alternate names used by different libs / configs
                "ks_2samp",
                "kstest",
                "ttest",
                "ttest_ind",
                "paired_t_test",
                "wilcoxon_signed_rank",
                "f_test",
            }

            # Distance / divergence measures (higher => more different). For these,
            # treat drift as True when distance >= threshold (if threshold provided).
            DISTANCE_METHODS = {
                "wasserstein",       # Earth-mover / Wasserstein distance
                "kl_div",            # Kullback-Leibler divergence (asymmetric)
                "kl_divergence",
                "jensenshannon",     # Jensen-Shannon divergence (symmetric variant of KL)
                "hellinger",         # Hellinger distance
                "psi",               # Population Stability Index (PSI)
                "ed",                # Energy distance
                "euclidean",         # Euclidean distance between distributions (if used)
                "cosine",            # Cosine distance
                "emd",               # earth mover's distance alias
                "l1", "l2",          # L1/L2 norms
                "js",                # js divergence alias
                "wasserstein_distance",
                "kolmogorov_smirnov_distance",  # some implementations name it distance
                "chi_square_stat",   # chi-square statistic (treat as distance if returned)
            }

            # Keep iterating over reported Evidently metrics and collect ValueDrift per column
            for m in drift_results.get("metrics", []):
                metric_id = m.get("metric_id", "")
                if not metric_id.startswith("ValueDrift(column="):
                    continue

                # Extract the base column name:
                # metric_id example: "ValueDrift(column=Col_name,method=wasserstein,threshold=0.1)"
                raw = metric_id[len("ValueDrift(column="):]  # "Col_name,method=wasserstein,threshold=0.1)"
                col_name = raw.split(",", 1)[0].rstrip(")")

                # Evidently sometimes puts the 'value' as a dict or number; handle both
                val = m.get("value", None)
                # If value is dict, try to extract a numeric 'result' or 'distance' or 'value'
                if isinstance(val, dict):
                    # common keys: "distance", "statistic", "result", "value", "p_value"
                    if "distance" in val:
                        drift_score = float(val["distance"])
                    elif "statistic" in val:
                        drift_score = float(val["statistic"])
                    elif "result" in val:
                        try:
                            drift_score = float(val["result"])
                        except Exception:
                            drift_score = float(val.get("value", 0.0))
                    elif "p_value" in val:
                        drift_score = float(val["p_value"])
                    else:
                        # fallback to top-level 'value' if present
                        drift_score = float(val.get("value", 0.0)) if "value" in val else 0.0
                else:
                    # if val is a scalar numeric or None
                    try:
                        drift_score = float(val)
                    except Exception:
                        drift_score = 0.0

                # Reconstruct which method was applied to this column
                if (
                    "per_column_method" in preset_kwargs
                    and col_name in preset_kwargs["per_column_method"]
                ):
                    method_name = str(preset_kwargs["per_column_method"][col_name]).lower()
                else:
                    is_numeric_col = (
                        col_name in training_data.columns
                        and pd.api.types.is_numeric_dtype(training_data[col_name])
                    )
                    if is_numeric_col:
                        method_name = (
                            preset_kwargs.get("num_method", "")
                            or preset_kwargs.get("method", "")
                            or "ks"
                        ).lower()
                    else:
                        method_name = (
                            preset_kwargs.get("cat_method", "")
                            or preset_kwargs.get("method", "")
                            or "chi-square"
                        ).lower()

                # normalize some common aliases
                method_name_norm = method_name.replace("-", "_").replace(" ", "_").lower()

                # Determine threshold for this column (per-column overrides > global)
                threshold = None
                if (
                    "per_column_threshold" in preset_kwargs
                    and col_name in preset_kwargs["per_column_threshold"]
                ):
                    threshold = float(preset_kwargs["per_column_threshold"][col_name])
                else:
                    if (
                        col_name in training_data.columns
                        and pd.api.types.is_numeric_dtype(training_data[col_name])
                    ):
                        if "num_threshold" in preset_kwargs:
                            threshold = float(preset_kwargs["num_threshold"])
                    else:
                        if "cat_threshold" in preset_kwargs:
                            threshold = float(preset_kwargs["cat_threshold"])

                # Decide data_drift_flag according to method type and threshold semantics:
                # - If a p-value test (in P_VALUE_METHODS), drift if p_value <= threshold
                #   (if no threshold provided, we cannot infer drift from p-value reliably -> default False)
                # - If a distance method (in DISTANCE_METHODS), drift if distance >= threshold
                #   (if no threshold provided, default to drift if distance > 0)
                # - Otherwise, default to distance semantics (drift if score >= threshold), or >0 if no threshold
                method_alias = method_name_norm
                if threshold is None:
                    # No threshold provided: fall back to default behavior from original code
                    if method_alias in P_VALUE_METHODS:
                        data_drift_flag = False  # p-value without threshold -> assume not drifted
                    else:
                        # distance/other -> consider drift if drift_score > 0.0
                        data_drift_flag = (drift_score > 0.0)
                else:
                    # threshold provided: apply correct inequality based on method family
                    if method_alias in P_VALUE_METHODS:
                        # p-value test: smaller means more evidence of drift
                        data_drift_flag = (drift_score <= threshold)
                    elif method_alias in DISTANCE_METHODS:
                        # distance: larger means more difference
                        data_drift_flag = (drift_score >= threshold)
                    else:
                        # Unknown method: use distance-style comparison (>=)
                        data_drift_flag = (drift_score >= threshold)

                # Compute REF_MEAN & SCORE_MEAN for numeric columns
                if (
                    col_name in training_data.columns
                    and pd.api.types.is_numeric_dtype(training_data[col_name])
                ):
                    ref_mean = (
                        float(training_data[col_name].mean())
                        if not training_data[col_name].dropna().empty else None
                    )
                    score_mean = (
                        float(scoring_data[col_name].mean())
                        if not scoring_data[col_name].dropna().empty else None
                    )
                else:
                    ref_mean = None
                    score_mean = None

                # Column type string
                if (
                    col_name in training_data.columns
                    and pd.api.types.is_numeric_dtype(training_data[col_name])
                ):
                    col_type_str = "Numerical"
                else:
                    col_type_str = "Categorical"

                metrics.append({
                    "TIMESTAMP":     timestamp,
                    "MODEL_NAME":    model_name,
                    "FEATURE_NAME":  col_name,
                    "DATA_DRIFT":    data_drift_flag,
                    "STAT_TEST":     method_name,
                    "DRIFT_SCORE":   drift_score,
                    "TYPE":          col_type_str,
                    "REF_MEAN":      ref_mean,
                    "SCORE_MEAN":    score_mean,
                    "THRESHOLD":     threshold,
                    "TRAINING_DATA": training_table,
                    "SCORING_DATA":  scoring_table,
                    "FRANCHISE":     self.config.get("DEFAULT", "FRANCHISE", fallback="N/A"),
                    "BRAND":         self.config.get("DEFAULT", "BRAND", fallback="N/A"),
                    "RANK":          0,
                    "USERMAIL":      self.config.get("DEFAULT", "USER_EMAIL", fallback=""),
                })

            # Return a DataFrame
            return pd.DataFrame(metrics)

        except Exception as e:
            logger.error(f"Error occurred while calculating drift metrics: {e}")
            raise RuntimeError("Failed to calculate drift metrics") from e


    def export_results(self, metrics_df: pd.DataFrame):
        """
        Platform-agnostic export function (replacement for export_to_delta).
        Supports:
        - CSV (.csv) [Appends if already exists]
        - Parquet (.parquet)
        - Spark table (if self.spark available and path looks like a table)
        - Unity Catalog / Delta table append behavior
        """
        try:
            logger.info("Exporting metrics (platform-agnostic).")
            out = getattr(self, "drift_output_path", None) or getattr(self, "unity_catalog_drift_table", None)
            if out is None:
                raise RuntimeError("No output path/table configured for drift metrics export.")

            # ----------------------------------------------------------------
            # LOCAL FILE EXPORTS (CSV + Parquet)
            # ----------------------------------------------------------------
            if isinstance(out, str) and out.endswith(".csv"):
                file_exists = os.path.isfile(out)
                metrics_df.to_csv(
                    out,
                    mode="a" if file_exists else "w",  # Append if exists
                    header=not file_exists,            # Header only for new file
                    index=False
                )
                logger.info(f"{'Appended' if file_exists else 'Saved'} drift metrics locally: {out}")
                return

            if isinstance(out, str) and out.endswith(".parquet"):
                # Parquet append (if supported)
                if os.path.exists(out):
                    existing_df = pd.read_parquet(out)
                    combined_df = pd.concat([existing_df, metrics_df], ignore_index=True)
                    combined_df.to_parquet(out, index=False)
                    logger.info(f"Appended metrics to existing Parquet file: {out}")
                else:
                    metrics_df.to_parquet(out, index=False)
                    logger.info(f"Saved new Parquet metrics file: {out}")
                return

            # ----------------------------------------------------------------
            # ✅ SPARK / DELTA EXPORT (Databricks)
            # ----------------------------------------------------------------
            if getattr(self, "spark", None) is not None:
                try:
                    from pyspark.sql.types import (
                        StructType, StructField, StringType, FloatType,
                        BooleanType, IntegerType, TimestampType
                    )
                    schema = StructType([
                        StructField("TIMESTAMP", TimestampType(), True),
                        StructField("MODEL_NAME", StringType(), True),
                        StructField("FEATURE_NAME", StringType(), True),
                        StructField("DATA_DRIFT", BooleanType(), True),
                        StructField("STAT_TEST", StringType(), True),
                        StructField("DRIFT_SCORE", FloatType(), True),
                        StructField("TYPE", StringType(), True),
                        StructField("REF_MEAN", FloatType(), True),
                        StructField("SCORE_MEAN", FloatType(), True),
                        StructField("THRESHOLD", FloatType(), True),
                        StructField("TRAINING_DATA", StringType(), True),
                        StructField("SCORING_DATA", StringType(), True),
                        StructField("FRANCHISE", StringType(), True),
                        StructField("BRAND", StringType(), True),
                        StructField("RANK", IntegerType(), True),
                        StructField("USERMAIL", StringType(), True),
                    ])
                    spark_df = self.spark.createDataFrame(metrics_df.to_dict(orient="records"), schema=schema)

                    # Try Unity Catalog / Delta append first
                    if hasattr(self, "unity_catalog_drift_table") and isinstance(self.unity_catalog_drift_table, str):
                        try:
                            spark_df.write.format("delta").mode("append").saveAsTable(self.unity_catalog_drift_table)
                            logger.info(f"Metrics appended to Delta table {self.unity_catalog_drift_table}")
                            return
                        except Exception:
                            # Fallback to generic Spark write
                            try:
                                spark_df.write.mode("append").save(self.unity_catalog_drift_table)
                                logger.info(f"Metrics written to Spark location {self.unity_catalog_drift_table}")
                                return
                            except Exception as e_write:
                                logger.warning(f"Spark write attempts failed: {e_write}")

                    # Fallback: direct Spark path write
                    if isinstance(out, str):
                        spark_df.write.mode("append").save(out)
                        logger.info(f"Metrics written to Spark path {out}")
                        return

                except Exception as e_spark:
                    logger.warning(f"Spark-based export failed or Spark not configured: {e_spark}")

            # ----------------------------------------------------------------
            # If nothing worked
            # ----------------------------------------------------------------
            raise RuntimeError("Failed to export metrics: no supported destination or all exports failed.")

        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")
            raise RuntimeError("Failed to export metrics") from e


    def run(self, training_source: str, scoring_source: str, model_name: str, user_email: str,
                target_column: str) -> pd.DataFrame:
        """
        Execute the full data drift pipeline:
        1. Load data
        2. Compute drift metrics
        3. Export results
        4. Return DataFrame of metrics
        """
        # Load datasets
        training_df = self.load_data(training_source)
        scoring_df = self.load_data(scoring_source)

        # Calculate drift metrics
        metrics_df = self.calculate_drift_metrics(
            training_data=training_df,
            scoring_data=scoring_df,
            model_name=model_name,
            training_table=training_source,
            scoring_table=scoring_source,
            user_email=user_email,
            target_column=target_column
        )

        # Export results
        self.export_results(metrics_df)
        logger.info("✅ Drift detection pipeline completed successfully.")
        return metrics_df

