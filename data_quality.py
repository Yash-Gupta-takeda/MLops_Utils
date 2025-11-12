import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any
import logging
import configparser
import os
from pandas import DataFrame

# Optional: Spark/SQL imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
except ImportError:
    SparkSession = None

from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config_path = "config.ini"
config = configparser.ConfigParser()
config.read(config_path)

# COLUMN_METRIC_CAT = config.get('DEFAULT', 'COLUMN_METRIC_CAT', fallback="")
# COLUMN_METRIC_SCORE_CAT = config.get('DEFAULT', 'COLUMN_METRIC_SCORE_CAT', fallback="")


class BaseSDK:
    """Abstracts platform-specific read/write logic."""
    def __init__(self, platform: str, connection_params: dict = None, spark=None):
        self.platform = platform.lower()
        self.connection_params = connection_params or {}
        self.spark = spark
        self.engine = None

        if self.platform == "databricks":
            if not self.spark:
                if SparkSession:
                    self.spark = SparkSession.builder.appName("DataQualitySDK").getOrCreate()
                else:
                    raise ImportError("pyspark is required for Databricks platform")
        elif self.platform == "redshift":
            conn_str = (
                f"postgresql+psycopg2://{connection_params['user']}:"
                f"{connection_params['password']}@"
                f"{connection_params['host']}:{connection_params.get('port', 5439)}/"
                f"{connection_params['database']}"
            )
            self.engine = create_engine(conn_str)
        elif self.platform == "local":
            # Local file-based storage; nothing to initialize
            pass
        else:
            raise ValueError(f"Unsupported platform: {self.platform}")

    def read_table(self, table_name: str) -> pd.DataFrame:
        if self.platform == "databricks":
            logger.info(f"Reading table from Databricks: {table_name}")
            return self.spark.sql(f"SELECT * FROM {table_name}").toPandas()
        elif self.platform == "redshift":
            logger.info(f"Reading table from Redshift: {table_name}")
            return pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
        elif self.platform == "local":
            # if not os.path.exists(table_name):
            #     raise FileNotFoundError(f"File not found: {table_name}")
            # return pd.read_csv(table_name)
            logger.info(f"Reading local CSV file: {table_name}")
            return pd.read_csv(table_name)
    
    def write_table(self, df: pd.DataFrame, table_name: str):
        if self.platform == "databricks":
            logger.info(f"Writing table to Databricks Delta: {table_name}")
            schema = StructType([
                StructField("MODEL_NAME", StringType(), True),
                StructField("TIMESTAMP", TimestampType(), True),
                StructField("FEATURE_NAME", StringType(), True),
                StructField("DATA_TYPE", StringType(), True),
                StructField("COLUMN_METRIC", StringType(), True),
                StructField("COLUMN_METRIC_SCORE", FloatType(), True),
                StructField("COLUMN_METRIC_CAT", StringType(), True),
                StructField("COLUMN_METRIC_SCORE_CAT", StringType(), True),
                StructField("USERMAIL", StringType(), True),
            ])
            spark_df = self.spark.createDataFrame(df.to_dict(orient="records"), schema=schema)
            spark_df.write.format("delta").mode("append").saveAsTable(table_name)
        elif self.platform == "redshift":
            logger.info(f"Writing table to Redshift: {table_name}")
            df.to_sql(table_name, self.engine, if_exists="append", index=False)
        elif self.platform == "local":
            logger.info(f"Writing local CSV: {table_name}")
            df.to_csv(table_name, index=False)


class DataQualitySDK(BaseSDK):
    """Platform-agnostic DataQuality SDK."""
    
    def __init__(self, output_table: str, platform: str, connection_params: dict = None, spark=None):
        super().__init__(platform, connection_params, spark)
        self.output_table = output_table

    # --- Metrics computation ---
    def calculate_statistics(self, df: pd.DataFrame, statistic: str) -> Dict[str, Any]:
        stats = {}
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        for column in numerical_cols:
            if statistic == 'mean':
                stats[column] = df[column].mean()
            elif statistic == 'std':
                stats[column] = df[column].std()
            elif statistic == 'var':
                stats[column] = df[column].var()
        return stats

    def missing_values(self, df: pd.DataFrame) -> Dict[str, int]:
        return df.isnull().sum().to_dict()

    def unique_values(self, df: pd.DataFrame) -> Dict[str, list]:
        return {col: df[col].unique().tolist() for col in df.columns}

    def cardinality(self, df: pd.DataFrame) -> Dict[str, int]:
        return df.nunique().to_dict()

    def most_common_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {col: df[col].mode().iloc[0] if not df[col].mode().empty else None for col in df.columns}

    # --- Summary DataFrame ---
    def create_summary_df(self, df: pd.DataFrame, model_name: str, timestamp: datetime) -> pd.DataFrame:
        column_names = ["modelName", "timeStamp", "columnName", "dataType", "columnMetric", "columnMetricScore"]
        summary_data = []

        total_rows = len(df)
        mean_vals = self.calculate_statistics(df, 'mean')
        std_vals = self.calculate_statistics(df, 'std')
        var_vals = self.calculate_statistics(df, 'var')
        missing_vals = self.missing_values(df)
        most_common_vals = self.most_common_values(df)
        cardinality_vals = self.cardinality(df)

        for col in df.columns:
            dtype = "numerical" if pd.api.types.is_numeric_dtype(df[col]) else "categorical"
            summary_data.extend([
                [model_name, timestamp, col, dtype, "count", total_rows],
                [model_name, timestamp, col, dtype, "missing", missing_vals.get(col, "NA")],
                [model_name, timestamp, col, dtype, "unique", cardinality_vals.get(col, "NA")]
            ])
            if dtype == "numerical":
                summary_data.extend([
                    [model_name, timestamp, col, dtype, "mean", mean_vals.get(col, "NA")],
                    [model_name, timestamp, col, dtype, "std_dev", std_vals.get(col, "NA")],
                    [model_name, timestamp, col, dtype, "variance", var_vals.get(col, "NA")]
                ])
            else:
                summary_data.append([model_name, timestamp, col, dtype, "most_common", most_common_vals.get(col, "NA")])

        summary_df = pd.DataFrame(summary_data, columns=column_names)
        summary_df["columnMetricScore"] = pd.to_numeric(summary_df["columnMetricScore"], errors="coerce")
        summary_df["COLUMN_METRIC_CAT"] = config.get('DEFAULT', 'COLUMN_METRIC_CAT', fallback="")
        summary_df["COLUMN_METRIC_SCORE_CAT"] = config.get('DEFAULT', 'COLUMN_METRIC_SCORE_CAT', fallback="")
        return summary_df

    # --- Run method ---
    def run(self, input_table: str, model_name: str, user_email: str):
        logger.info(f"Running DataQualitySDK for model: {model_name}")
        timestamp = datetime.now(timezone.utc)
        df = self.read_table(input_table)
        metrics_df = self.create_summary_df(df, model_name, timestamp)
        metrics_df["USERMAIL"] = user_email

        # Rename to final output schema
        metrics_df.rename(columns={
            "modelName": "MODEL_NAME",
            "timeStamp": "TIMESTAMP",
            "columnName": "FEATURE_NAME",
            "dataType": "DATA_TYPE",
            "columnMetric": "COLUMN_METRIC",
            "columnMetricScore": "COLUMN_METRIC_SCORE",
        }, inplace=True)

        self.write_table(metrics_df, self.output_table)
        logger.info(f"Metrics exported successfully to {self.output_table}")

