import pandas as pd
from core.log import logger

NULL_THRESHOLD = 0.2  # 20% nulls as warning threshold

def audit_data(df: pd.DataFrame, stage: str):
    logger.info(f"--- AUDIT [{stage}] ---")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Nulls:\n{df.isna().sum()}")
    logger.info(f"Duplicate rows: {df.duplicated().sum()}")
    logger.info(f"Unique values per column:\n{df.nunique()}")
    logger.info(f"Column datatypes:\n{df.dtypes}")
    logger.info("----------------------")

def log_changes(df_before: pd.DataFrame, df_after: pd.DataFrame):
    logger.info("--- CHANGE LOG ---")
    logger.info(f"Rows before: {len(df_before)}")
    logger.info(f"Rows after : {len(df_after)}")
    logger.info("------------------")

def check_null_thresholds(df: pd.DataFrame, schema: dict):
    # canonicalize schema keys for matching
    schema_columns = {k.strip().lower().replace(" ", "_"): v for k, v in schema["columns"].items()}
    for col, col_info in schema_columns.items():
        if col in df.columns:
            null_ratio = df[col].isna().mean()
            if null_ratio > NULL_THRESHOLD:
                logger.warning(f"[{col}] Null ratio {null_ratio:.2%} exceeds threshold ({NULL_THRESHOLD*100:.0f}%)")
