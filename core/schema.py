import pandas as pd
from core.log import logger

TYPE_MAPPING = {
    "Int": "Int64",
    "int": "Int64",
    "float": "float64",
    "str": "object",
    "date": "datetime64[ns]",
    "bool": "bool"
}

def validate_schema(df: pd.DataFrame, schema: dict):
    logger.info("Starting schema validation")
    errors = []

    expected_cols = [col["name"] for col in schema.get("columns", [])]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    for col_def in schema.get("columns", []):
        col_name = col_def["name"]
        expected_type = TYPE_MAPPING.get(col_def["type"])
        if col_name in df.columns:
            actual_type = str(df[col_name].dtype)
            if expected_type == "datetime64[ns]":
                try:
                    df[col_name] = pd.to_datetime(df[col_name])
                except Exception:
                    errors.append(f"Column '{col_name}' cannot be converted to datetime")
            elif actual_type != expected_type:
                errors.append(f"Column '{col_name}' type mismatch: expected {expected_type}, got {actual_type}")

    for col_def in schema.get("columns", []):
        col_name = col_def["name"]
        nullable = col_def.get("nullable", True)
        if not nullable and col_name in df.columns:
            null_count = df[col_name].isna().sum()
            if null_count > 0:
                errors.append(f"Column '{col_name}' has {null_count} null values but is not nullable")

    if errors:
        for e in errors:
            logger.error(e)
        raise ValueError("Schema validation failed")

    logger.info("Schema validation passed")
