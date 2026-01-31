import yaml
import pandas as pd
from core.log import logger

class SchemaValidationError(Exception):
    pass

def load_schema(schema_path: str) -> dict:
    logger.info(f"Loading schema from {schema_path}")
    with open(schema_path, "r") as f:
        schema = yaml.safe_load(f)
    logger.info("Schema loaded successfully")
    return schema


def validate_required_columns(df: pd.DataFrame, required_columns: list, table_name: str):
    logger.info(f"[{table_name}] Validating required columns...")
    missing = set(required_columns) - set(df.columns)
    if missing:
        logger.error(f"[{table_name}] Missing required columns: {missing}")
        raise SchemaValidationError(f"[{table_name}] Missing required columns: {missing}")
    else:
        logger.info(f"[{table_name}] Required columns validation passed")


def validate_primary_key(df: pd.DataFrame, primary_key: list, table_name: str):
    logger.info(f"[{table_name}] Validating primary key: {primary_key}...")
    if df.duplicated(subset=primary_key).any():
        logger.error(f"[{table_name}] Duplicate values found in primary key: {primary_key}")
        raise SchemaValidationError(f"[{table_name}] Duplicate values found in primary key: {primary_key}")
    else:
        logger.info(f"[{table_name}] Primary key validation passed")


def validate_dtypes(df: pd.DataFrame, dtype_rules: dict, table_name: str):
    logger.info(f"[{table_name}] Validating column data types...")
    for col, expected_type in dtype_rules.items():
        if col not in df.columns:
            logger.warning(f"[{table_name}] Column {col} not found for dtype validation, skipping")
            continue

        if expected_type == "int":
            if not pd.api.types.is_integer_dtype(df[col]):
                logger.error(f"[{table_name}] {col} is not int")
                raise SchemaValidationError(f"[{table_name}] {col} is not int")
        elif expected_type == "float":
            if not pd.api.types.is_float_dtype(df[col]):
                logger.error(f"[{table_name}] {col} is not float")
                raise SchemaValidationError(f"[{table_name}] {col} is not float")
        elif expected_type == "string":
            if not pd.api.types.is_object_dtype(df[col]):
                logger.error(f"[{table_name}] {col} is not string")
                raise SchemaValidationError(f"[{table_name}] {col} is not string")
        elif expected_type == "date":
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                logger.error(f"[{table_name}] {col} is not date")
                raise SchemaValidationError(f"[{table_name}] {col} is not date")
        else:
            logger.warning(f"[{table_name}] Unknown dtype {expected_type} for column {col}, skipping")

        logger.info(f"[{table_name}] Column {col} dtype validation passed")


def validate_table(df: pd.DataFrame, table_schema: dict, table_name: str):
    logger.info(f"Starting validation for table: {table_name}")
    validate_required_columns(df, table_schema["required_columns"], table_name)
    validate_primary_key(df, table_schema["primary_key"], table_name)
    validate_dtypes(df, table_schema["dtypes"], table_name)
    logger.info(f"Completed validation for table: {table_name}")


def validate_all_tables(dataframes: dict, schema_path: str):
    schema = load_schema(schema_path)

    for table_name, df in dataframes.items():
        logger.info(f"Checking table: {table_name}")
        if table_name not in schema["tables"]:
            logger.error(f"Unexpected table found: {table_name}")
            raise SchemaValidationError(f"Unexpected table: {table_name}")

        validate_table(
            df,
            schema["tables"][table_name],
            table_name
        )

    logger.info(" Schema validation passed for all tables")
