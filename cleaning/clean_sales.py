# clean_sales.py

import os
import pandas as pd
import yaml

from core.log import logger
from core.audit_data import audit_data, log_changes, check_null_thresholds

# -----------------------------
# Schema Loader
# -----------------------------
def load_schema(table_name: str, schema_path: str) -> dict:
    logger.info(f"Loading schema for table '{table_name}' from {schema_path}")

    with open(schema_path, "r") as f:
        schema = yaml.safe_load(f)

    if table_name not in schema:
        raise KeyError(f"Table '{table_name}' not found in schema")

    return schema[table_name]


# -----------------------------
# Canonicalization
# -----------------------------
def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Canonicalizing column names")

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


# -----------------------------
# Standardization
# -----------------------------
def standardize_table(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    logger.info("Standardizing table (schema + datatypes)")

    df = canonicalize_columns(df)

    schema_columns = {
        k.strip().lower().replace(" ", "_"): v
        for k, v in schema.get("columns", {}).items()
    }

    for col, rules in schema_columns.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

        col_type = rules.get("type")

        if col_type == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif col_type in ["string", "category"]:
            df[col] = df[col].astype("string").str.strip()

        elif col_type == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce")

        else:
            raise ValueError(f"Unsupported type '{col_type}' for column '{col}'")

    return df


# -----------------------------
# Business Cleaning
# -----------------------------
def clean_table(
    df: pd.DataFrame,
    schema: dict,
    fk_tables: dict | None = None
) -> pd.DataFrame:

    logger.info("Applying business rules")

    initial_rows = len(df)

    # -----------------------------
    # Primary Key Enforcement
    # -----------------------------
    pk_cols = [
        c.strip().lower().replace(" ", "_")
        for c in schema.get("primary_key", [])
    ]

    df = df.dropna(subset=pk_cols)
    df = df.drop_duplicates(subset=pk_cols)

    # -----------------------------
    # Quantity Validation
    # -----------------------------
    if "quantity" in df.columns:
        df["quantity_flagged"] = df["quantity"] <= 0

    # -----------------------------
    # Order Date Validation
    # -----------------------------
    if "order_date" in df.columns:
        df["order_date_flagged"] = df["order_date"].isna()

    # -----------------------------
    # Delivery Date Logic (FACT SAFE)
    # -----------------------------
    if "delivery_date" in df.columns and "order_date" in df.columns:
        df["delivery_flagged"] = False

        # Delivery before order → invalid
        df.loc[
            df["delivery_date"] < df["order_date"],
            "delivery_flagged"
        ] = True

        # Missing delivery → unknown
        df.loc[
            df["delivery_date"].isna(),
            "delivery_flagged"
        ] = True

        # Business assumption:
        # Missing delivery = same-day delivery (logically safe)
        df.loc[
            df["delivery_date"].isna(),
            "delivery_date"
        ] = df["order_date"]

    # -----------------------------
    # Normalize Flags
    # -----------------------------
    for col in [
        "quantity_flagged",
        "order_date_flagged",
        "delivery_flagged"
    ]:
        if col in df.columns:
            df[col] = df[col].fillna(False)

    # -----------------------------
    # Foreign Key Validation
    # -----------------------------
    if fk_tables:
        fk_mappings = {
            "customerkey": "customers",
            "storekey": "stores",
            "productkey": "products"
        }

        for col, table_name in fk_mappings.items():
            if col in df.columns and table_name in fk_tables:
                valid_keys = fk_tables[table_name].iloc[:, 0].unique()
                df[f"{col}_flagged"] = ~df[col].isin(valid_keys)

    dropped = initial_rows - len(df)
    logger.info(f"Cleaning completed. Dropped rows: {dropped}")

    return df


# -----------------------------
# Main Pipeline
# -----------------------------
def main():
    BASE_DIR = r"D:\business-decision-intelligence-system"

    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    STD_DIR = os.path.join(BASE_DIR, "data", "standardized")
    CLEAN_DIR = os.path.join(BASE_DIR, "data", "cleaned")
    SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "sales_schema.yml")

    TABLE_NAME = "sales"

    os.makedirs(STD_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)

    raw_file = os.path.join(RAW_DIR, f"{TABLE_NAME}.csv")
    std_file = os.path.join(STD_DIR, f"{TABLE_NAME}.csv")
    clean_file = os.path.join(CLEAN_DIR, f"{TABLE_NAME}.csv")

    logger.info(f"Starting cleaning for table: {TABLE_NAME}")

    # -----------------------------
    # Load RAW
    # -----------------------------
    df_raw = pd.read_csv(raw_file)
    audit_data(df_raw, stage="RAW")

    # -----------------------------
    # Load Schema
    # -----------------------------
    schema = load_schema(TABLE_NAME, SCHEMA_PATH)

    # -----------------------------
    # Standardize
    # -----------------------------
    df_std = standardize_table(df_raw, schema)
    audit_data(df_std, stage="STANDARDIZED")

    # Schema-driven null checks
    check_null_thresholds(df_std, schema)

    df_std.to_csv(std_file, index=False)

    # -----------------------------
    # Load FK Tables (cleaned)
    # -----------------------------
    fk_tables = {}
    for fk in ["customers", "stores", "products"]:
        fk_file = os.path.join(CLEAN_DIR, f"{fk}.csv")
        if os.path.exists(fk_file):
            fk_tables[fk] = pd.read_csv(fk_file)

    # -----------------------------
    # Business Cleaning
    # -----------------------------
    df_clean = clean_table(df_std, schema, fk_tables=fk_tables)
    df_clean.to_csv(clean_file, index=False)

    audit_data(df_clean, stage="CLEANED")
    log_changes(df_raw, df_clean, stage="CLEANED")

    logger.info("Sales cleaning pipeline completed successfully.")


if __name__ == "__main__":
    main()
