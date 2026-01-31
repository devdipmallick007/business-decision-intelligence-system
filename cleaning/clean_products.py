# clean_products.py

import os
import pandas as pd
from core.log import logger
from core.audit_data import audit_data, log_changes, check_null_thresholds
import yaml

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
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

# -----------------------------
# Standardization
# -----------------------------
def standardize_table(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    logger.info("Standardizing table (schema + datatypes)")
    df = canonicalize_columns(df)

    schema_columns = {k.strip().lower().replace(" ", "_"): v for k, v in schema.get("columns", {}).items()}

    for col, rules in schema_columns.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        col_type = rules.get("type")
        if col_type == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif col_type == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif col_type in ["string", "category"]:
            df[col] = df[col].astype("string").str.strip()
        else:
            raise ValueError(f"Unsupported type '{col_type}' for column '{col}'")
    return df

# -----------------------------
# Business Cleaning
# -----------------------------
def clean_table(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    logger.info("Applying business rules")
    initial_rows = len(df)

    # Primary Key Enforcement
    pk_cols = [c.strip().lower().replace(" ", "_") for c in schema.get("primary_key", [])]
    df = df.dropna(subset=pk_cols)
    df = df.drop_duplicates(subset=pk_cols)

    # Quantity/Price validation
    if "unit_cost_usd" in df.columns:
        df.loc[df["unit_cost_usd"] < 0, "unit_cost_flagged"] = True
        df["unit_cost_flagged"] = df["unit_cost_flagged"].fillna(False)

    if "unit_price_usd" in df.columns:
        df.loc[df["unit_price_usd"] < 0, "unit_price_flagged"] = True
        df["unit_price_flagged"] = df["unit_price_flagged"].fillna(False)

    # Check cost <= price
    df.loc[df["unit_cost_usd"] > df["unit_price_usd"], "price_mismatch_flagged"] = True
    df["price_mismatch_flagged"] = df["price_mismatch_flagged"].fillna(False)

    dropped = initial_rows - len(df)
    logger.info(f"Cleaning completed. Dropped rows: {dropped}")
    return df

# -----------------------------
# Main Pipeline Function
# -----------------------------
def main():
    BASE_DIR = r"D:\business-decision-intelligence-system"
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    STD_DIR = os.path.join(BASE_DIR, "data", "standardized")
    CLEAN_DIR = os.path.join(BASE_DIR, "data", "cleaned")
    SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "products_schema.yml")
    TABLE_NAME = "products"

    os.makedirs(STD_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)

    raw_file = os.path.join(RAW_DIR, f"{TABLE_NAME}.csv")
    std_file = os.path.join(STD_DIR, f"{TABLE_NAME}.csv")
    clean_file = os.path.join(CLEAN_DIR, f"{TABLE_NAME}.csv")

    logger.info(f"Starting cleaning for table: {TABLE_NAME}")
    df_raw = pd.read_csv(raw_file)
    audit_data(df_raw, "RAW")

    schema = load_schema(TABLE_NAME, SCHEMA_PATH)
    check_null_thresholds(df_raw, schema)

    df_std = standardize_table(df_raw, schema)
    audit_data(df_std, "STANDARDIZED")
    df_std.to_csv(std_file, index=False)

    df_clean = clean_table(df_std, schema)
    df_clean.to_csv(clean_file, index=False)
    audit_data(df_clean, "CLEANED")
    log_changes(df_raw, df_clean)

    logger.info(f"Products cleaning pipeline completed successfully.")

if __name__ == "__main__":
    main()
