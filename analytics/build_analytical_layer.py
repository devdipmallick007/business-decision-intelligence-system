
import pandas as pd
from pathlib import Path

from core.log import logger
from core.audit_data import (
    audit_data,
    check_grain_uniqueness,
    check_null_thresholds,
    check_row_count_invariant,
    check_value_range,
    check_required_columns
)

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
DATA_DIR = Path("data/cleaned")
OUTPUT_DIR = Path("data/analytical")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

STAGE = "analytical_sales_fact"

# ------------------------------------------------------------------
# LOAD CLEANED DATA
# ------------------------------------------------------------------
logger.info("Loading cleaned tables")

sales = pd.read_csv(DATA_DIR / "sales.csv")
products = pd.read_csv(DATA_DIR / "products.csv")
customers = pd.read_csv(DATA_DIR / "customers.csv")
stores = pd.read_csv(DATA_DIR / "stores.csv")
exchange_rates = pd.read_csv(DATA_DIR / "exchangerates.csv")

base_row_count = sales.shape[0]
logger.info(f"Base sales row count: {base_row_count}")

# ------------------------------------------------------------------
# BUILD ANALYTICAL FACT (NO AGGREGATION)
# Grain: order_number + line_item
# ------------------------------------------------------------------
df = (
    sales
        .merge(products, on="productkey", how="left")
        .merge(stores, on="storekey", how="left")
        .merge(customers, on="customerkey", how="left")
        .merge(
            exchange_rates,
            left_on=["order_date", "currency_code"],
            right_on=["rate_date", "currency"],
            how="left"
        )
)

# ------------------------------------------------------------------
# GENERIC AUDIT (VISIBILITY ONLY)
# ------------------------------------------------------------------
logger.info("Running analytical audits")
audit_data(df, stage=STAGE)

# ------------------------------------------------------------------
# HARD ANALYTICAL CONTRACTS
# ------------------------------------------------------------------

# 1️⃣ Grain enforcement
check_grain_uniqueness(
    df,
    grain_cols=["order_number", "line_item"],
    stage=STAGE
)

# 2️⃣ Row count invariant (joins must not explode/shrink)
check_row_count_invariant(
    df,
    expected_count=base_row_count,
    stage=STAGE,
    context="dimension joins"
)

# 3️⃣ Required columns (analytical minimum contract)
check_required_columns(
    df,
    required_cols=[
        "order_number",
        "line_item",
        "order_date",
        "customerkey",
        "storekey",
        "productkey",
        "quantity",
        "currency_code",
        "exchange"
    ],
    stage=STAGE
)

# 4️⃣ Null sanity (global thresholds from audit file)
check_null_thresholds(df, stage=STAGE)

# 5️⃣ Measure sanity
check_value_range(
    df,
    column="quantity",
    min_value=1,
    stage=STAGE
)

# ------------------------------------------------------------------
# SAVE ANALYTICAL FACT
# ------------------------------------------------------------------
output_path = OUTPUT_DIR / "analytical_sales_fact.csv"
df.to_csv(output_path, index=False)

logger.info(f"Analytical layer built successfully {output_path}")
