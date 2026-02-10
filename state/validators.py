def validate_sales_state(df):
    required_cols = [
        "state_date",
        "storekey",
        "productkey",
        "total_quantity",
        "avg_unit_price",
        "avg_unit_cost",
        "revenue_usd",
        "cost_usd",
        "total_margin",
        "price_changed_flag",
    ]

    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # First state per (store, product) must NOT be price changed
    first_rows = (
        df.sort_values("state_date")
          .groupby(["storekey", "productkey"])
          .first()
    )

    if first_rows["price_changed_flag"].any():
        raise ValueError("First price change flag must be False")

    # Quantities must be non-negative
    if (df["total_quantity"] < 0).any():
        raise ValueError("Negative quantity detected")

    return True
