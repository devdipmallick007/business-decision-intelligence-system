import pandas as pd

def build_sales_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds DAILY business state at:
    (state_date, storekey, productkey)
    """

    # 1️⃣ Aggregate analytical facts → state grain
    state = (
        df.groupby(
            ["order_date", "storekey", "productkey"],
            as_index=False
        )
        .agg(
            total_quantity=("quantity", "sum"),
            avg_unit_price=("unit_price_usd", "mean"),
            avg_unit_cost=("unit_cost_usd", "mean"),
        )
    )

    # 2️⃣ Rename date
    state = state.rename(columns={"order_date": "state_date"})

    # 3️⃣ Economic metrics
    state["revenue_usd"] = state["total_quantity"] * state["avg_unit_price"]
    state["cost_usd"] = state["total_quantity"] * state["avg_unit_cost"]
    state["total_margin"] = state["revenue_usd"] - state["cost_usd"]

    # 4️⃣ Temporal ordering (MANDATORY)
    state = state.sort_values(
        ["storekey", "productkey", "state_date"]
    )

    # 5️⃣ Price change flag (STATE SEMANTIC)
    prev_price = (
        state.groupby(["storekey", "productkey"])["avg_unit_price"]
             .shift(1)
    )

    state["price_changed_flag"] = (
        (state["avg_unit_price"] != prev_price) &
        prev_price.notna()
    )

    return state.reset_index(drop=True)
