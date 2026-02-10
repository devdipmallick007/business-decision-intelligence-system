import pandas as pd

def build_state_calendar(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    calendar = (
        df[[date_col]]
        .drop_duplicates()
        .sort_values(date_col)
        .rename(columns={date_col: "state_date"})
    )

    calendar["day"] = calendar["state_date"].dt.day
    calendar["month"] = calendar["state_date"].dt.month
    calendar["year"] = calendar["state_date"].dt.year
    calendar["week"] = calendar["state_date"].dt.isocalendar().week

    return calendar.reset_index(drop=True)
