import pandas as pd
from core.log import logger

def load_csv(path: str, date_cols=None) -> pd.DataFrame:
    logger.info(f"Loading file: {path}")
    df = pd.read_csv(path, parse_dates=date_cols)
    logger.info(f"Rows loaded: {len(df)}")
    return df
