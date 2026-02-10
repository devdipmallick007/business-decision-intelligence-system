from pathlib import Path
from core.log import logger
from core.io.file_loader import load_csv
from state.state_calendar import build_state_calendar
from state.sales_state_builder import build_sales_state
from state.versioning import add_state_version
from state.validators import validate_sales_state

ANALYTICAL_PATH = r"D:\business-decision-intelligence-system\data\analytical\analytical_sales_fact.csv"
STATE_OUTPUT_PATH = r"D:\business-decision-intelligence-system\data\state\business_state_sales_daily.csv"
CALENDAR_OUTPUT_PATH = r"D:\business-decision-intelligence-system\data\state\state_calendar.csv"


def run():
    logger.info("===== BUSINESS STATE PIPELINE STARTED =====")

    df = load_csv(ANALYTICAL_PATH, date_cols=["order_date"])

    # Ensure output folders exist
    Path(STATE_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(CALENDAR_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

    # Calendar
    calendar = build_state_calendar(df, "order_date")
    calendar.to_csv(CALENDAR_OUTPUT_PATH, index=False)
    logger.info("State calendar saved")

    # Business state
    state = build_sales_state(df)
    state = add_state_version(state, version=0)

    # Validation
    validate_sales_state(state)

    # Save
    state.to_csv(STATE_OUTPUT_PATH, index=False)
    logger.info("Business state saved")
    logger.info(f"Negative margin rows: {(state['total_margin'] < 0).sum()}")
    logger.info(f"Price change events: {state['price_changed_flag'].sum()}")

    logger.info("===== BUSINESS STATE PIPELINE COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    run()
