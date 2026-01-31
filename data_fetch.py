# main.py
import os
from core.log import logger
from core.featcher import DataFetcher
from validation.schema_validator import validate_all_tables



def run_data_fetch_pipeline():
    logger.info("===== Business Decision Intelligence System Started =====")

    # 1️⃣ Fetch ALL tables (single source of truth)
    fetcher = DataFetcher(schema_name="salesdata")
    all_data = fetcher.fetch_all_tables()

    logger.info("===== DATA FETCH SUMMARY =====")
    for table_name, df in all_data.items():
        logger.info(
            f"Table '{table_name}' | Rows: {len(df)} | Columns: {len(df.columns)}"
        )

    # 2️⃣ Schema validation (FAIL FAST)
    validate_all_tables(
        dataframes=all_data,
        schema_path=r"D:\business-decision-intelligence-system\schema\schema.yml"
    )

    raw_data = r"D:\business-decision-intelligence-system\data\raw"
    os.makedirs(raw_data, exist_ok= True)

    for table_name, df in all_data.items():
        file_path = os.path.join(raw_data, f"{table_name}.csv")
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"Saved {table_name} to {file_path}")

    logger.info("===== Pipeline Finished Successfully =====")


if __name__ == "__main__":
    run_data_fetch_pipeline()
