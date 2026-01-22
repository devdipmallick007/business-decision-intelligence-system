# core/db.py
import sys
from pathlib import Path

# Add parent directory to path so 'core' module can be found
sys.path.append(str(Path(__file__).parent.parent.resolve()))

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from core.config import settings
from core.log import logger  # <- our universal logger

def get_engine():
    """
    Create and return a SQLAlchemy engine for MySQL database.
    """
    if settings.DB_TYPE.lower() != "mysql":
        logger.error(f"Unsupported DB_TYPE: {settings.DB_TYPE}")
        raise ValueError("Only MySQL supported currently")

    connection_url = (
        f"mysql+pymysql://{settings.DB_USER}:"
        f"{settings.DB_PASSWORD}@"
        f"{settings.DB_HOST}:"
        f"{settings.DB_PORT}/"
        f"{settings.DB_NAME}"
    )

    try:
        engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            pool_recycle=1800
        )
        logger.info("Database engine created successfully")
        return engine
    except SQLAlchemyError:
        logger.exception("Failed to create database engine")
        raise

# ===== Example Usage =====
if __name__ == "__main__":
    try:
        engine = get_engine()
        logger.info("Engine test completed successfully")
    except Exception as e:
        logger.error(f"Engine test failed: {e}")
