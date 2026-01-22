# core/log.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Make sure logs folder exists
log_path = Path(__file__).parent.parent / "logs"
log_path.mkdir(exist_ok=True)

# Log file
LOG_FILE = log_path / "app.log"

# Create logger
logger = logging.getLogger("project_logger")
logger.setLevel(logging.INFO)  # Minimum level to log

# Formatter
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

# File handler with rotation
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Add handlers to logger
if not logger.hasHandlers():  # avoid duplicate handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
