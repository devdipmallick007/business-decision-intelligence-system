# core/audit_data.py

import pandas as pd
from core.log import logger

# ------------------------------------------------------------------
# GLOBAL DEFAULTS
# ------------------------------------------------------------------
DEFAULT_WARNING_THRESHOLD = 0.20   # 20% → warning
DEFAULT_FAIL_THRESHOLD = 0.80      # 80% → hard fail (analytical layer)

# ------------------------------------------------------------------
# GENERIC DATA PROFILING (ALL LAYERS)
# ------------------------------------------------------------------
def audit_data(df: pd.DataFrame, stage: str):
    """
    Generic audit for visibility & diagnostics.
    Safe to use in ALL pipeline stages.
    """
    logger.info(f"--- AUDIT [{stage}] ---")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Duplicate rows (full row): {df.duplicated().sum()}")
    logger.info(f"Null counts:\n{df.isna().sum()}")
    logger.info(f"Unique values per column:\n{df.nunique()}")
    logger.info(f"Column dtypes:\n{df.dtypes}")
    logger.info("----------------------")


# ------------------------------------------------------------------
# CHANGE TRACKING
# ------------------------------------------------------------------
def log_changes(df_before: pd.DataFrame, df_after: pd.DataFrame, stage: str):
    """
    Logs row-level changes between pipeline stages.
    """
    logger.info(f"--- CHANGE LOG [{stage}] ---")
    logger.info(f"Rows before : {len(df_before)}")
    logger.info(f"Rows after  : {len(df_after)}")
    logger.info(f"Delta rows : {len(df_after) - len(df_before)}")
    logger.info("-----------------------------")


# ------------------------------------------------------------------
# NULL THRESHOLD CHECK (WARNING + FAIL)
# ------------------------------------------------------------------
def check_null_thresholds(
    df: pd.DataFrame,
    stage: str,
    thresholds: dict | None = None,
    default_warning: float = DEFAULT_WARNING_THRESHOLD,
    default_fail: float = DEFAULT_FAIL_THRESHOLD
):
    """
    Enforces null sanity.

    Supported threshold formats:
    1) {"col": 0.8}                     -> fail threshold only
    2) {"col": {"warn": 0.2, "fail": 0.8}}
    """

    thresholds = thresholds or {}

    for col in df.columns:
        null_ratio = df[col].isna().mean()

        col_cfg = thresholds.get(col)

        # Normalize config
        if isinstance(col_cfg, dict):
            warn_th = col_cfg.get("warn", default_warning)
            fail_th = col_cfg.get("fail", default_fail)

        elif isinstance(col_cfg, (int, float)):
            warn_th = default_warning
            fail_th = float(col_cfg)

        else:
            warn_th = default_warning
            fail_th = default_fail

        # Enforce thresholds
        if null_ratio > fail_th:
            raise AssertionError(
                f"[{stage}] Column '{col}' null ratio {null_ratio:.2%} "
                f"exceeds FAIL threshold ({fail_th:.0%})"
            )

        if null_ratio > warn_th:
            logger.warning(
                f"[{stage}] Column '{col}' null ratio {null_ratio:.2%} "
                f"exceeds WARNING threshold ({warn_th:.0%})"
            )


# ------------------------------------------------------------------
# ANALYTICAL-GRADE AUDITS (HARD CONTRACTS)
# ------------------------------------------------------------------
def check_grain_uniqueness(
    df: pd.DataFrame,
    grain_cols: list,
    stage: str
):
    """
    Enforces analytical grain.
    MUST be used in Integrated Analytical Layer.
    """
    duplicates = df.duplicated(subset=grain_cols).sum()

    if duplicates > 0:
        raise AssertionError(
            f"[{stage}] Grain violation: {duplicates} duplicate rows "
            f"for grain {grain_cols}"
        )

    logger.info(f"[{stage}] Grain uniqueness PASSED: {grain_cols}")


def check_row_count_invariant(
    df: pd.DataFrame,
    expected_count: int,
    stage: str,
    context: str
):
    """
    Ensures joins do not explode or shrink data.
    """
    actual_count = df.shape[0]

    if actual_count != expected_count:
        raise AssertionError(
            f"[{stage}] Row count invariant FAILED after {context}: "
            f"expected {expected_count}, got {actual_count}"
        )

    logger.info(
        f"[{stage}] Row count invariant PASSED after {context}: {actual_count}"
    )


def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_value=None,
    max_value=None,
    stage: str = "UNKNOWN"
):
    """
    Enforces numeric sanity (e.g., quantity >= 0).
    """
    if min_value is not None:
        violations = (df[column] < min_value).sum()
        if violations > 0:
            raise AssertionError(
                f"[{stage}] Column '{column}': {violations} values below {min_value}"
            )

    if max_value is not None:
        violations = (df[column] > max_value).sum()
        if violations > 0:
            raise AssertionError(
                f"[{stage}] Column '{column}': {violations} values above {max_value}"
            )

    logger.info(
        f"[{stage}] Value range PASSED for '{column}' "
        f"(min={min_value}, max={max_value})"
    )


# ------------------------------------------------------------------
# REQUIRED COLUMN CHECK
# ------------------------------------------------------------------
def check_required_columns(
    df: pd.DataFrame,
    required_cols: list,
    stage: str
):
    """
    Ensures required columns exist.
    """
    missing = set(required_cols) - set(df.columns)

    if missing:
        raise AssertionError(
            f"[{stage}] Missing required columns: {sorted(missing)}"
        )

    logger.info(f"[{stage}] Required columns present")
