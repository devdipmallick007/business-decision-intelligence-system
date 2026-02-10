def add_state_version(df, version: int = 0):
    df["state_version"] = version
    return df
