import pandas as pd
import duckdb
from typing import Optional


def delay_sec(s: str) -> Optional[int]:
    try:
        if s[0] == "-":
            neg = -1
            s_split = s[11 : len(s) - 5].split("M")
        else:
            neg = 1
            s_split = s[10 : len(s) - 5].split("M")
        return neg * (60 * int(s_split[0]) + int(s_split[1]))
    except:
        return None


def stop_id(s: str) -> Optional[str]:
    try:
        return s[-4:]
    except:
        return None


def body_to_df(filepath: str, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes filepath to the JSON-file of a GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    One row per stop in onward calls.

    params:
        - filepath: str, path to the JSON-file,
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of dropped rows with missing values,
                    >= 2 - print the number of errors when converting delays and stop ids.
    """

    query = f"""
    WITH body AS (
        FROM read_json_auto('{filepath}')
        SELECT unnest(body, recursive := true)
    ), exploded AS (
        FROM body
        SELECT *, unnest(onwardCalls, recursive := true)
    )
    FROM exploded
    SELECT
        recordedAtTime AS Recorded_At,
        lineRef AS Line,
        directionRef AS Direction,
        dateFrameRef AS Date,
        longitude AS Lon,
        latitude AS Lat,
        delay AS Delay,
        originAimedDepartureTime AS Departure_Time,
        stopPointRef AS Stop,
        "order" AS Stop_Order
    """
    df = duckdb.sql(query).df()

    init_len = len(df)
    df = df.dropna()
    if logging >= 1:
        print(f"{init_len-len(df)} row(s) with NULLs dropped.")

    df["Delay"] = df["Delay"].map(delay_sec)
    init_len = len(df)
    df = df.dropna()
    if logging >= 2:
        print(f"{init_len-len(df)} row(s) with malformatted delays dropped.")
    df["Delay"] = df["Delay"].astype(int)

    df["Stop"] = df["Stop"].map(stop_id)
    init_len = len(df)
    df = df.dropna()
    if logging >= 2:
        print(f"{init_len-len(df)} row(s) with malformatted stop ids dropped.")

    return df


def body_to_df_buses(filepath: str, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes the body of the GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    Only one row per bus/tram in body.

    params:
        - filepath: str, path to the JSON-file,
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of dropped rows with missing values,
                    >= 2 - print the number of errors when converting delays.
    """

    query = f"""
    WITH body AS (
        FROM read_json_auto('{filepath}')
        SELECT unnest(body, recursive := true)
    )
    FROM body
    SELECT
        recordedAtTime AS Recorded_At,
        lineRef AS Line,
        directionRef AS Direction,
        dateFrameRef AS Date,
        longitude AS Lon,
        latitude AS Lat,
        delay AS Delay,
        originAimedDepartureTime AS Departure_Time
    """
    df = duckdb.sql(query).df()

    init_len = len(df)
    df = df.dropna()
    if logging >= 1:
        print(f"{init_len-len(df)} row(s) with NULLs dropped.")

    df["Delay"] = df["Delay"].map(delay_sec)
    init_len = len(df)
    df = df.dropna()
    if logging >= 2:
        print(f"{init_len-len(df)} row(s) with malformatted delays dropped.")
    df["Delay"] = df["Delay"].astype(int)

    return df
