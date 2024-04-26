import pandas as pd
import duckdb


def delay_sec(s: str) -> int:
    if s[0] == "-":
        neg = -1
        s = s[11 : len(s) - 5].split("M")
    else:
        neg = 1
        s = s[10 : len(s) - 5].split("M")
    return neg * (60 * int(s[0]) + int(s[1]))


def stop_id(s: str) -> str:
    return s[-4:]


def body_to_df(filepath: str, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes filepath to the JSON-file of a GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    One row per stop in onward calls.

    params:
        - filepath: str, path to the JSON-file,
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of dropped rows with missing values.
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

    df["Delay"] = df["Delay"].map(delay_sec)
    df["Stop"] = df["Stop"].map(stop_id)

    init_len = len(df)
    df = df.dropna()
    if logging >= 1:
        print(f"{init_len-len(df)} row(s) with NULLs dropped.")

    return df


def body_to_df_buses(filepath: str, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes the body of the GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    Only one row per bus/tram in body.

    params:
        - filepath: str, path to the JSON-file,
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of dropped rows with missing values.
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

    df["Delay"] = df["Delay"].map(delay_sec)

    init_len = len(df)
    df = df.dropna()
    if logging >= 1:
        print(f"{init_len-len(df)} row(s) with NULLs dropped.")

    return df
