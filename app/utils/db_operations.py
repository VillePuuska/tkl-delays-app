import duckdb
import pandas as pd

POSTGRES_CONN = "postgresql://airflow:airflow@localhost:5432/airflow"


def get_buses_and_stops(
    min_recorded_time: str = None, max_recorded_time: str = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches stored bus and stop data from Postgres.
    Returns the results as a tuple of two dataframes: (df_buses, df_stops)

    If min_recorded_time and/or max_recorded_time are set, then filters rows based on the column `recorded_at`.
    Left inclusive, right exclusive.
    """
    if min_recorded_time is None:
        min_recorded_time = "1970-01-01 00:00:00.000+00"
    if max_recorded_time is None:
        max_recorded_time = "9999-12-31 23:59:59.999+00"

    duckdb.sql(
        f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH '{POSTGRES_CONN}' AS pg (TYPE POSTGRES);
        """
    )
    df_buses = duckdb.sql(
        f"""
        FROM pg.buses
        WHERE recorded_at >= '{min_recorded_time}'
        AND recorded_at < '{max_recorded_time}'
        """
    ).to_df()
    df_stops = duckdb.sql(
        f"""
        FROM pg.stops
        WHERE recorded_at >= '{min_recorded_time}'
        AND recorded_at < '{max_recorded_time}'
        """
    ).to_df()
    duckdb.sql("DETACH pg")

    return (df_buses, df_stops)


def get_buses_and_stops_aggregated(
    min_recorded_time: str = None, max_recorded_time: str = None
):
    """
    Aggregates the bus and stop data.
    Returns the results as a tuple of two dataframes: (df_bus_agg, df_stop_agg)

    If min_recorded_time and/or max_recorded_time are set, then rows are filtered
    based on the column `recorded_at` before aggregation.
    Left inclusive, right exclusive.
    """
    df_buses, df_stops = get_buses_and_stops(
        min_recorded_time=min_recorded_time, max_recorded_time=max_recorded_time
    )

    df_bus_agg = duckdb.sql(
        f"""
        FROM df_buses
        SELECT
            line AS 'Bus line',
            AVG(delay) AS 'Average delay',
            MIN(delay) AS 'Minimum delay',
            MAX(delay) AS 'Maximum delay',
        GROUP BY ALL
        ORDER BY line
        """
    ).df()

    df_stop_agg = duckdb.sql(
        f"""
        FROM df_stops
        SELECT
            stop AS 'Bus stop',
            AVG(delay) AS 'Average delay',
            MIN(delay) AS 'Minimum delay',
            MAX(delay) AS 'Maximum delay',
        GROUP BY ALL
        ORDER BY stop
        """
    ).df()

    return (df_bus_agg, df_stop_agg)
