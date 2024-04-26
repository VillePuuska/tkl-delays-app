from dags.utils.flatten import delay_sec, stop_id, body_to_df, body_to_df_buses
import json
import pandas as pd


def test_delay_sec():
    # Test truncating fractions of a second
    assert delay_sec("P0Y0M0DT0H1M13.111S") == 73

    # Test parsing negative delays
    assert delay_sec("-P0Y0M0DT0H1M26.000S") == -86

    # Test ignoring months, days, and hours
    assert delay_sec("P1Y1M1DT1H0M39.000S") == 39


def test_stop_id():
    assert (
        stop_id("https://data.itsfactory.fi/journeys/api/1/stop-points/0087") == "0087"
    )
    assert (
        stop_id("https://data.itsfactory.fi/journeys/api/1/stop-points/1500") == "1500"
    )


def test_body_to_df():
    # Test with valid data
    df = pd.DataFrame(
        {
            "Recorded_At": ["A", "A", "A2"],
            "Line": ["C", "C", "C2"],
            "Direction": ["D", "D", "D2"],
            "Date": ["E", "E", "E2"],
            "Lon": ["G", "G", "G2"],
            "Lat": ["H", "H", "H2"],
            "Delay": [671, 671, -1],
            "Departure_Time": ["P", "P", "P2"],
            "Stop": ["SSSS", "WWWW", "S2S2"],
            "Stop_Order": ["1", "1", "2"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df("tests/test_data/api-response-1.txt").reset_index(drop=True)
    )

    # Test with a missing field for one item:
    # recordedAtTime missing for first bus
    df = pd.DataFrame(
        {
            "Recorded_At": ["A2"],
            "Line": ["C2"],
            "Direction": ["D2"],
            "Date": ["E2"],
            "Lon": ["G2"],
            "Lat": ["H2"],
            "Delay": [-1],
            "Departure_Time": ["P2"],
            "Stop": ["S2S2"],
            "Stop_Order": ["2"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df("tests/test_data/api-response-2.txt").reset_index(drop=True)
    )

    # Test with a field with invalid format:
    # delay with a non-integer minute value for second bus
    df = pd.DataFrame(
        {
            "Recorded_At": ["A", "A"],
            "Line": ["C", "C"],
            "Direction": ["D", "D"],
            "Date": ["E", "E"],
            "Lon": ["G", "G"],
            "Lat": ["H", "H"],
            "Delay": [671, 671],
            "Departure_Time": ["P", "P"],
            "Stop": ["SSSS", "WWWW"],
            "Stop_Order": ["1", "1"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df("tests/test_data/api-response-3.txt").reset_index(drop=True)
    )


def test_body_to_df_buses():
    # Test with valid data
    df = pd.DataFrame(
        {
            "Recorded_At": ["A", "A2"],
            "Line": ["C", "C2"],
            "Direction": ["D", "D2"],
            "Date": ["E", "E2"],
            "Lon": ["G", "G2"],
            "Lat": ["H", "H2"],
            "Delay": [671, -1],
            "Departure_Time": ["P", "P2"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df_buses("tests/test_data/api-response-1.txt").reset_index(drop=True)
    )

    # Test with a missing field for one item:
    # recordedAtTime missing for first bus
    df = pd.DataFrame(
        {
            "Recorded_At": ["A2"],
            "Line": ["C2"],
            "Direction": ["D2"],
            "Date": ["E2"],
            "Lon": ["G2"],
            "Lat": ["H2"],
            "Delay": [-1],
            "Departure_Time": ["P2"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df_buses("tests/test_data/api-response-2.txt").reset_index(drop=True)
    )

    # Test with a field with invalid format:
    # delay with a non-integer minute value for second bus
    df = pd.DataFrame(
        {
            "Recorded_At": ["A"],
            "Line": ["C"],
            "Direction": ["D"],
            "Date": ["E"],
            "Lon": ["G"],
            "Lat": ["H"],
            "Delay": [671],
            "Departure_Time": ["P"],
        }
    )

    pd.testing.assert_frame_equal(
        df, body_to_df_buses("tests/test_data/api-response-3.txt").reset_index(drop=True)
    )
