from dags.utils.flatten import delay_sec, stop_id, body_to_df, body_to_df_buses

def test_delay_sec():
    # Test truncating fractions of a second
    assert delay_sec("P0Y0M0DT0H1M13.111S") == 73

    # Test parsing negative delays
    assert delay_sec("-P0Y0M0DT0H1M26.000S") == -86

    # Test ignoring months, days, and hours
    assert delay_sec("P1Y1M1DT1H0M39.000S") == 39

def test_stop_id():
    assert stop_id("https://data.itsfactory.fi/journeys/api/1/stop-points/0087") == "0087"
    assert stop_id("https://data.itsfactory.fi/journeys/api/1/stop-points/1500") == "1500"
