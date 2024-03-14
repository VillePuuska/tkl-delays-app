import pandas as pd

def delay_sec(s: str) -> int:
    if s[0] == '-':
        neg = -1
        s = s[11:len(s)-5].split('M')
    else:
        neg = 1
        s = s[10:len(s)-5].split('M')
    return neg * (60*int(s[0]) + int(s[1]))

def stop_id(s: str) -> str:
    return s[-4:]

def body_to_df(body: list, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes the body of the GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    One row per stop in onward calls.

    params:
        - body: requests.get(URL).json()['body']
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of errors when flattening,
                    >= 2 print every error message
    """

    header = ['Recorded_At', 'Line', 'Direction', 'Date', 'Lon',
              'Lat', 'Delay', 'Departure_Time', 'Stop', 'Stop_Order']
    
    lines = []
    broken_lines = 0
    for bus in body:
        try:
            rec_ts = bus['recordedAtTime']
            line = bus['monitoredVehicleJourney']['lineRef']
            direction = bus['monitoredVehicleJourney']['directionRef']
            date = bus['monitoredVehicleJourney']['framedVehicleJourneyRef']['dateFrameRef']
            lon = bus['monitoredVehicleJourney']['vehicleLocation']['longitude']
            lat = bus['monitoredVehicleJourney']['vehicleLocation']['latitude']
            delay = delay_sec(bus['monitoredVehicleJourney']['delay'])
            dep_time = bus['monitoredVehicleJourney']['originAimedDepartureTime']

            const_line = [rec_ts, line, direction, date, lon, lat, delay, dep_time]

            for onward_call in bus['monitoredVehicleJourney']['onwardCalls']:
                lines.append(const_line +
                             [stop_id(onward_call['stopPointRef']), int(onward_call['order'])])
        except Exception as e:
            if logging > 1:
                print(e)
            broken_lines += 1
    if logging > 0:
        print('Total # of broken records (entire vehicle journeys or onward calls):', broken_lines)
    
    return pd.DataFrame(lines, columns=header)

def body_to_df_buses(body: list, logging: int = 0) -> pd.DataFrame:
    """
    Function that takes the body of the GET request to JourneysAPI Vehicle Activity endpoint
    and flattens it to a dataframe.

    Only one row per bus/tram in body.

    params:
        - body: requests.get(URL).json()['body']
        - logging:  <= 0 - no logging/printing,
                    >= 1 - print number of errors when flattening,
                    >= 2 print every error message
    """

    header = ['Recorded_At', 'Line', 'Direction', 'Date', 'Lon',
              'Lat', 'Delay', 'Departure_Time']
    
    lines = []
    broken_lines = 0
    for bus in body:
        try:
            rec_ts = bus['recordedAtTime']
            line = bus['monitoredVehicleJourney']['lineRef']
            direction = bus['monitoredVehicleJourney']['directionRef']
            date = bus['monitoredVehicleJourney']['framedVehicleJourneyRef']['dateFrameRef']
            lon = bus['monitoredVehicleJourney']['vehicleLocation']['longitude']
            lat = bus['monitoredVehicleJourney']['vehicleLocation']['latitude']
            delay = delay_sec(bus['monitoredVehicleJourney']['delay'])
            dep_time = bus['monitoredVehicleJourney']['originAimedDepartureTime']

            lines.append([rec_ts, line, direction, date, lon, lat, delay, dep_time])
        except Exception as e:
            if logging > 1:
                print(e)
            broken_lines += 1
    if logging > 0:
        print('Total # of broken records (entire vehicle journeys):', broken_lines)
    
    return pd.DataFrame(lines, columns=header)
