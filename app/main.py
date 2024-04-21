import streamlit as st
from utils.db_operations import get_buses_and_stops_aggregated

df_bus_agg, df_stop_agg = get_buses_and_stops_aggregated()

st.write(df_bus_agg)
st.write(df_stop_agg)
