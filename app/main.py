import streamlit as st
import streamlit.components.v1 as components
from utils.db_operations import get_buses_and_stops_aggregated

df_bus_agg, df_stop_agg = get_buses_and_stops_aggregated()

st.header("Latest bus locations.")

try:
    with open("/workspaces/tkl-delays-app/data/map.html") as f:
        map_html = f.read()
    components.html(html=map_html, height=750, width=1000)
except FileNotFoundError:
    st.warning(
        "Could not find map.html. Make sure the filepath in `app/main.py` is correct."
    )

st.dataframe(data=df_bus_agg, hide_index=True)
st.dataframe(data=df_stop_agg, hide_index=True)
