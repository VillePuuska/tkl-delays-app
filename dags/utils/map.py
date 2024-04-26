from folium import Map, Figure, Circle  # type: ignore
import pandas as pd


def make_and_save_map(in_filename: str, out_filename: str):
    df = pd.read_csv(in_filename)

    TILES = "cartodbdark_matter"
    WIDTH = 750
    HEIGHT = 700
    CENTER = [61.49398541579429, 23.76282953958757]
    ZOOM = 12
    f = Figure(width=WIDTH, height=HEIGHT)
    m = Map(location=CENTER, tiles=TILES, zoom_start=ZOOM).add_to(f)

    for _, row in df.iterrows():
        coords = [float(row.Lat), float(row.Lon)]
        Circle(
            coords,
            color="blue",
            popup=f"Line {row.Line}\nDelay {row.Delay}",
            fill=True,
            weight=0,
            fillOpacity=0.7,
            radius=100,
        ).add_to(m)

    m.save(out_filename)
