import asyncio
from typing import TypedDict

import branca.colormap as cm
from dash import callback, html, no_update, register_page, Input, Output, State
from dash_extensions.javascript import arrow_function, assign
import dash_leaflet as dl
import dash_leaflet.express as dlx
import numpy as np

from src.color import rgba_to_hex
from src.const import DB_DIR
from src.finn import choropleth_polys
from src.utils import load_json

register_page(__name__, path="/")


class TileProps(TypedDict):
  url: str
  themes: dict[str, str]
  attr: str


# Map tiles
tiles: dict[str, TileProps] = {
  "Stadia Maps": {
    "url": "https://tiles.stadiamaps.com/tiles/{}/{{z}}/{{x}}/{{y}}{{r}}.png",
    "themes": {
      "Alidade": "alidade_smooth",
      "Alidade Dark": "alidade_smooth_dark",
      "OSM Bright": "osm_bright",
    },
    "attr": '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> ',
  },
  "Stamen": {
    "url": "http://{{s}}.tile.stamen.com/{}/{{z}}/{{x}}/{{y}}.png",
    "themes": {"Toner": "toner", "Terrain": "terrain"},
    "attr": (
      'Map tiles by <a href="http://stamen.com">Stamen Design</a>, under '
      '<a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. '
      'Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under '
      '<a href="http://www.openstreetmap.org/copyright">ODbL</a>. '
    ),
  },
}


def base_layer(default_theme: str = "Alidade"):
  layers = []
  for v in tiles.values():
    for t in v["themes"]:
      layers.append(
        dl.BaseLayer(
          dl.TileLayer(url=v["url"].format(v["themes"][t]), attribution=v["attr"]),
          name=t,
          checked=t == default_theme,
        )
      )

  return layers


def get_info(feature: dict | None = None):
  if feature is None or (props := feature.get("properties")) is None:
    return [html.P("Hover over a polygon")]

  label = props.get("name")

  return [
    html.B(label),
    f": {props.get('average_sqm_price', 0):.0f} NOK/m²",
  ]


def make_hideout(unit: str, prop: str, style: dict, classes: int = 5):
  path = DB_DIR / "colorbar_values.json"
  vmin, vmax = load_json(path)[unit]

  ctg = [0, *np.linspace(vmin, vmax, classes)]

  colormap = cm.LinearColormap(
    ["gray", "green", "yellow", "red"],
    vmin=vmin,
    vmax=vmax,
    index=[0.0, vmin, (vmax - vmin) / 2, vmax],
  ).to_step(classes + 1)
  colorscale = [rgba_to_hex(c) for c in colormap.colors]

  return {"classes": ctg, "colorscale": colorscale, "style": style, "colorProp": prop}


asyncio.run(choropleth_polys())
default_unit = "municipality"

style = dict(weight=1, opacity=1, color="black", fillOpacity=0.3)
hideout = make_hideout(default_unit, "average_sqm_price", style)

style_handle = assign(
  """function(feature, context) {
  const {classes, colorscale, style, colorProp} = context.hideout
  const value = feature.properties[colorProp]
  
  if (value === null) {
    style.fillColor = colorscale[0]
    return style
  }

  for (let i=0; i < classes.length; i++) {
    if (value > classes[i]) {
      style.fillColor = colorscale[i]
    }
  }
  return style
}"""
)

info_box = html.Div(
  className="absolute bottom-5 left-5 bg-white rounded p-5",
  children=[html.H3("Real estate price level"), html.Div(id="div:realestate:info")],
  style={"position": "absolute", "bottom": "3rem", "left": "1rem", "zIndex": "999"},
)

layout = [
  dl.Map(
    id="map:realestate",
    className="h-full",
    zoom=9,
    center=(59.90, 10.75),
    children=[
      info_box,
      dl.LayersControl(children=base_layer()),
      dl.GeoJSON(
        id="geojson:realestate:choropleth",
        # data=data.to_json(),
        url=f"/assets/choropleth_{default_unit}.json",
        # format='geobuf',
        style=style_handle,
        hideout=hideout,
        hoverStyle=arrow_function(dict(weight=2, color="white")),
        zoomToBoundsOnClick=True,
      ),
      dlx.categorical_colorbar(
        id="colorbar:realestate",
        categories=[f"{c:.2E}" for c in hideout["classes"]],
        colorscale=hideout["colorscale"],
        unit="/m2",
        width=500,
        height=10,
        position="bottomleft",
      ),
    ],
  ),
]


@callback(
  Output("geojson:realestate:choropleth", "url"),
  Output("geojson:realestate:choropleth", "hideout"),
  Output("colorbar:realestate", "tickText"),
  Output("colorbar:realestate", "colorscale"),
  Input("map:realestate", "zoom"),
  State("geojson:realestate:choropleth", "url"),
  prevent_initial_call=True,
)
def update_geojson(zoom: int, url: str):
  unit = url.split("/")[-1].split(".")[0]

  if zoom > 11:
    if unit == "postal_area":
      return no_update

    unit = "postal_area"

  else:
    if unit == "municipality":
      return no_update

    unit = "municipality"

  url = f"/assets/choropleth_{unit}.json"
  hideout = make_hideout(unit, "average_sqm_price", style)
  ctg = [f"{c:.2E}" for c in hideout["classes"]]

  return url, hideout, ctg, hideout["colorscale"]


@callback(
  Output("div:realestate:info", "children"),
  Input("geojson:realestate:choropleth", "hoverData"),
)
def info_hover(feature: dict):
  return get_info(feature)
