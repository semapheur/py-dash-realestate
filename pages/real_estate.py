import asyncio
from datetime import datetime as dt
from typing import TypedDict

import branca.colormap as cm
from dash import callback, html, no_update, register_page, Input, Output, State
from dash_extensions.javascript import arrow_function, assign
import dash_leaflet as dl
import dash_leaflet.express as dlx
import numpy as np

from src.color import rgba_to_hex
from src.const import DB_DIR, STATIC_DIR
from src.finn import choropleth_polys, finn_ads
from src.utils import load_json

register_page(__name__, path="/")

# Viridis: ['#440154', '#482777', '#3f4a8a', '#31678e', '#26838f', '#1f9d8a', '#6cce5a', '#b6de2b', '#fee825']


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


def make_choropleth_hideout(unit: str, prop: str, style: dict, classes: int = 5):
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


def make_ad_hideout(prop: str, style: dict, colorscale: list[str]):
  path = DB_DIR / "colorbar_values.json"
  vmin, vmax = load_json(path)["ad"]

  return {
    "min": vmin,
    "max": vmax,
    "colorscale": colorscale,
    "style": style,
    "colorProp": prop,
  }


async def update_geodata():
  folder_path = STATIC_DIR / "geodata"
  folder_path.mkdir(exist_ok=True, parents=True)
  today = dt.today().date()
  today_prefix = today.strftime("%Y%m%d")

  for f in folder_path.iterdir():
    if f.is_file() and not f.name.startswith(today_prefix):
      f.unlink()

  choropleth_files = set(folder_path.glob(f"{today_prefix}*.json"))
  required_files = {
    f"{today_prefix},choropleth_{unit}.json" for unit in ("municipality", "postal_area")
  }
  if choropleth_files != required_files:
    await choropleth_polys()

  ads_file = folder_path / f"{today_prefix},finn_ads.json"
  if not ads_file.exists():
    await finn_ads(5e6)


asyncio.run(update_geodata())
default_unit = "municipality"
today = dt.today().date()
today_prefix = today.strftime("%Y%m%d")

path = DB_DIR / "colorbar_values.json"
vmin, vmax = load_json(path)["county"]

colorscale = "Viridis"

style_handle = assign("""
function(feature, context) {
  const {min, max, colorscale, style, colorProp} = context.hideout
  const csc = chroma.scale(colorscale).domain([min, max])

  const value = feature.properties[colorProp]
  
  if (value === null) {
    return style
  }

  style.fillColor = csc(value)
  return style
}""")

ad_tooltip = assign("""
function(feature, layer, context){
  if (feature.properties.price_total === undefined) {
    return
  }
                    
  layer.bindTooltip(`
    <b>Total price:</b> ${feature.properties.price_total} NOK</br>
    <b>Ask price:</b> ${feature.properties.price_suggestion} NOK</br>
    <b>Sqm price:</b> ${feature.properties.sqm_price} NOK/m²</br>
    <b>Area:</b> ${feature.properties.area} m²</br>
    <b>Bedrooms:</b> ${feature.properties.bedrooms}
  `)
}
""")

ad_point = assign("""
function(feature, latlng, context){
  const {min, max, colorscale, style, colorProp} = context.hideout
  const csc = chroma.scale(colorscale).domain([min, max])

  const value = feature.properties[colorProp]

  if (value === undefined) {
    return L.circleMarker(latlng, style)
  }

  style.fillColor = csc(value)
  return L.circleMarker(latlng, style)
}""")

ad_cluster = assign("""
function(feature, latlng, index, context) {
  const {min, max, colorscale, style, colorProp} = context.hideout
  const csc = chroma.scale(colorscale).domain([min, max])
                    
  const leaves = index.getLeaves(feature.properties.cluster_id)
  let valueSum = 0
  for (let i = 0; i < leaves.length; ++i) {
    valueSum += leaves[i].properties[colorProp]
  }
  const valueMean = valueSum / leaves.length

  const scatterIcon = L.DivIcon.extend({
    createIcon: function(oldIcon) {
      let icon = L.DivIcon.prototype.createIcon.call(this, oldIcon);
      icon.style.backgroundColor = this.options.color;
      return icon;
    }
  })
  const icon = new scatterIcon({
    html: '<div style="background-color:white;"><span>' + feature.properties.point_count_abbreviated + '</span></div>',
    className: "marker-cluster",
    iconSize: L.point(40, 40),
    color: csc(valueMean)
  });
  return L.marker(latlng, {icon : icon})
}""")

info_box = html.Div(
  className="absolute bottom-5 left-5 bg-white rounded p-5 z-[999]",
  children=[html.H3("Real estate price level"), html.Div(id="div:realestate:info")],
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
        id="geojson:realestate:ads",
        url=f"/assets/geodata/{today_prefix},finn_ads.json",
        cluster=True,
        pointToLayer=ad_point,
        clusterToLayer=ad_cluster,
        onEachFeature=ad_tooltip,
        hideout=dict(
          min=vmin,
          max=vmax,
          colorscale=colorscale,
          style=dict(
            fillColor="black",
            fillOpacity=1,
            stroke=False,
            radius=10,
          ),
          colorProp="sqm_price",
        ),
      ),
      dl.GeoJSON(
        id="geojson:realestate:choropleth",
        url=f"/assets/geodata/{today_prefix},choropleth_{default_unit}.json",
        zoomToBoundsOnClick=True,
        style=style_handle,
        hoverStyle=arrow_function(dict(weight=2, color="white")),
        hideout=dict(
          min=vmin,
          max=vmax,
          colorscale="Viridis",
          style=dict(
            weight=1,
            opacity=1,
            color="black",
            fillColor="black",
            fillOpacity=0.3,
          ),
          colorProp="average_sqm_price",
        ),
      ),
      dl.Colorbar(
        colorscale=colorscale,
        width=20,
        height=200,
        min=vmin,
        max=vmax,
        unit="NOK/m²",
        position="topright",
      ),
      # dlx.categorical_colorbar(
      #  id="colorbar:realestate",
      #  categories=[f"{c:.2E}" for c in ctg],
      #  colorscale=colormap,
      #  unit="NOK/m²",
      #  width=500,
      #  height=10,
      #  position="bottomleft",
      # ),
    ],
  ),
]


@callback(
  Output("geojson:realestate:choropleth", "url"),
  # Output("geojson:realestate:choropleth", "hideout"),
  # Output("colorbar:realestate", "tickText"),
  # Output("colorbar:realestate", "colorscale"),
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

  url = f"/assets/geodata/{today_prefix},choropleth_{unit}.json"
  # hideout = make_choropleth_hideout(unit, "average_sqm_price", choropleth_style)
  # ctg = [f"{c:.2E}" for c in hideout["classes"]]

  return url  # hideout, ctg, hideout["colorscale"]


@callback(
  Output("div:realestate:info", "children"),
  Input("geojson:realestate:choropleth", "hoverData"),
)
def info_hover(feature: dict):
  return get_info(feature)
