import asyncio
from datetime import datetime as dt
from typing import TypedDict

from dash import callback, html, no_update, register_page, Input, Output, State
from dash_extensions.javascript import arrow_function, assign
import dash_ag_grid as dag
import dash_leaflet as dl
import geopandas as gpd

from src.const import DB_DIR, STATIC_DIR
from src.finn import choropleth_polys, finn_ads
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
    <b>Total price:</b> ${feature.properties.price_total.toLocaleString('en-US', { style: 'currency', currency: 'NOK' })}</br>
    <b>Ask price:</b> ${feature.properties.price_suggestion.toLocaleString('en-US', { style: 'currency', currency: 'NOK' })} NOK</br>
    <b>Sqm price:</b> ${feature.properties.sqm_price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} NOK/m<sup>2</sup></br>
    <b>Area:</b> ${feature.properties.area.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} m<sup>2</sup></br>
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
  className="absolute bottom-5 left-5 bg-white/50 backdrop-blur-sm rounded p-1 z-[999]",
  children=[html.H3("Real estate price level"), html.Div(id="div:realestate:info")],
)

column_defs = [
  {
    "field": "address",
    "headerName": "Link",
    "cellRenderer": "FinnLink",
    "tooltipField": "description",
  },
  {
    "field": "date_published",
    "headerName": "Published",
    "valueFormatter": {
      "function": "d3.timeFormat('%Y-%m-%d')(new Date(params.value * 1000))"
    },
  },
  {"field": "property_type", "headerName": "Property type"},
  {"field": "owner_type", "headerName": "Ownership"},
  {
    "field": "price_total",
    "headerName": "Total price",
    "valueFormatter": {"function": "d3.format('(,.2f')(params.value)"},
  },
  {
    "field": "price_suggestion",
    "headerName": "Ask price",
    "valueFormatter": {"function": "d3.format('(,.2f')(params.value)"},
  },
  {
    "field": "sqm_price",
    "headerName": "Price/sqm",
    "valueFormatter": {"function": "d3.format('(,.2f')(params.value)"},
  },
  {
    "field": "area",
    "headerName": "Area",
    "valueFormatter": {"function": "d3.format('(,.2f')(params.value)"},
  },
  {"field": "bedrooms", "headerName": "Bedrooms"},
]

data_path = STATIC_DIR / "geodata" / f"{today_prefix},finn_ads.json"
ad_data = gpd.read_file(data_path)
columns = [
  "address",
  "date_published",
  "property_type",
  "owner_type",
  "price_total",
  "price_suggestion",
  "sqm_price",
  "area",
  "bedrooms",
  "ad_id",
  "description",
]

layout = [
  dag.AgGrid(
    id="table:realestate",
    columnDefs=column_defs,
    rowData=ad_data[columns].to_dict("records"),
    defaultColDef=dict(filter=True),
    style=dict(height="100%"),
  ),
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
    ],
  ),
]


@callback(
  Output("geojson:realestate:choropleth", "url"),
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
  return url


@callback(
  Output("div:realestate:info", "children"),
  Input("geojson:realestate:choropleth", "hoverData"),
)
def info_hover(feature: dict):
  return get_info(feature)
