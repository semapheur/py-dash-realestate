import aiometer
from functools import partial

import geopandas as gpd
import pandas as pd
import httpx
from shapely.geometry import Point, Polygon
import topojson as tj

from src.const import HEADERS
from src.utils import fetch_json


def postal_area_polys() -> gpd.GeoDataFrame:
  url = (
    "https://raw.githubusercontent.com/"
    "ivanhjel/postnummer/master/postnummeromrader.geojson"
  )
  with httpx.Client() as client:
    response = client.get(url, headers=HEADERS)
    raw = response.text

  rename = {
    "kommune": "municipality",
    "postnummer": "postal_code",
    "poststedsnavn": "name",
  }
  gdf = gpd.read_file(raw, driver="GeoJSON", encoding="utf-8")
  gdf.rename(columns=rename, inplace=True)
  gdf.drop("cartodb_id", axis=1, inplace=True)
  return gdf


def get_municipalities_() -> pd.DataFrame:
  url = "https://ws.geonorge.no/kommuneinfo/v1/kommuner"
  parse = fetch_json(url)

  rename = {
    "kommunenavn": "name_local",
    "kommunenavnNorsk": "name",
    "kommunenummer": "id",
  }
  df = pd.DataFrame.from_records(parse)
  df.rename(columns=rename, inplace=True)
  return df


def get_municipalities(crs: int = 4258) -> pd.DataFrame:
  url = "https://ws.geonorge.no/kommuneinfo/v1/fylkerkommuner"
  params = {"utkoordsys": crs}
  parse = fetch_json(url, params=params)
  records: list[dict[str, str]] = []

  for counties in parse:
    for municipality in counties["kommuner"]:
      records.append(
        {
          "municipality_id": municipality.get("kommunenummer"),
          "municipality": municipality.get("kommunenavnNorsk"),
          "county_id": counties.get("fylkesnummer"),
          "county": counties.get("fylkesnavn"),
        }
      )

  df = pd.DataFrame.from_records(records)
  return df


def search_municipality(query: str) -> dict:
  url = "https://ws.geonorge.no/kommuneinfo/v1/sok"
  params = {
    "knavn": query,
  }
  return fetch_json(url, params=params)


def municipality_info(id: str) -> dict:
  url = f"https://ws.geonorge.no/kommuneinfo/v1/kommuner/{id}"
  return fetch_json(url)


def find_municipality(point: Point, crs: int = 4258) -> dict:
  url = "https://ws.geonorge.no/kommuneinfo/v1/punkt"
  params = {
    "ost": point.x,
    "nord": point.y,
    "koordsys": crs,
  }
  return fetch_json(url, params=params)


async def municipality_poly(id: str, crs: int = 4258, timeout: int = 30) -> Polygon:
  url = f"https://ws.geonorge.no/kommuneinfo/v1/kommuner/{id}/omrade"
  params = {"utkoordsys": crs}

  async with httpx.AsyncClient(timeout=timeout) as client:
    response = await client.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
      raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

    parse = response.json()

  poly = Polygon(parse["omrade"]["coordinates"][0][0])
  return poly


async def municipality_polys(
  tolerance: float = 0.0, crs: int = 4258
) -> gpd.GeoDataFrame:
  df = get_municipalities()

  tasks = [partial(municipality_poly, id, crs) for id in df["municipality_id"]]
  polys = await aiometer.run_all(tasks, max_per_second=10)

  gdf = gpd.GeoDataFrame(df, crs=crs, geometry=polys)
  if tolerance > 0:
    topo = tj.Topology(gdf, prequantize=False)
    gdf = topo.toposimplify(tolerance).to_gdf()

  return gdf


def get_counties():
  url = "https://ws.geonorge.no/kommuneinfo/v1/fylker"
  parse = fetch_json(url)

  rename = {
    "fylkesnavn": "name",
    "fylkesnummer": "id",
  }
  df = pd.DataFrame.from_records(parse)
  df.rename(columns=rename, inplace=True)
  return df


async def county_poly(id: str, crs: int = 4258, timeout: int = 30) -> Polygon:
  url = f"https://ws.geonorge.no/kommuneinfo/v1/fylker/{id}/omrade"
  params = {"utkoordsys": crs}
  async with httpx.AsyncClient(timeout=timeout) as client:
    response = await client.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
      raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

    parse = response.json()

  poly = Polygon(parse["omrade"]["coordinates"][0][0])
  return poly


async def county_polys(tolerance: float = 0.0, crs: int = 4258) -> gpd.GeoDataFrame:
  df = get_counties()

  tasks = [partial(county_poly, id, crs) for id in df["id"]]
  polys = await aiometer.run_all(tasks, max_per_second=10)

  gdf = gpd.GeoDataFrame(df, crs=crs, geometry=polys)
  if tolerance > 0:
    topo = tj.Topology(gdf, prequantize=False)
    gdf = topo.toposimplify(tolerance).to_gdf()

  return gdf
