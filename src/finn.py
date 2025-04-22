import aiometer
from datetime import datetime as dt
from enum import Enum
from functools import partial
from typing import Literal, TypedDict

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkb
from shapely.geometry import Point

from src.const import DB_DIR, STATIC_DIR
from src.duck import duckdb_connection, gdf_to_duckdb
from src.geonorge import county_polys, municipality_polys, postal_area_polys
from src.utils import fetch_json, fetch_json_async, update_json

type BBox = tuple[float, float, float, float]


class AreaTypes(Enum):
  country = 1024
  county = 128
  municipality = 16
  borough = 10
  borough_oslo = 6
  postal_area = 4


class AreaInfo(TypedDict):
  name: str
  type: Literal["country", "county", "municipality", "borough"]


class PriceInfo(TypedDict, total=False):
  area_code: int
  sqm_price: float
  sold_units: int


class RealEstateInfo(TypedDict):
  ad_id: int
  geometry: Point
  time_published: int
  property_type: str
  owner_type: str
  price_total: float
  price_suggestion: float
  shared_cost: float
  area: float
  bedrooms: int
  description: str
  address: str


def fetch_finn_statistics() -> dict:
  url = "https://www.finn.no/realestate/boligmarkedet/"
  params = {
    "_data": "routes/realestate.boligmarkedet",
  }
  return fetch_json(url, params=params)


def finn_statistics() -> pd.DataFrame:
  parse = fetch_finn_statistics()
  statistics = pd.DataFrame.from_records(parse["sqmPrice"])
  rename = {
    "postalOrAreaCode": "area_id",
    "avgSqmPrice": "average_sqm_price",
    "soldAds": "sold_units",
  }
  statistics.rename(columns=rename, inplace=True)
  statistics["area_id"] = statistics["area_id"].astype(int)
  statistics["average_sqm_price"] = statistics["average_sqm_price"].astype(float)

  areas = pd.DataFrame.from_records(parse["flattenedAreas"])
  statistics = statistics.merge(
    areas[["area_id", "area_type"]], how="left", on="area_id"
  )
  statistics["area_type"] = statistics["area_type"].fillna(4)

  return statistics


def finn_areas():
  parse = fetch_finn_statistics()
  areas = pd.DataFrame.from_records(parse["flattenedAreas"])
  rename = {"description": "name"}
  areas.rename(columns=rename, inplace=True)

  values = {
    "Frogn - Drøbak": "Frogn",
    "Våler (Østfold)": "Våler",
    "Våler (Innlandet)": "Våler",
    "Os (Innlandet)": "Os",
    "Herøy (M.R.)": "Herøy",
    "Sande (M.R.)": "Sande",
    "Bø (Nordland)": "Bø",
    "Herøy (Nordland)": "Herøy",
  }
  for key, value in values.items():
    areas.loc[areas["name"] == key, "name"] = value

  columns = ["area_id", "name", "area_type", "parent_area_id", "bbox"]
  return areas[columns]


async def area_polys(tolerance: float = 0.0):
  async def process_counties(
    areas_df: pd.DataFrame,
    tolerance: float,
  ) -> gpd.GeoDataFrame:
    counties_df = areas_df[areas_df["area_type"] == 128].copy()
    counties_gdf = await county_polys(tolerance=tolerance)
    merged_df = counties_df.merge(
      counties_gdf[["name", "geometry"]], how="left", on="name"
    )
    return gpd.GeoDataFrame(merged_df, geometry="geometry", crs=counties_gdf.crs)

  async def process_municipalities(
    areas_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    tolerance: float,
  ) -> gpd.GeoDataFrame:
    municipalities_df = areas_df[areas_df["area_type"] == 16].copy()
    municipalities_gdf = await municipality_polys(tolerance)
    municipalities_gdf.rename({"municipality": "name"}, axis=1, inplace=True)

    municipality_dfs: list[pd.DataFrame] = []

    for county_id, county_name in zip(counties_df["area_id"], counties_df["name"]):
      county_municipalities_df = municipalities_df.loc[
        municipalities_df["parent_area_id"] == county_id, :
      ]
      county_municipalities_gdf = municipalities_gdf.loc[
        municipalities_gdf["county"] == county_name, :
      ]

      municipality_dfs.append(
        county_municipalities_df.merge(
          county_municipalities_gdf[["name", "geometry"]], how="left", on="name"
        )
      )

    all_municipalities_df = pd.concat(municipality_dfs, axis=0)
    return gpd.GeoDataFrame(
      all_municipalities_df, geometry="geometry", crs=municipalities_gdf.crs
    )

  areas = finn_areas()

  async with duckdb_connection("data/geodata_no.db") as con:
    counties = await process_counties(areas, tolerance)
    await gdf_to_duckdb(con, counties, "county", True)

    municipalities = await process_municipalities(areas, counties, tolerance)
    await gdf_to_duckdb(con, municipalities, "municipality", True)

    postal_areas = postal_area_polys()
    rename = {
      "postal_code": "area_id",
      "postal_area": "name",
    }
    postal_areas.rename(rename, axis=1, inplace=True)
    await gdf_to_duckdb(con, postal_areas, "postal_area", True)


async def choropleth_polys() -> None:
  data = finn_statistics()

  units = ("county", "municipality", "postal_area")
  async with duckdb_connection("data/geodata_no.db") as con:
    extrema = {}
    for unit in units:
      query = f"SELECT area_id, name, geometry FROM '{unit}'"
      df = con.execute(query).fetchdf()
      unit_df = data.loc[data["area_type"] == AreaTypes[unit].value, :].copy()
      unit_df = unit_df.merge(df, how="left", on="area_id")
      unit_df.drop(["area_id", "area_type"], axis=1, inplace=True)
      unit_df.dropna(inplace=True)

      unit_df["geometry"] = unit_df["geometry"].apply(lambda x: wkb.loads(bytes(x)))
      unit_gdf = gpd.GeoDataFrame(unit_df, geometry="geometry", crs=4258)

      extrema[unit] = [
        unit_df["average_sqm_price"].min(),
        unit_df["average_sqm_price"].max(),
      ]

      today = dt.today().date().strftime("%Y%m%d")
      path = STATIC_DIR / "geodata" / f"{today},choropleth_{unit}.json"
      unit_gdf.to_file(path, driver="GeoJSON", encoding="utf-8")

  path = DB_DIR / "colorbar_values.json"
  update_json(path, extrema)


def finn_map_ads(bbox: BBox = (4.3, 57.8, 31.3, 71.2), rows: int = 300):
  url = "https://www.finn.no/map/podium-resource/content/api/map/realestate/SEARCH_ID_REALESTATE_HOMES"
  params = {"bbox": ",".join(map(str, bbox)), "rows": str(rows)}

  return fetch_json(url, params=params)


async def finn_ads(upper_price: float | None = None):
  async def fetch_ads(page: int, price_to: float | None = None):
    url = "https://www.finn.no/realestate/homes/search.html"

    params = {
      "_data": "routes/realestate+/_search+/$subvertical.search[.html]",
      "sort": "PRICE_DESC",
      "property_type": ["1", "2", "3", "4"],
      "page": str(page),
    }
    if price_to:
      params["price_collective_to"] = str(int(price_to))

    return await fetch_json_async(url, params=params)

  async def iterate_pages(price_to: float | None = None):
    parse = await fetch_ads(1)

    pages = parse["results"]["metadata"]["paging"]["last"]
    if pages != 50:
      print(pages)
    records = parse_json(parse["results"]["docs"])

    tasks = [partial(fetch_ads, page, price_to) for page in range(2, pages + 1)]
    parses = await aiometer.run_all(tasks, max_per_second=10)

    for parse in parses:
      records.extend(parse_json(parse["results"]["docs"]))

    return records

  def parse_json(docs: list[dict]):
    records: list[RealEstateInfo] = []
    for doc in docs:
      lat = doc["coordinates"]["lat"]
      lon = doc["coordinates"]["lon"]

      if int(lat) == 0 or int(lon) == 0:
        continue

      point = Point(lon, lat)

      area_: dict = doc.get("area_range", doc.get("area"))
      area: float = area_.get("size_from", area_.get("size"))

      if area == 0:
        continue

      price = {}
      for sfx in ["total", "suggestion"]:
        temp = doc.get(f"price_{sfx}", doc.get(f"price_range_{sfx}"))
        price[f"price_{sfx}"] = temp.get("amount", temp.get("amount_from"))

      if 0.0 in price.values():
        continue

      shared_cost = doc.get("price_shared_cost", np.nan)
      if isinstance(shared_cost, dict):
        shared_cost = shared_cost.get("amount")

      beds = doc.get("number_of_bedrooms", doc.get("bedrooms_range"))
      if isinstance(beds, dict):
        beds = beds.get("start")

      records.append(
        RealEstateInfo(
          ad_id=doc["ad_id"],
          geometry=point,
          time_published=doc["timestamp"],
          property_type=doc["property_type_description"],
          owner_type=doc["owner_type_description"],
          price_total=price["price_total"],
          price_suggestion=price["price_suggestion"],
          shared_cost=shared_cost,
          area=area,
          bedrooms=beds,
          description=doc["heading"],
          address=doc["location"],
        )
      )

    return records

  parse = await fetch_ads(1, upper_price)
  total_ads = parse["results"]["metadata"]["result_size"]["match_count"]

  pages = parse["results"]["metadata"]["paging"]["last"]
  records = parse_json(parse["results"]["docs"])

  tasks = [partial(fetch_ads, page, upper_price) for page in range(2, pages + 1)]
  parses = await aiometer.run_all(tasks, max_per_second=10)

  for parse in parses:
    records.extend(parse_json(parse["results"]["docs"]))

  price_to = records[-1]["price_total"]

  while price_to > 0.0 and len(records) < total_ads:
    print(f"Price: {price_to}|Ads: {len(records)}")
    records_ = await iterate_pages(price_to)
    records.extend(records_)
    lower_price = records_[-1]["price_total"]
    if lower_price >= price_to:
      break
    price_to = lower_price

  df = pd.DataFrame.from_records(records)
  df.drop_duplicates(inplace=True)
  df["sqm_price"] = df["price_total"] / df["area"]

  extrema = {"ad": [df["sqm_price"].min(), df["sqm_price"].max()]}
  path = DB_DIR / "colorbar_values.json"
  update_json(path, extrema)

  gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=4258)

  today = dt.today().date().strftime("%Y%m%d")
  path = STATIC_DIR / "geodata" / f"{today},finn_ads.json"
  gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
