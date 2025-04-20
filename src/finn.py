from typing import Literal, TypedDict
from dateutil.relativedelta import relativedelta
from functools import partial
from pathlib import Path

import duckdb
import geopandas as gpd
import httpx
import numpy as np
import pandas as pd
from shapely import wkb
from shapely.geometry import Point

from src.const import DB_DIR, HEADERS, STATIC_DIR
from src.duck import duckdb_connection, gdf_to_duckdb
from src.geonorge import county_polys, municipality_polys, postal_area_polys
from src.virdi import choropleth_polys, load_geodata
from src.utils import fetch_json, update_json

area_types = {
  1024: "country",
  128: "county",
  16: "municipality",
  10: "borough",
  6: "borough_oslo",
}


class AreaInfo(TypedDict):
  name: str
  type: Literal["country", "county", "municipality", "borough"]


class PriceInfo(TypedDict, total=False):
  area_code: int
  sqm_price: float
  sold_units: int


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
  return statistics


def finn_areas():
  parse = fetch_finn_statistics()
  areas = pd.DataFrame.from_records(parse["flattenedAreas"])
  rename = {
    "area_id": "area_id",
    "description": "name",
    "parent_area_id": "parent_area_id",
  }
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
          county_municipalities_gdf[["municipality", "geometry"]], how="left", on="name"
        )
      )

    all_municipalities_df = pd.concat(municipality_dfs, axis=0)
    return gpd.GeoDataFrame(
      all_municipalities_df, geometry="geometry", crs=municipalities_gdf.crs
    )

  areas = finn_areas()

  async with duckdb_connection("data/geodata_no.db") as con:
    counties = await process_counties(areas, tolerance)
    await gdf_to_duckdb(con, counties, "counties", True)

    municipalities = await process_municipalities(areas, counties, tolerance)
    await gdf_to_duckdb(con, municipalities, "municipalities", True)

    postal_areas = postal_area_polys()
    await gdf_to_duckdb(con, postal_areas, "postal_areas", True)


async def choropleths() -> None:
  data = finn_statistics()

  units = ("county", "municipality", "postal_area")
  for unit in units:
    async with duckdb_connection("data/geodata_no.db") as con:
      query = f"""
        SELECT 
          area_id, name,
          ST_GeomFromWKB(geometry) AS geom_wkb,
        FROM '{unit}'
      """
      df = con.execute(query).fetchdf()

    unit_df = data.loc[data["area_id"].isin(df["area_id"]), :]
    unit_df = unit_df.merge(df, how="left", on="area_id")
    unit_df["geometry"] = unit_df["geom_wkb"].apply(wkb.loads)
    unit_gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=4258)

    extrema = {
      unit: [unit_df["average_sqm_price"].min(), df["average_sqm_price"].max()]
    }
    path = DB_DIR / "colorbar_values.json"
    update_json(path, extrema)

    path = STATIC_DIR / f"choropleth_{unit}.json"
    unit_gdf.to_file(path, driver="GeoJSON", encoding="utf-8")


def finn_sales():
  def parse_json(docs: list[dict]):
    scrap = []
    for doc in docs:
      lat = doc["coordinates"]["lat"]
      lon = doc["coordinates"]["lon"]

      if int(lat) == 0 or int(lon) == 0:
        continue

      pnt = Point(lon, lat)

      area: dict = doc.get("area_range", doc.get("area"))
      area = area.get("size_from", area.get("size"))

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

      # Municipality
      # mun = doc['location'].split(', ')[-1]
      # if mun not in setMun:
      #
      #    if mun not in dctMun:
      #        temp = gn.findMunicipality(pnt)['kommunenummer']
      #        dctMun[mun] = temp
      #        mun = temp
      #    else:
      #        mun = dctMun[mun]

      scrap.append(
        {
          "id": doc["ad_id"],
          "time_published": doc["timestamp"],
          "geometry": pnt,
          "address": doc["location"],
          #'municipality': mun,
          "price_total": price["price_total"],
          "price_ask": price["price_suggestion"],
          "shared_cost": shared_cost,
          "area": doc["area_range"]["size_from"],
          "bedrooms": beds,
          "property_type": doc["property_type_description"],
          "owner_type": doc["owner_type_description"],
          "link": doc["ad_link"],
        }
      )
    return scrap

  def iterate_pages(scrap, params, startPage):
    for p in range(startPage, 51):
      params[-1] = ("page", str(p))

      with httpx.Client() as client:
        rs = client.get(
          "https://www.finn.no/api/search-qf", headers=HEADERS, params=params
        )
        parse: dict = rs.json()

      if "docs" not in parse["docs"]:
        continue

      scrap.extend(parse_json(parse["docs"]))

    if parse["docs"]:
      last: dict = parse["docs"][-1]
      price_to = last.get("price_suggestion", last.get("price_range_suggestion"))
      price_to = price_to.get("amount", price_to.get("amount_from"))

    else:
      priceTo = 0

    return scrap, priceTo

  params = {
    "searchkey": "SEARCH_ID_REALESTATE_HOMES",
    "lifecycle": "1",
    "property_type": ["1", "2", "3", "4"],
    "sort": "PRICE_ASKING_DESC",
    "price_to": "",
    "page": "1",
  }

  with httpx.Client() as client:
    rs = client.get("https://www.finn.no/api/search-qf", headers=HEADERS, params=params)
    parse = rs.json()

  nUnits = parse["metadata"]["result_size"]["match_count"]

  scrap = []
  scrap.extend(parse_json(parse["docs"]))

  scrap, price_to = iterate_pages(scrap, params, 2)

  while (price_to > 0) and (len(scrap) <= nUnits):
    params["price_to"] = str(price_to)
    scrap, price_to = iterate_pages(scrap, params, 1)

  gdf = gpd.GeoDataFrame(scrap, crs=4258)
  gdf.drop_duplicates(inplace=True)
  gdf["priceArea"] = gdf["priceTotal"] / gdf["area"]

  # Additional data
  for scope in ("municipality", "postal_code"):
    path = Path.cwd() / "data" / "dgi" / f"virdi_{scope}.json"
    parser = partial(choropleth_polys, scope)
    choro_polys = load_geodata(parser, path, relativedelta(months=6))

    gdf = gdf.sjoin(
      choro_polys[["geometry", scope, f"price_{scope}"]], how="left", predicate="within"
    )
    gdf.drop("index_right", axis=1, inplace=True)

    # Price delta
    gdf[f"delta_{scope}"] = gdf["price_area"] - gdf[f"price_{scope}"]
    gdf.drop(f"price_{scope}", axis=1, inplace=True)

  return gdf
