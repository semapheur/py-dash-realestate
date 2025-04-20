from functools import partial
from typing import Literal, TypedDict

import aiometer
import geopandas as gpd
import httpx
import pandas as pd
from shapely.geometry import Point

from src.const import DB_DIR, STATIC_DIR, HEADERS
from src.geo import country_poly, hextiles, rect_poly
from src.geonorge import municipality_polys
from src.utils import update_json


VIRDI_PATH = DB_DIR / "hjemla.json"


class Virdi(TypedDict):
  geometry: Point
  municipality: str
  borough: str
  postal_code: int
  street_id: int
  address: str
  unit_type: str
  area: float
  estimated_price: float
  estimated_common_debt: float
  fixed_price: float
  common_debt: float
  asking_price: float


async def load_geodata(unit: str) -> gpd.GeoDataFrame:
  path = DB_DIR / f"nor_{unit}.json"
  if not path.exists():
    if unit == "municipality":
      gdf = await municipality_polys(0.001)
    elif unit == "postal_code":
      gdf = postal_code_polys()

    gdf.to_file(path, driver="GeoJSON", encoding="utf-8")

  else:
    gdf = gpd.read_file(path, driver="GeoJSON", encoding="utf-8")

  return gdf


async def fetch_virdi_data(params: dict, timeout: int = 10) -> dict:
  async with httpx.AsyncClient(timeout=timeout) as client:
    response = await client.get(
      "https://consumer-service.hjemla.no/public/market/address-list",
      headers=HEADERS,
      params=params,
    )

    if response.status_code != 200:
      raise Exception(f"Error fetching data: {response.status_code}")

    return response.json()


async def real_estate_price_data(
  size_range: tuple[int, int] = (30, 150),
  sw_coord: tuple[float, float] = (57.8, 4.3),
  ne_coord: tuple[float, float] = (71.2, 31.3),
  limit: int | None = None,
) -> gpd.GeoDataFrame:
  params = {
    "marketType": "currentmarket",
    "period": "12",
    "sizemin": "0",
    "sizemax": "500",
    "marketStates": "sold,forsale,considerselling",
    "unittypes": "apartment,semidetatchedhouse,house,serialhouse",
    "swLat": sw_coord[0],
    "swLng": sw_coord[1],
    "neLat": ne_coord[0],
    "neLng": ne_coord[1],
  }

  if limit is not None:
    params["limit"] = limit

  scrap: list[Virdi] = []

  tasks = []
  for size in range(size_range[0], size_range[1]):
    params["sizemin"] = str(size)
    params["sizemax"] = str(size)

    tasks.append(partial(fetch_virdi_data, params))

  parses = await aiometer.run_all(tasks, max_per_second=3)

  for parse in parses:
    if "response" not in parse:
      continue

    for street in parse["response"]:
      pnt = Point(street["coordinatesLng"], street["coordinatesLat"])
      for unit in street["units"]:
        scrap.append(
          Virdi(
            geometry=pnt,
            municipality=street["municipalityName"],
            borough=street["boroughName"],
            postal_code=int(street["postalCode"]),
            street_id=int(street["streetId"]),
            address=street["slug"],
            unit_type=unit["unitType"],
            area=size,
            estimated_price=unit["estimatedPrice"],
            estimated_common_debt=unit["estimateCommonDebt"],
            fixed_price=unit["fixedPrice"],
            common_debt=unit["commonDebt"],
            asking_price=unit["askingPrice"],
          )
        )

  gdf = gpd.GeoDataFrame(scrap, crs=4258)

  cols = ["estimated_price", "fixed_price", "asking_price"]
  gdf.dropna(how="all", subset=cols, inplace=True)

  gdf["price_per_area"] = gdf[cols].max(axis=1) / gdf["area"]

  return gdf


def load_price_data():
  if not VIRDI_PATH.exists():
    price = real_estate_price_data()
    price.to_file(VIRDI_PATH, driver="GeoJSON")
  else:
    price = gpd.read_file(VIRDI_PATH, encoding="utf-8")

  return price


def spatial_price_stats(price: gpd.GeoDataFrame, unit: str) -> pd.DataFrame:
  price = price[[unit, "price_per_area"]]
  stats = price.groupby([unit]).agg(
    price_per_area=("price_per_area", "mean"),
    price_per_area_std=("price_per_area", "std"),
  )
  return stats


def hex_choropleth(hex_sizes: list[float]):
  def norway_poly():
    path = DB_DIR / "dgi/no_mainland_3857.json"
    if not path.exists():
      p1 = Point(4.49, 57.95)
      p2 = Point(31.18, 71.21)

      mask = rect_poly(p1, p2)
      no_poly = country_poly("norway", save_path=path, mask=mask)
    else:
      no_poly = gpd.read_file(path)

    if no_poly.crs == 3857:
      no_poly.set_crs(4326, inplace=True)

    return no_poly.geometry[0]

  price_gdf = load_price_data()

  for hs in hex_sizes:
    path = DB_DIR / f"realestate_choro_hex{int(hs)}.json"
    if not path.exists():
      no_poly = norway_poly()
      hex_gdf = gpd.GeoDataFrame(hextiles(no_poly, 1e6), crs=4326)
      hex_gdf.set_crs(3857, inplace=True)
      hex_gdf.to_file(path, driver="GeoJSON")
    else:
      hex_gdf = gpd.read_file(path)

    rm_cols = {"price_per_area", "price_per_area_std"}
    hex_gdf = hex_gdf[hex_gdf.columns.difference(rm_cols)]

    hex_gdf.reset_index(name=f"hex{int(hs)}", index=True)
    price_gdf = price_gdf.sjoin(hex_gdf, how="left")
    stats_df = spatial_price_stats(price_gdf, f"hex{int(hs)}")
    hex_gdf = hex_gdf.join(stats_df, on=f"hex{int(hs)}")
    hex_gdf.to_file(path)


async def choropleth_polys(unit: Literal["municipality", "postal_code"]):
  price = load_price_data()

  df = spatial_price_stats(price, unit)
  gdf = await load_geodata(unit)
  gdf = gdf.join(df, on=unit)
  # gdf = gdf[['geometry', 'postal_code', 'price_per_area', 'price_per_area_std']]

  extrema = {unit: [df["price_per_area"].min(), df["price_per_area"].max()]}
  path = DB_DIR / "colorbar_values.json"
  update_json(path, extrema)

  path = STATIC_DIR / f"realestate_choro_{unit}.json"
  gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
