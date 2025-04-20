from contextlib import asynccontextmanager

import duckdb
import geopandas as gpd
import pandas as pd


@asynccontextmanager
async def duckdb_connection(db_path: str):
  con = None
  try:
    con = duckdb.connect(db_path)
    yield con
  finally:
    if con is not None:
      con.close()


async def df_to_duckdb(
  con: duckdb.DuckDBPyConnection,
  df: pd.DataFrame,
  table_name: str,
  overwrite: bool = False,
) -> None:
  try:
    con.register("df_view", df)

    if overwrite:
      con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view")

  except Exception as e:
    raise Exception(f"Failed to insert data into {table_name}: {e}")


async def gdf_to_duckdb(
  con: duckdb.DuckDBPyConnection,
  gdf: gpd.GeoDataFrame,
  table_name: str,
  overwrite: bool = False,
) -> None:
  try:
    gdf_copy = gdf.copy()
    gdf_copy["geometry"] = gdf_copy["geometry"].apply(lambda x: x.wkb)

    con.register("gdf_view", gdf_copy)

    if overwrite:
      con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM gdf_view")

  except Exception as e:
    raise Exception(f"Failed to insert data into {table_name}: {e}")
