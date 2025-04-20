import json
from pathlib import Path
import time

import httpx

from src.const import HEADERS


def load_json(path: str | Path) -> dict:
  with open(path, "r") as f:
    return json.load(f)


def update_json(path: str | Path, data: dict):
  if isinstance(path, str):
    path = Path(path)

  if path.suffix != ".json":
    path = path.with_suffix(".json")

  try:
    with open(path, "r") as f:
      file_data = json.load(f)

  except (FileNotFoundError, json.JSONDecodeError):
    file_data = {}

  file_data.update(data)

  with open(path, "w") as f:
    json.dump(file_data, f)


def minify_json(path: str | Path, new_name: str | None = None):
  if isinstance(path, str):
    path = Path(path)

  with open(path, "r") as f:
    data = json.load(f)

  if not new_name:
    new_path = path.with_name(f"{path.stem}_mini.json")
  else:
    new_path = path.with_name(new_name).with_suffix(".json")

  with open(new_path, "w") as f:
    json.dump(data, f, separators=(",", ":"))


def fetch_json(
  url: str,
  params: dict | None = None,
  headers: dict[str, str] | None = None,
  timeout: int = 10,
  retries: int = 3,
  backoff_factor: float = 0.5,
  retry_on_status_codes: tuple = (408, 429, 500, 502, 503, 504),
) -> dict:
  request_headers = headers or globals().get("HEADERS", {})

  retry_count = 0

  while retry_count < retries:
    try:
      with httpx.Client(timeout=timeout) as client:
        response = client.get(url, headers=request_headers, params=params)
        response.raise_for_status()

        try:
          return response.json()
        except ValueError as json_error:
          raise ValueError(
            f"Failed to parse JSON response: {json_error}. Response text: {response.text}"
          )

    except httpx.HTTPStatusError as e:
      last_exception: Exception = e
      status_code = e.response.status_code

      if status_code in retry_on_status_codes and retry_count < retries:
        sleep_time = backoff_factor * (2**retry_count)
        time.sleep(sleep_time)
        retry_count += 1
        continue
      else:
        message = f"API error: {status_code} - {e.response.text}"
        raise httpx.HTTPStatusError(
          message, request=e.request, response=e.response
        ) from e

    except httpx.RequestError as e:
      last_exception = e

      if retry_count < retries:
        sleep_time = backoff_factor * (2**retry_count)
        time.sleep(sleep_time)
        retry_count += 1
        continue
      else:
        message = f"Request error: {e}"
        raise httpx.RequestError(message, request=e.request) from e

    except Exception as e:
      last_exception = e
      raise Exception(f"Unexpected error: {str(e)}") from e
    break

  if last_exception:
    raise last_exception

  return {}
