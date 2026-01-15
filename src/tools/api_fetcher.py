"""API Fetcher tool for retrieving JSON data from public APIs."""

import json
from typing import Any

import requests

from src.config import get_settings


def fetch_api_data(url: str) -> dict[str, Any]:
    """
    Fetch JSON data from a public API URL.

    This tool makes an HTTP GET request to the specified URL and returns
    the JSON response along with metadata about the request.

    Args:
        url: The public API URL to fetch data from.

    Returns:
        A dictionary containing:
        - data: The JSON response data (list or dict)
        - status_code: HTTP status code
        - record_count: Number of records if data is a list
        - is_array: Whether the response is an array of objects
        - error: Error message if the request failed (None if successful)
    """
    settings = get_settings()

    try:
        response = requests.get(
            url,
            timeout=settings.request_timeout,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()

        data = response.json()

        # Determine if response is array or single object
        is_array = isinstance(data, list)
        record_count = len(data) if is_array else 1

        return {
            "data": data,
            "status_code": response.status_code,
            "record_count": record_count,
            "is_array": is_array,
            "error": None,
        }

    except requests.exceptions.Timeout:
        return {
            "data": None,
            "status_code": 408,
            "record_count": 0,
            "is_array": False,
            "error": f"Request timed out after {settings.request_timeout} seconds",
        }
    except requests.exceptions.HTTPError as e:
        return {
            "data": None,
            "status_code": e.response.status_code if e.response else 500,
            "record_count": 0,
            "is_array": False,
            "error": f"HTTP error: {str(e)}",
        }
    except requests.exceptions.RequestException as e:
        return {
            "data": None,
            "status_code": 500,
            "record_count": 0,
            "is_array": False,
            "error": f"Request failed: {str(e)}",
        }
    except json.JSONDecodeError as e:
        return {
            "data": None,
            "status_code": 200,
            "record_count": 0,
            "is_array": False,
            "error": f"Invalid JSON response: {str(e)}",
        }
