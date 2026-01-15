"""Utility for extracting table names from API URLs."""

import re
from urllib.parse import urlparse

from src.config import get_settings


def extract_table_name(url: str, custom_name: str | None = None) -> str:
    """
    Extract or generate a valid PostgreSQL table name from an API URL.

    Args:
        url: The API URL to extract the table name from.
        custom_name: Optional custom table name to use instead of auto-deriving.

    Returns:
        A sanitized PostgreSQL table name with the configured prefix.

    Examples:
        >>> extract_table_name("https://api.example.com/users")
        'api_users'
        >>> extract_table_name("https://api.example.com/api/v3/posts")
        'api_posts'
        >>> extract_table_name("https://api.example.com/data", custom_name="my_table")
        'api_my_table'
    """
    settings = get_settings()
    prefix = settings.table_prefix

    if custom_name:
        name = custom_name
    else:
        name = _extract_name_from_url(url)

    # Sanitize the name for PostgreSQL
    sanitized = _sanitize_identifier(name)

    # Apply prefix (avoid double prefix)
    if sanitized.startswith(prefix):
        return sanitized

    return f"{prefix}{sanitized}"


def _extract_name_from_url(url: str) -> str:
    """
    Extract a meaningful name from the URL path.

    Tries to find the most specific path segment that represents the resource.

    Args:
        url: The URL to parse.

    Returns:
        The extracted name from the URL path.
    """
    parsed = urlparse(url)
    path = parsed.path.strip("/")

    if not path:
        return "data"

    # Split path into segments
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "data"

    # Filter out common non-resource segments
    ignored_segments = {"api", "v1", "v2", "v3", "v4", "rest", "public", "data"}

    # Find the last meaningful segment
    for segment in reversed(segments):
        # Skip version numbers and common prefixes
        if segment.lower() in ignored_segments:
            continue
        # Skip numeric segments (like IDs)
        if segment.isdigit():
            continue
        return segment

    # Fallback to last segment if nothing else works
    return segments[-1]


def _sanitize_identifier(name: str) -> str:
    """
    Sanitize a string to be a valid PostgreSQL identifier.

    Args:
        name: The name to sanitize.

    Returns:
        A valid PostgreSQL identifier.
    """
    # Convert to lowercase
    name = name.lower()

    # Replace hyphens and spaces with underscores
    name = name.replace("-", "_").replace(" ", "_")

    # Remove any characters that aren't alphanumeric or underscore
    name = re.sub(r"[^a-z0-9_]", "", name)

    # Ensure it doesn't start with a number
    if name and name[0].isdigit():
        name = f"t_{name}"

    # Ensure it's not empty
    if not name:
        name = "data"

    # Truncate to PostgreSQL's identifier limit (63 characters)
    return name[:63]
