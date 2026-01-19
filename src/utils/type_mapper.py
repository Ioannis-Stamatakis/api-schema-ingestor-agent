"""Python to PostgreSQL type mapping utilities."""

from typing import Any

# Mapping of Python types to PostgreSQL types
PYTHON_TO_POSTGRES: dict[type, str] = {
    str: "TEXT",
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    list: "JSONB",
    dict: "JSONB",
    type(None): "TEXT",
}

# Type precedence for conflict resolution (higher index = more permissive)
TYPE_PRECEDENCE = ["BOOLEAN", "BIGINT", "DOUBLE PRECISION", "TEXT", "JSONB"]


def flatten_key(path: list[str]) -> str:
    """
    Convert a path of keys into a flattened column name.

    Args:
        path: List of nested keys, e.g., ['user', 'address', 'city']

    Returns:
        Flattened column name, e.g., 'user_address_city'
    """
    return "_".join(path)


def flatten_record(
    record: dict[str, Any],
    depth: int,
    current_depth: int = 0,
    prefix: list[str] | None = None,
) -> dict[str, Any]:
    """
    Recursively flatten nested dictionaries to the specified depth.

    Arrays are never flattened - they stay as-is (will become JSONB).

    Args:
        record: The dictionary to flatten.
        depth: Maximum depth to flatten (1 = flatten one level).
        current_depth: Current recursion depth (internal use).
        prefix: Current key path prefix (internal use).

    Returns:
        Flattened dictionary with underscored keys.
    """
    if prefix is None:
        prefix = []

    result: dict[str, Any] = {}

    for key, value in record.items():
        current_path = prefix + [key]

        if isinstance(value, dict) and current_depth < depth:
            # Recurse into nested dict
            nested_flattened = flatten_record(
                value, depth, current_depth + 1, current_path
            )
            result.update(nested_flattened)
        else:
            # Either not a dict, or we've reached max depth
            flat_key = flatten_key(current_path) if prefix else key
            result[flat_key] = value

    return result


def detect_column_collision(
    records: list[dict[str, Any]], depth: int
) -> tuple[dict[str, str], list[str]]:
    """
    Detect naming collisions when flattening records.

    A collision occurs when two different source paths produce the same
    flattened column name (e.g., 'user_name' key + 'user.name' nested).

    Args:
        records: List of records to check.
        depth: Flattening depth to use.

    Returns:
        Tuple of (column_sources, warnings):
        - column_sources: Maps flattened column names to their first source path
        - warnings: List of collision warning messages
    """
    column_sources: dict[str, str] = {}
    warnings: list[str] = []

    for record in records:
        flattened = flatten_record(record, depth)

        # Track original paths for collision detection
        def get_paths(
            rec: dict[str, Any],
            depth: int,
            current_depth: int = 0,
            prefix: list[str] | None = None,
        ) -> dict[str, str]:
            """Get mapping of flattened keys to their original paths."""
            if prefix is None:
                prefix = []
            paths: dict[str, str] = {}

            for key, value in rec.items():
                current_path = prefix + [key]

                if isinstance(value, dict) and current_depth < depth:
                    nested_paths = get_paths(
                        value, depth, current_depth + 1, current_path
                    )
                    paths.update(nested_paths)
                else:
                    flat_key = flatten_key(current_path) if prefix else key
                    paths[flat_key] = ".".join(current_path)

            return paths

        paths = get_paths(record, depth)

        for flat_key, source_path in paths.items():
            if flat_key in column_sources:
                if column_sources[flat_key] != source_path:
                    warning = (
                        f"Column name collision: '{flat_key}' from both "
                        f"'{column_sources[flat_key]}' and '{source_path}' "
                        f"(using first occurrence)"
                    )
                    if warning not in warnings:
                        warnings.append(warning)
            else:
                column_sources[flat_key] = source_path

    return column_sources, warnings


def resolve_type_conflict(type1: str, type2: str) -> str:
    """
    Resolve a type conflict by picking the more permissive type.

    Precedence (most to least permissive): JSONB > TEXT > DOUBLE PRECISION > BIGINT > BOOLEAN

    Args:
        type1: First PostgreSQL type.
        type2: Second PostgreSQL type.

    Returns:
        The more permissive type.
    """
    if type1 == type2:
        return type1

    idx1 = TYPE_PRECEDENCE.index(type1) if type1 in TYPE_PRECEDENCE else -1
    idx2 = TYPE_PRECEDENCE.index(type2) if type2 in TYPE_PRECEDENCE else -1

    return type1 if idx1 >= idx2 else type2


def infer_postgres_type(value: Any) -> str:
    """
    Infer the PostgreSQL type from a Python value.

    Args:
        value: The Python value to analyze.

    Returns:
        The corresponding PostgreSQL type as a string.
    """
    if value is None:
        return "TEXT"

    value_type = type(value)

    # Direct type match
    if value_type in PYTHON_TO_POSTGRES:
        return PYTHON_TO_POSTGRES[value_type]

    # Handle nested structures as JSONB
    if isinstance(value, (list, dict)):
        return "JSONB"

    # Fallback to TEXT for unknown types
    return "TEXT"


def infer_column_types(
    records: list[dict[str, Any]],
    flatten: bool = False,
    depth: int = 1,
) -> tuple[dict[str, str], list[str]]:
    """
    Infer PostgreSQL column types from a list of records.

    Analyzes all records to determine the most appropriate type for each column,
    handling cases where values might be None in some records.

    Args:
        records: List of dictionaries representing data records.
        flatten: If True, flatten nested dicts into separate columns.
        depth: Maximum depth to flatten (only used if flatten=True).

    Returns:
        Tuple of (column_types, warnings):
        - column_types: Dictionary mapping column names to PostgreSQL types.
        - warnings: List of warning messages (e.g., type conflicts, collisions).
    """
    if not records:
        return {}, []

    warnings: list[str] = []

    # Pre-flatten records if flatten mode is enabled
    if flatten:
        flattened_records = [flatten_record(r, depth) for r in records]
        _, collision_warnings = detect_column_collision(records, depth)
        warnings.extend(collision_warnings)
    else:
        flattened_records = records

    column_types: dict[str, str] = {}

    # Analyze all records to infer types
    for record in flattened_records:
        for key, value in record.items():
            inferred = infer_postgres_type(value) if value is not None else None

            if key not in column_types:
                # First non-None value determines the type
                if inferred is not None:
                    column_types[key] = inferred
            elif inferred is not None and column_types[key] != inferred:
                # Type conflict - resolve by picking more permissive type
                old_type = column_types[key]
                new_type = resolve_type_conflict(old_type, inferred)
                if new_type != old_type:
                    warnings.append(
                        f"Type conflict for column '{key}': {old_type} vs {inferred}, "
                        f"using {new_type}"
                    )
                    column_types[key] = new_type

    # Ensure all columns have a type (default to TEXT if all values were None)
    all_columns: set[str] = set()
    for record in flattened_records:
        all_columns.update(record.keys())

    for col in all_columns:
        if col not in column_types:
            column_types[col] = "TEXT"

    return column_types, warnings
