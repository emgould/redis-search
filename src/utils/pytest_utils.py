"""
Pytest utilities for testing.

This module provides utilities for pytest tests, including snapshot creation
for visual inspection of test results.

Usage:
    from utils.pytest_utils import write_snapshot

    # In a test function (flat structure - default)
    async def test_my_function(self):
        result = await some_async_call()
        write_snapshot(result, "my_result.json")
        # Creates: snapshots_inspection/my_result.json

    # With hierarchical structure (optional)
    write_snapshot(result, "my_result.json", hierarchical=True)
    # Creates: snapshots_inspection/TestClassName/test_my_function/my_result.json
"""

import inspect
import json
from pathlib import Path
from typing import Any

from utils.cache import EnhancedJSONEncoder


def write_snapshot(data: Any, filename: str | None = None, hierarchical: bool = False) -> Path:
    """Write JSON data to a snapshot file for visual inspection.

    The snapshot path is automatically determined based on the calling test function.
    Uses 'snapshots_inspection' directory to avoid conflicts with pytest-snapshots.

    By default, uses a flat structure (NO subdirectories):
    - All files go directly in: snapshots_inspection/{filename}

    With hierarchical=True, creates subdirectories for each test function:
    - For test methods in a class: snapshots_inspection/{class_name}/{test_method}/{filename}
    - For standalone test functions: snapshots_inspection/{test_function}/{filename}

    The function automatically finds the tests directory by walking up from the
    calling test file until it finds a directory named "tests".

    String Handling:
    - If data is a string, the function attempts to parse it as JSON
    - If parsing succeeds, the parsed JSON is written with proper formatting
    - If parsing fails, the string is written as-is

    Args:
        data: The data to serialize to JSON. Can be:
              - Any JSON-serializable object (dict, list, etc.)
              - A JSON string (will be parsed and formatted)
              - A plain string (will be written as-is if not valid JSON)
        filename: Name of the snapshot file. If None, defaults to "result.json"
        hierarchical: If True, creates subdirectories for each test function.
                     Defaults to False (flat structure).

    Returns:
        Path to the created snapshot file

    Example:
        ```python
        async def test_get_trending_news(self):
            result = await get_trending_news_async()
            # Flat structure (default)
            write_snapshot(result, "trending_news_result.json")
            # Creates: snapshots_inspection/trending_news_result.json

            # Hierarchical structure
            write_snapshot(result, "trending_news_result.json", hierarchical=True)
            # Creates: snapshots_inspection/testhandlers/test_get_trending_news/trending_news_result.json

            # String handling - JSON string will be parsed and formatted
            json_string = '{"key": "value"}'
            write_snapshot(json_string, "formatted.json")
            # Creates properly formatted JSON file
        ```
    """
    # Ensure hierarchical is a boolean (defensive check)
    if not isinstance(hierarchical, bool):
        raise TypeError(f"hierarchical must be a boolean, got {type(hierarchical).__name__}")

    # Get the calling frame (skip this function and the caller's immediate frame)
    frame = inspect.currentframe()
    if frame is None:
        raise RuntimeError("Could not get current frame")

    # Go up two frames: skip write_snapshot itself and the immediate caller
    caller_frame = frame.f_back
    if caller_frame is None:
        raise RuntimeError("Could not get caller frame")

    # Get the test function/class info
    test_function_name = caller_frame.f_code.co_name
    test_class_name = None

    # Check if we're in a test class by looking at the 'self' variable
    if "self" in caller_frame.f_locals:
        self_obj = caller_frame.f_locals["self"]
        test_class_name = self_obj.__class__.__name__

    # Find the tests directory by walking up from the calling file
    caller_file = Path(caller_frame.f_code.co_filename)
    tests_dir = None

    # Walk up the directory tree to find the "tests" directory
    current_dir = caller_file.parent
    while current_dir != current_dir.parent:  # Stop at filesystem root (parent == self at root)
        if current_dir.name == "tests":
            tests_dir = current_dir
            break
        current_dir = current_dir.parent

    if tests_dir is None:
        raise RuntimeError(
            f"Could not find 'tests' directory in path hierarchy from {caller_file}. "
            "Ensure the test file is within a 'tests' directory."
        )

    # Build snapshot path
    # Use 'snapshots_inspection' to avoid conflicts with pytest-snapshots which uses 'snapshots'
    if hierarchical:
        # Hierarchical structure: create subdirectories for each test function
        if test_class_name:
            # For test methods: snapshots_inspection/{class_name}/{test_method}/
            snapshot_dir = (
                tests_dir / "snapshots_inspection" / test_class_name.lower() / test_function_name
            )
        else:
            # For standalone functions: snapshots_inspection/{test_function}/
            snapshot_dir = tests_dir / "snapshots_inspection" / test_function_name
    else:
        # Flat structure: NO subdirectories at all - files go directly in snapshots_inspection/
        snapshot_dir = tests_dir / "snapshots_inspection"

    # Ensure the directory exists, creating parent directories if needed
    # Create the directory first, then resolve to normalize the path
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    # Resolve the path after creation to normalize it (resolve requires path to exist)
    snapshot_dir = snapshot_dir.resolve()

    # Verify the directory was created correctly (not as a file)
    if not snapshot_dir.is_dir():
        raise RuntimeError(f"Snapshot directory path exists but is not a directory: {snapshot_dir}")

    # Defensive check: ensure NO subdirectories are created when hierarchical=False
    if not hierarchical:
        # The path should end with 'snapshots_inspection' and nothing more
        expected_parts = len((tests_dir / "snapshots_inspection").parts)
        actual_parts = len(snapshot_dir.parts)
        if actual_parts > expected_parts:
            raise RuntimeError(
                f"BUG: Flat structure created subdirectories. Expected {expected_parts} path parts, "
                f"got {actual_parts}. Path: {snapshot_dir}"
            )

    # Determine filename
    if filename is None:
        filename = "result.json"
    elif not filename.endswith(".json"):
        filename = f"{filename}.json"

    # Write JSON file with nice formatting
    snapshot_path: Path = snapshot_dir / filename

    with open(snapshot_path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            # If data is a string, try to parse it as JSON first
            try:
                parsed_data = json.loads(data)
                # If successful, write the parsed data with formatting
                json.dump(
                    parsed_data,
                    f,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                    cls=EnhancedJSONEncoder,
                )
            except (json.JSONDecodeError, TypeError):
                # If it's not valid JSON, write it as-is
                f.write(data)
        else:
            # For non-string data, serialize it directly
            json.dump(
                data, f, indent=2, sort_keys=True, ensure_ascii=False, cls=EnhancedJSONEncoder
            )

    return snapshot_path
