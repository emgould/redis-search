import re


def normalize(name: str) -> str:
    """
    Normalize a name for comparison by:
    - Converting to lowercase
    - Removing special characters
    - Removing extra whitespace

    Args:
        name: The name to normalize

    Returns:
        Normalized name string
    """
    if not name:
        return ""

    # Convert to lowercase
    normalized = name.lower().strip()

    # Remove special characters (keep only letters, numbers, and spaces)
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)

    # Remove extra whitespace
    normalized = ' '.join(normalized.split())

    return normalized
