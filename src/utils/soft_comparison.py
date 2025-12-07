"""
Soft comparison utility using Levenshtein distance for fuzzy string matching.

Provides normalized string comparison with configurable thresholds for matching.
"""

from utils.normalize import normalize


def _levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate the Levenshtein edit distance between two strings.

    Args:
        s1: First string
        s2: Second string

    Returns:
        The minimum number of single-character edits (insertions, deletions, substitutions)
        required to transform s1 into s2
    """
    # Create a matrix to store distances
    len1, len2 = len(s1), len(s2)

    # Initialize the matrix
    dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

    # Initialize first row and column
    for i in range(len1 + 1):
        dp[i][0] = i
    for j in range(len2 + 1):
        dp[0][j] = j

    # Fill the matrix using dynamic programming
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if s1[i - 1] == s2[j - 1]:
                # Characters match, no edit needed
                dp[i][j] = dp[i - 1][j - 1]
            else:
                # Take minimum of insert, delete, or substitute
                dp[i][j] = 1 + min(
                    dp[i - 1][j],  # deletion
                    dp[i][j - 1],  # insertion
                    dp[i - 1][j - 1],  # substitution
                )

    return dp[len1][len2]


def soft_compare(
    str1: str, str2: str, threshold_ratio: float = 0.3, min_threshold: int = 3
) -> tuple[bool, bool]:
    """
    Compare two strings using normalized comparison and Levenshtein distance.

    Args:
        str1: First string to compare
        str2: Second string to compare
        threshold_ratio: Ratio of string length to use for distance threshold (default: 0.3)
                         For example, 0.3 means 30% of the string length
        min_threshold: Minimum distance threshold for strings <= min_threshold length (default: 3)
                       For strings shorter than this, only exact matches are considered

    Returns:
        tuple[bool, bool]: (match, exact_match)
        - match: True if strings match within the distance threshold
        - exact_match: True if strings match exactly (case-insensitive, normalized)
    """
    # Normalize both strings
    normalized1 = normalize(str1)
    normalized2 = normalize(str2)

    # Check for exact match (case-insensitive, normalized)
    exact_match = normalized1 == normalized2

    # If exact match, return early
    if exact_match:
        return (True, True)

    # Calculate Levenshtein distance
    distance = _levenshtein_distance(normalized1, normalized2)

    # Calculate threshold based on the shorter string length
    # This prevents false positives with very different length strings
    shorter_length = min(len(normalized1), len(normalized2))

    # For very short strings, require exact match
    if shorter_length <= min_threshold:
        return (False, False)

    # Calculate threshold: max of min_threshold or ratio of string length
    threshold = max(min_threshold, int(shorter_length * threshold_ratio))

    # Check if distance is within threshold
    match = distance <= threshold

    return (match, False)
