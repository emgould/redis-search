"""
Soft comparison utility using Levenshtein distance for fuzzy string matching.

Provides normalized string comparison with configurable thresholds for matching.
Also includes autocomplete-specific prefix matching for typeahead functionality.
"""

from core.search_queries import STOPWORDS
from utils.normalize import normalize


def is_autocomplete_match(query: str, name: str) -> bool:
    """
    Check if name is a valid autocomplete match for the query.

    For autocomplete/typeahead, we need prefix matching where:
    - Each complete word in the query must match a word in the name
    - The last word in the query can be a prefix of a word in the name
    - Once the user has typed past a name, it should no longer match

    Examples:
        - "Rhea" matches "Rhea Seehorn" ✓
        - "Rhea S" matches "Rhea Seehorn" ✓
        - "Rhea Se" matches "Rhea Seehorn" ✓
        - "Rhea Se" does NOT match "Rhea Sun" ✗
        - "Rhea Seeh" does NOT match "Rhea Sun" ✗
        - "Rhea Seeh" does NOT match "RHEA" ✗ (query is longer/more specific)
        - "The Beat" matches "The Beatles" ✓
        - "Beatles" matches "The Beatles" ✓

    Args:
        query: The user's search query (partial input)
        name: The candidate name to match against

    Returns:
        True if name is a valid autocomplete match for query
    """
    if not query or not name:
        return not query  # Empty query matches everything, empty name matches nothing

    query_lower = query.lower().strip()
    name_lower = name.lower().strip()

    # Quick exact match check
    if query_lower == name_lower:
        return True

    # If query is a prefix of the full name string, it's a match
    if name_lower.startswith(query_lower):
        return True

    # Word-by-word matching for more complex cases
    query_words = query_lower.split()
    name_words = name_lower.split()

    if not query_words:
        return True  # Empty query matches everything

    # Single word query: check if any name word starts with the query
    if len(query_words) == 1:
        return any(word.startswith(query_words[0]) for word in name_words)

    # Multi-word query: need to match words in sequence
    # All query words except the last must have a matching word in name
    # The last query word can be a prefix of a name word

    # Try to find a matching sequence in the name
    return _match_word_sequence(query_words, name_words)


def is_person_autocomplete_match(query: str, name: str) -> bool:
    """
    Person-specific autocomplete matching used only for people index filtering.

    This variant ignores stopwords and allows non-final tokens to match exact words
    while the final token can match by prefix.
    """
    if not query or not name:
        return not query

    query_lower = query.lower().strip()
    name_lower = name.lower().strip()

    if query_lower == name_lower:
        return True

    if name_lower.startswith(query_lower):
        return True

    query_raw_words = query_lower.split()
    query_words = [word for word in query_raw_words if word and word not in STOPWORDS]
    name_words = name_lower.split()

    if not query_words:
        return True

    if len(query_words) == 1:
        if len(query_raw_words) > 1 and query_raw_words[0] in STOPWORDS:
            if len(query_words[0]) <= 3:
                return query_words[0] in name_words
            return any(word.startswith(query_words[0]) for word in name_words)
        return any(word.startswith(query_words[0]) for word in name_words)

    return _match_person_word_sequence(query_words, name_words)


def _match_word_sequence(query_words: list[str], name_words: list[str]) -> bool:
    """
    Check if query words can be matched against name words in sequence.

    The matching is greedy - it tries to match each query word with a name word,
    where earlier query words must match completely and the last query word
    can be a prefix.

    Args:
        query_words: List of words from the query (lowercase)
        name_words: List of words from the name (lowercase)

    Returns:
        True if the query words match a subsequence of name words
    """
    if not query_words:
        return True
    if not name_words:
        return False

    # Try matching starting from each position in name_words
    for start_idx in range(len(name_words)):
        if _try_match_from(query_words, name_words, start_idx):
            return True

    return False


def _try_match_from(
    query_words: list[str], name_words: list[str], start_idx: int
) -> bool:
    """
    Try to match query words starting from a specific position in name words.

    Args:
        query_words: List of words from the query
        name_words: List of words from the name
        start_idx: Starting index in name_words

    Returns:
        True if all query words can be matched starting from start_idx
    """
    name_idx = start_idx
    for i, query_word in enumerate(query_words):
        if name_idx >= len(name_words):
            return False

        is_last_word = i == len(query_words) - 1
        name_word = name_words[name_idx]

        if is_last_word:
            if not name_word.startswith(query_word):
                return False
        else:
            if not name_word.startswith(query_word):
                return False

        name_idx += 1

    return True


def _match_person_word_sequence(query_words: list[str], name_words: list[str]) -> bool:
    if not query_words:
        return True
    if not name_words:
        return False

    for start_idx in range(len(name_words)):
        if _try_match_person_from(query_words, name_words, start_idx):
            return True

    return False


def _try_match_person_from(
    query_words: list[str], name_words: list[str], start_idx: int
) -> bool:
    name_idx = start_idx
    query_len = len(query_words)

    for i, query_word in enumerate(query_words):
        is_last_word = i == query_len - 1
        found_match = False

        while name_idx < len(name_words):
            name_word = name_words[name_idx]
            if is_last_word:
                if name_word.startswith(query_word):
                    found_match = True
                    break
            else:
                if query_word == name_word:
                    found_match = True
                    break
            name_idx += 1

        if not found_match:
            return False

        name_idx += 1

    return True


def is_author_name_match(query: str, name: str) -> bool:
    """
    Check if an author name matches the query using exact word matching.

    Unlike ``is_autocomplete_match`` (which allows prefix matching on the last
    word), this requires every query word to correspond to a *complete* word in
    the name.  This prevents "tennis" from matching "Jeni Tennison" while still
    allowing "tennis" to match "Jeni Tennis".

    Examples:
        - "tennis" matches "Jeni Tennis" (complete word match)
        - "tennis" does NOT match "Jeni Tennison" (partial word match)
        - "jeni tennis" matches "Jeni Tennis" (both words match)
        - "mark twain" matches "Mark Twain" (both words match)

    Args:
        query: The user's search query
        name: The candidate author name to match against

    Returns:
        True if every query word matches a complete word in the name
    """
    if not query or not name:
        return not query  # Empty query matches everything, empty name matches nothing

    query_lower = query.lower().strip()
    name_lower = name.lower().strip()

    # Quick exact match check
    if query_lower == name_lower:
        return True

    query_words = query_lower.split()
    name_words = name_lower.split()

    if not query_words:
        return True

    # Every query word must match a complete word in the name
    return all(qw in name_words for qw in query_words)


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
