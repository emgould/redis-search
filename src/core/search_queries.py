
# Common stopwords that Redis Search ignores
STOPWORDS = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "is", "it"}


def build_autocomplete_query(q: str):
    """
    Build a prefix search query for autocomplete.
    Handles multi-word queries and filters out stopwords.
    """
    words = q.lower().split()
    # Filter out stopwords and empty strings
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        # If only stopwords, return a broad match
        return "*"

    # For multi-word: match documents containing all words (last word as prefix)
    if len(words) == 1:
        return f"@search_title:{words[0]}*"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(words[:-1])
        prefix_word = words[-1]
        return f"@search_title:({exact_words} {prefix_word}*)"


def build_fuzzy_fulltext_query(q: str):
    """Build a fuzzy full-text search query."""
    words = q.lower().split()
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        return "*"

    # Fuzzy match on each word
    fuzzy_terms = " ".join(f"%{w}%" for w in words)
    return f"@search_title:({fuzzy_terms})"
