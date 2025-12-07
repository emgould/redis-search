import simplejson as jsonplus
from commentjson import loads as cjson_loads


def extract_json_text(jsonstr):
    json_start = jsonstr.find("{")
    if json_start == -1:
        return jsonstr

    json_end = jsonstr.rfind("}")
    if json_end == -1:
        return jsonstr
    return jsonstr[json_start : json_end + 1]


def parse_json(json_string):
    json_string = extract_json_text(json_string)
    """
    Attempts to parse a JSON string using simplejson and commentjson.

    Args:
        json_string (str): The JSON string to parse.

    Returns:
        dict: The parsed JSON data if successful.
        None: If parsing fails with both methods.
    """
    try:
        return jsonplus.loads(json_string)
    except Exception:
        try:
            return cjson_loads(json_string)
        except Exception:
            return None
