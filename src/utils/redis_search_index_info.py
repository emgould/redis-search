"""Parse FT.INFO payloads across redis-py RESP2 and RESP3 shapes."""

from __future__ import annotations

from typing import Any

_STANDALONE_FLAGS = frozenset({"SORTABLE", "UNF", "NOSTEM", "NOINDEX", "CASESENSITIVE"})

SchemaField = dict[str, str | bool | float]


def extract_index_prefix(index_definition: dict[str, Any] | list[Any]) -> str:
    """Return the first key prefix from an FT.INFO index_definition value."""
    if isinstance(index_definition, dict):
        prefixes = index_definition.get("prefixes")
        if isinstance(prefixes, list) and prefixes:
            return str(prefixes[0])
        return ""

    for j in range(0, len(index_definition), 2):
        if index_definition[j] == "prefixes":
            prefixes = index_definition[j + 1]
            if isinstance(prefixes, list) and prefixes:
                return str(prefixes[0])
            break
    return ""


def parse_attribute_field(attr: dict[str, Any] | list[Any]) -> SchemaField:
    """Normalize one FT.INFO attribute entry to a flat field dictionary."""
    field_info: SchemaField = {}
    if isinstance(attr, dict):
        flags = attr.get("flags")
        for key, value in attr.items():
            if key == "flags":
                continue
            if isinstance(key, str) and key in _STANDALONE_FLAGS:
                field_info[key] = True
            else:
                field_info[key] = value
        if isinstance(flags, list):
            for flag in flags:
                if isinstance(flag, str) and flag in _STANDALONE_FLAGS:
                    field_info[flag] = True
        return field_info

    k = 0
    while k < len(attr):
        token = str(attr[k])
        if token in _STANDALONE_FLAGS:
            field_info[token] = True
            k += 1
        elif k + 1 < len(attr):
            field_info[token] = attr[k + 1]
            k += 2
        else:
            field_info[token] = True
            k += 1
    return field_info


def parse_index_schema_fields(info: dict[str, Any]) -> list[SchemaField]:
    """Extract normalized schema field definitions from FT.INFO."""
    attrs = info.get("attributes")
    if not isinstance(attrs, list):
        return []
    return [parse_attribute_field(attr) for attr in attrs if attr is not None]
