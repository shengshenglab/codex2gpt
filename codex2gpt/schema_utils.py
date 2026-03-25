def is_record(value):
    return isinstance(value, dict)


def prepare_json_schema(schema):
    if not is_record(schema):
        return schema
    cloned = _clone(schema)
    _inject_additional_properties(cloned, set())
    return cloned


def _clone(value):
    if isinstance(value, dict):
        return {key: _clone(current) for key, current in value.items()}
    if isinstance(value, list):
        return [_clone(item) for item in value]
    return value


def _inject_additional_properties(node, seen):
    if not is_record(node):
        if isinstance(node, list):
            for item in node:
                _inject_additional_properties(item, seen)
        return
    marker = id(node)
    if marker in seen:
        return
    seen.add(marker)
    node_type = node.get("type")
    if node_type == "object" and "additionalProperties" not in node:
        node["additionalProperties"] = False
    for value in list(node.values()):
        _inject_additional_properties(value, seen)
