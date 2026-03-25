"""Gemini protocol helpers.

This module is intentionally self-contained so the main service can import it
before the broader package refactor lands.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Iterator


@dataclass(frozen=True)
class ParsedModelAction:
    model: str
    action: str


@dataclass(frozen=True)
class SchemaPreparationResult:
    schema: dict[str, Any]
    original_schema: dict[str, Any] | None


@dataclass(frozen=True)
class GeminiTranslationResult:
    codex_request: dict[str, Any]
    tuple_schema: dict[str, Any] | None


def parse_model_action(param: str) -> ParsedModelAction | None:
    """Parse a Gemini model action like ``gemini-2.5-pro:generateContent``."""
    if not isinstance(param, str):
        return None
    index = param.rfind(":")
    if index <= 0 or index == len(param) - 1:
        return None
    return ParsedModelAction(model=param[:index], action=param[index + 1 :])


def prepare_schema(schema: dict[str, Any]) -> SchemaPreparationResult:
    """Normalize a JSON schema for Codex text.format.json_schema.

    Tuple schemas are converted into object form so upstreams that reject
    ``prefixItems``/tuple arrays can still accept the request. The original
    schema is preserved for response reconversion.
    """

    cloned = json.loads(json.dumps(schema))
    original_schema = json.loads(json.dumps(schema)) if has_tuple_schema(cloned) else None
    normalized = _normalize_schema(cloned)
    return SchemaPreparationResult(schema=normalized, original_schema=original_schema)


def has_tuple_schema(schema: Any) -> bool:
    if not isinstance(schema, dict):
        return False
    if isinstance(schema.get("prefixItems"), list):
        return True
    items = schema.get("items")
    if isinstance(items, list):
        return True
    properties = schema.get("properties")
    if isinstance(properties, dict):
        return any(has_tuple_schema(value) for value in properties.values())
    defs = schema.get("$defs")
    if isinstance(defs, dict):
        return any(has_tuple_schema(value) for value in defs.values())
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        return any(has_tuple_schema(value) for value in any_of)
    one_of = schema.get("oneOf")
    if isinstance(one_of, list):
        return any(has_tuple_schema(value) for value in one_of)
    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        return any(has_tuple_schema(value) for value in all_of)
    items = schema.get("items")
    if isinstance(items, dict):
        return has_tuple_schema(items)
    return False


def _normalize_schema(node: Any) -> Any:
    if isinstance(node, dict):
        node_type = node.get("type")
        if node_type == "object":
            node.setdefault("additionalProperties", False)
        tuple_items = None
        if isinstance(node.get("prefixItems"), list):
            tuple_items = node.pop("prefixItems")
            node.pop("items", None)
        elif isinstance(node.get("items"), list):
            tuple_items = node.pop("items")
        if tuple_items is not None:
            node["type"] = "object"
            node["additionalProperties"] = False
            properties: dict[str, Any] = {}
            required: list[str] = []
            for index, item in enumerate(tuple_items):
                key = f"item_{index}"
                properties[key] = _normalize_schema(item)
                required.append(key)
            node["properties"] = properties
            node["required"] = required
            node["x-codex-original-type"] = "tuple_array"
        for key, value in list(node.items()):
            node[key] = _normalize_schema(value)
        return node
    if isinstance(node, list):
        return [_normalize_schema(item) for item in node]
    return node


def reconvert_tuple_values(data: Any, schema: dict[str, Any] | None) -> Any:
    if schema is None:
        return data
    return _reconvert_tuple_value(data, schema)


def _reconvert_tuple_value(data: Any, schema: Any) -> Any:
    if not isinstance(schema, dict):
        return data
    tuple_schema = schema.get("prefixItems")
    if not isinstance(tuple_schema, list):
        items = schema.get("items")
        if isinstance(items, list):
            tuple_schema = items
    if tuple_schema is not None:
        if not isinstance(data, dict):
            return data
        result = []
        for index, item_schema in enumerate(tuple_schema):
            key = f"item_{index}"
            if key in data:
                result.append(_reconvert_tuple_value(data[key], item_schema))
        return result
    schema_type = schema.get("type")
    if schema_type == "object" and isinstance(data, dict):
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return data
        return {
            key: _reconvert_tuple_value(value, properties.get(key))
            for key, value in data.items()
        }
    if schema_type == "array" and isinstance(data, list):
        item_schema = schema.get("items")
        return [_reconvert_tuple_value(value, item_schema) for value in data]
    return data


def translate_gemini_request(
    request: dict[str, Any],
    gemini_model: str,
    *,
    default_reasoning_effort: str = "medium",
) -> GeminiTranslationResult:
    """Translate Gemini generateContent payload into a Codex-like request."""
    system_instruction = request.get("systemInstruction")
    instructions = _flatten_parts((system_instruction or {}).get("parts") or []) if isinstance(system_instruction, dict) else ""
    if not instructions:
        instructions = "You are a helpful assistant."

    input_items: list[dict[str, Any]] = []
    for content in request.get("contents") or []:
        if not isinstance(content, dict):
            continue
        role = "assistant" if content.get("role") == "model" else "user"
        input_items.extend(_parts_to_input_items(role, content.get("parts") or []))
    if not input_items:
        input_items.append(
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": ""}],
            }
        )

    payload: dict[str, Any] = {
        "model": gemini_model,
        "instructions": instructions,
        "input": input_items,
        "stream": True,
        "store": False,
        "reasoning": {"effort": _thinking_budget_to_effort(request.get("generationConfig", {}).get("thinkingConfig", {}).get("thinkingBudget"), default_reasoning_effort)},
    }

    tools = _gemini_tools_to_codex(request.get("tools"))
    if tools:
        payload["tools"] = tools
    tool_choice = _gemini_tool_config_to_codex(request.get("toolConfig"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice

    tuple_schema = None
    generation_config = request.get("generationConfig")
    if isinstance(generation_config, dict) and generation_config.get("responseMimeType") == "application/json":
        response_schema = generation_config.get("responseSchema")
        if isinstance(response_schema, dict) and response_schema:
            prepared = prepare_schema(response_schema)
            tuple_schema = prepared.original_schema
            payload["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "gemini_schema",
                    "schema": prepared.schema,
                    "strict": True,
                }
            }
        else:
            payload["text"] = {"format": {"type": "json_object"}}

    return GeminiTranslationResult(codex_request=payload, tuple_schema=tuple_schema)


def codex_response_to_gemini(
    response: dict[str, Any],
    model: str,
    *,
    tuple_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert a Codex-style response payload into Gemini response format."""
    full_text = _response_output_text(response)
    if tuple_schema and full_text:
        try:
            full_text = json.dumps(reconvert_tuple_values(json.loads(full_text), tuple_schema), ensure_ascii=False)
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    parts: list[dict[str, Any]] = []
    if full_text:
        parts.append({"text": full_text})

    for tool_call in _response_tool_calls(response):
        parts.append(
            {
                "functionCall": {
                    "name": tool_call["name"],
                    "args": _parse_json_object(tool_call["arguments"]),
                }
            }
        )

    if not parts:
        parts.append({"text": ""})

    usage = response.get("usage") or {}
    input_tokens = _as_int(usage.get("input_tokens"))
    output_tokens = _as_int(usage.get("output_tokens"))
    usage_metadata = {
        "promptTokenCount": input_tokens,
        "candidatesTokenCount": output_tokens,
        "totalTokenCount": input_tokens + output_tokens,
    }
    cached_tokens = _as_int((usage.get("input_tokens_details") or {}).get("cached_tokens"))
    if cached_tokens:
        usage_metadata["cachedContentTokenCount"] = cached_tokens

    return {
        "candidates": [
            {
                "content": {
                    "parts": parts,
                    "role": "model",
                },
                "finishReason": "STOP" if response.get("status") != "incomplete" else "MAX_TOKENS",
                "index": 0,
            }
        ],
        "usageMetadata": usage_metadata,
        "modelVersion": model,
    }


def stream_gemini_sse_from_codex_events(
    events: Iterable[dict[str, Any]],
    model: str,
    *,
    tuple_schema: dict[str, Any] | None = None,
) -> Iterator[str]:
    """Translate raw Codex SSE events into Gemini SSE payload chunks."""
    buffered_text = "" if tuple_schema else None
    input_tokens = 0
    output_tokens = 0
    cached_tokens = 0
    emitted_any_content = False

    for item in events:
        event_name = item.get("event")
        data = item.get("data") or {}
        if event_name == "response.output_text.delta":
            delta = data.get("delta")
            if isinstance(delta, str):
                emitted_any_content = True
                if buffered_text is not None:
                    buffered_text += delta
                else:
                    yield _format_sse(
                        {
                            "candidates": [
                                {
                                    "content": {"parts": [{"text": delta}], "role": "model"},
                                    "index": 0,
                                }
                            ],
                            "modelVersion": model,
                        }
                    )
            continue

        if event_name in {"response.function_call_arguments.done", "response.output_item.done"}:
            tool_call = _tool_call_from_event_data(data)
            if tool_call is not None:
                emitted_any_content = True
                yield _format_sse(
                    {
                        "candidates": [
                            {
                                "content": {
                                    "parts": [
                                        {
                                            "functionCall": {
                                                "name": tool_call["name"],
                                                "args": _parse_json_object(tool_call["arguments"]),
                                            }
                                        }
                                    ],
                                    "role": "model",
                                },
                                "index": 0,
                            }
                        ],
                        "modelVersion": model,
                    }
                )
            continue

        if event_name == "response.completed":
            response = data.get("response") or {}
            usage = response.get("usage") or {}
            input_tokens = _as_int(usage.get("input_tokens"))
            output_tokens = _as_int(usage.get("output_tokens"))
            cached_tokens = _as_int((usage.get("input_tokens_details") or {}).get("cached_tokens"))

            if buffered_text:
                emitted_any_content = True
                text = buffered_text
                if tuple_schema:
                    try:
                        text = json.dumps(reconvert_tuple_values(json.loads(buffered_text), tuple_schema), ensure_ascii=False)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        text = buffered_text
                yield _format_sse(
                    {
                        "candidates": [
                            {
                                "content": {"parts": [{"text": text}], "role": "model"},
                                "index": 0,
                            }
                        ],
                        "modelVersion": model,
                    }
                )
            elif not emitted_any_content:
                yield _format_sse(
                    {
                        "candidates": [
                            {
                                "content": {
                                    "parts": [{"text": "[Error] Codex returned an empty response. Please retry."}],
                                    "role": "model",
                                },
                                "index": 0,
                            }
                        ],
                        "modelVersion": model,
                    }
                )

            final_chunk = {
                "candidates": [
                    {
                        "content": {"parts": [{"text": ""}], "role": "model"},
                        "finishReason": "STOP" if response.get("status") != "incomplete" else "MAX_TOKENS",
                        "index": 0,
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": input_tokens,
                    "candidatesTokenCount": output_tokens,
                    "totalTokenCount": input_tokens + output_tokens,
                },
                "modelVersion": model,
            }
            if cached_tokens:
                final_chunk["usageMetadata"]["cachedContentTokenCount"] = cached_tokens
            yield _format_sse(final_chunk)


def _format_sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _thinking_budget_to_effort(thinking_budget: Any, default: str) -> str:
    budget = _as_int(thinking_budget)
    if budget <= 0:
        return default
    if budget < 512:
        return "low"
    if budget < 2048:
        return "medium"
    if budget < 8192:
        return "high"
    return "xhigh"


def _flatten_parts(parts: list[dict[str, Any]]) -> str:
    chunks = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        if part.get("thought"):
            continue
        text = part.get("text")
        if isinstance(text, str):
            chunks.append(text)
    return "\n".join(chunks)


def _parts_to_input_items(role: str, parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    message_parts = _parts_to_message_content(parts)
    has_function_io = any(isinstance(part, dict) and (part.get("functionCall") or part.get("functionResponse")) for part in parts)
    if message_parts or not has_function_io:
        items.append(
            {
                "type": "message",
                "role": role,
                "content": message_parts or [{"type": "input_text" if role == "user" else "output_text", "text": ""}],
            }
        )

    call_ids_by_name: dict[str, list[str]] = {}
    call_counter = 0
    for part in parts:
        if not isinstance(part, dict):
            continue
        function_call = part.get("functionCall")
        if isinstance(function_call, dict) and isinstance(function_call.get("name"), str):
            call_id = f"fc_{call_counter}"
            call_counter += 1
            name = function_call["name"]
            call_ids_by_name.setdefault(name, []).append(call_id)
            items.append(
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": json.dumps(function_call.get("args") or {}, ensure_ascii=False),
                }
            )
            continue
        function_response = part.get("functionResponse")
        if isinstance(function_response, dict):
            name = function_response.get("name")
            known_ids = call_ids_by_name.get(name) or []
            call_id = known_ids.pop(0) if known_ids else f"fc_{call_counter}"
            if not known_ids and name in call_ids_by_name:
                call_ids_by_name.pop(name, None)
            elif known_ids:
                call_ids_by_name[name] = known_ids
            call_counter += 1
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(function_response.get("response") or {}, ensure_ascii=False),
                }
            )
    return items


def _parts_to_message_content(parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = []
    for part in parts:
        if not isinstance(part, dict) or part.get("thought"):
            continue
        text = part.get("text")
        if isinstance(text, str):
            content.append({"type": "input_text", "text": text})
            continue
        inline_data = part.get("inlineData")
        if isinstance(inline_data, dict):
            mime_type = inline_data.get("mimeType")
            data = inline_data.get("data")
            if isinstance(mime_type, str) and isinstance(data, str):
                content.append({"type": "input_image", "image_url": f"data:{mime_type};base64,{data}"})
    return content


def _gemini_tools_to_codex(tools: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not isinstance(tools, list):
        return normalized
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        declarations = tool.get("functionDeclarations")
        if not isinstance(declarations, list):
            continue
        for declaration in declarations:
            if not isinstance(declaration, dict):
                continue
            name = declaration.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            normalized.append(
                {
                    "type": "function",
                    "name": name.strip(),
                    "description": declaration.get("description"),
                    "parameters": declaration.get("parameters") if isinstance(declaration.get("parameters"), dict) else {"type": "object", "properties": {}},
                }
            )
    return normalized


def _gemini_tool_config_to_codex(tool_config: Any) -> Any:
    if not isinstance(tool_config, dict):
        return None
    function_calling_config = tool_config.get("functionCallingConfig")
    if not isinstance(function_calling_config, dict):
        return None
    mode = str(function_calling_config.get("mode") or "").upper()
    if mode in {"AUTO", ""}:
        return "auto"
    if mode == "NONE":
        return "none"
    if mode in {"ANY", "REQUIRED"}:
        return "required"
    allowed = function_calling_config.get("allowedFunctionNames")
    if mode == "SPECIFIC" and isinstance(allowed, list) and allowed:
        name = allowed[0]
        if isinstance(name, str) and name.strip():
            return {"type": "function", "name": name.strip()}
    return None


def _response_output_text(response: dict[str, Any]) -> str:
    chunks: list[str] = []
    for item in response.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "message" or item.get("role") != "assistant":
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                chunks.append(content["text"])
    return "".join(chunks)


def _response_tool_calls(response: dict[str, Any]) -> list[dict[str, str]]:
    tool_calls: list[dict[str, str]] = []
    for item in response.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "function_call":
            continue
        name = item.get("name")
        if not isinstance(name, str):
            continue
        arguments = item.get("arguments")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments or {}, ensure_ascii=False)
        tool_calls.append({"name": name, "arguments": arguments})
    return tool_calls


def _tool_call_from_event_data(data: dict[str, Any]) -> dict[str, str] | None:
    item = data.get("item")
    if isinstance(item, dict):
        if item.get("type") == "function_call" and isinstance(item.get("name"), str):
            arguments = item.get("arguments")
            if not isinstance(arguments, str):
                arguments = json.dumps(arguments or {}, ensure_ascii=False)
            return {"name": item["name"], "arguments": arguments}
    if data.get("type") == "response.function_call_arguments.done":
        name = data.get("name")
        arguments = data.get("arguments")
        if isinstance(name, str):
            if not isinstance(arguments, str):
                arguments = json.dumps(arguments or {}, ensure_ascii=False)
            return {"name": name, "arguments": arguments}
    return None


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
