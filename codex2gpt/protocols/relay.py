"""Helpers for translating relay-provider payloads and streams."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Iterable, Iterator


def _is_record(value: Any) -> bool:
    return isinstance(value, dict)


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _json_or_text_response(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return {"output": value if value is not None else ""}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {"output": value}


def _parse_sse_block(block: str) -> dict[str, Any] | None:
    event_name = "message"
    data_parts: list[str] = []
    for raw_line in block.splitlines():
        line = raw_line.rstrip("\r")
        if not line:
            continue
        if line.startswith("event:"):
            event_name = line[6:].strip() or "message"
        elif line.startswith("data:"):
            data_parts.append(line[5:].lstrip())
    if not data_parts:
        return None
    data_text = "\n".join(data_parts)
    if data_text == "[DONE]":
        return {"event": event_name, "data": "[DONE]"}
    try:
        data = json.loads(data_text)
    except json.JSONDecodeError:
        data = data_text
    return {"event": event_name, "data": data}


def _iter_sse_messages(chunks: Iterable[str | bytes]) -> Iterator[dict[str, Any]]:
    buffer = ""
    for chunk in chunks:
        if isinstance(chunk, bytes):
            buffer += chunk.decode("utf-8", errors="replace")
        else:
            buffer += str(chunk)
        while "\n\n" in buffer:
            block, buffer = buffer.split("\n\n", 1)
            parsed = _parse_sse_block(block)
            if parsed is not None:
                yield parsed
    if buffer.strip():
        parsed = _parse_sse_block(buffer)
        if parsed is not None:
            yield parsed


def _input_items_to_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    instructions = str(payload.get("instructions") or "").strip()
    if instructions:
        messages.append({"role": "system", "content": instructions})

    pending_assistant_index: int | None = None
    for item in payload.get("input") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            role = "assistant" if item.get("role") == "assistant" else "user"
            content = item.get("content") or []
            if role == "assistant":
                message: dict[str, Any] = {"role": "assistant", "content": ""}
                parts = []
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") in {"input_text", "output_text"} and isinstance(part.get("text"), str):
                        parts.append(part["text"])
                message["content"] = "".join(parts)
                messages.append(message)
                pending_assistant_index = len(messages) - 1
                continue

            if any(isinstance(part, dict) and part.get("type") == "input_image" for part in content):
                rendered = []
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "input_text":
                        rendered.append({"type": "text", "text": str(part.get("text") or "")})
                    elif part.get("type") == "input_image":
                        rendered.append({"type": "image_url", "image_url": {"url": str(part.get("image_url") or "")}})
                messages.append({"role": "user", "content": rendered})
            else:
                text = "".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict) and part.get("type") in {"input_text", "output_text"}
                )
                messages.append({"role": "user", "content": text})
            pending_assistant_index = None
            continue

        if item_type == "function_call":
            tool_call = {
                "id": str(item.get("call_id") or f"call_{len(messages)}"),
                "type": "function",
                "function": {
                    "name": str(item.get("name") or ""),
                    "arguments": _stringify(item.get("arguments") or "{}"),
                },
            }
            if pending_assistant_index is not None and messages[pending_assistant_index]["role"] == "assistant":
                messages[pending_assistant_index].setdefault("tool_calls", []).append(tool_call)
            else:
                messages.append({"role": "assistant", "content": "", "tool_calls": [tool_call]})
                pending_assistant_index = len(messages) - 1
            continue

        if item_type == "function_call_output":
            message = {
                "role": "tool",
                "tool_call_id": str(item.get("call_id") or ""),
                "content": str(item.get("output") or ""),
            }
            if item.get("name"):
                message["name"] = str(item.get("name") or "")
            messages.append(message)
            pending_assistant_index = None
    return messages


def codex_request_to_openai_chat(payload: dict[str, Any]) -> dict[str, Any]:
    request = {
        "model": str(payload.get("model") or ""),
        "messages": _input_items_to_messages(payload),
        "stream": True,
    }
    if isinstance(payload.get("tools"), list):
        request["tools"] = [
            {
                "type": "function",
                "function": {
                    "name": str(tool.get("name") or ""),
                    "description": tool.get("description"),
                    "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
                },
            }
            for tool in payload["tools"]
            if isinstance(tool, dict)
        ]
    if payload.get("tool_choice") is not None:
        tool_choice = payload.get("tool_choice")
        if isinstance(tool_choice, dict) and tool_choice.get("type") == "function":
            request["tool_choice"] = {"type": "function", "function": {"name": str(tool_choice.get("name") or "")}}
        else:
            request["tool_choice"] = tool_choice
    text_config = payload.get("text") if isinstance(payload.get("text"), dict) else {}
    response_format = text_config.get("format") if isinstance(text_config.get("format"), dict) else None
    if isinstance(response_format, dict):
        if response_format.get("type") == "json_object":
            request["response_format"] = {"type": "json_object"}
        elif response_format.get("type") == "json_schema":
            request["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_format.get("name") or "schema",
                    "strict": bool(response_format.get("strict", True)),
                    "schema": response_format.get("schema") or {"type": "object", "properties": {}},
                },
            }
    return request


def codex_request_to_anthropic(payload: dict[str, Any]) -> dict[str, Any]:
    messages = []
    for message in _input_items_to_messages(payload):
        role = message.get("role")
        if role == "system":
            continue
        if role == "tool":
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": str(message.get("tool_call_id") or ""),
                            "content": str(message.get("content") or ""),
                        }
                    ],
                }
            )
            continue
        if role == "assistant":
            content = []
            text = message.get("content")
            if isinstance(text, str) and text:
                content.append({"type": "text", "text": text})
            for tool_call in message.get("tool_calls") or []:
                function = tool_call.get("function") or {}
                try:
                    arguments = json.loads(function.get("arguments") or "{}")
                except json.JSONDecodeError:
                    arguments = {}
                content.append(
                    {
                        "type": "tool_use",
                        "id": str(tool_call.get("id") or ""),
                        "name": str(function.get("name") or ""),
                        "input": arguments if isinstance(arguments, dict) else {},
                    }
                )
            messages.append({"role": "assistant", "content": content or [{"type": "text", "text": ""}]})
            continue
        user_content = message.get("content")
        if isinstance(user_content, list):
            parts = []
            for part in user_content:
                if isinstance(part, dict) and part.get("type") == "text":
                    parts.append({"type": "text", "text": str(part.get("text") or "")})
            messages.append({"role": "user", "content": parts or [{"type": "text", "text": ""}]})
        else:
            messages.append({"role": "user", "content": [{"type": "text", "text": str(user_content or "")}]})

    request = {
        "model": str(payload.get("model") or ""),
        "max_tokens": int(payload.get("max_output_tokens") or 4096),
        "stream": True,
        "messages": messages,
    }
    instructions = str(payload.get("instructions") or "").strip()
    if instructions:
        request["system"] = [{"type": "text", "text": instructions}]
    if isinstance(payload.get("tools"), list):
        request["tools"] = [
            {
                "name": str(tool.get("name") or ""),
                "description": tool.get("description"),
                "input_schema": tool.get("parameters") or {"type": "object", "properties": {}},
            }
            for tool in payload["tools"]
            if isinstance(tool, dict)
        ]
    if payload.get("tool_choice") is not None:
        tool_choice = payload.get("tool_choice")
        if isinstance(tool_choice, dict) and tool_choice.get("type") == "function":
            request["tool_choice"] = {"type": "tool", "name": str(tool_choice.get("name") or "")}
        elif tool_choice == "required":
            request["tool_choice"] = {"type": "any"}
        else:
            request["tool_choice"] = tool_choice
    reasoning = payload.get("reasoning") if isinstance(payload.get("reasoning"), dict) else {}
    effort = str(reasoning.get("effort") or "").lower()
    budget_map = {"low": 1024, "medium": 4096, "high": 8192, "xhigh": 16384}
    if effort in budget_map:
        request["thinking"] = {"enabled": True, "budget_tokens": budget_map[effort]}
    return request


def codex_request_to_gemini(payload: dict[str, Any]) -> dict[str, Any]:
    contents = []
    system_parts = []
    instructions = str(payload.get("instructions") or "").strip()
    if instructions:
        system_parts.append({"text": instructions})
    for message in _input_items_to_messages(payload):
        if message.get("role") == "system":
            continue
        role = "model" if message.get("role") == "assistant" else "user"
        parts = []
        if message.get("role") == "tool":
            parts.append(
                {
                    "functionResponse": {
                        "name": str(message.get("name") or message.get("tool_call_id") or "tool"),
                        "response": _json_or_text_response(message.get("content")),
                    }
                }
            )
        else:
            content = message.get("content")
            if isinstance(content, str):
                if content:
                    parts.append({"text": content})
            elif isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "text":
                        parts.append({"text": str(part.get("text") or "")})
                    elif part.get("type") == "image_url":
                        url = ((part.get("image_url") or {}).get("url") if isinstance(part.get("image_url"), dict) else part.get("image_url")) or ""
                        parts.append({"text": str(url)})
            for tool_call in message.get("tool_calls") or []:
                function = tool_call.get("function") or {}
                try:
                    arguments = json.loads(function.get("arguments") or "{}")
                except json.JSONDecodeError:
                    arguments = {}
                parts.append({"functionCall": {"name": str(function.get("name") or ""), "args": arguments}})
        contents.append({"role": role, "parts": parts or [{"text": ""}]})

    request = {"contents": contents}
    if system_parts:
        request["systemInstruction"] = {"parts": system_parts}
    if isinstance(payload.get("tools"), list):
        request["tools"] = [
            {
                "functionDeclarations": [
                    {
                        "name": str(tool.get("name") or ""),
                        "description": tool.get("description"),
                        "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
                    }
                ]
            }
            for tool in payload["tools"]
            if isinstance(tool, dict)
        ]
    if payload.get("tool_choice") is not None:
        tool_choice = payload.get("tool_choice")
        if isinstance(tool_choice, dict) and tool_choice.get("type") == "function":
            request["toolConfig"] = {
                "functionCallingConfig": {"mode": "SPECIFIC", "allowedFunctionNames": [str(tool_choice.get("name") or "")]}
            }
        elif tool_choice == "required":
            request["toolConfig"] = {"functionCallingConfig": {"mode": "ANY"}}
    text_config = payload.get("text") if isinstance(payload.get("text"), dict) else {}
    response_format = text_config.get("format") if isinstance(text_config.get("format"), dict) else None
    if isinstance(response_format, dict):
        generation_config = {}
        if response_format.get("type") == "json_object":
            generation_config["responseMimeType"] = "application/json"
        elif response_format.get("type") == "json_schema":
            generation_config["responseMimeType"] = "application/json"
            generation_config["responseSchema"] = response_format.get("schema") or {"type": "object", "properties": {}}
        if generation_config:
            request["generationConfig"] = generation_config
    reasoning = payload.get("reasoning") if isinstance(payload.get("reasoning"), dict) else {}
    effort = str(reasoning.get("effort") or "").lower()
    budget_map = {"low": 1024, "medium": 4096, "high": 8192, "xhigh": 16384}
    if effort in budget_map:
        request.setdefault("generationConfig", {})
        request["generationConfig"]["thinkingConfig"] = {"thinkingBudget": budget_map[effort]}
    return request


def openai_chat_to_codex_response(payload: dict[str, Any]) -> dict[str, Any]:
    choice = ((payload.get("choices") or [{}])[0]) if isinstance(payload.get("choices"), list) else {}
    message = choice.get("message") if isinstance(choice, dict) else {}
    output = []
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str) and content:
            output.append(
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": content}],
                }
            )
        for tool_call in message.get("tool_calls") or []:
            function = tool_call.get("function") or {}
            output.append(
                {
                    "type": "function_call",
                    "call_id": str(tool_call.get("id") or f"call_{len(output)}"),
                    "name": str(function.get("name") or ""),
                    "arguments": _stringify(function.get("arguments") or "{}"),
                }
            )
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    input_details = {}
    prompt_details = usage.get("prompt_tokens_details") if isinstance(usage.get("prompt_tokens_details"), dict) else {}
    if prompt_details.get("cached_tokens") is not None:
        input_details["cached_tokens"] = int(prompt_details.get("cached_tokens") or 0)
    output_details = {}
    completion_details = usage.get("completion_tokens_details") if isinstance(usage.get("completion_tokens_details"), dict) else {}
    if completion_details.get("reasoning_tokens") is not None:
        output_details["reasoning_tokens"] = int(completion_details.get("reasoning_tokens") or 0)
    response = {
        "id": payload.get("id") or f"resp_{uuid.uuid4().hex[:12]}",
        "object": "response",
        "created_at": int(payload.get("created") or time.time()),
        "model": payload.get("model") or "",
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": int(usage.get("prompt_tokens") or 0),
            "output_tokens": int(usage.get("completion_tokens") or 0),
        },
    }
    if input_details:
        response["usage"]["input_tokens_details"] = input_details
    if output_details:
        response["usage"]["output_tokens_details"] = output_details
    return response


def anthropic_to_codex_response(payload: dict[str, Any]) -> dict[str, Any]:
    output = []
    text_parts = []
    for block in payload.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text_parts.append(str(block.get("text") or ""))
        elif block.get("type") == "tool_use":
            output.append(
                {
                    "type": "function_call",
                    "call_id": str(block.get("id") or f"call_{len(output)}"),
                    "name": str(block.get("name") or ""),
                    "arguments": _stringify(block.get("input") or {}),
                }
            )
    if text_parts:
        output.insert(
            0,
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "".join(text_parts)}],
            },
        )
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    response = {
        "id": payload.get("id") or f"resp_{uuid.uuid4().hex[:12]}",
        "object": "response",
        "created_at": int(time.time()),
        "model": payload.get("model") or "",
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": int(usage.get("input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
        },
    }
    if usage.get("cache_read_input_tokens") is not None:
        response["usage"]["input_tokens_details"] = {"cached_tokens": int(usage.get("cache_read_input_tokens") or 0)}
    return response


def gemini_to_codex_response(payload: dict[str, Any]) -> dict[str, Any]:
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    candidate = candidates[0] if candidates else {}
    content = candidate.get("content") if isinstance(candidate, dict) else {}
    parts = content.get("parts") if isinstance(content, dict) else []
    output = []
    text_chunks = []
    for part in parts or []:
        if not isinstance(part, dict):
            continue
        if isinstance(part.get("text"), str):
            text_chunks.append(part["text"])
        elif isinstance(part.get("functionCall"), dict):
            function_call = part["functionCall"]
            output.append(
                {
                    "type": "function_call",
                    "call_id": f"call_{len(output)}",
                    "name": str(function_call.get("name") or ""),
                    "arguments": _stringify(function_call.get("args") or {}),
                }
            )
    if text_chunks:
        output.insert(
            0,
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "".join(text_chunks)}],
            },
        )
    usage = payload.get("usageMetadata") if isinstance(payload.get("usageMetadata"), dict) else {}
    response = {
        "id": f"resp_{uuid.uuid4().hex[:12]}",
        "object": "response",
        "created_at": int(time.time()),
        "model": payload.get("modelVersion") or "",
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": int(usage.get("promptTokenCount") or 0),
            "output_tokens": int(usage.get("candidatesTokenCount") or 0),
        },
    }
    if usage.get("cachedContentTokenCount") is not None:
        response["usage"]["input_tokens_details"] = {"cached_tokens": int(usage.get("cachedContentTokenCount") or 0)}
    return response


def _codex_sse(event_name: str, payload: dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _final_codex_completed(response: dict[str, Any]) -> str:
    return _codex_sse("response.completed", {"type": "response.completed", "response": response})


def openai_stream_to_codex_sse(events: Iterable[dict[str, Any]], model: str = "") -> Iterator[str]:
    response_id = f"resp_{uuid.uuid4().hex[:12]}"
    text_chunks: list[str] = []
    tool_calls: dict[int, dict[str, Any]] = {}
    usage_payload = {}
    created = False
    for event in events:
        data = event.get("data")
        if data == "[DONE]":
            continue
        if not isinstance(data, dict):
            continue
        if not created:
            created = True
            yield _codex_sse("response.created", {"type": "response.created", "response": {"id": response_id}})
        choices = data.get("choices") if isinstance(data.get("choices"), list) else []
        choice = choices[0] if choices else {}
        delta = choice.get("delta") if isinstance(choice, dict) else {}
        if isinstance(delta, dict):
            if isinstance(delta.get("content"), str):
                text_chunks.append(delta["content"])
                yield _codex_sse("response.output_text.delta", {"type": "response.output_text.delta", "delta": delta["content"]})
            for tool_call in delta.get("tool_calls") or []:
                if not isinstance(tool_call, dict):
                    continue
                index = int(tool_call.get("index") or 0)
                function = tool_call.get("function") if isinstance(tool_call.get("function"), dict) else {}
                current = tool_calls.setdefault(
                    index,
                    {
                        "id": str(tool_call.get("id") or f"call_{index}"),
                        "name": str(function.get("name") or ""),
                        "arguments": "",
                    },
                )
                if function.get("name"):
                    current["name"] = str(function.get("name"))
                if tool_call.get("id"):
                    current["id"] = str(tool_call.get("id"))
                    yield _codex_sse(
                        "response.output_item.added",
                        {
                            "type": "response.output_item.added",
                            "output_index": index,
                            "item": {"type": "function_call", "id": current["id"], "call_id": current["id"], "name": current["name"]},
                        },
                    )
                if isinstance(function.get("arguments"), str):
                    current["arguments"] += function["arguments"]
                    yield _codex_sse(
                        "response.function_call_arguments.delta",
                        {"type": "response.function_call_arguments.delta", "call_id": current["id"], "delta": function["arguments"]},
                    )
        if isinstance(data.get("usage"), dict):
            usage_payload = data["usage"]

    output = []
    if text_chunks:
        output.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "".join(text_chunks)}]})
    for _, tool_call in sorted(tool_calls.items()):
        output.append(
            {
                "type": "function_call",
                "call_id": tool_call["id"],
                "name": tool_call["name"],
                "arguments": tool_call["arguments"],
            }
        )
    response = {
        "id": response_id,
        "object": "response",
        "created_at": int(time.time()),
        "model": model,
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": int(usage_payload.get("prompt_tokens") or 0),
            "output_tokens": int(usage_payload.get("completion_tokens") or 0),
        },
    }
    yield _final_codex_completed(response)


def anthropic_stream_to_codex_sse(events: Iterable[dict[str, Any]], model: str = "") -> Iterator[str]:
    response_id = f"resp_{uuid.uuid4().hex[:12]}"
    text_chunks: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    usage_output_tokens = 0
    created = False
    current_tool: dict[str, Any] | None = None
    for event in events:
        event_name = event.get("event")
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        if not created:
            created = True
            yield _codex_sse("response.created", {"type": "response.created", "response": {"id": response_id}})
        if event_name == "content_block_start":
            block = data.get("content_block") if isinstance(data.get("content_block"), dict) else {}
            if block.get("type") == "tool_use":
                current_tool = {
                    "id": str(block.get("id") or f"call_{len(tool_calls)}"),
                    "name": str(block.get("name") or ""),
                    "arguments": "",
                }
                tool_calls.append(current_tool)
                yield _codex_sse(
                    "response.output_item.added",
                    {
                        "type": "response.output_item.added",
                        "output_index": len(tool_calls) - 1,
                        "item": {"type": "function_call", "id": current_tool["id"], "call_id": current_tool["id"], "name": current_tool["name"]},
                    },
                )
        elif event_name == "content_block_delta":
            delta = data.get("delta") if isinstance(data.get("delta"), dict) else {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
                text_chunks.append(delta["text"])
                yield _codex_sse("response.output_text.delta", {"type": "response.output_text.delta", "delta": delta["text"]})
            elif delta.get("type") == "thinking_delta" and isinstance(delta.get("thinking"), str):
                yield _codex_sse(
                    "response.reasoning_summary_text.delta",
                    {"type": "response.reasoning_summary_text.delta", "delta": delta["thinking"]},
                )
            elif delta.get("type") == "input_json_delta" and isinstance(delta.get("partial_json"), str) and current_tool is not None:
                current_tool["arguments"] += delta["partial_json"]
                yield _codex_sse(
                    "response.function_call_arguments.delta",
                    {"type": "response.function_call_arguments.delta", "call_id": current_tool["id"], "delta": delta["partial_json"]},
                )
        elif event_name == "message_delta":
            usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
            usage_output_tokens = int(usage.get("output_tokens") or 0)
        elif event_name == "content_block_stop" and current_tool is not None:
            yield _codex_sse(
                "response.function_call_arguments.done",
                {
                    "type": "response.function_call_arguments.done",
                    "call_id": current_tool["id"],
                    "name": current_tool["name"],
                    "arguments": current_tool["arguments"],
                },
            )
            current_tool = None

    output = []
    if text_chunks:
        output.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "".join(text_chunks)}]})
    for tool in tool_calls:
        output.append({"type": "function_call", "call_id": tool["id"], "name": tool["name"], "arguments": tool["arguments"]})
    response = {
        "id": response_id,
        "object": "response",
        "created_at": int(time.time()),
        "model": model,
        "status": "completed",
        "output": output,
        "usage": {"input_tokens": 0, "output_tokens": usage_output_tokens},
    }
    yield _final_codex_completed(response)


def gemini_stream_to_codex_sse(events: Iterable[dict[str, Any]], model: str = "") -> Iterator[str]:
    response_id = f"resp_{uuid.uuid4().hex[:12]}"
    text_chunks = []
    tool_calls = []
    usage = {}
    created = False
    for event in events:
        data = event.get("data")
        if data == "[DONE]" or not isinstance(data, dict):
            continue
        if not created:
            created = True
            yield _codex_sse("response.created", {"type": "response.created", "response": {"id": response_id}})
        candidates = data.get("candidates") if isinstance(data.get("candidates"), list) else []
        if candidates:
            content = candidates[0].get("content") if isinstance(candidates[0], dict) else {}
            parts = content.get("parts") if isinstance(content, dict) else []
            for part in parts or []:
                if not isinstance(part, dict):
                    continue
                if isinstance(part.get("text"), str) and part["text"]:
                    text_chunks.append(part["text"])
                    yield _codex_sse("response.output_text.delta", {"type": "response.output_text.delta", "delta": part["text"]})
                elif isinstance(part.get("functionCall"), dict):
                    function_call = part["functionCall"]
                    call = {
                        "id": f"call_{len(tool_calls)}",
                        "name": str(function_call.get("name") or ""),
                        "arguments": _stringify(function_call.get("args") or {}),
                    }
                    tool_calls.append(call)
                    yield _codex_sse(
                        "response.output_item.added",
                        {
                            "type": "response.output_item.added",
                            "output_index": len(tool_calls) - 1,
                            "item": {"type": "function_call", "id": call["id"], "call_id": call["id"], "name": call["name"]},
                        },
                    )
                    yield _codex_sse(
                        "response.function_call_arguments.delta",
                        {"type": "response.function_call_arguments.delta", "call_id": call["id"], "delta": call["arguments"]},
                    )
        if isinstance(data.get("usageMetadata"), dict):
            usage = data["usageMetadata"]

    output = []
    if text_chunks:
        output.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "".join(text_chunks)}]})
    for tool in tool_calls:
        output.append({"type": "function_call", "call_id": tool["id"], "name": tool["name"], "arguments": tool["arguments"]})
    response = {
        "id": response_id,
        "object": "response",
        "created_at": int(time.time()),
        "model": model,
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": int(usage.get("promptTokenCount") or 0),
            "output_tokens": int(usage.get("candidatesTokenCount") or 0),
        },
    }
    yield _final_codex_completed(response)


def relay_sse_bytes_to_events(body: bytes | str) -> list[dict[str, Any]]:
    if isinstance(body, str):
        body = body.encode("utf-8")
    return list(_iter_sse_messages([body]))


def openai_chat_response_to_codex(payload: dict[str, Any]) -> dict[str, Any]:
    return openai_chat_to_codex_response(payload)


def anthropic_response_to_codex(payload: dict[str, Any]) -> dict[str, Any]:
    return anthropic_to_codex_response(payload)


def gemini_response_to_codex(payload: dict[str, Any]) -> dict[str, Any]:
    return gemini_to_codex_response(payload)


def stream_openai_chat_to_codex_sse(source: Iterable[str | bytes]) -> Iterator[str]:
    yield from openai_stream_to_codex_sse(_iter_sse_messages(source))


def stream_anthropic_to_codex_sse(source: Iterable[str | bytes]) -> Iterator[str]:
    yield from anthropic_stream_to_codex_sse(_iter_sse_messages(source))


def stream_gemini_to_codex_sse(source: Iterable[str | bytes]) -> Iterator[str]:
    yield from gemini_stream_to_codex_sse(_iter_sse_messages(source))
