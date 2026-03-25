import json


def is_record(value):
    return isinstance(value, dict)


def parse_sse_lines(lines):
    event_name = "message"
    data_parts = []
    for raw_line in lines:
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


def iter_sse_messages(byte_iterable):
    buffer = ""
    for chunk in byte_iterable:
        if isinstance(chunk, bytes):
            text = chunk.decode("utf-8", errors="replace")
        else:
            text = str(chunk)
        buffer += text
        while "\n\n" in buffer:
            block, buffer = buffer.split("\n\n", 1)
            event = parse_sse_lines(block.splitlines())
            if event is not None:
                yield event
    if buffer.strip():
        event = parse_sse_lines(buffer.splitlines())
        if event is not None:
            yield event


def iter_sse_messages_from_reader(reader, chunk_size=4096):
    while True:
        chunk = reader.read(chunk_size)
        if not chunk:
            break
        yield from iter_sse_messages([chunk])


def parse_usage(response_payload):
    usage = response_payload.get("usage") if is_record(response_payload) else None
    if not is_record(usage):
        return None
    usage_info = {
        "input_tokens": int(usage.get("input_tokens") or 0),
        "output_tokens": int(usage.get("output_tokens") or 0),
    }
    input_details = usage.get("input_tokens_details")
    if is_record(input_details) and input_details.get("cached_tokens") is not None:
        usage_info["cached_tokens"] = int(input_details.get("cached_tokens") or 0)
    output_details = usage.get("output_tokens_details")
    if is_record(output_details) and output_details.get("reasoning_tokens") is not None:
        usage_info["reasoning_tokens"] = int(output_details.get("reasoning_tokens") or 0)
    return usage_info


def extract_event_details(event):
    data = event.get("data")
    name = event.get("event", "message")
    details = {
        "event": name,
        "data": data,
        "response_id": None,
        "text_delta": None,
        "reasoning_delta": None,
        "usage": None,
        "function_call_start": None,
        "function_call_delta": None,
        "function_call_done": None,
        "error": None,
    }

    if data == "[DONE]":
        return details

    if not is_record(data):
        return details

    if name in {"response.created", "response.in_progress", "response.completed", "response.failed"}:
        response = data.get("response")
        if is_record(response):
            details["response_id"] = response.get("id")
            details["usage"] = parse_usage(response)
        if name == "response.failed":
            error = data.get("error")
            if is_record(error):
                details["error"] = {
                    "code": str(error.get("code") or "unknown"),
                    "message": str(error.get("message") or ""),
                }
        return details

    if name == "response.output_text.delta" and isinstance(data.get("delta"), str):
        details["text_delta"] = data.get("delta")
        return details

    if name == "response.reasoning_summary_text.delta" and isinstance(data.get("delta"), str):
        details["reasoning_delta"] = data.get("delta")
        return details

    if name == "response.output_item.added":
        item = data.get("item")
        if is_record(item) and item.get("type") == "function_call":
            call_id = item.get("call_id")
            item_id = item.get("id")
            details["function_call_start"] = {
                "item_id": str(item_id or ""),
                "call_id": str(call_id or item_id or ""),
                "name": str(item.get("name") or ""),
                "output_index": int(data.get("output_index") or 0),
            }
        return details

    if name == "response.function_call_arguments.delta":
        details["function_call_delta"] = {
            "call_id": str(data.get("call_id") or ""),
            "delta": str(data.get("delta") or ""),
        }
        return details

    if name == "response.function_call_arguments.done":
        details["function_call_done"] = {
            "call_id": str(data.get("call_id") or ""),
            "name": str(data.get("name") or ""),
            "arguments": str(data.get("arguments") or ""),
        }
        return details

    if name == "error":
        error = data.get("error")
        if is_record(error):
            details["error"] = {
                "code": str(error.get("code") or "unknown"),
                "message": str(error.get("message") or ""),
            }
        else:
            details["error"] = {
                "code": "unknown",
                "message": json.dumps(data, ensure_ascii=False),
            }
    return details
