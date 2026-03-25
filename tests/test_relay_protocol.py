import json
import unittest

from codex2gpt.protocols.relay import (
    anthropic_response_to_codex,
    codex_request_to_anthropic,
    codex_request_to_gemini,
    codex_request_to_openai_chat,
    gemini_response_to_codex,
    openai_chat_response_to_codex,
    stream_anthropic_to_codex_sse,
    stream_gemini_to_codex_sse,
    stream_openai_chat_to_codex_sse,
)


def sse(event, payload):
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


class RelayProtocolTests(unittest.TestCase):
    def test_codex_request_to_openai_chat_maps_messages_tools_and_schema(self):
        payload = codex_request_to_openai_chat(
            {
                "model": "gpt-5.4",
                "instructions": "Reply with JSON.",
                "stream": True,
                "input": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "hello"}],
                    },
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "lookup",
                        "arguments": "{\"q\":\"cache\"}",
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "call_1",
                        "output": "result",
                    },
                ],
                "tools": [
                    {
                        "type": "function",
                        "name": "lookup",
                        "description": "Query cache",
                        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
                    }
                ],
                "tool_choice": {"type": "function", "name": "lookup"},
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "answer",
                        "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
                        "strict": True,
                    }
                },
            }
        )
        self.assertEqual(payload["messages"][0], {"role": "system", "content": "Reply with JSON."})
        self.assertEqual(payload["messages"][1], {"role": "user", "content": "hello"})
        self.assertEqual(payload["messages"][2]["tool_calls"][0]["function"]["name"], "lookup")
        self.assertEqual(payload["messages"][3], {"role": "tool", "tool_call_id": "call_1", "content": "result"})
        self.assertEqual(payload["tools"][0]["function"]["name"], "lookup")
        self.assertEqual(payload["tool_choice"]["function"]["name"], "lookup")
        self.assertEqual(payload["response_format"]["type"], "json_schema")

    def test_codex_request_to_anthropic_maps_tools_and_reasoning(self):
        payload = codex_request_to_anthropic(
            {
                "model": "claude-sonnet-4-6",
                "instructions": "Be precise.",
                "stream": True,
                "reasoning": {"effort": "high"},
                "input": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "hello"}],
                    },
                    {
                        "type": "function_call",
                        "call_id": "toolu_1",
                        "name": "lookup",
                        "arguments": "{\"q\":\"cache\"}",
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "toolu_1",
                        "output": "{\"ok\":true}",
                    },
                ],
                "tools": [
                    {
                        "type": "function",
                        "name": "lookup",
                        "description": "Query cache",
                        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
                    }
                ],
                "tool_choice": {"type": "function", "name": "lookup"},
            }
        )
        self.assertEqual(payload["system"][0]["text"], "Be precise.")
        self.assertEqual(payload["messages"][0]["content"][0]["text"], "hello")
        self.assertEqual(payload["messages"][1]["content"][0]["type"], "tool_use")
        self.assertEqual(payload["messages"][1]["content"][0]["input"], {"q": "cache"})
        self.assertEqual(payload["messages"][2]["content"][0]["type"], "tool_result")
        self.assertEqual(payload["tool_choice"], {"type": "tool", "name": "lookup"})
        self.assertEqual(payload["thinking"]["budget_tokens"], 8192)

    def test_codex_request_to_gemini_maps_tool_config_and_json_mode(self):
        payload = codex_request_to_gemini(
            {
                "instructions": "Only JSON.",
                "reasoning": {"effort": "medium"},
                "input": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "hello"}],
                    },
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "lookup",
                        "arguments": "{\"q\":\"cache\"}",
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "call_1",
                        "name": "lookup",
                        "output": "{\"ok\":true}",
                    },
                ],
                "tools": [
                    {
                        "type": "function",
                        "name": "lookup",
                        "description": "Query cache",
                        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
                    }
                ],
                "tool_choice": {"type": "function", "name": "lookup"},
                "text": {"format": {"type": "json_object"}},
            }
        )
        self.assertEqual(payload["systemInstruction"]["parts"][0]["text"], "Only JSON.")
        self.assertEqual(payload["contents"][0]["parts"][0]["text"], "hello")
        self.assertEqual(payload["contents"][1]["parts"][0]["functionCall"]["name"], "lookup")
        self.assertEqual(payload["contents"][2]["parts"][0]["functionResponse"]["response"], {"ok": True})
        self.assertEqual(
            payload["toolConfig"],
            {"functionCallingConfig": {"mode": "SPECIFIC", "allowedFunctionNames": ["lookup"]}},
        )
        self.assertEqual(payload["generationConfig"]["responseMimeType"], "application/json")
        self.assertEqual(payload["generationConfig"]["thinkingConfig"]["thinkingBudget"], 4096)

    def test_openai_chat_response_to_codex_maps_text_tool_calls_and_usage(self):
        response = openai_chat_response_to_codex(
            {
                "id": "chatcmpl_1",
                "model": "gpt-5.4",
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "hello",
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {"name": "lookup", "arguments": "{\"q\":\"cache\"}"},
                                }
                            ],
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 4,
                    "prompt_tokens_details": {"cached_tokens": 3},
                },
            }
        )
        self.assertEqual(response["output"][0]["content"][0]["text"], "hello")
        self.assertEqual(response["output"][1]["name"], "lookup")
        self.assertEqual(response["usage"]["input_tokens"], 10)
        self.assertEqual(response["usage"]["input_tokens_details"]["cached_tokens"], 3)

    def test_anthropic_response_to_codex_maps_text_tool_calls_and_usage(self):
        response = anthropic_response_to_codex(
            {
                "id": "msg_1",
                "model": "claude-sonnet-4-6",
                "content": [
                    {"type": "text", "text": "hello"},
                    {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {"q": "cache"}},
                ],
                "usage": {"input_tokens": 11, "output_tokens": 5, "cache_read_input_tokens": 2},
            }
        )
        self.assertEqual(response["output"][0]["content"][0]["text"], "hello")
        self.assertEqual(response["output"][1]["call_id"], "toolu_1")
        self.assertEqual(response["usage"]["input_tokens_details"]["cached_tokens"], 2)

    def test_gemini_response_to_codex_maps_text_tool_calls_and_usage(self):
        response = gemini_response_to_codex(
            {
                "modelVersion": "gemini-2.5-pro",
                "candidates": [
                    {
                        "content": {
                            "role": "model",
                            "parts": [
                                {"text": "hello"},
                                {"functionCall": {"name": "lookup", "args": {"q": "cache"}}},
                            ],
                        }
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 9,
                    "candidatesTokenCount": 2,
                    "cachedContentTokenCount": 1,
                },
            }
        )
        self.assertEqual(response["output"][0]["content"][0]["text"], "hello")
        self.assertEqual(response["output"][1]["name"], "lookup")
        self.assertEqual(response["usage"]["input_tokens"], 9)
        self.assertEqual(response["usage"]["input_tokens_details"]["cached_tokens"], 1)

    def test_stream_openai_chat_to_codex_sse_emits_created_delta_and_completed(self):
        frames = list(
            stream_openai_chat_to_codex_sse(
                [
                    sse(
                        "message",
                        {
                            "id": "chatcmpl_1",
                            "model": "gpt-5.4",
                            "choices": [{"delta": {"content": "Hel"}}],
                        },
                    ),
                    sse(
                        "message",
                        {
                            "id": "chatcmpl_1",
                            "model": "gpt-5.4",
                            "choices": [{"delta": {"content": "lo"}}],
                        },
                    ),
                    "data: [DONE]\n\n",
                ]
            )
        )
        self.assertEqual(len(frames), 4)
        created = _parse_codex_frame(frames[0])
        first_delta = _parse_codex_frame(frames[1])
        completed = _parse_codex_frame(frames[-1])
        self.assertEqual(created["event"], "response.created")
        self.assertEqual(first_delta["data"]["delta"], "Hel")
        self.assertEqual(completed["data"]["response"]["output"][0]["content"][0]["text"], "Hello")

    def test_stream_anthropic_to_codex_sse_emits_text_and_tool_events(self):
        frames = list(
            stream_anthropic_to_codex_sse(
                [
                    sse("message_start", {"message": {"id": "msg_1", "model": "claude-sonnet-4-6"}}),
                    sse("content_block_delta", {"index": 0, "delta": {"type": "text_delta", "text": "Hi"}}),
                    sse(
                        "content_block_start",
                        {"index": 1, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "lookup"}},
                    ),
                    sse(
                        "content_block_delta",
                        {"index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"q\":\"cache\"}"}},
                    ),
                    sse("content_block_stop", {"index": 1}),
                    sse("message_stop", {}),
                ]
            )
        )
        parsed = [_parse_codex_frame(frame) for frame in frames]
        tool_done = next(frame for frame in parsed if frame["event"] == "response.function_call_arguments.delta")
        completed = parsed[-1]
        self.assertEqual(tool_done["event"], "response.function_call_arguments.delta")
        self.assertEqual(tool_done["data"]["delta"], "{\"q\":\"cache\"}")
        self.assertEqual(completed["data"]["response"]["output"][1]["name"], "lookup")

    def test_stream_gemini_to_codex_sse_emits_text_tool_and_usage(self):
        frames = list(
            stream_gemini_to_codex_sse(
                [
                    sse(
                        "message",
                        {
                            "modelVersion": "gemini-2.5-pro",
                            "candidates": [{"content": {"parts": [{"text": "Hi"}], "role": "model"}}],
                        },
                    ),
                    sse(
                        "message",
                        {
                            "modelVersion": "gemini-2.5-pro",
                            "candidates": [
                                {
                                    "content": {
                                        "parts": [{"functionCall": {"name": "lookup", "args": {"q": "cache"}}}],
                                        "role": "model",
                                    }
                                }
                            ],
                            "usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 2},
                        },
                    ),
                ]
            )
        )
        parsed = [_parse_codex_frame(frame) for frame in frames]
        tool_done = next(frame for frame in parsed if frame["event"] == "response.function_call_arguments.delta")
        completed = parsed[-1]
        self.assertEqual(tool_done["data"]["call_id"], "call_0")
        self.assertEqual(completed["data"]["response"]["usage"]["input_tokens"], 7)
        self.assertEqual(completed["data"]["response"]["output"][1]["arguments"], "{\"q\": \"cache\"}")


def _parse_codex_frame(frame):
    lines = [line for line in frame.strip().splitlines() if line]
    return {
        "event": lines[0].split(":", 1)[1].strip(),
        "data": json.loads(lines[1].split(":", 1)[1].strip()),
    }


if __name__ == "__main__":
    unittest.main()
