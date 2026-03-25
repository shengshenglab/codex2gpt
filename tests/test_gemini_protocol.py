import json
import unittest

from codex2gpt.protocols.gemini import (
    codex_response_to_gemini,
    parse_model_action,
    prepare_schema,
    reconvert_tuple_values,
    stream_gemini_sse_from_codex_events,
    translate_gemini_request,
)


class GeminiProtocolTests(unittest.TestCase):
    def test_parse_model_action(self):
        parsed = parse_model_action("gemini-2.5-pro:generateContent")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.model, "gemini-2.5-pro")
        self.assertEqual(parsed.action, "generateContent")
        self.assertIsNone(parse_model_action("gemini-2.5-pro"))

    def test_prepare_schema_converts_tuple_arrays(self):
        prepared = prepare_schema(
            {
                "type": "array",
                "prefixItems": [
                    {"type": "string"},
                    {"type": "integer"},
                ],
            }
        )
        self.assertIsNotNone(prepared.original_schema)
        self.assertEqual(prepared.schema["type"], "object")
        self.assertEqual(prepared.schema["required"], ["item_0", "item_1"])
        self.assertFalse(prepared.schema["additionalProperties"])

    def test_reconvert_tuple_values(self):
        schema = {
            "type": "array",
            "prefixItems": [
                {"type": "string"},
                {"type": "integer"},
            ],
        }
        value = {"item_0": "alpha", "item_1": 3}
        self.assertEqual(reconvert_tuple_values(value, schema), ["alpha", 3])

    def test_translate_gemini_request_supports_schema_and_tools(self):
        translated = translate_gemini_request(
            {
                "systemInstruction": {"parts": [{"text": "只输出 JSON"}]},
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": "hi"},
                            {"functionCall": {"name": "lookup", "args": {"q": "cache"}}},
                            {"functionResponse": {"name": "lookup", "response": {"ok": True}}},
                        ],
                    }
                ],
                "tools": [
                    {
                        "functionDeclarations": [
                            {
                                "name": "lookup",
                                "description": "query cache",
                                "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
                            }
                        ]
                    }
                ],
                "toolConfig": {"functionCallingConfig": {"mode": "SPECIFIC", "allowedFunctionNames": ["lookup"]}},
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "responseSchema": {
                        "type": "array",
                        "prefixItems": [{"type": "string"}, {"type": "integer"}],
                    },
                    "thinkingConfig": {"thinkingBudget": 5000},
                },
            },
            "gemini-2.5-pro",
        )
        payload = translated.codex_request
        self.assertEqual(payload["model"], "gemini-2.5-pro")
        self.assertEqual(payload["instructions"], "只输出 JSON")
        self.assertEqual(payload["reasoning"]["effort"], "high")
        self.assertEqual(payload["tools"][0]["name"], "lookup")
        self.assertEqual(payload["tool_choice"], {"type": "function", "name": "lookup"})
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertIsNotNone(translated.tuple_schema)
        self.assertEqual(payload["input"][0]["role"], "user")
        self.assertEqual(payload["input"][1]["type"], "function_call")
        self.assertEqual(payload["input"][2]["type"], "function_call_output")

    def test_translate_gemini_request_supports_json_object_mode(self):
        translated = translate_gemini_request(
            {
                "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                "generationConfig": {"responseMimeType": "application/json"},
            },
            "gemini-2.0-flash",
        )
        self.assertEqual(translated.codex_request["text"]["format"]["type"], "json_object")
        self.assertIsNone(translated.tuple_schema)

    def test_codex_response_to_gemini_supports_tool_calls(self):
        response = codex_response_to_gemini(
            {
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "hello"}],
                    },
                    {
                        "type": "function_call",
                        "name": "lookup",
                        "arguments": "{\"q\":\"cache\"}",
                    },
                ],
                "usage": {
                    "input_tokens": 12,
                    "output_tokens": 4,
                    "input_tokens_details": {"cached_tokens": 2},
                },
            },
            "gemini-2.5-pro",
        )
        parts = response["candidates"][0]["content"]["parts"]
        self.assertEqual(parts[0]["text"], "hello")
        self.assertEqual(parts[1]["functionCall"]["name"], "lookup")
        self.assertEqual(parts[1]["functionCall"]["args"], {"q": "cache"})
        self.assertEqual(response["usageMetadata"]["cachedContentTokenCount"], 2)

    def test_codex_response_to_gemini_reconverts_tuple_output(self):
        schema = {"type": "array", "prefixItems": [{"type": "string"}, {"type": "integer"}]}
        response = codex_response_to_gemini(
            {
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "{\"item_0\":\"alpha\",\"item_1\":7}"}],
                    }
                ],
                "usage": {},
            },
            "gemini-2.5-pro",
            tuple_schema=schema,
        )
        self.assertEqual(response["candidates"][0]["content"]["parts"][0]["text"], json.dumps(["alpha", 7], ensure_ascii=False))

    def test_stream_gemini_sse_from_codex_events(self):
        chunks = list(
            stream_gemini_sse_from_codex_events(
                [
                    {
                        "event": "response.output_text.delta",
                        "data": {"type": "response.output_text.delta", "delta": "Hel"},
                    },
                    {
                        "event": "response.function_call_arguments.done",
                        "data": {
                            "type": "response.function_call_arguments.done",
                            "name": "lookup",
                            "arguments": "{\"q\":\"cache\"}",
                        },
                    },
                    {
                        "event": "response.completed",
                        "data": {
                            "type": "response.completed",
                            "response": {
                                "status": "completed",
                                "usage": {
                                    "input_tokens": 10,
                                    "output_tokens": 3,
                                    "input_tokens_details": {"cached_tokens": 1},
                                },
                            },
                        },
                    },
                ],
                "gemini-2.5-pro",
            )
        )
        self.assertEqual(len(chunks), 3)
        first = json.loads(chunks[0][6:].strip())
        second = json.loads(chunks[1][6:].strip())
        third = json.loads(chunks[2][6:].strip())
        self.assertEqual(first["candidates"][0]["content"]["parts"][0]["text"], "Hel")
        self.assertEqual(second["candidates"][0]["content"]["parts"][0]["functionCall"]["name"], "lookup")
        self.assertEqual(third["usageMetadata"]["totalTokenCount"], 13)

    def test_stream_gemini_sse_reconverts_tuple_buffer(self):
        schema = {"type": "array", "prefixItems": [{"type": "string"}, {"type": "integer"}]}
        chunks = list(
            stream_gemini_sse_from_codex_events(
                [
                    {
                        "event": "response.output_text.delta",
                        "data": {"type": "response.output_text.delta", "delta": "{\"item_0\":\"x\",\"item_1\":9}"},
                    },
                    {
                        "event": "response.completed",
                        "data": {"type": "response.completed", "response": {"status": "completed", "usage": {}}},
                    },
                ],
                "gemini-2.5-pro",
                tuple_schema=schema,
            )
        )
        first = json.loads(chunks[0][6:].strip())
        self.assertEqual(first["candidates"][0]["content"]["parts"][0]["text"], json.dumps(["x", 9], ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
