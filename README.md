# codex2gpt

一个独立的轻量 Python 版本，用来把 Codex 订阅直接转成本地 Responses API。默认模型是 `gpt-5.4`，也可以切到其他 Codex 支持的模型。

特点：

- 纯 Python 标准库
- 不依赖前端、数据库、Redis
- 多个 `oauth.json` 账号池
- 某个账号限流或失败时自动切到下一个账号
- 启动后直接提供本地 `/v1/responses` 接口
- 模型名直接透传给 Codex 上游

## 目录结构

```text
codex2gpt/
├── app.py
├── run.sh
├── README.md
└── runtime/
    ├── lite.env
    ├── server.pid
    ├── server.log
    └── accounts/
        ├── oauth-01.json
        ├── oauth-02.json
        └── ...
```

## 前提

- macOS / Linux
- 已安装 `python3`
- 已登录 Codex，并且本机存在 `~/.codex/auth.json`

如果还没登录：

```bash
codex login
```

## 一键启动

```bash
cd codex2gpt
./run.sh start
```

首次启动会自动：

1. 创建 `runtime/`
2. 生成 `runtime/lite.env`
3. 把当前 `~/.codex/auth.json` 导入为 `runtime/accounts/oauth-01.json`
4. 启动本地服务

## 模型选择

默认配置里会声明这几个模型：

- `gpt-5.4`
- `gpt-5.3-codex`

它们会出现在 `/v1/models` 里；默认请求模型由 `LITE_MODEL` 控制，可选模型列表由 `LITE_MODELS` 控制。

你可以在 `runtime/lite.env` 里改成自己的列表，例如：

```bash
LITE_MODEL=gpt-5.3-codex
LITE_MODELS=gpt-5.4,gpt-5.3-codex,gpt-5.1-codex,gpt-5.1-codex-max,gpt-5.1-codex-mini
```

重启后生效：

```bash
./run.sh restart
```

这个代理不会限制你请求里的 `model` 字段，实际能不能用，取决于你当前 Codex 账号对上游开放了哪些模型。

## 常用命令

```bash
./run.sh start
./run.sh stop
./run.sh restart
./run.sh status
./run.sh add-auth oauth-02
```

## 多账号

如果你切换了另一个 Codex 账号并重新登录：

```bash
codex login
./run.sh add-auth oauth-02
./run.sh restart
```

如果你已经有别的账号文件，也可以直接放进去：

```bash
cp /path/to/auth.json ./runtime/accounts/oauth-03.json
./run.sh restart
```

代理会按轮询使用这些账号；如果某个号返回 429/403/5xx 或网络错误，会自动切下一个号，并把失败账号短暂冷却。

## API

启动成功后会输出：

- Base URL
- API Key 状态

默认地址：

- Base URL: `http://127.0.0.1:18100/v1`
- Default Model: `gpt-5.4`

`LITE_API_KEY` 是可选的，默认留空。留空时不校验 API Key；如果你想加一层本地鉴权，手动在 `runtime/lite.env` 里填一个值后重启即可。

## 默认请求配置

这个代理除了透传请求参数，也会在你没有显式传值时补一组默认配置：

```json
{
  "model": "gpt-5.4",
  "reasoning": {"effort": "high"},
  "text": {"verbosity": "low"}
}
```

对应的本地配置项是：

```bash
LITE_MODEL=gpt-5.4
LITE_MODELS=gpt-5.4,gpt-5.3-codex
LITE_REASONING_EFFORT=high
LITE_TEXT_VERBOSITY=low
LITE_MODEL_CONTEXT_WINDOW=258400
LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT=232560
```

这两个值可以按 Codex 默认目录下的客户端配置生成：

- `LITE_REASONING_EFFORT` 优先读取 `~/.codex/config.toml` 里的 `model_reasoning_effort`
- `LITE_TEXT_VERBOSITY` 读取 `~/.codex/models_cache.json` 里当前模型的 `default_verbosity`
- `LITE_MODEL_CONTEXT_WINDOW` 优先读取 `~/.codex/config.toml`，没有时回退到 Codex 当前默认窗口
- `LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT` 优先读取 `~/.codex/config.toml`，没有时默认用窗口的 90%

## 高级参数

这个轻量代理会把大多数 Responses API 参数原样透传给 Codex 上游。当前已验证结果如下：

- `gpt-5.4`：`reasoning.effort=medium/high`、`text.verbosity=high`
- `gpt-5.3-codex`：默认可用；未显式指定时，上游返回 `reasoning.effort=medium`、`text.verbosity=medium`
- `text.verbosity=xhigh`：上游明确拒绝，当前正式可用值是 `low / medium / high`

也就是说，请求参数能透传，但具体支持哪些取值，仍然取决于模型本身。

示例：

```bash
curl http://127.0.0.1:18100/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-5.4",
    "input": "请用中文解释这段代码。",
    "stream": false,
    "reasoning": {"effort": "high"},
    "text": {"verbosity": "high"}
  }'
```

可选值建议：

- `reasoning.effort`：`low`、`medium`、`high`、`xhigh`
- `text.verbosity`：`low`、`medium`、`high`

需要区分两类配置：

- `reasoning.effort`、`text.verbosity` 这类是请求参数，代理可以直接透传。
- `context window`、最大输出上限这类主要是模型能力，不是这个代理自身的开关。
- 这个代理现在会按 `LITE_MODEL_CONTEXT_WINDOW` 和 `LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT` 做本地预检查，超过阈值会在本地直接拒绝，避免把明显超限的请求打到上游。

如果上游支持，同样可以继续传 `previous_response_id`、`truncation`、`prompt_cache_key` 等 Responses API 字段。

## 调用示例

```bash
curl http://127.0.0.1:18100/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.4","input":"用中文打个招呼。","stream":false}'
```

如果你配置了 `LITE_API_KEY`，再加上：

```bash
curl http://127.0.0.1:18100/v1/responses \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_LOCAL_API_KEY' \
  -d '{"model":"gpt-5.4","input":"用中文打个招呼。","stream":false}'
```

健康检查：

```bash
curl http://127.0.0.1:18100/health
```

模型列表：

如果你没有配置 `LITE_API_KEY`，模型列表也可以直接调用：

```bash
curl http://127.0.0.1:18100/v1/models
```

如果你配置了 `LITE_API_KEY`：

```bash
curl http://127.0.0.1:18100/v1/models \
  -H 'Authorization: Bearer YOUR_LOCAL_API_KEY'
```
