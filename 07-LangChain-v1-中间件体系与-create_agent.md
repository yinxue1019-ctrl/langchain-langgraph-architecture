---
title: 07-LangChain v1 中间件体系与 create_agent
tags: [AI, LangChain, v1, middleware, create_agent, 源码拆解, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 源码行号对应 langchain 1.4.8（2026-11-03）
---

# LangChain v1 中间件体系与 create_agent

**中间件（middleware）是 LangChain v1 最重要的新概念**，也是 v1 相对 v0 最本质的架构转变：Agent 不再是"一个大函数 + 一堆可选项参数"，而是"一个循环 + 一条中间件链"。

行号对应 `langchain` 1.4.8（2026-11-03 快照），源码位于 `langchain/libs/langchain_v1/langchain/agents/`。

## 一、v0 → v1 的范式转变

| | v0 | v1 |
|---|---|---|
| Agent 构造 | `AgentExecutor(agent=..., tools=..., memory=..., callbacks=...)` | `create_agent(model, tools, middleware=[...])` |
| 定制方式 | 继承子类、覆盖方法 | 组合中间件 |
| 扩展点 | 散落在各处 | 统一的 7 个钩子 |
| 状态 | 隐式在 agent scratchpad | 显式 state schema |

v0 的痛点：想加"工具重试"、"限流"、"摘要压缩"，要么继承覆盖内部方法，要么改源码。v1 把它变成"往列表里加一个对象"。

## 二、AgentMiddleware 基类

定义在 `agents/middleware/types.py:385`：

```python
class AgentMiddleware(Generic[StateT, ContextT, ResponseT]):
    state_schema: type[StateT] = cast("type[StateT]", _DefaultAgentState)
    tools: Sequence[BaseTool]
    trace_policy: TracePolicy | None = None
    transformers: Sequence[TransformerFactory] = ()
```

三个类属性：

| 属性 | 作用 |
|---|---|
| `state_schema` | 该中间件需要的额外状态字段 |
| `tools` | 该中间件注册的工具 |
| `trace_policy` | 控制该中间件的 trace span 记录什么（如 `omit_payload` 丢弃消息内容） |
| `transformers` | 注册流式投影转换器（每个 factory 收到 `scope` 参数，返回新实例） |

## 三、七个钩子：完整契约

### 1. `before_agent` / `abefore_agent`

`types.py:431` / `442`

```python
def before_agent(self, state: StateT, runtime: Runtime[ContextT]) -> dict[str, Any] | None
```

- **时机**：Agent 循环开始前，整个运行只执行一次
- **入参**：当前 state + runtime 上下文
- **出参**：返回 dict → 作为状态更新写入；返回 `None` → 不改状态
- **用途**：初始化会话、预加载数据、校验输入

### 2. `before_model` / `abefore_model`

`types.py:455` / `466`

```python
def before_model(self, state: StateT, runtime: Runtime[ContextT]) -> dict[str, Any] | None
```

- **时机**：每次模型调用前
- **用途**：注入动态提示、裁剪历史、附加检索结果

源码里还有装饰器版本 `before_model`（`types.py:934`），支持直接标注函数：

```python
@before_model
def inject_context(state, runtime): ...
```

`hook_config`（`types.py:879`）可以给钩子加跳转配置。

### 3. `wrap_model_call` / `awrap_model_call`

`types.py:503` / `598` —— **最有价值的钩子**

```python
def wrap_model_call(
    self,
    request: ModelRequest[ContextT],
    handler: Callable[[ModelRequest[ContextT]], ModelResponse[ResponseT]],
) -> ModelResponse[ResponseT] | AIMessage | ExtendedModelResponse[ResponseT]
```

源码文档（`types.py:508-515`）说得很清楚：

> The handler callback executes the model request and returns a `ModelResponse`. Middleware can call the handler **multiple times for retry logic**, **skip calling it to short-circuit**, or **modify the request/response**. Multiple middleware compose with **first in list as outermost layer**.

三个关键能力：

| 能力 | 做法 |
|---|---|
| 重试 | `for attempt in range(3): try: return handler(request) except: ...` |
| 短路 | 不调 handler，直接返回 `ModelResponse(result=[AIMessage(...)])` |
| 改写 | `request = request.override(...)` 后再调 handler |

**这就是"洋葱模型"**——middleware 嵌套包裹模型调用，列表里第一个是最外层。

对比 `before_model`：`before_model` 只能"改状态"，`wrap_model_call` 能"改请求 + 改响应 + 不调模型 + 调多次"。后者表达能力更强。

### 4. `after_model` / `aafter_model`

`types.py:479` / `490`

```python
def after_model(self, state: StateT, runtime: Runtime[ContextT]) -> dict[str, Any] | None
```

- **时机**：模型返回后、工具执行前
- **用途**：检查模型输出、强制终止循环、校验结构化输出

### 5. `wrap_tool_call` / `awrap_tool_call`

`types.py:674` / `756`

```python
def wrap_tool_call(
    self,
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
) -> ToolMessage | Command[Any]
```

- **时机**：每个工具调用执行时
- **入参**：`ToolCallRequest` 含 `request.tool_call`（字典）、`request.tool`（BaseTool 实例）、`request.state`、`request.runtime`
- **出参**：`ToolMessage` 或 `Command`
- **用途**：重试、改参数、监控、脱敏、模拟

文档强调（`types.py:696-698`）：*"The handler Callable can be invoked multiple times for retry logic. Each call to handler is independent and stateless."*

**同步/异步陷阱**：`wrap_tool_call` 的默认实现直接抛 `NotImplementedError`（`types.py:744-754`），错误提示写得很清楚——如果你只实现了 `awrap_tool_call` 却在同步上下文调用，会得到一条指明三条解决路径的异常。

### 6. `after_agent` / `aafter_agent`

`types.py:650` / `661`

- **时机**：Agent 循环结束后
- **用途**：清理资源、汇总统计、持久化最终结果

### 钩子全表

| 钩子 | 频率 | 能改状态 | 能改请求/响应 | 能短路 |
|---|---|---|---|---|
| `before_agent` | 1 次 | ✅ | ❌ | ❌ |
| `before_model` | 每轮 | ✅ | ❌ | ❌ |
| `wrap_model_call` | 每轮 | ❌ | ✅ | ✅ |
| `after_model` | 每轮 | ✅ | ❌ | ❌ |
| `wrap_tool_call` | 每个工具 | ❌ | ✅ | ✅ |
| `after_agent` | 1 次 | ✅ | ❌ | ❌ |

**记忆口诀**：带 `wrap_` 的能改请求和响应、能短路；不带 `wrap_` 的只能改状态。

## 四、ModelRequest：中间件的输入契约

`ModelRequest` 定义在 `types.py:88`，是一个可 `override()` 的请求对象：

```python
class ModelRequest(Generic[ContextT]):
    def __init__(self, ...)
    @property
    def system_prompt(self) -> str | None: ...
    def override(self, **overrides) -> ModelRequest[ContextT]: ...
```

可覆盖的字段（`_ModelRequestOverrides`，`types.py:74`）：`model`、`system_message`、`messages`、`tools`、`tool_choice` 等。

`override()` 返回**新对象**（`types.py:203`），不修改原对象——这保证了多个 middleware 各自 override 时互不干扰。

## 五、官方内置中间件清单

`agents/middleware/__init__.py:5-51` 导出了全部内置实现：

| 中间件 | 文件 | 用途 |
|---|---|---|
| `AgentMiddleware` | `types.py:385` | 基类 |
| `ModelRetryMiddleware` | `model_retry.py` | 模型调用失败重试 |
| `ModelFallbackMiddleware` | `model_fallback.py` | 模型降级链（主模型挂了换备用） |
| `ModelCallLimitMiddleware` | `model_call_limit.py` | 限制模型调用次数（防死循环） |
| `ToolRetryMiddleware` | `tool_retry.py` | 工具调用重试 |
| `ToolErrorMiddleware` | `tool_error.py` | 工具错误处理策略 |
| `ToolCallLimitMiddleware` | `tool_call_limit.py` | 限制工具调用次数 |
| `LLMToolSelectorMiddleware` | `tool_selection.py` | 用 LLM 动态挑选可用工具（工具太多时省 token） |
| `LLMToolEmulator` | `tool_emulator.py` | 用 LLM 模拟工具返回（工具未就绪时开发/测试） |
| `SummarizationMiddleware` | `summarization.py` | 历史消息摘要压缩（带 `TriggerClause` 触发条件） |
| `ContextEditingMiddleware` | `context_editing.py` | 上下文编辑（`ClearToolUsesEdit` 清理旧工具结果） |
| `TodoListMiddleware` | `todo.py` | 任务清单管理 |
| `PIIMiddleware` | `pii.py` | PII 检测与脱敏（`PIIMatch`、`PIIDetectionError`） |
| `HumanInTheLoopMiddleware` | `human_in_the_loop.py` | 人工确认（`InterruptOnConfig`） |
| `ShellToolMiddleware` | `shell_tool.py` | Shell 工具 + 执行策略（`DockerExecutionPolicy`、`CodexSandboxExecutionPolicy`、`HostExecutionPolicy`） |
| `FilesystemFileSearchMiddleware` | `file_search.py` | 文件检索 |
| `ProviderToolSearchMiddleware` | `provider_tool_search.py` | 供应商侧工具检索 |

**这份清单本身就是产品需求清单。** 小雪如果你在做 Agent 产品的功能规划，这 17 个中间件覆盖了 90% 的常见需求：

- 成本控制 → `LLMToolSelectorMiddleware`、`SummarizationMiddleware`、`ContextEditingMiddleware`
- 稳定性 → `ModelRetryMiddleware`、`ModelFallbackMiddleware`、`ToolRetryMiddleware`
- 安全 → `PIIMiddleware`、`ShellToolMiddleware` 的执行策略
- 人工介入 → `HumanInTheLoopMiddleware`
- 防失控 → `ModelCallLimitMiddleware`、`ToolCallLimitMiddleware`

## 六、create_agent：中间件如何组装成图

`agents/factory.py`（2164 行）实现 `create_agent`。它 **用 LangGraph 搭出 Agent**：

```python
from langgraph.graph.state import StateGraph
from langgraph.prebuilt.tool_node import ToolNode
from langgraph.types import Command, Send
from langgraph.constants import END, START
```

（`factory.py:31-36`）

### 状态 schema

`AgentState`（`types.py:349`）：

```python
class AgentState(TypedDict, Generic[ResponseT]):
    messages: Annotated[list[AnyMessage], add_messages]
    ...
```

`InputAgentState`（`types.py:357`）/ `OutputAgentState`（`types.py:363`）分别定义输入输出边界。

### 结构化输出

`agents/structured_output.py` 定义了四种策略：

| 策略 | 说明 |
|---|---|
| `ToolStrategy` | 用工具调用拿结构化输出（默认，兼容性最好） |
| `ProviderStrategy` | 用供应商原生能力（如 OpenAI 的 structured outputs） |
| `ProviderStrategyBinding` | 绑定形式 |
| `AutoStrategy` | 自动选择 |

错误分类：`StructuredOutputError`、`StructuredOutputValidationError`、`MultipleStructuredOutputsError`。

### 动态工具的错误提示

`factory.py:127-151` 有一段精心写的错误模板 `DYNAMIC_TOOL_ERROR_TEMPLATE`，说明了一个常见坑：

> Middleware added tools that the agent doesn't know how to execute.

如果中间件在 `wrap_model_call` 里往 `request.tools` 加了工具，但这些工具没在 `create_agent(tools=[...])` 注册，agent 会报这个错。模板给出了两条解法：注册到 agent，或在 `wrap_tool_call` 里自己执行。

**这类"错误信息就是文档"的做法值得学习**——比让用户去翻文档高效得多。

### 动态提示

`dynamic_prompt` 也在导出列表里（`types.py:46` 导出于 `__init__.py:46`）——支持运行时动态系统提示。

## 七、这一层的设计评价

**收益：**

- **组合优于继承**：17 个内置中间件全部可组合，需求覆盖度高
- **洋葱模型统一**：模型调用和工具调用都是"可包裹"语义，心智模型一致
- **可测试**：中间件是独立对象，可单独测试
- **可观测**：`trace_policy` + `omit_payload` 支持精细化埋点

**代价：**

- 6 个钩子 × 2（同步/异步）= 12 个方法要理解
- `wrap_*` 的同步/异步陷阱容易踩（默认抛 `NotImplementedError`）
- `before_model` vs `wrap_model_call` 的能力边界需要记忆
- 中间件数量多时，执行链的顺序和嵌套关系不易追踪

**给团队的实践建议**：
1. 优先用内置中间件，别自己从头写
2. 需要"改模型请求/响应"就用 `wrap_model_call`，别硬塞 `before_model`
3. 自定义中间件**同步异步都实现**，否则用户在同步场景直接崩
4. 中间件列表控制在 5 个以内，顺序写清楚注释

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 上一篇：[[06-LangChain-v1-架构与代码结构]] · 下一篇：[[08-生态与生产化：持久化实现、流式接口、MCP 与部署形态]]
