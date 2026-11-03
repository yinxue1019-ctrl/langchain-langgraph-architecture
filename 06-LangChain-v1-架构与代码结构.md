---
title: 06-LangChain v1 架构与代码结构
tags: [AI, LangChain, v1, Runnable, 源码拆解, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 源码行号对应 langchain-core 1.7.4 / langchain 1.4.8（2026-11-03）
---

# LangChain v1 架构与代码结构

LangChain 经历了 v0 的"万物皆 chain"、v1 的"agent-first"两次范式转移。这篇讲 v1 的代码结构，以及最核心的 Runnable 协议。

行号对应 `langchain-core` 1.7.4 与 `langchain` 1.4.8（2026-11-03 快照）。

## 一、仓库分层：三个包，三种命运

`langchain` 仓库的 `libs/` 布局：

| 包 | 发行名 | 版本 | 状态 |
|---|---|---|---|
| `libs/core` | `langchain-core` | 1.7.4 | 现行，基础协议层 |
| `libs/langchain_v1` | `langchain` | 1.4.8 | 现行，v1 主体（agents/middleware/tools/mcp） |
| `libs/langchain` | `langchain-classic` | 1.0.8 | **已改名**，v0 遗留兼容层 |
| `libs/text-splitters` | `langchain-text-splitters` | — | 独立发布 |
| `libs/partners` | 各种 `langchain-*-partner` | — | 第三方集成（openai/anthropic 等） |
| `libs/model-profiles` | — | — | 模型能力画像数据 |
| `libs/standard-tests` | — | — | 跨包一致性测试套件 |

### 关键变化：v0 包改名为 `langchain-classic`

这是本次快照里最容易被忽略但最重要的信号：`libs/langchain/` 目录还在，但**发行名已经改成 `langchain-classic`**（版本 1.0.8）。

含义很明确：v0 的那一堆（chains、agents、legacy memory、LLM 抽象）被正式标记为"经典/遗留"，冻结维护。而 `langchain` 这个包名现在指向 `libs/langchain_v1`。

**给团队的迁移含义**：新项目装 `langchain` 拿到的是 v1；老代码里的 `from langchain.chains import ...` 走的是 classic 兼容层。两套 API 会共存一段时间。

### 依赖方向

```
langchain-core  ←  langchain (v1)  ←  langchain-classic
      ↑                  ↑
      └──── langgraph ───┘
```

`langchain-core` 不依赖 langgraph。`langchain` (v1) 依赖 langgraph（`langchain/agents/factory.py:31-36` 导入了 `StateGraph`、`Command`、`Send`、`ToolNode`）。

## 二、langchain-core 模块地图

`langchain_core/` 的子包职责：

| 子包 | 职责 | 关键文件 |
|---|---|---|
| `language_models/` | LLM 抽象（chat/base） | `chat_models.py`、`llms.py` |
| `messages/` | 消息类型体系 | `base.py`、`ai.py`、`human.py`、`system.py`、`tool.py`、`content.py` |
| `prompts/` | 提示模板 | `base.py`、`chat.py`、`string.py`、`few_shot.py` |
| `runnables/` | **统一执行协议**（核心） | `base.py`(6729行)、`branch.py`、`config.py` |
| `tools/` | 工具定义 | `base.py`、`structured.py`、`convert.py` |
| `output_parsers/` | 输出解析 | `base.py`、`str.py`、`json.py`、`pydantic.py`、`xml.py` |
| `documents/` | 文档结构 | `base.py` |
| `vectorstores/` | 向量库接口 | `base.py`、`in_memory.py` |
| `retrievers/` | 检索器接口 | `retrievers.py` |
| `embeddings/` | 嵌入接口 | `embeddings.py` |
| `structured_query/` | LCEL 自文档 DSL | `langchain_core/structured_query/` |
| `stores/` | 长期记忆 KV 接口 | `stores.py` |
| `tracers/` | 回调/追踪 | `base.py`、`core.py`、`langchain.py` |
| `rate_limiters/` | 限流器 | `base.py` |
| `outputs/` | 生成结果结构 | `generation.py`、`chat_result.py`、`llm_result.py` |
| `utils/` | 工具函数 | `function_calling.py`、`json_schema.py` |

**为什么 RAG 组件在 core 里？** 因为 `runnables/` 是协议的根，而检索器、向量库都必须实现 Runnable 接口才能进链。把它们放在 core 保证依赖单向、无循环。

## 三、Runnable 协议：LangChain 的地基

`Runnable` 定义在 `runnables/base.py:134`，6729 行的文件里最核心的一段。

### 方法矩阵

| 方法 | 行号 | 语义 |
|---|---|---|
| `invoke` / `ainvoke` | 887 | 单输入 → 单输出 |
| `batch` / `abatch` | 932 | 多输入并行处理 |
| `batch_as_completed` / `abatch_as_completed` | 983 / 1116 | 多输入，流式返回先完成的 |
| `stream` / `astream` | 1195 | 单输入，流式输出 |
| `transform` / `atransform` | 1761 | 输入可迭代 → 输出可迭代（映射语义） |
| `astream_log` | 1238 | 流式输出 + 中间过程日志 |
| `astream_events` / `stream_events` | 1344 / 1675 | 结构化事件流（v1/v2 协议） |

### 内置优化的两个默认行为

源码文档字符串（`base.py:146-153`）明确写了两条：

1. **Batch 默认并行**：*"By default, batch runs invoke() in parallel using a thread pool executor. Override to optimize batching."*
2. **Async 默认降级**：*"Methods with 'a' prefix are asynchronous. By default, they execute the sync counterpart using asyncio's thread pool."*

第 2 条是很多性能问题的根源：**用了 `ainvoke` 不代表底层是异步的**。如果底层实现是同步的，asyncio 线程池会引入额外开销。真正原生的异步要靠模型集成自己实现。

### `|` 运算符

`__or__` 定义在 `base.py:649`：

```python
def __or__(self, other) -> RunnableSerializable[Input, Any]:
    return RunnableSequence(self, coerce_to_runnable(other))
```

三种重载（`base.py:629/634/641`）覆盖：

- `Mapping[str, Runnable]` → `RunnableParallel`
- `Callable[[Output], Runnable]` → 转 Runnable 后串联
- `Runnable` → `RunnableSequence`

**`coerce_to_runnable`（`base.py:6644`）做自动类型提升**：

| 输入类型 | 提升为 | 行号 |
|---|---|---|
| 已是 `Runnable` | 原样返回 | 6656 |
| 生成器函数 / 异步生成器 | `RunnableGenerator` | 6658 |
| 普通 callable | `RunnableLambda` | 6660 |
| `dict` | `RunnableParallel` | 6662 |
| 其他 | 抛 `TypeError` | 6664 |

所以 `prompt | model | parser` 里即使 `model` 不是 Runnable（比如是 `str`）也能隐式包装。

#### 序列拼接是扁平化的

**`RunnableSequence` 覆盖了 `__or__`（`base.py:3358`）**——这是 `|` 最重要的语义：

```python
if isinstance(other, RunnableSequence):
    return RunnableSequence(self.first, *self.middle, self.last,
                            other.first, *other.middle, other.last,
                            name=self.name or other.name)
```

即 **拼接而非嵌套**：`a | (b | c)` 得到 `RunnableSequence(a, b, c)`，不是 `RunnableSequence(a, RunnableSequence(b, c))`。

**为什么重要**：避免深层嵌套导致的递归栈问题、追踪树碎片化、schema 推导困难。同时 `name` 优先取左侧。

`__ror__`（`base.py:698`）对称处理 `"x" | runnable` 的反向场景；`RunnableSequence` 也覆盖了 `__ror__`（`base.py:3407`）做同样的扁平化。

**注意实现细节**（`base.py:668-672`）：**它不返回 `NotImplemented`**，注释说明了原因——*"to avoid delegating LCEL composition to unrelated reflected `|` implementations"*。这是个防御性设计：避免和 numpy 数组、pandas 序列等的 `|` 运算意外交互。

### 统一追踪外壳：_call_with_config

`base.py:2272` 的 `_call_with_config` 是所有 Runnable 的统一外壳：

```
Runnable.invoke(输入)
  ├─ ensure_config(config)                                  # :2291
  ├─ get_callback_manager_for_config(config)                 # :2292
  ├─ callback_manager.on_chain_start(...)                    # :2293-2299
  ├─ child_config = patch_config(config, callbacks=run_manager.get_child())  # :2301
  ├─ with set_config_context(child_config) as context:       # :2302
  │    context.run(call_func_with_variable_args, func, input_, config, run_manager)
  ├─ 成功: run_manager.on_chain_end(output)                  # :2318
  └─ 失败: run_manager.on_chain_error(e) → re-raise          # :2314-2316
```

三个要点：

1. **回调 span 与业务逻辑解耦**——每个 Runnable 调用自动产生一个 chain run，父子关系通过 `run_manager.get_child()` 向下传递（LangSmith 追踪树的来源）
2. **`set_config_context` 用 ContextVar 传递 config**——嵌套 Runnable 无需显式传参即可继承 `tags`/`metadata`/`callbacks`
3. **`call_func_with_variable_args` 按函数签名决定传什么**——这是 `RunnableLambda` 支持 `(x)`、`(x, config)`、`(x, run_manager, config)` 三种签名的机制

异步版 `_acall_with_config`（`:2321`）结构对称。批量路径 `_batch_with_config`（`:2364`）、流式路径 `_transform_stream_with_config`（`:2506`）同理。

### 组合原语

`runnables/__init__.py:63-93` 导出了全部原语：

| 类 | 位置 | 作用 |
|---|---|---|
| `RunnableSequence` | `base.py:3079` | **线性组合**，按序执行，前一步输出即后一步输入 |
| `RunnableParallel` | `base.py:3868` | **并行扇出**，输入同时喂给多个分支，输出为 `{key: result}` |
| `RunnableLambda` | `base.py:4707` | 包装任意可调用对象，支持同步/异步/生成器/异步生成器四种签名 |
| `RunnableGenerator` | `base.py:4403` | 把生成器函数包装为流式 Runnable |
| `RunnablePassthrough` | `passthrough.py:74` | 恒等透传；若输入是 dict 可附加新 key |
| `RunnableAssign` | `passthrough.py:352` | 基于 `RunnableParallel` 计算后**合并回原 dict**，即 `x \| {"k": fn}` 的底层实现 |
| `RunnablePick` | `passthrough.py:679` | 从 dict 中挑选指定 key |
| `RunnableBranch` | `branch.py:43` | 条件分支：按 `(condition, runnable)` 列表**取第一个为真的分支**，全不中走 default（至少 2 个分支，`branch.py:97`） |
| `RouterRunnable` | `router.py:46` | 模型决定路由到哪个分支的自路由 Runnable |
| `RunnableWithFallbacks` | `fallbacks.py:37` | 主 Runnable 失败时按顺序尝试备选（主模型挂了降级到备用供应商） |
| `RunnableRetry` | `retry.py:50` | 基于 tenacity 的指数退避重试；官方建议**只包住最可能失败的那一段**而非整条链 |
| `RunnableConfigurableFields` | `configurable.py:317` | 运行时通过 `config={"configurable": {...}}` 动态改写字段（如 temperature） |
| `RunnableConfigurableAlternatives` | `configurable.py:474` | 运行时在多个候选实现间切换（换模型供应商） |
| `RunnableWithMessageHistory` | `history.py:40` | 自动从 `BaseChatMessageHistory` 读入历史、调用后写回；按 `config["configurable"]["session_id"]` 隔离会话 |
| `RunnableBinding` / `RunnableBindingBase` | `base.py:5855` / `:6382` | 把 kwargs、config、listeners 绑定到目标 Runnable 上的通用包装器 |
| `RunnableSerializable` | `base.py:2831` | 增加 `to_json` / `configurable_fields` / `configurable_alternatives` 的可序列化基类 |

`RunnableLambda` 的 `deps` 参数（`base.py:5073`）能声明依赖关系，用来建立追踪边。

其他文件：

| 文件 | 类 |
|---|---|
| `runnables/graph.py` | `RunnableGraph`（可视化用） |
| `runnables/graph_mermaid.py` / `graph_ascii.py` / `graph_png.py` | 多种渲染后端 |

### 统一协议的收益

所有组件都实现同一套方法，带来三个直接好处：

1. **组合免费**：拼出来的链自动获得 sync/async/batch/streaming 四种能力
2. **可观测统一**：tracer 只要认 Runnable，就能追踪整条链
3. **可替换**：任何组件都能换成同协议的实现（如换模型、换向量库）

**代价**：Runnable 的方法矩阵很大，实现一个自定义组件要覆盖很多方法（虽然基类给了默认降级实现）。

## 四、这一层的设计评价

**收益：**

- 统一协议是 LangChain 最有价值的抽象——一次定义，四种执行模式
- `|` 语法让声明式链可读性极高
- core 不依赖 langgraph，依赖方向干净

**代价：**

- `base.py` 6729 行，单文件过大
- async 默认降级成线程池，容易误判性能
- `__or__` 的非标准行为（不返回 `NotImplemented`）在与其他库混用时需要留意

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 上一篇：[[05-持久化与中断：checkpointer、interrupt 与时间旅行]] · 下一篇：[[07-LangChain-v1-中间件体系与-create_agent]]
