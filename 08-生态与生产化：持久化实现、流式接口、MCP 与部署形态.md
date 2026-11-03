---
title: 08-生态与生产化：持久化实现、流式接口、MCP 与部署形态
tags: [AI, LangGraph, 生态, 持久化, streaming, MCP, 部署, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 版本号对应 2026-11-03 源码快照
---

# 生态与生产化：持久化实现、流式接口、MCP 与部署形态

前面几篇讲的是"怎么用"。这篇讲"上生产要准备什么"。

行号对应 2026-11-03 源码快照。

## 一、版本现状与依赖矩阵

| 包 | 发行名 | 版本 | Python |
|---|---|---|---|
| `libs/langgraph` | `langgraph` | 1.2.19 | >=3.10 |
| `libs/checkpoint` | `langgraph-checkpoint` | 4.3.1 | >=3.10 |
| `libs/checkpoint-postgres` | `langgraph-checkpoint-postgres` | 3.2.0 | >=3.10 |
| `libs/checkpoint-sqlite` | `langgraph-checkpoint-sqlite` | 3.1.3 | >=3.10 |
| `libs/prebuilt` | `langgraph-prebuilt` | 1.1.1 | >=3.10 |
| `libs/sdk-py` | `langgraph-sdk` | — | >=3.10 |
| `libs/core` | `langchain-core` | 1.7.4 | >=3.10 |
| `libs/langchain_v1` | `langchain` | 1.4.8 | >=3.10 |
| `libs/langchain` | `langchain-classic` | 1.0.8 | >=3.10 |

**已放弃 Python 3.9**——全部包统一 `requires-python = ">=3.10"`。

关键依赖：

- `langgraph-checkpoint` → `langchain-core>=0.2.38`、`ormsgpack>=1.12.0`
- `langgraph-checkpoint-postgres` → `psycopg>=3.2.0`、`psycopg-pool>=3.2.0`

**序列化用 ormsgpack 而不是标准库 pickle**——性能与安全兼顾（比 pickle 安全，比 json 快）。

## 二、Checkpointer 实现家族

`BaseCheckpointSaver` 定义在 `langgraph/checkpoint/base/__init__.py`（889 行）。同步接口与行号：

| 方法 | 行号 | 作用 |
|---|---|---|
| `get` | 250 | 取 checkpoint |
| `get_tuple` | 262 | 取 checkpoint 元组（含 pending_writes） |
| `list` | 277 | 列出历史（时间旅行） |
| `put` | 301 | 写入 checkpoint |
| `put_writes` | 324 | 写入任务中间结果 |
| `delete_thread` | 344 | 删除整条线程 |

异步版本从 `base/__init__.py:441` 开始（`aget`、`aget_tuple`、`alist`、`aput`、`aput_writes`、`adelete_thread`）。

### 三种实现的存储布局

| 实现 | 存储 | 适用场景 |
|---|---|---|
| `InMemorySaver`（`checkpoint/memory/__init__.py`，707 行） | 进程内字典 | 开发测试，单进程 |
| `SqliteSaver`（`checkpoint-sqlite`） | 单文件 SQLite | 单机部署、边缘场景 |
| `PostgresSaver`（`checkpoint-postgres`） | Postgres 表 | **生产标准**，多实例 |

### Postgres 的同步/异步分离

`checkpoint-postgres` 目录下的实际文件结构：

```
langgraph/checkpoint/postgres/__init__.py     ← PostgresSaver（同步）+ __all__
langgraph/checkpoint/postgres/aio.py         ← AsyncPostgresSaver（异步）
langgraph/checkpoint/postgres/base.py        ← BasePostgresSaver + MIGRATIONS
langgraph/checkpoint/postgres/_internal.py   ← 内部辅助
langgraph/checkpoint/postgres/_ainternal.py  ← 内部辅助（异步）
langgraph/checkpoint/postgres/shallow.py     ← ShallowPostgresSaver
```

**同步版**：`langgraph.checkpoint.postgres.PostgresSaver`（`postgres/__init__.py:45`），用 `psycopg.Connection`，内部 `self.lock = threading.Lock()`。

**异步版**：`langgraph.checkpoint.postgres.aio.AsyncPostgresSaver`（`postgres/aio.py:45`），用 `asyncio.Lock`。

⚠️ **一个容易踩的坑**：异步版**不在 `__all__` 里**（`postgres/__init__.py:677` 只导出 `["PostgresSaver", "BasePostgresSaver", "ShallowPostgresSaver", "Conn"]`），必须显式导入：

```python
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
```

两者共享 `BasePostgresSaver`（`postgres/base.py:547`）里的 SQL 常量与迁移定义。

### setup() 迁移机制

`setup()` 定义在 `postgres/__init__.py:90-115`，流程是：

1. 执行 `MIGRATIONS[0]` 建 `checkpoint_migrations` 表
2. `SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1` 读当前版本（无记录则 `-1`）
3. 顺序执行 `MIGRATIONS[version+1:]`，每条执行后写 `INSERT INTO checkpoint_migrations (v) VALUES (%s)`

`MIGRATIONS` 定义在 `postgres/base.py:46-94`，共 9 条：建 migrations 表 → checkpoints → checkpoint_blobs → checkpoint_writes → blob 允许 NULL → no-op 占位 → 三个 `CREATE INDEX CONCURRENTLY` → `checkpoint_writes` 加 `task_path` 列。

⚠️ **`CREATE INDEX CONCURRENTLY` 不能在事务块内执行**。`setup()` 依赖 `autocommit=True`（`from_conn_string` 里 `Connection.connect(..., autocommit=True)`，`postgres/__init__.py:81-83`）。如果在事务中手动调用 `setup()` 会直接报错。

### Postgres 表结构与并发能力

4 张表：

| 表 | 内容 |
|---|---|
| `checkpoints` | JSONB，存 checkpoint 主数据 |
| `checkpoint_blobs` | BYTEA，把大 value 按 channel+version 拆出，避免 JSONB 膨胀 |
| `checkpoint_writes` | BYTEA，存 pending writes |
| `checkpoint_migrations` | 迁移版本号 |

主键设计：

- checkpoints：`(thread_id, checkpoint_ns, checkpoint_id)`
- blobs：`(thread_id, checkpoint_ns, channel, version)`
- writes：`(thread_id, checkpoint_ns, checkpoint_id, task_id, idx)`

写入全部使用 `ON CONFLICT ... DO UPDATE/DO NOTHING`——**天然幂等**。writes 主键含 `task_id`，允许同一 checkpoint 的不同任务各写一行。

**Pipeline 模式**：`pipe` 参数支持 psycopg pipeline 批量提交，但**只能配单个 Connection，不能配 ConnectionPool**（`postgres/__init__.py:57-60`）。生产并发建议用 `ConnectionPool` + `max_size`。

### ⚠️ DeltaChannel 与 prune 的致命陷阱

这是本次源码分析里最值得单独拎出来的生产风险。

`prune` / `delete_for_runs` / `copy_thread` 三个方法的 docstring（`checkpoint/base/__init__.py:364-371, 385-395, 411-438`）都带显式警告：

> 如果 `"keep_latest"` 策略丢弃了中间 checkpoint 及其 writes，会切断 `DeltaChannel` 的父链，导致该 channel **静默重建为空（不报错）**。

三种安全做法：

1. 保留到最近 `_DeltaSnapshot` 的完整祖先链
2. 在保留点强制重写 snapshot
3. 对使用 DeltaChannel 的线程禁用 prune

Postgres 侧还有 `PRUNE_DELTA_THREADS_SQL`（`checkpoint-postgres/.../base.py:182-200`）主动识别并拒绝这类线程。

**给产品的判断**：如果你用了 `DeltaChannel`（大对象增量更新），就**不要用 `prune` 自动清理历史**，否则会遇到"数据莫名变空但不报错"的事故。

### 写顺序契约

`base/__init__.py:190-196` 有一段重要注释：

> Live execution applies a super-step's tasks in this order, so a saver that replays stored writes, as `get_delta_channel_history` does, must sort them by it too, or an order-sensitive reducer rebuilds a different value than the run produced.

**自定义 checkpointer 时必须遵守写顺序**，否则顺序敏感的 reducer 会重建出不同的值。这是个隐蔽但致命的坑。

## 三、Store 实现

`BaseStore` 定义在 `store/base/__init__.py:708`：

**只有 2 个抽象方法**（其余都是基于它们的便利方法）：

| 方法 | 行号 | 作用 |
|---|---|---|
| `batch(ops)` | 733 | 批量执行操作（抽象） |
| `abatch(ops)` | 745 | 异步批量（抽象） |

**便利方法**（都以 `batch` 为后端）：

| 方法 | 行号 | 作用 |
|---|---|---|
| `get` | 756 | 按 namespace + key 读取 |
| `search` | 784 | 按 namespace 前缀搜索 |
| `put` | 866 | 写入 |
| `delete` | 952 | 删除（复用 `PutOp(value=None)`） |
| `list_namespaces` | 966 | 枚举命名空间 |

异步版本从 `store/base/__init__.py:1027` 开始（`aget`、`asearch`、`aput`、`adelete`、`alist_namespaces`）。

**这个设计值得注意**：要实现一个自定义 Store，只需实现 `batch`/`abatch` 两个方法，其余 5 个便利方法自动获得。对比 `BaseCheckpointSaver` 有 6 个抽象方法，Store 的扩展成本低得多。

### 能力开关

| 属性 | 行号 | 说明 |
|---|---|---|
| `supports_ttl` | 727 | 是否支持 TTL 过期 |
| `ttl_config` | 728 | TTL 配置 |

**语义搜索必须创建时传 `index`**，否则 `index` 参数无效（`store/base/__init__.py:716-721` 明确说明）：

```python
store.put(ns, key, value, index={"dims": 1536, "embed": embed_fn, "fields": ["content"]})
```

### namespace 校验

`_validate_namespace_labels`（`store/base/__init__.py:776-777, 809-811`）：label 必须是非空字符串，且**不能包含 `.`**。

`list_namespaces` 支持 prefix / suffix 通配符（`MatchCondition` 支持 `*`，如 `("cache", "*")`，`store/base/__init__.py:348-358`）。

### namespace 语义

namespace 是**层级元组**，如 `("users", "u123", "preferences")`。`search` 接受 namespace 前缀，返回该子树下的所有条目。

`search` 的文档（`store/base/__init__.py:700-704`）提到三种查询能力：

- 文档缺失的字段会被跳过
- 数组写法会为每个元素生成独立 embedding
- 支持复杂嵌套路径（如 `"a.b[*].c.d"`）

### MemoryStore

`checkpoint/store/memory/__init__.py:136` 的 `InMemoryStore`（592 行），开发测试用。内部用 `dict` + 可选 numpy 向量索引（`_cosine_similarity`，`:493`）。

### Postgres Store vs SQLite Store

两者结构高度对称（`BasePostgresStore` / `BaseSqliteStore`），API 完全一致，差异在存储层：

| 维度 | PostgresStore | SqliteStore |
|---|---|---|
| 主表 | `store(prefix, key, value jsonb, ...)`，PK `(prefix,key)` | `store(prefix, key, value text, ...)`，PK `(prefix,key)` |
| **值类型** | **jsonb** — 结构化查询友好 | **text** — 需序列化，查询能力弱 |
| prefix 索引 | `btree (prefix text_pattern_ops)`，支持前缀 LIKE 优化 | 普通 btree |
| TTL | `expires_at TIMESTAMP WITH TIME ZONE` + `ttl_minutes INT` | `expires_at TIMESTAMP` + `ttl_minutes REAL` |
| **ANN 索引** | **pgvector 原生**：`kind: Literal["hnsw","ivfflat","flat"]`，`vector_type: Literal["vector","halfvec"]`；HNSW 默认 `m=16, ef_construction=64` | **无 ANN 索引**——`SqliteIndexConfig` 是空壳，BLOB 向量只能暴力扫描 |
| 语义搜索 | 服务端 pgvector 距离算子 + 可选 HNSW/IVFFlat | Python 侧 cosine 暴力匹配 |
| 额外能力 | `sweep_ttl` / `start_ttl_sweeper` / `stop_ttl_sweeper` | 同样有 |
| 连接管理 | `PoolConfig{min_size, max_size, kwargs}` | `sqlite3.Connection` / `aiosqlite` |

**选型结论**：数据量小、单机嵌入式 → SQLite Store 够用；要跨进程共享长期记忆、要 pgvector 语义检索、要 TTL 扫表 → Postgres Store。**SQLite 版没有 ANN 索引**，embedding 规模一大就是 O(n) 暴力扫描。

## 四、StreamMode 全谱（权威取值）

`StreamMode` 定义在 `langgraph/types.py:131`：

```python
StreamMode = Literal[
    "values", "updates", "checkpoints", "tasks", "debug", "messages", "custom"
]
```

源码文档逐条说明（`types.py:134-145`）：

| 模式 | 语义（源码原文要点） |
|---|---|
| `"values"` | Emit all values in the state after each step, including interrupts. 用 Functional API 时只在结束时发一次 |
| `"updates"` | 仅节点/任务返回的更新；同一步多个节点更新**分别发出** |
| `"custom"` | 节点/任务内通过 `StreamWriter` 发的自定义数据 |
| `"messages"` | LLM 消息**逐 token** + metadata |
| `"checkpoints"` | checkpoint 创建时发事件，格式同 `get_state()` |
| `"tasks"` | 任务**开始/结束**事件，含结果与错误 |
| `"debug"` | 合并 `"checkpoints"` + `"tasks"` |

**注意 `"messages"` 是 token 级流式**，且带 LLM 调用元数据——这是做前端打字机效果的标准接口。

### ⚠️ SDK 的 stream_mode 比 Python 运行时多两个

这是最容易踩的坑。**协议层和运行时层的 stream_mode 集合不一样**：

| 位置 | 取值集合 |
|---|---|
| Python 运行时 `langgraph/types.py:131` | values, updates, checkpoints, tasks, debug, messages, custom（**7 种**） |
| SDK `langgraph_sdk/schema.py:51-61` | values, messages, updates, **events**, tasks, checkpoints, debug, custom, **messages-tuple**（**9 种**） |

SDK 多出的两个：

- `"events"` —— 执行事件流
- `"messages-tuple"` —— `(message, metadata)` 二元组形式

**这两个在 Python 的 `astream(stream_mode=...)` 里不接受**，只在远程 SDK 协议层存在。写代码时别把 SDK 的文档当运行时的文档看。

### 多路输出

`stream_mode` 支持传列表，同时订阅多种模式。`pregel/main.py:2784-2785` 明确说明：*"You can pass a list as the `stream_mode` parameter to stream multiple modes at once. The streamed outputs will be tuples of `(mode, data)`."*

启用 `subgraphs=True` 时形状再变一层（`pregel/main.py:2804-2807`）：`(namespace, data)` 或 `(namespace, mode, data)`，namespace 是子图路径元组，如 `("parent_node:<task_id>", "child_node:<task_id>")`。

### 自定义流：StreamWriter

`StreamWriter` 定义在 `types.py:147`：

```python
StreamWriter = Callable[[Any], None]
```

文档说明：*"Always injected into nodes if requested as a keyword argument, but it's a no-op when not using `stream_mode="custom"`."*

```python
def my_node(state):
    writer = get_stream_writer()
    writer({"progress": "step 1"})
    return {...}
```

`pregel/main.py:718-720` 补充：`stream_eager` 会为 `"messages"` 和 `"custom"` 模式自动开启——保证 token 级 / 自定义事件即时推送而非缓冲到步末。

### interrupt 在流中的表现

`pregel/_loop.py:1607-1634` 精确定义：

| 模式 | 表现 |
|---|---|
| `"updates"` | 发 `{"__interrupt__": (...)}` 形式的 chunk（`:1613-1625`） |
| `"values"` | output_keys 是 dict → 把 `__interrupt__` 塞进完整状态 dict；output_keys 是字符串 → 只发 interrupt chunk（`:1629-1634`） |

退出时 `_suppress_interrupt`（`:1519-1522`）还会额外 push 一个 graph lifecycle event `"interrupt"`，带上 `interrupts` 元组。

另外注意：**PUSH 路径的 interrupt 会被子图吞掉、由父图发出**（`_loop.py:1611-1612`）。

### Stream Transformer 扩展点

`stream/_types.py:44` 定义 `StreamTransformer`——流式投影的扩展点：

- 子类实现 `init` + 至少一个 `process`/`aprocess`
- `finalize`/`afinalize`/`fail`/`afail` 可选
- `before_builtins = True` 让转换器在内置转换器之前运行（用于内容修改，如 PII 脱敏）
- 文档明确标注了 **Foot-gun**：在 before_builtins 通道里改 `namespace` 或 `id`/`result`/`error`/`interrupts` 字段会导致内置转换器的记账错乱

**这是给做内容审核、PII 脱敏、流式计量的团队准备的扩展点**，但文档的警示写得很实在：可以自由观察，只能改没人读���字段。

## 五、MCP 支持

LangChain v1 内置 MCP，位于 `langchain_v1/langchain/mcp/`：

| 文件 | 内容 |
|---|---|
| `adapter.py` | 把 MCP 工具适配成 LangChain `BaseTool` |
| `tools.py` | MCP 工具加载 |
| `elicitation.py` | MCP elicitation（服务端向客户端请求补充信息） |
| `__init__.py` | `MultiServerMCPClient` 等导出 |

测试目录 `tests/unit_tests/mcp/` 显示了完整的能力面：`test_tools.py`、`test_schema.py`、`test_namespace.py`、`test_elicitation.py`、`test_adapter.py`。

**MCP 的接入方式**：通过 `MultiServerMCPClient` 连接 MCP server，把工具转成 LangChain 工具，再传给 `create_agent(tools=[...])`。工具因此自动获得中间件的 retry / limit / error handling 能力——**这是 MCP 集成最大的价值：统一的工具治理**。

## 六、SDK 与 CLI

### langgraph-sdk

`libs/sdk-py` 提供 `LangGraphClient`，用于与 LangGraph API server 通信：

- threads（线程管理）
- runs（运行触发与流式订阅）
- assistants（助手配置）
- cron（定时任务）
- store（长期记忆）
- 部署 API

依赖：`httpx>=0.25.2`、`orjson>=3.11.5`。

### langgraph CLI

`libs/cli` 提供：

| 命令 | 作用 |
|---|---|
| `langgraph dev` | 本地开发服务器，支持热重载和调试 |
| `langgraph build` | 构建 Docker 镜像 |
| `langgraph up` | 本地起完整环境 |

**`langgraph dev` 的意义**：本地就能用和线上一样的协议跑起来，开发和生产行为一致，避免"本地好好的，上线就崩"。

## 七、部署形态对比

| 形态 | 组成 | 适用 | 成本 |
|---|---|---|---|
| **自托管（进程）** | Python 进程 + InMemory/Sqlite/Postgres checkpointer | 中小规模、内部工具、预算敏感 | 只有服务器成本 |
| **自托管（容器）** | Docker + Postgres + `langgraph up` | 需要多实例、高并发的生产系统 | 服务器 + DB |
| **LangGraph Platform** | 托管运行时 + 托管持久化 | 想快速上线、要现成的部署/观测 | 按用量订阅 |
| **LangSmith** | 可观测 + 评测平台 | 需要 trace 分析、离线/在线评测 | 按用量订阅 |

**LangSmith 与 LangGraph 的关系**：LangGraph 是运行时，LangSmith 是可观测与评测层。**LangGraph 核心完全开源、无需账号**（`langgraph`、`langgraph-checkpoint*`、`langgraph-sdk`）。

但账号要求分情况：

| 场景 | 是否需要 LangSmith 账号 |
|---|---|
| `langgraph dev` 本地开发 | ❌ 不需要（只需本地 Python 3.11+） |
| `langgraph up` 本地部署 | ✅ 需要 `LANGSMITH_API_KEY` |
| Platform **生产**部署 | ✅ 需要 `LANGGRAPH_CLOUD_LICENSE_KEY`（**付费**） |

证据：`libs/cli/langgraph_cli/cli.py:296-299` 明写 *"For local dev, requires env var `LANGSMITH_API_KEY` with access to LangSmith Deployment. For production use, requires a license key in env var `LANGGRAPH_CLOUD_LICENSE_KEY`"*。

**凭证体系是联通的**：SDK 的 `get_client`（`sdk-py/langgraph_sdk/_async/client.py:29`）默认按 `LANGGRAPH_API_KEY` → `LANGSMITH_API_KEY` → `LANGCHAIN_API_KEY` 顺序读环境变量（`:51-56`）。

**Tracing 数据流**：运行时通过 `RunnableConfig` 携带 `callbacks`/`tags`/`metadata` 下沉；SDK 侧有专门的 `langsmith_tracing: LangSmithTracing | None` 参数（`_async/runs.py:390, 420`），文档说明 *"Allows routing traces to a specific project or associating with a dataset example"*（`:486-487`）。

`pregel/main.py:165-166` 的 `TaskPayload.metadata` 注释提到 metadata 会传给 `messages` 流消费者（`lc_agent_name`、`langgraph_node`、`langgraph_step`）——这些正是 LangSmith trace 上显示的字段。

**未在源码中找到的能力**（明确声明，不做断言）：LangSmith 的 evaluation / dataset 实现代码、offline vs online eval 实现、LangGraph API server 本体（`langgraph-api`）——均属于服务端而非 OSS 仓库。`langgraph-cli` 通过 `langgraph_api.cli.run_server` 延迟导入它（`cli.py:791`），依赖声明 `langgraph-api>=0.5.35,<1.0.0`（`libs/cli/pyproject.toml:26`）。

## 八、这一层的设计评价

**收益：**

- 持久化接口干净，Postgres 实现成熟，是生产可用的
- StreamMode 覆盖了从调试到生产的全场景（token 流 + 任务级事件 + checkpoint）
- MCP 通过适配器接入，工具治理统一
- CLI 让本地/生产行为一致

**代价：**

- Postgres 同步/异步两套实现，选错会踩坑
- 自托管必须手动跑 `setup()` 迁移
- 自定义 checkpointer 要遵守写顺序约定（隐蔽）
- 完整可观测能力需要付费的 LangSmith

**上生产前的检查清单**：

1. ✅ checkpointer 用 `PostgresSaver`，跑过 `setup()`
2. ✅ `durability` 按副作用情况选 `sync`/`async`
3. ✅ `thread_id` 生成策略明确（不要用随机 UUID 每次新会话）
4. ✅ 副作用节点幂等（考虑 `interrupt()` 会重执行）
5. ✅ Store 用于长期记忆，Checkpointer 用于会话状态
6. ✅ 流式接口按前端需求选 `stream_mode`（打字机用 `messages`）
7. ✅ 至少接一个 tracer 观察实际调用链

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 上一篇：[[07-LangChain-v1-中间件体系与-create_agent]] · 下一篇：[[09-架构对比与选型决策]]
