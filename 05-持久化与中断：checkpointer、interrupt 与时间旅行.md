---
title: 05-持久化与中断：checkpointer、interrupt 与时间旅行
tags: [AI, LangGraph, 持久化, HITL, 源码拆解, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 源码行号对应 langgraph 1.2.19（commit 3c916d7，2026-11-03）
---

# 持久化与中断：checkpointer、interrupt 与时间旅行

LangGraph 的持久化不是"日志记录"，而是**执行模型的一部分**。同一个机制同时提供了三件事：崩溃恢复、人工介入、时间旅行调试。

行号对应 `langgraph` 1.2.19（commit `3c916d7`），源码位于 `langgraph/libs/langgraph/langgraph/` 与 `langgraph/libs/checkpoint/langgraph/checkpoint/`。

## 一、为什么持久化是"一等公民"

对比两种心智模型：

| | 隐式消息历史型 | LangGraph 显式状态型 |
|---|---|---|
| 状态存在哪 | 消息数组里 | 独立的 state schema |
| 恢复怎么做 | 重新喂消息 | 从 checkpoint 恢复 channel 快照 |
| 人工介入 | 得自己造机制 | `interrupt()` 原生支持 |
| 时间旅行 | 基本做不到 | checkpoint 序列直接回放 |
| 并发写 | 靠消息追加顺序 | 靠 channel 合并规则 |

**关键差异**：在隐式模型里，"恢复"意味着把历史重放一遍；在 LangGraph 里，恢复只是把 channel 状态装回去。这就是为什么 LangGraph 的 HITL 是原生的，而不是外挂的。

## 二、Checkpointer 接口

`BaseCheckpointSaver` 定义在 `langgraph/checkpoint/base/__init__.py`（889 行）。核心方法：

| 方法 | 行号 | 作用 |
|---|---|---|
| `get` | 250 | 取 checkpoint |
| `get_tuple` | 262 | 取 checkpoint 元组（含 pending_writes） |
| `list` | 277 | 列出历史 checkpoint（时间旅行用） |
| `put` | 301 | 写入新 checkpoint |
| `put_writes` | 324 | 写入单个任务的中间写 |
| `delete_thread` | 344 | 删除整条线程 |

**源码比常见文档多出 4 组方法**（1.x 增量能力，容易被漏掉）：

| 方法 | 行号 | 作用 |
|---|---|---|
| `delete_for_runs` | 355 | 按 run_id 批量删除 |
| `copy_thread` | 374 | 复制一条线程（做 A/B 或分叉） |
| `prune` | 398 | 裁剪历史，`strategy="keep_latest"` |
| `get_delta_channel_history` | 607 | 取 DeltaChannel 的祖先链 |

每个方法都有异步版本（`aget`/`aget_tuple`/`alist`/`aput`/`aput_writes`/`adelete_thread` 等，从 `base/__init__.py:441` 开始）。

⚠️ **`prune` 与 DeltaChannel 有致命交互**，详见 [[08-生态与生产化：持久化实现、流式接口、MCP 与部署形态]]。

### 特殊写入的负索引

`checkpoint/base/__init__.py:824` 定义：

```python
WRITES_IDX_MAP = {ERROR: -1, SCHEDULED: -2, INTERRUPT: -3, RESUME: -4}
```

特殊类型的写入（错误、调度、中断、恢复）用**负索引**，避免与普通写入的顺序索引冲突。

### Checkpoint 数据结构

`Checkpoint`（`base/__init__.py:93`）是 TypedDict：

| 字段 | 含义 |
|---|---|
| `v` | 版本号（用于 schema 迁移，见 `graph/state.py:1626` 的 `_migrate_checkpoint`） |
| `id` | checkpoint 唯一 ID |
| `ts` | 时间戳 |
| `channel_values` | 所有 channel 的值快照 |
| `channel_versions` | 每个 channel 的版本 |
| `versions_seen` | 已见版本 |
| `updated_channels` | 本步更新的 channel |

`CheckpointTuple`（`base/__init__.py:140`）= `(config, checkpoint, metadata, parent_config, pending_writes)`。

**`pending_writes` 是理解中断的关键**：它记录了已经执行但结果还没合并进状态的任务输出。崩溃后重放时，这些写入会被识别并跳过，避免重复执行副作用。

### 写入时机

checkpoint 的写入有三个时机（对应 `durability` 参数，见 [[02-Pregel执行内核：超步模型如何驱动一次图运行]]）：

- `sync`：下步开始前落盘
- `async`：下一步执行时异步落盘（默认）
- `exit`：图退出时落盘

源码位置：`pregel/_loop.py:1237` 的 `_put_checkpoint()`。

## 三、Store：长期记忆，不是会话状态

这是最常见的概念混淆。LangGraph 有**两套**持久化：

| | Checkpointer | Store |
|---|---|---|
| 作用域 | 单个 thread 内 | 跨 thread |
| 存什么 | 某次会话的图状态 | 长期记忆、用户画像、跨会话知识 |
| 隔离 | thread_id | namespace 元组 |
| 接口 | `BaseCheckpointSaver` | `BaseStore`（`store/base/__init__.py:708`） |
| 实现 | MemorySaver / SqliteSaver / PostgresSaver | MemoryStore / PostgresStore / SqliteStore |

`BaseStore` 接口：

| 方法 | 作用 |
|---|---|
| `put(namespace, key, value, index)` | 写入 |
| `get(namespace, key)` | 读取 |
| `search(namespace_prefix, ...)` | 语义/前缀搜索 |
| `delete(namespace, key)` | 删除 |
| `list_namespaces(prefix)` | 枚举命名空间 |

**namespace 是层级元组**，如 `("users", "u123", "preferences")`。这让记忆天然分层：用户级偏好、会话级摘要、组织级知识。

**判断标准**：需要"下次接着聊"→ checkpointer；需要"这个用户一直是这样"→ store。

## 四、interrupt：人工介入的原生机制

`interrupt()` 定义在 `langgraph/types.py:903`。

### 用法

```python
def node(state: State):
    answer = interrupt("你今年多大？")   # 首次调用抛 GraphInterrupt
    return {"human_value": answer}
```

```python
graph = builder.compile(checkpointer=checkpointer)   # 必须有 checkpointer
config = {"configurable": {"thread_id": str(uuid.uuid4())}}

for chunk in graph.stream({"foo": "abc"}, config):
    print(chunk)
# > {'__interrupt__': (Interrupt(value='what is your age?', id='45fda847...', response_schema=None),)}

graph.invoke(Command(resume="some input from a human!!!"), config)   # 恢复
```

### 源码契约（`types.py:906-925`）

文档字符串里有 5 条关键约束，每条都有实际后果：

**1. "The graph resumes from the start of the node, re-executing all logic."**

恢复时**从节点开头重新执行**，不是从 `interrupt()` 调用处继续。这意味着：

- `interrupt()` 之前的代码会再跑一遍
- 副作用（写库、发请求）必须幂等，或放在 `interrupt()` 之后
- 这是**重执行**而非**重放**，成本要按"整个节点重跑"算

**这是 LangGraph HITL 最容易踩的坑。** 做产品设计时，节点里的昂贵操作（LLM 调用、检索）要么缓存，要么拆成独立节点。

**2. "If a node contains multiple `interrupt` calls, LangGraph matches resume values to interrupts based on their order in the node."**

多个 interrupt 按**节点内出现顺序**匹配恢复值，不是按 ID 匹配。恢复值列表 scoped 到具体 task，不跨 task 共享。

**3. "To use an `interrupt`, you must enable a checkpointer"**

没有 checkpointer 就没有 interrupt。

**4. "This list of resume values is scoped to the specific task executing the node"**

并行任务各自有独立的 resume 值序列。

**5. `None` 不能作为恢复值**

`_algo.py:1295-1297` 的注释说明了原因：*"None cannot be used as a resume value, because it would be difficult to distinguish from missing when used over http"*。HTTP 传输时无法区分"传了 None"和"没传"。

### 恢复值的路由：scratchpad

恢复值通过 `_scratchpad()`（`_algo.py:1279`）分发。它按优先级找三种来源：

1. **全局恢复值**（`NULL_TASK_ID` + `RESUME` channel）
2. **task 专属恢复值**（`w[0] == task_id and w[1] == RESUME`）
3. **namespace 映射恢复值**（`resume_map[namespace_hash]`）

支持按 `response_schema` 做类型校验（`types.py:886-892`），校验失败标记 `_mark_invalid_resume`。

## 五、时间旅行

因为 checkpoint 是有序序列，回滚很简单：

```python
# 查看历史
list(graph.get_state_history(config))

# 回滚到某个 checkpoint 并分叉
graph.update_state(config, values, as_node="...")

# 从历史点重放
graph.invoke(None, config)   # None = 不带新输入，继续
```

**时间旅行的真正价值在调试**：agent 出问题时，可以回退到出错前的 checkpoint，改一下输入重跑，看是哪一步的决策导致的偏差。这比加日志有效得多——日志告诉你"发生了什么"，时间旅行让你"换个可能重新发生"。

## 六、durability 的实战选择

`Durability` 类型定义在 `langgraph/types.py:98-104`：

```python
Durability = Literal["sync", "async", "exit"]
```

配置键常量是 `CONFIG_KEY_DURABILITY = "__pregel_durability"`（`_internal/_constants.py:71`），可以放进 `RunnableConfig` 的 `configurable` 传给子图（`pregel/main.py:2951-2953`）。

### 三种模式的实现路径

| 模式 | 实现 | 崩溃损失 | 延迟影响 |
|---|---|---|---|
| `"sync"` | `pregel/main.py:3080-3081`：`loop._put_checkpoint_fut.result()` —— 每个 super-step 后**阻塞等待**落盘 | 几乎不丢 | 每步串行一次 DB round-trip，步数多的图吞吐受损明显 |
| `"async"`（默认） | `_loop.py:1366-1374`：`self.submit(...)` 后台提交，不阻塞主循环 | 通常不丢已提交步，极端崩溃可能丢最后 1~2 步 | 写入与计算重叠，吞吐最好 |
| `"exit"` | `_loop.py:1289-1291`：中间步一律不写盘，只在 `_suppress_interrupt`（`:1507-1517`）退出时补写 | **整个 run 中间状态全丢**，无法从中间点恢复 | 最快，但失去 step 级恢复能力 |

补充：`"exit"` 模式下 `_loop.py:1323-1333` 会在保存前 sanitize TASK channel 中的 `UntrackedValue`，走的是不同的数据准备路径。

**核心区别**：durability 控制的是「checkpoint 写盘时机」，「从哪恢复」由 `checkpoint_id`/`thread_id` 决定。`"exit"` 模式下唯一可恢复点就是最终那一个 checkpoint。

### 弃用项

`checkpoint_during: bool` 已 deprecated（`pregel/main.py:2814-2824`），映射为 `durability = "async" if checkpoint_during else "exit"`，且与 `durability` 互斥（同时传报错）。SDK 侧同样标记（`_async/runs.py:442`）。

### 选择建议

| 场景 | 建议 | 理由 |
|---|---|---|
| 涉及外部副作用（发邮件、下单、扣款） | `sync` | 崩溃后不能重复执行副作用 |
| 纯推理 / 检索 | `async` | 默认值，吞吐最优 |
| 批量离线任务，可重跑 | `exit` | 最快 |
| 长时间 agent 任务 | `sync` 或 `async` + 定期 checkpoint | 崩溃损失可控 |

配合 `put_writes` 的存在，即使在 `async` 模式下，崩溃后也能识别"哪些任务已经执行过"，避免重复副作用——但这依赖任务 ID 的稳定性。

## 七、设计评价

**收益：**

- 持久化、恢复、HITL、时间旅行共用一套机制，不是四套代码
- 超步边界天然是安全暂停点
- `pending_writes` 让"部分执行"的恢复成为可能

**代价：**

- **节点重执行语义反直觉**——这是 HITL 最大的坑，需要在团队规范里明确
- checkpoint 存储成本随步数线性增长（尤其 channel_values 存全量）
- 必须理解 thread_id / namespace 两套标识体系

**给团队的实践建议**：
1. 节点内昂贵操作放在 `interrupt()` 之后，或加缓存
2. 副作用操作独立成节点，配合 `sync` durability
3. 状态设计时区分"会话状态"和"长期记忆"，别都塞 checkpointer

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 上一篇：[[04-StateGraph与图构建API]] · 下一篇：[[06-LangChain-v1-架构与代码结构]]
