---
title: 02-Pregel 执行内核：超步模型如何驱动一次图运行
tags: [AI, LangGraph, Pregel, 源码拆解, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 源码行号对应 langgraph 1.2.19（commit 3c916d7，2026-11-03）
---

# Pregel 执行内核：超步模型如何驱动一次图运行

LangGraph 的运行时不是"一个大 while 循环"，而是一个 **BSP（Bulk Synchronous Parallel）超步模型**。理解这一点，就理解了这个框架 80% 的设计取舍。

本文所有行号对应 `langgraph` 1.2.19（commit `3c916d75`，2026-11-03），源码位于 `langgraph/libs/langgraph/langgraph/`。

## 一、为什么叫 Pregel

Pregel 是 Google 论文《Pregel: A System for Large-Scale Graph Processing》的名字。LangGraph 借用了它的模型：

- **Actor（节点）**：只读自己订阅的 channel，不读全局状态
- **Channel（通道）**：状态容器，节点通过写 channel 通信
- **Superstep（超步）**：所有可执行节点在同一超步内并行跑，步末统一合并写入

关键约束：**同一超步内的节点看不到彼此的写入**。这带来确定性，也带来代价。

## 二、目录地图：pregel 子包

`langgraph/pregel/` 是执行内核，本包代码量最大（`main.py` 单文件 4557 行）。

| 文件 | 行数 | 职责 |
|---|---|---|
| `main.py` | 4557 | `Pregel` 类主体，公开 API（invoke/stream/astream/get_state/update_state） |
| `_loop.py` | 2172 | `PregelLoop`：超步推进的核心状态机 |
| `_algo.py` | 1459 | 任务准备算法：谁该跑、读什么、写什么 |
| `_read.py` / `_write.py` | — | channel 读取与写入的原子操作 |
| `_executor.py` | — | 任务执行调度（同步/异步、并发度） |
| `_checkpoint.py` | — | checkpoint 写入与读取策略 |
| `_retry.py` | — | 节点级重试策略 |
| `_config.py` | — | 配置归一化 |
| `protocol.py` / `types.py` | — | `PregelProtocol` 抽象与类型定义 |

`main.py` 里的 `Pregel` 类从 `pregel/main.py:456` 开始。它是 LangGraph 的**唯一公开入口**——`StateGraph.compile()` 返回的就是 `CompiledStateGraph`（`graph/state.py:1404`），而它内部持有并包装一个 `Pregel` 实例。

## 三、公开 API 矩阵

`Pregel` 的对外方法（行号取自 `pregel/main.py`）：

| 方法 | 行号 | 说明 |
|---|---|---|
| `stream` | 2744 | 同步流式执行 |
| `astream` | 3117 | 异步流式执行 |
| `invoke` | 3906 | 同步一次性执行 |
| `get_state` / `aget_state` | 约 1300 | 读取当前状态快照 |
| `get_graph` | 909 | 导出可视化图结构 |
| `get_subgraphs` | 1140 | 枚举子图 |

**关键点：`invoke` 与 `stream` 共享同一个内核。** `invoke` 本质是把 `stream` 的结果聚合到最后一个值——不存在"两套执行引擎"这种常见误解。异步版本 `astream`/`ainvoke` 走 `AsyncPregelLoop`（`_loop.py:1908`），与同步版 `SyncPregelLoop`（`_loop.py:1652`）共用 `PregelLoop` 基类（`_loop.py:170`）的逻辑。

## 四、一次超步（tick）的完整生命周期

核心在 `PregelLoop.tick()`（`_loop.py:650`）和 `after_tick()`（`_loop.py:700`）。

### tick() —— 超步开始前（`_loop.py:650-698`）

```
1. 检查步数上限        step > stop → status = "out_of_steps"，结束
2. 准备本步任务        _prepare_tasks_and_emit_checkpoint()
3. 无任务则结束        status = "done"
4. 重放上一轮待写      _reapplies_pending_writes → _reapply_writes_to_succeeded_nodes
5. 检查中断点          interrupt_before → raise GraphInterrupt()
6. 检查 drain 请求     control.drain_requested → 提前退出
7. 发射 tasks 事件     self._emit("tasks", map_debug_tasks, ...)
```

第 2 步 `_prepare_tasks_and_emit_checkpoint()`（`_loop.py:778`）调用 `prepare_next_tasks()`（`_algo.py:348/370/391`），这是**任务调度的决策点**：

- 读取当前 checkpoint 的 channel 值
- 根据「哪个 channel 被更新了」+「哪些节点订阅了它」+「节点的触发条件」决定本步执行哪些节点
- PULL 模式：节点订阅 channel，有新值才跑
- PUSH 模式：由 `Send` 显式指定目标节点（`prepare_push_task_send`，`_algo.py:937`）

### after_tick() —— 超步结束后（`_loop.py:700-776`）

```
1. 收集本步所有写入    writes = [w for t in tasks for w in t.writes]
2. 合并写入到 channel  apply_writes(...)  ← 这一步才是状态真正变化的地方
3. 发射 values 事件    map_output_values
4. 清空待写队列        checkpoint_pending_writes.clear()
5. 保存 checkpoint      _put_checkpoint({"source": "loop"})
6. 检查中断点          interrupt_after → raise GraphInterrupt()
```

### 为什么这个顺序重要

**所有节点的写入先被收集，再统一 merge。** `apply_writes()`（`_algo.py:233`）是这一步的入口。这直接回答了一个高频困惑：

> 两个节点并行写同一个 key，谁赢？

答案：**都不是"赢"，而是由 channel 的合并规则决定**。如果该 key 声明了 reducer（比如 `operator.add`），两个值会被合并；如果没声明，默认是 LastValue（后者覆盖前者）。详见 [[03-Channel机制与状态合并规则]]。

## 五、Checkpoint：状态即时间轴

`after_tick()` 每次都会 `_put_checkpoint({"source": "loop"})`（`_loop.py:766`，实现见 `_loop.py:1237`）。

一次 checkpoint 存了什么：

| 字段 | 含义 |
|---|---|
| `channel_values` | 所有 channel 的当前快照 |
| `next` | 下一步要执行的节点 |
| `metadata` | step、来源、时间戳等 |
| `versions` | 每个 channel 的版本号 |

这带来两个能力：

1. **持久化**：进程崩溃后可以从最后 checkpoint 恢复
2. **时间旅行**：可以回滚到任意历史 checkpoint 分叉重跑

`thread_id` 是这一切的主键——同一次会话的所有 checkpoint 靠它串起来。

## 五补、任务如何被触发：版本号比对

这是 Pregel 最精妙也最容易看漏的一段。

### 触发判定的本质

`_triggers()`（`_algo.py:1259`）的语义是：**任一订阅 channel 可用，且其版本 > 该节点已读版本**。

依据两个字段：

- `checkpoint["channel_versions"]` —— 每个 channel 的单调版本号
- `checkpoint["versions_seen"][node]` —— 每个节点已消费到的版本

### 下一批任务的产生逻辑

本步写了哪些 channel → 哪些节点被这些 channel 触发 → 下一步跑哪些节点。

**这就是"数据流驱动控制流"的核心。** 没有传统的 `if/else` 控制流判断，节点该不该跑完全由"我订阅的数据变没变"决定。

### 任务 ID 的确定性

`prepare_single_task`（`_algo.py:523`）用 checkpoint_id + namespace + step + name + PULL + triggers 经 **xxhash/uuid5** 生成任务 ID（`_algo.py:614-622`，`_xxhash_str` 在 `_algo.py:1403`）。

**任务 ID 是内容哈希，不是随机数。** 这意味着：同样的输入 + 同样的 checkpoint 序列 → 同样的任务 ID → 重放时能精确识别"这个任务已经执行过"。这是崩溃恢复不重复执行副作用的基础。

### local_read：让条件边读到本节点的写入

`local_read`（`_algo.py:189`）是一个注入到任务 config 里的读函数，源码注释说得很清楚：

> Used by conditional edges to read a copy of the state with reflecting the writes from that node only.

它的实现（`_algo.py:200-222`）：

1. 收集**本任务自己**的 writes（`task.writes`）
2. `fresh=True` 时，复制所有 channel，把自己的写入应用到副本上
3. 读这个副本

**为什么要这么设计？** 因为超步内节点看不到彼此的写入，但如果条件边看不到本节点刚写的值，就没法做路由了。`local_read` 在**副本**上应用自己的写入，既满足路由需求，又不破坏"超步内不可见"的隔离语义。

`ChannelRead.do_read`（`_read.py:73`）从 `config[CONF][CONFIG_KEY_READ]` 取出这个注入的读函数。

## 六、durability：什么时候落盘

LangGraph 1.x 提供 `durability` 参数，控制 checkpoint 写入时机。源码定义在 `pregel/main.py:2794-2800`：

| 值 | 语义（源码原文） |
|---|---|
| `"sync"` | Changes are persisted synchronously before the next step starts. |
| `"async"` | Changes are persisted asynchronously while the next step executes. |
| `"exit"` | Changes are persisted only when the graph exits. |

默认值是 `"async"`（`pregel/main.py:2692`）。

这个参数是**可靠性与吞吐的显式权衡**：

- `"sync"`：崩溃不丢状态，但每个超步都要等 IO
- `"async"`：IO 与计算重叠，吞吐更高，极端崩溃可能丢最后一步
- `"exit"`：最快，但中途崩溃全丢

对做 AI 产品的判断：涉及资金/外部副作用的链路用 `"sync"`；纯对话或可重放任务用 `"async"`。

## 七、错误处理与重试

- **重试策略**：`pregel/_retry.py` 定义 `RetryPolicy`，通过 `NodeBuilder.add_retry_policies()`（`pregel/main.py:355`）挂到节点上
- **错误分类**：`langgraph/errors.py` 定义 `GraphInterrupt`（人工介入信号，不是错误）、`GraphRecursionError`（超过递归/步数上限）
- **节点级错误处理器**：`_algo.py:1109` 的 `prepare_node_error_handler_task` 支持为单个节点配置失败处理逻辑

设计取向很清晰：**重试是节点级配置，不是全局开关**。这符合真实业务——调用外部 API 该重试，本地计算不该重试。

## 八、这一层的设计评价

**收益：**

- 确定性：给定相同输入和 checkpoint 序列，执行路径可复现
- 可中断：任意超步边界都是安全的暂停点
- 可回溯：时间旅行调试是天然能力，不是补丁
- 并行安全：节点间无共享可变状态

**代价：**

- 心智负担高于"直接写个 while 循环"
- 超步边界是隐式的并行屏障，可能浪费并行度（快节点等慢节点）
- 状态必须显式建模，不能偷懒依赖消息历史

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 下一步：[[03-Channel机制与状态合并规则]] · 源码版本：[[09-来源索引与版本勘误]]
