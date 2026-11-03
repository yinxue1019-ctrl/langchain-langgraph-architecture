# LangChain v1 / LangGraph 源码级架构拆解

基于**本地 clone 的真实源码**（非文档转述）拆解 LangChain v1 与 LangGraph 的架构。

**源码基准**

| 包 | 版本 | 来源 |
|---|---|---|
| `langgraph` | 1.2.19 | commit `3c916d750575b22c464790e9774494d810ccd322`（2026-11-03） |
| `langchain-core` | 1.7.4 | `langchain-ai/langchain` |
| `langchain` (v1) | 1.4.8 | `langchain-ai/langchain` |
| `langchain-classic` | 1.0.8 | v0 冻结兼容层 |

所有技术结论均标注 `文件:行号`。**未运行任何代码**，纯静态源码分析。

---

## 文章列表

| # | 文章 | 核心问题 |
|---|---|---|
| 00 | [目录与索引](00-目录与索引.md) | 阅读地图与核心结论速览 |
| 01 | [全景图：两个包、两种心智模型](01-全景图：两个包、两种心智模型.md) | LangChain 和 LangGraph 什么关系 |
| 02 | [Pregel 执行内核：超步模型如何驱动一次图运行](02-Pregel执行内核：超步模型如何驱动一次图运行.md) | 一次图运行为什么按"超步"推进 |
| 03 | [Channel 机制与状态合并规则](03-Channel机制与状态合并规则.md) | 两个节点并行写同一个 key，谁赢 |
| 04 | [StateGraph 与图构建 API](04-StateGraph与图构建API.md) | 图怎么声明、条件分支怎么实现 |
| 05 | [持久化与中断：checkpointer、interrupt 与时间旅行](05-持久化与中断：checkpointer、interrupt 与时间旅行.md) | 状态怎么存、人工审批怎么插进来 |
| 06 | [LangChain v1 架构与代码结构](06-LangChain-v1-架构与代码结构.md) | Runnable 协议、`\|` 运算符、仓库分层 |
| 07 | [LangChain v1 中间件体系与 create_agent](07-LangChain-v1-中间件体系与-create_agent.md) | v1 的 6 个钩子、17 个内置中间件 |
| 08 | [生态与生产化：持久化实现、流式接口、MCP 与部署形态](08-生态与生产化：持久化实现、流式接口、MCP 与部署形态.md) | 上生产要准备什么 |
| 09 | [架构对比与选型决策](09-架构对比与选型决策.md) | 我该不该用、用哪个 |
| 10 | [来源索引与版本勘误](10-来源索引与版本勘误.md) | 哪些是源码结论，哪些是推论 |

---

## 核心结论速览

1. **LangGraph 的运行时是 BSP 超步模型**（源自 Google Pregel 论文），同超步内节点互相不可见
2. **`invoke` 和 `stream` 共享同一个内核** —— `invoke` 内部就是消费 `stream` 的最后一个 values（`pregel/main.py:4000-4044`）
3. **条件分支内部是伪 channel**（`branch:to:{node}`，`graph/state.py:98`），不是特殊控制流
4. **reducer 靠 `Annotated` 元数据自动识别**，签名必须恰好 `(a, b) -> c`（`graph/state.py:1904-1922`）
5. **`interrupt()` 恢复时从节点开头重跑**，副作用必须幂等（`types.py:918`）
6. **LangChain v1 的 middleware 是洋葱模型**，`wrap_*` 能改请求响应、能短路
7. **`langchain-classic` 是 v0 的冻结兼容层**，新包名 `langchain` 指向 v1
8. **`create_agent` 底层就是 LangGraph**（`agents/factory.py:31-36` 直接 import `StateGraph`）

## 三个必须知道的坑

1. **`messages: list` 不带 `Annotated` → 并行写静默丢失**，要用 `Annotated[list[AnyMessage], add_messages]`
2. **`prune` + `DeltaChannel` → 数据静默变空不报错**（`checkpoint/base/__init__.py:364-371` 有显式警告）
3. **SDK 的 `stream_mode` 比 Python 运行时多两个**（`events` / `messages-tuple`），后者不接受

---

## 复现方式

```bash
mkdir -p /tmp/lg_src && cd /tmp/lg_src
git clone --depth 1 https://github.com/langchain-ai/langgraph.git langgraph
git clone --depth 1 https://github.com/langchain-ai/langchain.git langchain
cd langgraph && git log -1 --format="%H %ci"     # 3c916d7... 2026-11-03
```

> `--depth 1` 拉到的 HEAD 会随时间变化。复现时若行号对不上，先确认 commit 是否一致。

## License

MIT
