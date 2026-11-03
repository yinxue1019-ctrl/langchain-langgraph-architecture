---
title: 04-StateGraph 与图构建 API
tags: [AI, LangGraph, StateGraph, 源码拆解, 专题]
created: 2026-11-03
updated: 2026-11-03
status: 现行专题
evidence: 源码行号对应 langgraph 1.2.19（commit 3c916d7，2026-11-03）
---

# StateGraph 与图构建 API

前面两篇讲的是"图怎么跑"，这一篇讲"图怎么写"。LangGraph 的图构建 API 设计得相当克制，值得单独拆。

行号对应 `langgraph` 1.2.19（commit `3c916d7`），源码位于 `langgraph/libs/langgraph/langgraph/graph/`。

## 一、StateGraph 是什么

`StateGraph`（`graph/state.py:131`）是 LangGraph 的主入口类，泛型签名：

```python
class StateGraph(Generic[StateT, ContextT, InputT, OutputT]):
```

四个类型参数：

| 参数 | 含义 |
|---|---|
| `StateT` | 状态 schema 类型（`TypedDict` / `dataclass` / Pydantic） |
| `ContextT` | 运行时上下文类型（不变的配置，如用户 ID、租户） |
| `InputT` | 输入 schema（可与 StateT 不同） |
| `OutputT` | 输出 schema（可与 StateT 不同） |

**State 与 Context 的分离是 v1 的重要变化。** 状态是可变的、跨节点流转的；上下文是不可变的、整个运行期恒定的。把"本次运行的用户身份"放进 State 是旧写法，现在应该放 Context。

## 二、构建 API 全表

`StateGraph` 的方法（行号取自 `graph/state.py`）：

| 方法 | 行号 | 作用 |
|---|---|---|
| `__init__` | 216 | 声明 schema |
| `set_node_defaults` | 272 | 为所有节点设默认重试/缓存/超时 |
| `add_node` | 376 / 446 / 520 / 590 / 667 | 添加节点（5 个重载） |
| `add_edge` | 928 | 固定边 |
| `add_conditional_edges` | 982 | 条件边 |
| `add_sequence` | 1032 | 批量添加序列 |
| `set_entry_point` | 1079 | 设置入口 |
| `set_conditional_entry_point` | 1092 | 条件入口 |
| `set_finish_point` | 1116 | 设置终点 |
| `validate` | 1129 | 校验图结构 |
| `compile` | 1177 | 编译成可执行图 |

## 三、add_node 的 5 个重载

`add_node` 有 5 个重载签名（`state.py:376` 起），本质是同一件事的不同输入形态：

```python
# 1. 函数 + 名称
builder.add_node("analyze", analyze_fn)

# 2. Runnable（自动取名）
builder.add_node(prompt | llm)

# 3. (名称, callable) 元组
builder.add_node(("analyze", analyze_fn))

# 4. 带输入输出 schema 的形式
builder.add_node("analyze", analyze_fn, input_schema=..., output_schema=...)

# 5. 状态参数名可自定义
builder.add_node("analyze", analyze_fn, input_schema=...)
```

节点签名决定它读什么：

```python
def analyze(state: State) -> dict:      # 读全部 state
def analyze(state: OverallState) -> ... # 读指定 schema
```

**节点不接收 channel，而是接收 state 快照。** 这是 LangGraph 的关键简化——节点看到的是"此刻合并后的完整状态"，不用自己处理 channel 依赖。

### 返回值即写入

节点返回 `dict`，key 对应 state 字段，value 是更新值。框架按 [[03-Channel机制与状态合并规则]] 的规则合并。

节点也可以返回 `Command(goto=..., update=...)` 来同时改变状态和路由。

## 四、条件分支：内部是伪 channel

这是本文最有价值的一节。

`add_conditional_edges`（`state.py:982`）的签名：

```python
def add_conditional_edges(
    self,
    source: str,
    path: Callable[..., Hashable | Sequence[Hashable]] | ...,
    path_map: dict[Hashable, str] | list[str] | None = None,
) -> Self
```

### 实现机制

分支逻辑实现在 `graph/_branch.py`：

- `BranchSpec.from_path()`（`_branch.py:95` 附近）创建分支规格
- `BranchSpec.run()`（`_branch.py:122`）把分支包装成 `ChannelWrite.register_writer(...)`
- `BranchSpec._route()`（`_branch.py:146`）调用 `self.path.invoke(value, config)` 拿到目标节点名
- `BranchSpec._finish()`（`_branch.py:192`）把目标转成写入条目

关键在 `_branch.py:202-211`：

```python
destinations = [r if isinstance(r, Send) else self.ends[r] for r in result]
if any(dest is None or dest == START for dest in destinations):
    raise ValueError("Branch did not return a valid destination")
if any(p.node == END for p in destinations if isinstance(p, Send)):
    raise InvalidUpdateError("Cannot send a packet to the END node")
```

**分支不是控制流，而是"写入一个特殊 channel"。** 伪 channel 名字模板定义在 `state.py:98`：

```python
_CHANNEL_BRANCH_TO = "branch:to:{}"
```

也就是说，条件分支的结果是写进 `branch:to:node_name` 这样的 channel，下游节点订阅它。目标节点因此**自动在下一超步被唤醒**。

### 这个设计的意义

分支用 channel 表达，意味着：

- **分支天然可并行**：一次返回多个目标 → 多个节点同时被唤醒
- **分支天然可 checkpoint**：路由决策本身是状态的一部分，可以回放
- **不需要特殊调度器**：Pregel 的通用超步机制就够了

`Send` 对象（`langgraph/types.py:738`）让分支能携带数据给目标节点，这是实现 map-reduce 式并行 fan-out 的标准手段。

### path_map 的自动推断

`_branch.py:112-114` 有个贴心设计：

```python
if rtn_type := get_type_hints(func).get("return"):
    if get_origin(rtn_type) is Literal:
        path_map_ = {name: name for name in get_args(rtn_type)}
```

如果你的条件函数标注了 `-> Literal["foo", "bar"]`，框架自动生成 path_map。同时 `state.py:1007-1009` 的警告说明：**没有类型标注时，图可视化会认为该边可能连向任意节点**——影响可读性，不影响执行。

## 五、add_sequence：批量建链

`add_sequence`（`state.py:1032`）把一串节点建成链：

```python
builder.add_sequence(["load", "transform", "save"])
```

内部就是循环调 `add_node` + `add_edge`（`state.py:1071-1073`）。它有两个显式校验：

- 空序列 → `ValueError("Sequence requires at least one node.")`
- 重复名字 → `ValueError("Node names must be unique: ...")`，并提示用 `(name, callable)` 元组解决

## 六、compile：从 StateGraph 到 Pregel

`compile()`（`state.py:1177`）是分水岭。它产出的 `CompiledStateGraph`（`state.py:1404`）内部持有一个 `Pregel` 实例。

编译时做的三件事：

1. **把 schema 转成 channel 字典**——`_get_channels()`（`state.py:1815`），每个 state 字段对应一个 channel
2. **把边/分支转成触发关系**——`attach_edge()`（`state.py:1551`）、`attach_branch()`（`state.py:1577`），建立"channel 变化 → 哪个节点该跑"的映射
3. **挂载节点实现**——`attach_node()`（`state.py:1444`），包装成 `PregelNode`

编译后还可以：

- `get_input_jsonschema()`（`state.py:1424`）/ `get_output_jsonschema()`（`state.py:1434`）——给前端/LLM 用
- `get_graph()`（`pregel/main.py:909`）——导出可视化结构

## 七、图结构示例

```python
from typing import Annotated, TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    intent: str
    result: str

def classify(state: State) -> dict:
    return {"intent": "search"}

def search(state: State) -> dict:
    return {"result": "..."}

def chat(state: State) -> dict:
    return {"result": "..."}

builder = StateGraph(State)
builder.add_node("classify", classify)
builder.add_node("search", search)
builder.add_node("chat", chat)

builder.add_edge(START, "classify")
builder.add_conditional_edges(
    "classify",
    lambda state: "search" if state["intent"] == "search" else "chat",
    path_map={"search": "search", "chat": "chat"},
)
builder.add_edge("search", END)
builder.add_edge("chat", END)

graph = builder.compile()
```

## 八、设计评价

**收益：**

- 声明式，建图逻辑一目了然
- 分支/并行/循环统一为 channel 机制，无需专门语法
- schema 即契约，输入输出自动有 JSON Schema
- 编译期校验（`validate()`）把错误提前到构建期

**代价：**

- `add_node` 5 个重载，类型提示复杂
- 分支伪装成 channel，调试时要在 channel 层看路由，可读性下降
- `Command` + `Send` + 条件边三种路由方式并存，初学者容易混淆该用哪个

---

返回 [[00-目录与索引|LangChain/LangGraph 专题索引]] · 上一篇：[[03-Channel机制与状态合并规则]] · 下一篇：[[05-持久化与中断：checkpointer、interrupt 与时间旅行]]
