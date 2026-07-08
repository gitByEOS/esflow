# Runner

`esflow.runner` — `from esflow import Runner, BreakKind`

加载并执行 flow,产出 [`JobEvent`](JobEvent.md) 事件流,支持并行调度、静态副本/动态扇出、人机协作控制循环、单点调试、定点续跑。

## 核心 API 速查

| 场景 | 调用 |
|---|---|
| skill 入口(跑到断点) | `events, kind, ev = await runner.run_to_break()` |
| 翻译断点为退出码 + envelope | `code, env = Runner.to_envelope(kind, ev)` |
| 流式消费(view/debug/交互) | `async for ev in runner.run(): ...` |
| 加载 flow | `runner = Runner.load("./my_flow")` |
| checkpoint 暂停后唤醒 | `runner.resume()` / `retry("X")` / `abort()` |

## 构造

```python
runner = Runner.load("./my_flow")
runner = Runner.load("./my_flow", job_dir=Path("./runs/a"))
runner = Runner.load("./my_flow", node_args={"resolve": {"input": "x"}})
```

`Runner.load(flow_dir, output_root=DEFAULT_OUTPUT_ROOT, debug=False, job_dir=None, node_args=None)`

`node_args` 是 `{base_id: kwargs_dict}`,注入到 base 与所有副本的 `Node.kwargs`,不持久化(resume 时显式重传)。

### 实例属性

| 属性 | 用途 |
|---|---|
| `flow` | [`FlowDefine`](FlowDefine.md)(动态扇出后边会改写) |
| `runs` | `dict[str, Node]` 所有运行实例(动态扇出后新增副本) |
| `state` | [`JobState`](JobState.md) 事件折叠状态 |
| `artifacts` | `dict[str, Any]` 当前内存产物 |
| `job_dir` | 产物根目录 |
| `max_depth` | DAG 最大拓扑深度,`from_depth` 越界校验用 |

### job_dir 推导

| 模式 | job_dir | 产物落盘 |
|---|---|---|
| 默认 run | `output_root/<flow_id>/<job_id>/` | `output_root` 默认 `/tmp/esflow/outputs`,系统自动清理 |
| `--out DIR` | `DIR/`(无 job_id 层) | 显式持久目录,不清理 |
| debug | `DEBUG_OUTPUT_ROOT/<flow_id>/`(固定) | `/tmp/esflow/debug`,累积复用 |

全持久化:所有 flow 都落盘 `artifact.json` 到 `job_dir/.esflow/<run_id>/`,所有 flow 都能 `--resume` / `from_node` / `from_depth`。默认 `/tmp` 根享受自动清理,要长期保留就显式 `--out`。节点业务产物落 `job_dir/<run_id>/`,框架元数据隔离到 `job_dir/.esflow/`。

## run(...)

```python
async for event in runner.run():
    if event.type == "checkpoint":
        runner.resume()        # 或 retry("review") / abort()
```

6 个参数,5 个**目标参数互斥**(优先级 `resume > from_node > from_depth > nodes > only`),1 个修饰参数 `break_before`。

| 目标参数 | 行为 | 加载策略 |
|---|---|---|
| 默认 | 全跑 | `--out` 显式指定时清空重跑,否则加载已有产物跳过已完成 |
| `only={"X"}` | 跑 X 及其必需上游 | 全量加载,跑必需闭包 |
| `nodes={"X"}` | 只跑 X 本身,上游须已完成 | 加载已有,跳过 X 强制重跑(单点调试) |
| `from_node="X"` | 重跑 X 及下游,上游复用 | 加载跳过 X 及下游,再清掉重跑 |
| `from_depth=N` | 重跑 `depth >= N`,上游复用 | 加载跳过 `depth>=N`,再清掉重跑 |
| `resume=True` | 跑 pending TO_AGENT + 下游 | 全量加载,TO_AGENT 扫文件构造 artifact |
| `break_before={"X"}` | X 就绪后 emit `checkpoint` 暂停等 `resume` | 与任意目标参数组合 |

`from_depth` 越界(`< 0` 或 `> max_depth`)抛 `RuntimeError`。

CLI flag 与库式完整对照见 [cli.md](../cli.md)。

## run_to_break(...) -> (events, break_kind, break_event)

高层 API:跑到断点停下。不做 raise/exit 决策,skill 拿 `break_kind` + `break_event` 自己决定。

`BreakKind = Literal["end", "to_agent", "error"]`:

| kind | break_event | 含义 |
|---|---|---|
| `end` | end event 或 None | job 正常结束(全跑完或被 abort) |
| `to_agent` | checkpoint event(含 `resume_hint`) | TO_AGENT 检查点,等外部 agent 写产物 |
| `error` | error event | 节点抛异常,调 `as_exception()` 还原 |

TO_HUMAN checkpoint 不算断点(等 stdin 控制信号),需用 `run()` 自己驱动。

## to_envelope(break_kind, break_event) -> (exit_code, envelope)

静态方法。把断点翻译成 `(exit_code, envelope)`,消灭 skill 入口的翻译胶水。

| break_kind | exit_code | envelope |
|---|---|---|
| `end` | `0` | `{"status": "end"}` |
| `to_agent` | `2` | `{"status": "to_agent", "node_id":..., "resume_hint":...}` |
| `error` | `1` | `{"status": "error", "message":..., "exc_type":..., "exc_attrs":...}` |

envelope 是纯 dict,skill 直接 `json.dumps` 输出。error 时 `raise break_event.as_exception()` 与本方法正交,由 skill 自行决定。

## to_agent_hint(event, resume_cmd=None) -> str

静态方法。把 TO_AGENT checkpoint event 格式化成介入指引字符串,skill 直接 `print(..., file=sys.stderr)`。

`resume_cmd` 是续跑命令模板,**必须含 `{job_dir}` 占位符**,框架填充;不含则 `raise ValueError`。不传则只输出节点目录与上游产物。

## 控制信号

checkpoint 暂停时外部调用,唤醒等待中的 `run()`:

| 方法 | 行为 |
|---|---|
| `resume()` | paused 节点转 done(`TO_HUMAN`)或转 idle(`break_before`),跑下一轮 |
| `retry(from_node)` | 清 `from_node` 及下游的 artifact 与状态,上游复用。debug 模式额外清磁盘 `.esflow/<rid>/artifact.json` |
| `abort()` | 中止 job,emit `error("aborted")` + `end` |

## 调试辅助

| 方法 / 属性 | 用途 |
|---|---|
| `clear_debug()` | debug 模式清空 `job_dir` 下产物,下次从头跑;非 debug 无操作 |
| `missing_upstream(nodes)` | 返回 `nodes` 上游中磁盘无 `artifact.json` 的节点 id,单调试预检用 |
| `has_break_to_agent()` | `job_dir/.esflow/` 下是否有未完成的 TO_AGENT 节点 |
| `pending_break_to_agent()` | 返回 pending 的 TO_AGENT 节点 id 列表 |

## 执行语义

- **并行**:同一轮就绪节点 `asyncio.gather`,同步 `run` 用 `asyncio.to_thread` 包
- **拓扑深度**:`depth(node) = 1 + max(depth(upstream))`,入口 0,同层 depth 相同
- **serial**:同轮就绪节点中属于 `serial` 的,只启动 `nodes` 顺序最靠前的那个,其他等下一轮(用于 fallback 兜底链,详见 [`FlowDefine`](FlowDefine.md))
- **产物持久化**:全持久化,所有 flow 都落盘 `artifact.json` 到 `.esflow/<run_id>/`。默认 `/tmp` 根享受自动清理,详见 [../artifacts.md](../artifacts.md)
- **动态扩图**:节点 `run` 返回 [`FanOut`](FanOut.md) 时,框架创建 N 个副本、改写边、重建邻接表

## 模块常量

| 常量 | 值 | 用途 |
|---|---|---|
| `DEFAULT_OUTPUT_ROOT` | `/tmp/esflow/outputs` | 默认产物根(系统自动清理) |
| `DEBUG_OUTPUT_ROOT` | `/tmp/esflow/debug` | debug 模式产物根(固定,复用) |

## 相关

- [`FlowDefine`](FlowDefine.md) / [`Node`](Node.md) — 加载与执行的输入
- [`JobEvent`](JobEvent.md) — `run()` 产出的事件流
- [`JobState`](JobState.md) — `runner.state` 事件折叠状态
- [`FanOut`](FanOut.md) — 动态扇出
- 调试模式见 [../debug.md](../debug.md)
- 产物与续跑见 [../artifacts.md](../artifacts.md)
