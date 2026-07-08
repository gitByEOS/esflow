# CLI 总览

`esflow` 命令行四个子命令。本页给一张完整 flag 矩阵,跨子命令对照查表;每条 flag 旁注等价的库式调用,方便 CLI 与 `Runner` API 互转。

## 子命令 × flag 全矩阵

| 子命令 | flag | 等价库式调用 | 用途 |
|---|---|---|---|
| `run` | `FLOW_DIR`(位置参数,`--resume` 模式可省略) | `Runner.load(FLOW_DIR)` | flow 目录路径 |
| `run` | `--node X [Y ...]` | `runner.run(only={X, Y, ...})` | 只跑指定节点及其必需上游 |
| `run` | `--from NODE` | `runner.run(from_node="NODE")` | 从 NODE 起续跑到末端,上游从 `--out` 目录复用 |
| `run` | `--from-depth N` | `runner.run(from_depth=N)` | 重跑 depth>=N 的所有节点,上游 depth<N 从 `--out` 复用 |
| `run` | `--out DIR` | `Runner.load(FLOW_DIR, job_dir=Path(DIR))` | 指定产物目录,节点产物写入 `DIR/<node>/artifact.json` |
| `run` | `--resume DIR` | `Runner.load(flow_dir, job_dir=DIR)` + `runner.run(resume=True)` | 续跑 TO_AGENT 节点:框架扫产物文件构造 artifact + `deliver` 校验,跑下游。flow_dir 从 `DIR/_flow_dir.txt` 找回 |
| `debug` | `FLOW_DIR`(位置参数) | `Runner.load(FLOW_DIR, debug=True)` | flow 目录路径 |
| `debug` | `--node X [Y ...]` | `runner.run(nodes={X, Y, ...}, break_before={X, Y, ...})` | 单点调试:上游从 debug 目录复用,在指定节点前暂停等 resume |
| `debug` | `--clear` | `runner.clear_debug()` | 清空 debug 目录持久化产物后从头跑 |
| `view` | `FLOW_DIR`(位置参数) | `Runner.load(FLOW_DIR)` | flow 目录路径,浏览器界面 |
| `new` | `NAME`(位置参数) | —— | 生成 skill 模板(含可跑 demo flow) |
| 非 CLI | `python scripts/run.py` | `Runner.load + run + esflow_event`(+ 可选 `pass_check`) | skill 自定义启动逻辑,脚手架生成 |

## 仅库式,无 CLI flag

| 库式参数 | 说明 |
|---|---|
| `runner.run(break_before={X})` | 在指定节点就绪后暂停;CLI 不直接暴露,`debug --node X` 内部已含 `break_before={X}` |
| `runner.run(only=...)` + 自定义 `break_before` | 需要库式调用组合 |

## 互斥规则

- `run` 的 `--node` / `--from` / `--from-depth` 三个 flag 互斥(对应库式 `only` / `from_node` / `from_depth` 互斥)
- `run --resume DIR` 独占,不与 `--node` / `--from` / `--from-depth` / 位置参数 `FLOW_DIR` 组合(等价 `--out DIR` + `runner.run(resume=True)`)
- `debug` 的 `--node` 与 `--clear` 可组合,但 `--clear` 清空所有产物后 `--node X` 会因上游无产物触发 `missing_upstream` 报错退出——实际用法是 `--clear` 单独用(清空从头全跑),或先全跑落产物后再 `--node X` 单调试
- `run --from` / `run --from-depth` 必须搭配 `--out`(续跑依赖持久化产物目录)

## 返回码

`esflow run` 退出码语义:

| 码 | 含义 |
|---|---|
| `0` | job 跑到 `end` |
| `1` | error(节点异常 / `deliver` 失败 / 预检失败 / 参数错误) |
| `2` | 跑到 `TO_AGENT` checkpoint,进程退出等外部 agent 写产物 |
| `130` | Ctrl+C 中断 |

## 检查与预检

CLI 的磁盘预检(`runner.missing_upstream`)只在持久化模式触发:

- `run --from X` / `run --from-depth N`(搭配 `--out`)— 预检 X / depth>=N 的上游产物,缺失则报错退出
- `debug --node X` — 预检 X 的上游产物,缺失则报错退出
- `run --node X`(默认 run 模式,无 `--out`)— **不预检**,直接跑 X 及必需上游

库式调用时用户自行决定是否预检,详见 [`Runner.missing_upstream`](ref/Runner.md#missing_upstream)。

## 选择指引

| 你想干的事 | 用什么 |
|---|---|
| 全跑一次,看结果 | `esflow run ./flow` |
| 全跑并保留产物供续跑 | `esflow run ./flow --out ./runs/a` |
| 改了某节点,从它起续跑到末端 | `esflow run ./flow --out ./runs/a --from translate` |
| 改了 depth>=2 的所有节点,上游复用 | `esflow run ./flow --out ./runs/a --from-depth 2` |
| 临时跑某节点及其上游 | `esflow run ./flow --node worker#2` |
| 浏览器看 DAG + 手动点 start | `esflow view ./flow` |
| 浏览器调试 + 持久化 + 单点钉住 | `esflow debug ./flow --node worker#2` |
| 清空 debug 目录从头调试 | `esflow debug ./flow --clear` |
| 跑到 agent 节点退出,等 AI 介入 | `esflow run ./flow --out ./runs/a`(`TO_AGENT` 节点) |
| agent 写产物后续跑下游 | `esflow run --resume ./runs/a` |
| 生成新 skill 模板 | `esflow new my_skill` |

## 相关

- [quickstart.md](quickstart.md) — 跑通第一个 flow
- [debug.md](debug.md) — debug/view 模式详解
- [artifacts.md](artifacts.md) — 产物目录与续跑
- [ref/Runner.md](ref/Runner.md) — `run()` 库式 API 详解
