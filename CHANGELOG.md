# Changelog

本项目遵循语义化版本思路。`0.x` 阶段 API 仍可能调整,破坏性变更会在对应版本中说明。

## 0.1.2

### 新增功能

- 全持久化:所有 flow 都落盘 `artifact.json`,所有 flow 都能 `--resume` / `from_node` / `from_depth`(默认 `/tmp/esflow/outputs` 享受系统自动清理)
- `JobEvent` 异常透传:`exc` / `exc_type` / `as_exception()`(跨进程按 `exc_type` import 还原)
- `JobEvent.resume_hint`
- `Runner.to_agent_hint(event, resume_cmd=None)`
- `Runner.run_to_break(...)` + `BreakKind`
- `Runner.to_envelope(break_kind, break_event)`:断点 → `(exit_code, envelope)` 消灭翻译胶水
- `Runner.load(flow_dir, node_args={...})`
- `Node.kwargs`
- `TO_AGENT` 节点可设 `self.output_dir`
- 导出 `BreakKind`

### 改进

- `_run_resume` 切到 `run_to_break`
- `_run_one` TO_AGENT 分支统一 deliver 校验
- `error()` 经 `_error_from_exc` 透传 exc

## 0.1.1

`Checkpoint.AFTER` 重命名为 `Checkpoint.TO_HUMAN`,新增 `Checkpoint.TO_AGENT` 支持 AI agent 介入。

### 破坏性变更

- `Checkpoint.AFTER` → `Checkpoint.TO_HUMAN`(value `"after"` → `"to_human"`)

### 新增功能

- `Checkpoint.TO_AGENT`:节点不实现 `run`,框架就绪时 emit checkpoint 退出进程(exit 2),
- `examples/agent_flow/`:TO_AGENT 链路示例
- CLI `esflow run --resume <job_dir>`:续跑 TO_AGENT 节点
- CLI 返回码:`0` end / `1` error / `2` 待 agent / `130` Ctrl+C

## 0.1.0

首个正式版本。PyPI 包名 `esflow`。

### 支持功能

- DAG flow 定义:`@flow`、`edge()`、节点目录约定
- Runner 拓扑调度,支持并行执行、checkpoint、resume、retry、abort
- 节点契约:`accept` 接手确认、`deliver` 脱手确认、`output_dir` 产物目录
- 静态 replicas 扇出/扇入
- 动态 FanOut 运行时扩图
- serial 同层顺序执行,用于 fallback 链
- CLI:`esflow new`、`esflow run`、`esflow debug`、`esflow view`
- `run --out DIR --from NODE` 定点续跑,复用上游 artifact,重跑指定节点及下游
- `run --out DIR --from-depth N` 按层续跑,重跑 depth>=N 的所有节点
- `run --out DIR --node NODE#i` 单节点调试,只跑指定副本及其必需上游
- debug 模式 artifact 持久化与单点调试
- 标准库 HTML view,展示 DAG、节点状态、artifact、事件流
- `pass_check` 启动预检,失败带 `fix` 修复指引
- 示例 flow 与测试覆盖
