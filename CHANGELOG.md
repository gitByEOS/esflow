# Changelog

本项目遵循语义化版本思路。`0.x` 阶段 API 仍可能调整,破坏性变更会在对应版本中说明。

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

### 文档

- `docs/cli.md`:子命令 × flag × 等价库式调用全参数矩阵
- `docs/ref/Runner.md` 表格加 "CLI flag" + "典型场景" 列,明确 `only` vs `nodes` 区别
- `docs/ref/DepthScope.md` 用户视角三类能力,`ctx.get` 三状态行为表格化,`gather` vs `upstream_ids` 选择指引
- `docs/ref/FlowDefine.md` `serial` 提升为"调度策略"独立小节,补 `accept=False` fallback 完整示例,静态副本边展开示例
- `docs/ref/Checkpoint.md` "两种暂停点来源"对比表,`break_before + AFTER` 共存时发两次 checkpoint
- `docs/ref/JobEvent.md` 节点生命周期事件时序图,`TraceStatus`/`NodeStatus` 关系,`delta` 当前不产,`FanOut` 节点不 emit `final`
- `docs/ref/Node.md` `accept`/`deliver` 对照表 "False → skip vs error" 后果差异,静态副本 `self.index` 切片示例
- `docs/quickstart.md` 统一目录结构说明,`esflow run` vs `python run.py` 选择
- `docs/artifacts.md` `--from` 措辞与 `Runner.md` 统一,明说必须搭配 `--out`
