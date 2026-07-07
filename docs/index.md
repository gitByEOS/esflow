# esflow

<div align="center">

轻量 Python DAG workflow 框架

Agent Skill 时代下,高效人机协作控制循环:单步调试、流程编排、暂停确认、定点续跑

</div>

## 教程

- [Quickstart](quickstart.md) — 快速上手与第一个 flow
- [Artifacts](artifacts.md) — output_dir、artifact.json、--out、--from
- [Debug](debug.md) — debug/view 调试模式
- [Pass-check](pass_check.md) — 启动预检
- [CLI](cli.md) — CLI 总览
- [如何打包发布](how_to_release.md)

## 参考手册

- [参考手册导览](ref/README.md)
- [Node](ref/Node.md) — 节点基类
- [FlowDefine](ref/FlowDefine.md) — `@flow` / `edge` / FlowDefine
- [Runner](ref/Runner.md) — 执行器
- [Checkpoint](ref/Checkpoint.md) / [FanOut](ref/FanOut.md) / [DepthScope](ref/DepthScope.md)
- [JobEvent](ref/JobEvent.md) / [JobState](ref/JobState.md)
- [CheckResult](ref/CheckResult.md) / [FlowLoadError](ref/FlowLoadError.md)

源码与示例见 [GitHub 仓库](https://github.com/gitByEOS/esflow)。
