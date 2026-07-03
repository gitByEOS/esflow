# Changelog

本项目遵循语义化版本思路。`0.x` 阶段 API 仍可能调整,破坏性变更会在对应版本中说明。

## 0.1.0-alpha

首个可试用版本,面向内部验证和早期用户反馈。

### Added

- DAG flow 定义:`@flow`、`edge()`、节点目录约定
- Runner 拓扑调度,支持并行执行、checkpoint、resume、retry、abort
- 节点契约:`accept` 接手确认、`deliver` 脱手确认、`output_dir` 产物目录
- 静态 replicas 扇出/扇入
- 动态 FanOut 运行时扩图
- serial 同层顺序执行,用于 fallback 链
- CLI:`easyflow new`、`easyflow run`、`easyflow debug`、`easyflow view`
- `run --out DIR --from NODE` 定点续跑,复用上游 artifact,重跑指定节点及下游
- debug 模式 artifact 持久化与单点调试
- 标准库 HTML view,展示 DAG、节点状态、artifact、事件流
- `pass_check` 启动预检
- 示例 flow 与测试覆盖

### Known Limits

- 动态 FanOut 的运行时图未持久化,不承诺从动态副本内部续跑
- HTML view 是调试工具,不是生产监控面板
- artifact 以 JSON 文件作为轻量契约,复杂对象需要节点自行转成可序列化结构
- `0.x` 阶段公开 API 仍可能收敛
