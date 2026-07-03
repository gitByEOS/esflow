# 后续

- 动态副本挂 checkpoint:支持 `split`/`worker#i` 暂停 + retry 重建副本
- `only` 单调试支持指定动态副本内部(展开后命中)
- `ctx.replicas_of(base)` 语义化枚举(静态副本也统一接口)
- LLM 节点:作为可插拔 callable,后补 provider 抽象
- 持久化恢复:`resume_from=<job_id>` 从磁盘加载 artifact,跳过已完成节点;store 接口 + SQLite 支持进程重启恢复(含动态副本图状态)
- view 适配并行多节点高亮 + 动态副本展开动画
