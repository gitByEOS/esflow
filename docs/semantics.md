# 关键语义

## 语义清单

- **accept 语义**:返回 `False` 不是错误,而是"不接手"——框架跳过本节点(emit `skipped`,artifact 置 `None`,下游可推进)。`deliver` 失败才是错误(emit `error`)
- **serial 语义**:同轮就绪节点中属于 `serial` 集合的,只启动 `nodes` 声明顺序最靠前的那个,其他 serial 节点等下一轮;非 serial 节点照常并行
- **replicas**:loader 把 base id 展开成 `base#0..base#N-1`,边自动扇出/扇入;副本节点用 `self.index` 切片,扇入节点用 `ctx.upstream_ids()` 或 `ctx.gather("worker")` 收集
- **FanOut + dynamic**:节点 `run` 返回 `FanOut(base, payload)`,`payload` 必须是 list,`n = len(payload)`,第 i 个副本拿 `payload[i]`;flow 声明 `dynamic = {"worker"}` 让 loader 不预实例化,运行时由 `FanOut` 创建副本并动态连边;副本节点用 `ctx.fanout_payload` 取载荷
- **并行执行**:同轮就绪节点 `asyncio.gather` 并行(同步 `run` 用 `asyncio.to_thread` 包),checkpoint 时整 job 暂停
- **产物持久化**:每节点 `self.output_dir` 由框架注入(`/tmp/easyflow/outputs/<flow_id>/<job_id>/<step_id>/`),节点把大文件写到这里,`run` 返回的 dict 登记文件路径供下游读取;skip 节点不创建目录
- **拓扑深度**:`self.depth` 由框架注入(入口节点 0),同层节点 depth 相同;`ctx.layer(d)` 返回该深度所有已完成节点产物 list(按声明顺序,skip 的为 None),同层 fallback 用 `ctx.layer(self.depth)` 拿前序,跨层拿上游用 `ctx.layer(self.depth - 1)`

## 静态 replicas vs 动态 FanOut

- 副本数固定、要加载时无环校验 + view 静态预览 + `--node worker#2` 单调试 → 用 `replicas`
- 副本数依赖运行时产物 → 用 `dynamic` + `FanOut`
- 同一 flow 可混用:部分节点 `replicas`,部分节点 `dynamic`

## 动态扇出当前限制(最小模型)

- 动态副本节点(`split` / `worker#i`)不挂 checkpoint,扇入节点(`merge`)可以
- `only` 单调试暂不支持指定动态副本内部(展开前不存在),可指定到 `split` 级
