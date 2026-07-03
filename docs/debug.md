# debug 模式

调试时反复跑同一 flow,run 模式每次换 `job_id`、产物不复用,缺上游产物就中断。debug 模式把产物固定到一个目录,artifact 持久化到磁盘,重跑时已完成节点跳过、单调试复用上游。

## 与 run 的区别

|              | run                                                   | debug                                      |
| ------------ | ----------------------------------------------------- | ------------------------------------------ |
| 产物路径         | `/tmp/easyflow/outputs/<flow_id>/<job_id>/<step_id>/` | `/tmp/easyflow/debug/<flow_id>/<step_id>/` |
| job_id       | 每次新生成                                                 | 无,目录固定                                     |
| artifact 持久化 | 否                                                     | `output_dir/artifact.json`                 |
| 启动加载已有产物     | 否                                                     | 是,已完成节点跳过                                  |
| retry 后磁盘    | 无影响                                                   | 清下游 `artifact.json`,防下次加载旧产物               |
| 交互           | CLI 文本                                                                                | 浏览器 view 界面                                  |

## CLI

```bash
easyflow debug ./my_flow                  # 调出 view,点 start 全跑
easyflow debug ./my_flow --node worker#2  # 单点调试:上游产物从磁盘复用,在 worker#2 前暂停等 resume
easyflow debug ./my_flow --clear          # 清空 debug 目录后从头跑
easyflow debug ./my_flow --clear --node worker#2  # 清空后单调试(上游没产物,X 不就绪)
```

`debug` 直接启动浏览器 view 界面,不走命令行文本流——DAG 拓扑、节点状态、artifact、事件流更直观。

### `--node` 单点调试语义

- **不跑上游**:`--node X` 只把 X 列入执行范围,上游节点必须已在 debug 目录有 `artifact.json`(之前全跑过),从磁盘加载后 X 才就绪
- **暂停在 X 前**:X 就绪后 runner emit `checkpoint(X)` 暂停,等 view 点 `resume` 才执行 X
- **跑完即 end**:X 执行完 job 结束,view 退出。反复跑 X 重新执行 `easyflow debug ./my_flow --node X`
- **上游缺产物**:若上游没全部 done,X 不就绪,job 直接 end。先 `easyflow debug ./my_flow` 全跑落产物

## 库式调用

```python
from easyflow import Runner

runner = Runner.load("./my_flow", debug=True)
# 全跑
async for event in runner.run():
    ...

# 单点调试:scope 限定只跑 X(上游从磁盘复用),break_before 在 X 前暂停
async for event in runner.run(scope={"worker#2"}, break_before={"worker#2"}):
    if event.type == "checkpoint":
        runner.resume()  # 跑 X
```

`scope` 与 `only` 的区别:
- `only={"X"}` → _target 含 X 及其全部上游(跑上游 + X,run 模式无持久化时用)
- `scope={"X"}` → _target 只含 X(上游必须已完成,debug 单点调试用)

## 行为细节

- **持久化时机**:节点 `done` 或 `skipped` 后写 `artifact.json`(`skip` 节点 artifact 为 `None` 也写,下游可推进)
- **加载时机**:`runner.run()` 启动时扫描 `job_dir/<sid>/artifact.json`,加载到 `self.artifacts` 并标 `done`/`skipped`,`_ready_nodes` 自然跳过
- **retry 清磁盘**:checkpoint 时 `retry <step>` 会清 `from_step` 及下游的 `artifact.json`,防止下次启动加载到旧产物;重跑后写新产物
- **动态扇出**:副本节点产物同样落 debug 目录,`artifact.json` 按 `base#i` 持久化
- **非 JSON 类型**:`Path` 等用 `default=str` 兜底序列化,加载时是字符串,节点取用需自行处理

## 清空 debug 目录

debug 目录是累积的,要重头来过用 `--clear`:

```bash
easyflow debug ./my_flow --clear          # 清空 debug 目录后从头跑
easyflow debug ./my_flow --clear --node worker#2  # 清空后单调试
```

`--clear` 在 runner 启动前 `shutil.rmtree(job_dir)`,等价于库式 `runner.clear_debug()`。仅 `debug` 子命令支持,`run` 模式产物本就不持久化。
