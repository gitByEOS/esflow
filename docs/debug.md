# debug 模式

debug 是交互式调试入口,用于在浏览器里看 DAG、节点状态、artifact 和事件流。正式定点续跑走 `run --out DIR --from NODE`,见 [artifacts.md](artifacts.md)。

## 与 run 的区别

| 能力 | run | debug |
| --- | --- | --- |
| 交互 | CLI 文本 | 浏览器 view |
| 默认产物目录 | `/tmp/easyflow/outputs/<flow_id>/<job_id>/` | `/tmp/easyflow/debug/<flow_id>/` |
| job_id | 每次新生成 | 固定目录,无 job_id |
| 单点调试 | `--node` 会跑目标及上游 | `--node` 只跑目标,上游从 debug 目录复用 |

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
- **上游缺产物**:CLI 会提示先全跑 debug 落产物

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

`scope` 与 `only` 是内部 API 区分:
- `only={"X"}` 跑 X 及上游
- `scope={"X"}` 只跑 X,上游必须已完成

## 清空 debug 目录

debug 目录是累积的,要重头来过用 `--clear`:

```bash
easyflow debug ./my_flow --clear          # 清空 debug 目录后从头跑
easyflow debug ./my_flow --clear --node worker#2  # 清空后单调试
```

`--clear` 在 runner 启动前清空 debug 目录,等价于库式 `runner.clear_debug()`。
