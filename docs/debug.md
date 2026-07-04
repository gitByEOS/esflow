# debug 模式

debug 是交互式调试入口,启动后打开浏览器 **view 界面**。`esflow` 有两个相关 CLI 子命令,共用同一个浏览器前端:

- `esflow view ./flow` — 只打开浏览器界面,产物目录用默认临时路径,等用户点 start 才开跑
- `esflow debug ./flow` — 浏览器界面 + debug 持久化目录 + 可选 `--node` 单点调试

view 用来在浏览器里看 DAG、节点状态、artifact 和事件流。正式定点续跑走 `run --out DIR --from NODE`,见 [artifacts.md](artifacts.md)。

## 三种 CLI 形态对比

| 能力 | `run` | `view` | `debug` |
| --- | --- | --- | --- |
| 交互 | CLI 文本流(`esflow_event`) | 浏览器界面,等用户点 start | 浏览器界面,可选自动开跑 |
| 产物目录 | 默认 `/tmp/esflow/outputs/<flow_id>/<job_id>/<run_id>/`,`--out DIR` 可指定为 `DIR/<run_id>/` | 默认 `/tmp/esflow/outputs/<flow_id>/<job_id>/<run_id>/`(临时,不累积) | 固定 `/tmp/esflow/debug/<flow_id>/<run_id>/`(累积复用) |
| job_id | 每次新生成 | 每次新生成 | 固定目录,无 job_id |
| 持久化复用 | `--out DIR` 后可续跑 | 不适合续跑(临时目录) | 自动持久化,适合反复单点调试 |
| 单点调试 | `--node X` 跑 X 及上游 | 无 | `--node X` 只跑 X,上游从 debug 目录复用 |
| 典型场景 | 全跑/续跑/CI | 一次性预览 DAG 跑通效果 | 反复单点调试某节点 |

view 跑完产物落在默认临时目录 `/tmp/esflow/outputs/<flow_id>/<job_id>/`,不累积,适合一次性预览;需要续跑请用 `run --out DIR`。

## CLI

```bash
esflow debug ./my_flow                  # 调出 view,点 start 全跑
esflow debug ./my_flow --node worker#2  # 单点调试:上游产物从磁盘复用,在 worker#2 前暂停等 resume
esflow debug ./my_flow --clear          # 清空 debug 目录后从头跑
esflow debug ./my_flow --clear --node worker#2  # 清空后 --node:上游无产物,触发 missing_upstream 报错(实际用法:--clear 单独用,或先全跑再 --node)
```

`debug` 直接启动浏览器 view 界面,不走命令行文本流——DAG 拓扑、节点状态、artifact、事件流更直观。

### `--node` 单点调试语义

- **不跑上游**:`--node X` 只把 X 列入执行范围,上游节点必须已在 debug 目录有 `artifact.json`(之前全跑过),从磁盘加载后 X 才就绪
- **暂停在 X 前**:X 就绪后 runner emit `checkpoint(X)` 暂停,等 view 点 `resume` 才执行 X
- **跑完即 end**:X 执行完 job 结束,view 退出。反复跑 X 重新执行 `esflow debug ./my_flow --node X`
- **上游缺产物**:CLI 会提示先全跑 debug 落产物

## 库式调用

```python
from esflow import Runner

runner = Runner.load("./my_flow", debug=True)
# 全跑
async for event in runner.run():
    ...

# 单点调试:nodes 限定只跑 X(上游从磁盘复用),break_before 在 X 前暂停
async for event in runner.run(nodes={"worker#2"}, break_before={"worker#2"}):
    if event.type == "checkpoint":
        runner.resume()  # 跑 X
```

`nodes` 与 `only` 是内部 API 区分:
- `only={"X"}` 跑 X 及上游
- `nodes={"X"}` 只跑 X,上游必须已完成

## 清空 debug 目录

debug 目录是累积的,要重头来过用 `--clear`:

```bash
esflow debug ./my_flow --clear          # 清空 debug 目录后从头跑
esflow debug ./my_flow --clear --node worker#2  # 清空后单调试
```

`--clear` 在 runner 启动前清空 debug 目录,等价于库式 `runner.clear_debug()`。
