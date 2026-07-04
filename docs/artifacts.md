# Artifacts

EasyFlow 的续跑能力建立在一个简单契约上:节点把大文件写进 `output_dir`,再用 `artifact.json` 记录下游需要读取的结构化结果。

## output_dir

每个节点运行前,框架会注入:

```python
self.output_dir
```

节点应把文件产物写到这个目录里:

```python
class Export(Node):
    id = "export"

    def run(self, ctx) -> dict:
        text = ctx.get("ocr")["text"]
        path = self.output_dir / "result.txt"
        path.write_text(text + "\n", encoding="utf-8")
        return {"out_path": str(path), "chars": len(text)}
```

默认 run 模式下,目录形如:

```text
/tmp/easyflow/outputs/<flow_id>/<job_id>/<run_id>/
```

指定 `--out` 后,目录形如:

```text
<out>/<run_id>/
```

## artifact.json

当节点 `done` 或 `skipped` 后,框架会在可持久化模式下写入:

```text
<run_id>/artifact.json
```

它保存的是 `run()` 返回值:

```json
{
  "out_path": "./runs/video-a/export/result.txt",
  "chars": 120
}
```

下游通过 `ctx.get("upstream_id")` 读取上游 artifact。文件本体由节点自己读写,框架只负责保存 artifact 的 JSON 结构。

## --out

`--out` 指定一次运行的完整产物目录:

```bash
easyflow run ./my_flow --out ./runs/video-a
```

这会让每个节点写入:

```text
./runs/video-a/<run_id>/
```

并在节点完成后写:

```text
./runs/video-a/<run_id>/artifact.json
```

## --from

人工修正某个节点产物后,从它的下一步继续跑:

```bash
easyflow run ./my_flow --out ./runs/video-a --from translate
```

语义:

- 加载 `translate` 上游节点的 `artifact.json`
- 清理 `translate` 及所有下游节点的旧产物目录
- 重跑 `translate` 及下游
- 不重跑 `translate` 的上游

如果修正的是 `parse_srt` 产物,下一步是 `translate`,就执行:

```bash
easyflow run ./video_flow --out ./runs/video-a --from translate
```

## 人工修改产物

如果只改文件内容,路径没变,不用改 `artifact.json`:

```text
./runs/video-a/parse_srt/subtitle.srt
```

如果换了文件路径,必须同步修改对应节点的 `artifact.json`,否则下游仍会读旧路径。

## 限制

- `artifact.json` 必须是 JSON 可序列化结构
- `Path` 等对象会被序列化成字符串
- 动态 FanOut 的运行时图暂不持久化,不承诺从动态副本内部续跑
- `--from` 需要已有上游 artifact,缺失时 CLI 会提前失败
