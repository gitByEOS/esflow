# Artifacts

esflow 的续跑能力建立在一个简单契约上:节点把大文件写进 `output_dir`,再用 `artifact.json` 记录下游需要读取的结构化结果。

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
/tmp/esflow/outputs/<flow_id>/<job_id>/<run_id>/
```

默认根用 `/tmp`,享受系统自动清理。要长期保留就显式 `--out` 到持久目录。

指定 `--out` 后,目录形如:

```text
<out>/<run_id>/
```

## artifact.json

节点 `done` 或 `skipped` 后,框架写入:

```text
<job_dir>/.esflow/<run_id>/artifact.json
```

它保存的是 `run()` 返回值(skip 节点为 `null`):

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
esflow run ./my_flow --out ./runs/video-a
```

这会让每个节点写入:

```text
./runs/video-a/<run_id>/
```

并在节点完成后写:

```text
./runs/video-a/.esflow/<run_id>/artifact.json
```

## --from

人工修正某个节点产物后,从它的下一步继续跑(`--from` 必须搭配 `--out DIR`,续跑依赖持久化产物目录):

```bash
esflow run ./my_flow --out ./runs/video-a --from translate
```

等价库式 `runner.run(from_node="translate")`,详见 [ref/Runner.md](ref/Runner.md#run)。语义(与 `Runner.md` 一致):

- 加载 `translate` 上游节点的 `artifact.json`(上游产物复用,不重跑)
- 清掉 `translate` 及下游节点的旧产物
- 重跑 `translate` 及下游

如果修正的是 `parse_srt` 产物,下一步是 `translate`,就执行:

```bash
esflow run ./video_flow --out ./runs/video-a --from translate
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

## TO_AGENT 节点的产物

`checkpoint = Checkpoint.TO_AGENT` 的节点不调 `run`,产物由外部 agent 写入 `output_dir`,框架不写 `artifact.json`(等 `--resume` 时构造)。

agent 契约(零 JSON):

1. `esflow run <flow> --out <path>` 跑到 TO_AGENT 节点,进程退出(exit 2),stderr 打印上游产物 + 产物目录路径
2. 外部 agent 读 stderr 拿上游产物 → 写产物文件到 `<path>/<to_agent 节点>/`(如 `summary.txt`)
3. `esflow run --resume <path>` → 框架扫该节点 `output_dir` 下文件(排除隐藏文件),构造 `artifact = {"output_dir": <str>, "files": [< filenames >]}`,调 `deliver` 校验,通过则落盘 `artifact.json` + 转 DONE + 跑下游

`.esflow/break_to_agent.json`:

首次跑到 TO_AGENT 节点时,框架在 `<path>/.esflow/break_to_agent.json` 写 `{"pending": ["<节点 id>", ...]}`,记录未完成的 TO_AGENT 节点。`--resume` 完成节点后从 pending 移除,空了删文件。

防误跑:`--out` 目录有 `.esflow/break_to_agent.json` 时不带 `--resume` 直接报错退出,避免 agent 未完成就 silently 跑下游。

详见 [ref/Checkpoint.md](ref/Checkpoint.md#用法---to_agentagent-介入)。
