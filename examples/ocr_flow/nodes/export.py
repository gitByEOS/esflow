"""export 节点:识别文本写入 output_dir/result.txt 落盘。"""

from esflow import Node


class Export(Node):
    id = "export"
    title = "输出文本"

    def run(self, ctx) -> dict:
        upstream = ctx.get("ocr")
        text = upstream["text"]
        out_path = str(self.output_dir / "result.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        return {"out_path": out_path, "chars": len(text)}
