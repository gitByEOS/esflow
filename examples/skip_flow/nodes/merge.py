"""merge:汇总上一层所有 fetch 产物,取第一个有效 raw.txt 复制到本节点产物目录。"""

from pathlib import Path

from easyflow import Node


class Merge(Node):
    id = "merge"
    title = "汇总抓取"

    def accept(self, ctx) -> bool:
        for up in ctx.layer(self.depth - 1):
            data_file = (up or {}).get("data_file")
            if data_file and Path(data_file).exists():
                return True
        return False

    def run(self, ctx) -> dict:
        for up in ctx.layer(self.depth - 1):
            if up is None:
                continue
            data_file = up.get("data_file")
            if data_file and Path(data_file).exists():
                merged = self.output_dir / "merged.txt"
                merged.write_text(Path(data_file).read_text())
                return {"source": up["source"], "merged_file": str(merged)}
        return {"source": None, "merged_file": None}
