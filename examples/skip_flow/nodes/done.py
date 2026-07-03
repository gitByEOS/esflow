"""done:汇总上一层所有 parse 产物,取第一个有效格式复制到本节点产物目录。"""

from pathlib import Path

from easyflow import Node


class Done(Node):
    id = "done"
    title = "完成"

    def run(self, ctx) -> dict:
        merge_up = ctx.layer(self.depth - 2)[0] or {}
        for up in ctx.layer(self.depth - 1):
            if up is None:
                continue
            html_file = up.get("html_file")
            if html_file and Path(html_file).exists():
                final = self.output_dir / "final.html"
                final.write_text(Path(html_file).read_text())
                return {"format": "html", "final_file": str(final), "source": merge_up.get("source")}
            md_file = up.get("md_file")
            if md_file and Path(md_file).exists():
                final = self.output_dir / "final.md"
                final.write_text(Path(md_file).read_text())
                return {"format": "md", "final_file": str(final), "source": merge_up.get("source"), "fallback": True}
        return {"format": None, "final_file": None}
