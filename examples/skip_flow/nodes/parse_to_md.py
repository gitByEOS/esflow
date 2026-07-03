"""parse_to_md:降级解析格式,与 parse_to_html 同层(DAG 都依赖 merge),
serial 让它等 parse_to_html 跑完再启动。

accept 检查同层前序 parse_to_html 产物:有有效 output.html 则 skip,否则接手转 md。
"""

from pathlib import Path

from easyflow import Node


class ParseToMd(Node):
    id = "parse_to_md"
    title = "降级转 Markdown"

    def accept(self, ctx) -> bool:
        for up in ctx.layer(self.depth):
            html_file = (up or {}).get("html_file")
            if html_file and Path(html_file).exists():
                return False
        return True

    def run(self, ctx) -> dict:
        data = Path(ctx.layer(self.depth - 1)[0]["merged_file"]).read_text()
        md_file = self.output_dir / "output.md"
        md_file.write_text(f"# 正文\n\n{data}")
        return {"format": "md", "md_file": str(md_file), "fallback": True}
