"""parse_to_html:首选解析格式,把 merge 正文解析为 html 写到 output_dir/output.html。

把 run 改成 html_file.write_text("") 即可触发 parse_to_md 降级。
"""

from pathlib import Path

from esflow import Node


class ParseToHtml(Node):
    id = "parse_to_html"
    title = "解析为 HTML"

    def accept(self, ctx) -> bool:
        up = ctx.layer(self.depth - 1)[0] or {}
        merged_file = up.get("merged_file")
        return bool(merged_file and Path(merged_file).exists())

    def run(self, ctx) -> dict:
        data = Path(ctx.layer(self.depth - 1)[0]["merged_file"]).read_text()
        html_file = self.output_dir / "output.html"
        html_file.write_text(f"<html><body>{data}</body></html>")
        return {"format": "html", "html_file": str(html_file)}
