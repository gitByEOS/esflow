"""fetch_from_ssr:首选源,从 SSR 抓取,正文写到 output_dir/raw.txt。

把 run 改成 data_file.write_text("") 即可触发 fetch_from_wechat 兜底。
"""

from esflow import Node


class FetchFromSsr(Node):
    id = "fetch_from_ssr"
    title = "从 SSR 抓取"

    def accept(self, ctx) -> bool:
        up = ctx.layer(self.depth - 1)[0] or {}
        return bool(up.get("target"))

    def run(self, ctx) -> dict:
        target = ctx.layer(self.depth - 1)[0]["target"]
        data = f"<ssr 抓取到的 {target} 正文>"
        data_file = self.output_dir / "raw.txt"
        data_file.write_text(data)
        return {"source": "ssr", "data_file": str(data_file)}
