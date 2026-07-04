"""fetch_from_wechat:兜底源1,与 fetch_from_ssr 同层(DAG 都依赖 trigger),
serial 让它等 fetch_from_ssr 跑完再启动。

accept 检查同层前序产物:任一已有有效 raw.txt 则 skip,否则接手从微信抓取。
"""

from pathlib import Path

from esflow import Node


class FetchFromWechat(Node):
    id = "fetch_from_wechat"
    title = "从微信抓取"

    def accept(self, ctx) -> bool:
        for up in ctx.layer(self.depth):
            data_file = (up or {}).get("data_file")
            if data_file and Path(data_file).exists():
                return False
        return True

    def run(self, ctx) -> dict:
        target = ctx.layer(self.depth - 1)[0]["target"]
        data = f"<微信抓取到的 {target} 正文>"
        data_file = self.output_dir / "raw.txt"
        data_file.write_text(data)
        return {"source": "wechat", "data_file": str(data_file), "fallback": True}
