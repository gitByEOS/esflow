"""fetch_from_bili:兜底源2,与 fetch_from_wechat 同层,serial 让它等前序完成再启动。

accept 逻辑跟 fetch_from_wechat 完全一致:检查同层前序产物,任一有效则 skip。
加第四个源也不用改这里——同层前序自动覆盖。
"""

from pathlib import Path

from esflow import Node


class FetchFromBili(Node):
    id = "fetch_from_bili"
    title = "从 B 站抓取"

    def accept(self, ctx) -> bool:
        for up in ctx.layer(self.depth):
            data_file = (up or {}).get("data_file")
            if data_file and Path(data_file).exists():
                return False
        return True

    def run(self, ctx) -> dict:
        target = ctx.layer(self.depth - 1)[0]["target"]
        data = f"<B站抓取到的 {target} 正文>"
        data_file = self.output_dir / "raw.txt"
        data_file.write_text(data)
        return {"source": "bili", "data_file": str(data_file), "fallback": True}
