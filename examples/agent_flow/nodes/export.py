"""export 节点:读 agent_summary/summary.txt,导出最终结果。"""

from esflow import Node


class Export(Node):
    id = "export"
    title = "导出"

    def run(self, ctx) -> dict:
        agent_art = ctx.get("agent_summary")
        summary_path = agent_art["output_dir"] + "/summary.txt"
        with open(summary_path, encoding="utf-8") as f:
            summary = f.read()
        out_path = str(self.output_dir / "result.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"# 最终结果\n\n{summary}\n")
        return {"out_path": out_path, "summary": summary}
