"""report 节点:用 ctx.gather 收集所有 subagent 审查,生成 markdown 表格汇报。"""

from esflow import Node


class Report(Node):
    id = "report"
    title = "生成改动汇报"

    def accept(self, ctx) -> bool:
        # 接手:至少有一个副本产物
        return len(ctx.gather("review_by_subagent")) > 0

    def run(self, ctx) -> dict:
        reviews = ctx.gather("review_by_subagent")
        lines = [
            "# Git 改动汇报",
            "",
            f"共审查 {len(reviews)} 条 commit。",
            "",
            "| commit | 作者 | 日期 | 标题 | subagent 审查 |",
            "|---|---|---|---|---|",
        ]
        for r in reviews:
            # 竖线转义,避免破坏表格
            subject = r["subject"].replace("|", "\\|")
            review = r["review"].replace("|", "\\|")
            lines.append(
                f"| {r['hash']} | {r['author']} | {r['date']} | {subject} | {review} |"
            )
        table = "\n".join(lines)
        out_path = str(self.output_dir / "report.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(table)
        return {"report_path": out_path, "count": len(reviews), "report": table}

    def deliver(self, artifact) -> bool:
        # 脱手:有条目且报告文件写入
        return artifact["count"] > 0 and bool(artifact["report"])
