"""agent_fanout_flow:multi-subagent 并行审查 git 改动 → 表格汇报。

链路:
    python3 examples/agent_fanout_flow/run.py            # 默认审最近 5 条
    python3 examples/agent_fanout_flow/run.py --n 10     # 审最近 10 条

split 取 git log N 条 → FanOut 展开成 N 个 review_by_subagent 副本(框架层并行),
每个副本 run 内起一个独立的 cursor-agent 无头进程(multi-subagent:N 条 commit
由 N 个 subagent 并行各审一条)→ report 汇总成 markdown 表格。

需 CURSOR_API_KEY 环境变量 + cursor-agent 在 PATH。
"""

from esflow import flow, edge


@flow(id="agent_fanout_flow", title="Git 改动 multi-subagent 并行审查")
class GitReviewFlow:
    nodes = ["split", "review_by_subagent", "report"]
    edges = [
        edge("split", "review_by_subagent"),
        edge("review_by_subagent", "report"),
    ]
    dynamic = {"review_by_subagent"}
