"""agent_flow:演示 TO_AGENT checkpoint,外部 agent 介入写产物。

链路:
    esflow run examples/agent_flow --out /tmp/agent_run   # 跑到 agent_summary 退出
    # agent 读 stderr 拿上游产物,写 summary.txt 到 /tmp/agent_run/agent_summary/
    esflow run examples/agent_flow --resume /tmp/agent_run  # 框架扫文件 + deliver + 跑下游
"""

from esflow import flow, edge


@flow(id="agent_flow", title="Agent 介入示例")
class AgentFlow:
    nodes = ["gen_task", "agent_summary", "export"]
    edges = [
        edge("gen_task", "agent_summary"),
        edge("agent_summary", "export"),
    ]
