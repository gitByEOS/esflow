"""agent_summary 节点:TO_AGENT checkpoint,产物由外部 agent 写入 output_dir/summary.txt。

框架不调 run,就绪时 emit checkpoint 退出。agent 读 stderr 拿上游 gen_task 产物,
完成总结后写 summary.txt 到本节点 output_dir,调 --resume 续跑。

deliver 校验 summary.txt 存在;框架扫文件构造 artifact = {"output_dir", "files"}。
"""

from esflow import Node, Checkpoint


class AgentSummary(Node):
    id = "agent_summary"
    title = "Agent 总结"
    checkpoint = Checkpoint.TO_AGENT

    def deliver(self, artifact) -> bool:
        return "summary.txt" in artifact.get("files", [])
