"""worker 节点:并行副本,按 index 切片做自己那份题。

接手确认:上游 fetch 有任务;脱手确认:自己产物非空。
index/replica_id 由 runner 注入实例。
"""

from easyflow import Node


class Worker(Node):
    id = "worker"
    title = "并行处理"

    def accept(self, ctx) -> bool:
        # 接手:上游有任务可分
        return bool(ctx.get("fetch")["tasks"])

    def run(self, ctx) -> dict:
        tasks = ctx.get("fetch")["tasks"]
        # 5 副本各分一份,self.index 决定取哪些
        mine = [t for i, t in enumerate(tasks) if i % 5 == self.index]
        return {"worker_index": self.index, "results": [t * 10 for t in mine]}

    def deliver(self, artifact) -> bool:
        # 脱手:产物有结果
        return len(artifact["results"]) > 0
