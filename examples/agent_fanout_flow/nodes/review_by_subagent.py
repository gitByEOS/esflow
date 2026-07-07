"""review_by_subagent 节点:一个 subagent 审一条 commit,载体用 cursor-agent 无头模式。

框架层并行:split 的 FanOut 展开 N 个副本,runner asyncio.gather 并行跑,
每个副本 run 内调 self.subagent.run——对框架就是普通同步调用,to_thread 包并行。
"""

import subprocess

from esflow import Node


class _CursorAgent:
    """cursor-agent 无头模式客户端。需 CURSOR_API_KEY + cursor-agent 在 PATH。

    run 起独立子进程,无共享状态,to_thread 并行多副本天然安全。
    --yolo 跳过目录信任提示;--output-format text 只要纯文本结果;timeout 防卡死。
    """

    def run(self, prompt: str, context: dict | None = None) -> str:
        result = subprocess.run(
            ["cursor-agent", "-p", prompt, "--output-format", "text", "--yolo"],
            capture_output=True, text=True, timeout=180,
        )
        if result.returncode != 0:
            # 不吞 stderr,让框架 error 事件能看到 cursor-agent 真实失败原因
            raise RuntimeError(
                f"cursor-agent 退出码 {result.returncode}\nstderr:\n{result.stderr.strip()}"
            )
        return result.stdout.strip()


class ReviewBySubAgent(Node):
    id = "review_by_subagent"
    title = "multi-subagent 审查单条 commit"

    # 载体在节点里定:类属性,副本共享
    subagent = _CursorAgent()

    def accept(self, ctx) -> bool:
        # 接手:有自己的 commit
        return self.fanout_payload is not None

    def run(self, ctx) -> dict:
        commit = self.fanout_payload
        prompt = (
            f"审查 git commit {commit['hash']}\n"
            f"作者:{commit['author']}  日期:{commit['date']}\n"
            f"标题:{commit['subject']}\n"
            f"改动统计:\n{commit['diff']}\n\n"
            f"给出:改动类型、风险等级、合并建议"
        )
        review = self.subagent.run(prompt, context={"commit": commit})
        return {
            "hash": commit["hash"],
            "author": commit["author"],
            "date": commit["date"],
            "subject": commit["subject"],
            "review": review,
        }

    def deliver(self, artifact) -> bool:
        # 脱手:有审查意见
        return bool(artifact["review"])
