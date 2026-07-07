"""split 节点:取最近 N 条 git 记录,FanOut 展开成 N 个 review 副本。

N 来自环境变量 GIT_REVIEW_N(默认 5),run.py --n 设置。
每条 commit 携带 hash/author/date/subject/diff 作为副本 payload,
review_by_subagent 副本通过 self.fanout_payload 拿到自己那条。
"""

import os
import subprocess

from esflow import Node, FanOut


class Split(Node):
    id = "split"
    title = "取 git log 并扇出"

    def accept(self, ctx) -> bool:
        # 接手:GIT_REVIEW_N 能解析成正整数
        try:
            return int(os.environ.get("GIT_REVIEW_N", "5")) > 0
        except ValueError:
            return False

    def run(self, ctx) -> FanOut:
        n = int(os.environ.get("GIT_REVIEW_N", "5"))
        # tab 分隔取最近 N 条 commit 元信息
        log = subprocess.run(
            ["git", "log", f"-{n}", "--pretty=format:%H%x09%an%x09%ad%x09%s"],
            check=True, capture_output=True, text=True,
        ).stdout
        commits = []
        for line in log.strip().splitlines():
            hash_, author, date, subject = line.split("\t", 3)
            # 文件变更统计(不带 patch body,轻量)
            stat = subprocess.run(
                ["git", "show", "--stat", "--format=", hash_],
                check=True, capture_output=True, text=True,
            ).stdout.strip()
            commits.append({
                "hash": hash_[:8],
                "author": author,
                "date": date,
                "subject": subject,
                "diff": stat,
            })
        return FanOut(base="review_by_subagent", payload=commits)
