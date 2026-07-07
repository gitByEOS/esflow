#!/usr/bin/env python3
"""直接跑:python3 examples/agent_fanout_flow/run.py [--n N]

启动前 pass_check 预检(必须在 git 仓库内),--n 通过环境变量传给 split 节点。
"""

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path

from esflow import Runner, esflow_event
from esflow.check import pass_check


def check_git_repo() -> str | None:
    """必须在 git 仓库里跑,split 要取 git log。"""
    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            check=True, capture_output=True,
        )
        return None
    except Exception:
        return "不在 git 仓库内,无法取 git log"


async def main():
    parser = argparse.ArgumentParser(description="扇出 N 个 subagent 审查 git 改动")
    parser.add_argument("--n", type=int, default=5, help="审查最近 N 条 commit(默认 5)")
    args = parser.parse_args()

    pass_check(check_git_repo)
    os.environ["GIT_REVIEW_N"] = str(args.n)

    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        esflow_event(event)


if __name__ == "__main__":
    asyncio.run(main())
