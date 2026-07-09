#!/usr/bin/env python3
"""直接跑:python3 examples/agent_fanout_flow/run.py [--n N] [--out DIR] [--resume DIR]

启动前 pass_check 预检(必须在 git 仓库内),--n 通过环境变量传给 split 节点。
"""

import argparse
import asyncio
import os
import subprocess
from pathlib import Path

from esflow import run_flow_script


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

    def apply_args(args):
        os.environ["GIT_REVIEW_N"] = str(args.n)
        return None

    return await run_flow_script(
        Path(__file__).parent,
        checks=(check_git_repo,),
        parser=parser,
        node_args_builder=apply_args,
    )


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
