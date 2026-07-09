"""cli new 命令测试:生成 skill 模板且 demo 可加载执行。"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import esflow.htmlview as htmlview_mod
import esflow.runner as runner_mod
from esflow.cli import _handle_checkpoint_command, cmd_debug, cmd_new, main
from esflow.entrypoint import run_flow_script
from esflow.runner import Runner


OCR_EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "ocr_flow"


def _debug_root(monkeypatch, tmp_path: Path) -> Path:
    root = tmp_path / "debug_root"
    monkeypatch.setattr(runner_mod, "DEBUG_OUTPUT_ROOT", root)
    return root


class _CheckpointRunner:
    """记录 checkpoint 命令映射到哪个控制动作。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def resume(self) -> None:
        self.calls.append(("resume", None))

    def retry(self, run_id: str) -> None:
        self.calls.append(("retry", run_id))

    def abort(self) -> None:
        self.calls.append(("abort", None))


def test_new_generates_runnable_template(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    rc = main(["new", "demo_skill"])
    assert rc == 0

    root = tmp_path / "demo_skill"
    assert (root / "SKILL.md").exists()
    scripts = root / "scripts"
    assert (scripts / "flow.py").exists()
    assert (scripts / "run.py").exists()
    assert (scripts / "nodes" / "fetch.py").exists()
    assert (scripts / "nodes" / "analyze.py").exists()
    assert (scripts / "nodes" / "report.py").exists()

    # 生成的 demo 能加载并跑通
    runner = Runner.load(str(scripts))

    async def drive():
        async for _ in runner.run():
            pass

    asyncio.run(drive())
    assert runner.state.status == "done"
    assert "html 大小" in runner.artifacts["report"]["summary"]
    assert (Path(runner.artifacts["report"]["report_path"])).exists()


def test_new_template_runpy_supports_out_and_resume(tmp_path: Path, monkeypatch) -> None:
    """cli new 生成的 scripts/run.py 默认支持 --out 与 --resume。"""
    monkeypatch.chdir(tmp_path)
    rc = main(["new", "demo_skill"])
    assert rc == 0

    scripts = tmp_path / "demo_skill" / "scripts"
    out_dir = tmp_path / "run"
    env = os.environ.copy()
    repo_root = Path(__file__).resolve().parent.parent
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")

    first = subprocess.run(
        [sys.executable, str(scripts / "run.py"), "--out", str(out_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert first.returncode == 0, first.stderr
    assert (out_dir / runner_mod.ESFLOW_META_DIR / "report" / runner_mod._ARTIFACT_FILE).exists()

    second = subprocess.run(
        [sys.executable, str(scripts / "run.py"), "--resume", str(out_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert second.returncode == 0, second.stderr


def test_run_flow_script_resume_does_not_apply_builder_defaults(tmp_path: Path) -> None:
    """resume 默认只继承 metadata,不让 builder 默认值覆盖首跑入参。"""
    flow_dir = tmp_path / "args_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow\n"
        "@flow(id='args_flow')\n"
        "class F:\n"
        "    nodes=['export']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "export.py").write_text(
        "from esflow import Node\n"
        "class Export(Node):\n"
        "    id='export'\n"
        "    def run(self, ctx):\n"
        "        return {'out': self.kwargs.get('out')}\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "run"
    calls: list[str] = []

    def build_node_args(args):
        calls.append("called")
        return {"export": {"out": "default-out"}}

    rc = asyncio.run(
        run_flow_script(
            flow_dir,
            node_args_builder=build_node_args,
            argv=["--out", str(out_dir)],
        )
    )
    assert rc == 0
    assert calls == ["called"]

    meta = out_dir / runner_mod.ESFLOW_META_DIR / runner_mod._NODE_ARGS_FILE
    meta.write_text('{"export": {"out": "first-out"}}', encoding="utf-8")
    calls.clear()

    rc = asyncio.run(
        run_flow_script(
            flow_dir,
            node_args_builder=build_node_args,
            argv=["--resume", str(out_dir)],
        )
    )

    assert rc == 0
    assert calls == []
    assert '"first-out"' in meta.read_text(encoding="utf-8")
    assert '"default-out"' not in meta.read_text(encoding="utf-8")


def test_checkpoint_short_commands() -> None:
    """checkpoint stdin 只支持 c/r/a 短命令,回车等价于 continue。"""
    runner = _CheckpointRunner()

    _handle_checkpoint_command(runner, "", "review")
    _handle_checkpoint_command(runner, "c", "review")
    _handle_checkpoint_command(runner, "r", "review")
    _handle_checkpoint_command(runner, "a", "review")

    assert runner.calls == [
        ("resume", None),
        ("resume", None),
        ("retry", "review"),
        ("abort", None),
    ]


def test_new_refuses_existing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dup").mkdir()
    rc = main(["new", "dup"])
    assert rc == 1


def test_new_pascal_class_name(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    main(["new", "my-cool-flow"])
    flow_py = (tmp_path / "my-cool-flow" / "scripts" / "flow.py").read_text(
        encoding="utf-8"
    )
    assert "class MyCoolFlow:" in flow_py


def test_new_strips_path(tmp_path: Path, monkeypatch) -> None:
    """name 含路径时目录按完整路径创建,id/类名取末段(避免斜杠)。"""
    monkeypatch.chdir(tmp_path)
    rc = main(["new", "temp/okr"])
    assert rc == 0
    # 目录创建在 temp/okr(完整路径)
    assert (tmp_path / "temp" / "okr" / "SKILL.md").exists()
    flow_py = (tmp_path / "temp" / "okr" / "scripts" / "flow.py").read_text(
        encoding="utf-8"
    )
    # id/类名取末段 okr,不含斜杠
    assert "class Okr:" in flow_py
    assert 'id="okr"' in flow_py


def test_debug_node_refuses_when_upstream_missing(tmp_path: Path, monkeypatch, capsys) -> None:
    """debug --node X 上游产物缺失时,cmd_debug 返回 1 并打印提示,不启动 view。"""
    _debug_root(monkeypatch, tmp_path)
    example = Path(__file__).resolve().parent.parent / "examples" / "quickstart_flow"

    class _Args:
        flow_dir = str(example)
        node = ["export"]
        clear = True

    rc = cmd_debug(_Args())
    assert rc == 1
    err = capsys.readouterr().err
    assert "上游产物缺失" in err
    assert "esflow debug" in err


def test_run_from_requires_out(capsys) -> None:
    """run --from 必须指定 --out,否则无法复用上游产物。"""
    rc = main(["run", str(OCR_EXAMPLE), "--from", "ocr"])

    assert rc == 1
    assert "--from 需要同时指定 --out" in capsys.readouterr().err


def test_run_from_refuses_when_upstream_missing(tmp_path: Path, capsys) -> None:
    """run --from X 上游产物缺失时提前失败,不启动节点执行。"""
    rc = main(["run", str(OCR_EXAMPLE), "--out", str(tmp_path / "out"), "--from", "ocr"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "上游产物缺失" in err
    assert "esflow run" in err


def test_run_from_depth_requires_out(capsys) -> None:
    """run --from-depth 必须指定 --out,否则无法复用上游产物。"""
    rc = main(["run", str(OCR_EXAMPLE), "--from-depth", "2"])

    assert rc == 1
    assert "--from-depth 需要同时指定 --out" in capsys.readouterr().err


def test_run_from_depth_out_of_range(tmp_path: Path, capsys) -> None:
    """run --from-depth 越界提前失败,不启动节点执行。"""
    rc = main(["run", str(OCR_EXAMPLE), "--out", str(tmp_path / "out"), "--from-depth", "99"])

    assert rc == 1
    assert "--from-depth 越界" in capsys.readouterr().err


def test_run_from_and_from_depth_mutually_exclusive(tmp_path: Path) -> None:
    """--from 与 --from-depth 互斥,argparse 退出码 2。"""
    with pytest.raises(SystemExit) as exc:
        main(["run", str(OCR_EXAMPLE), "--out", str(tmp_path / "out"),
              "--from", "ocr", "--from-depth", "2"])
    assert exc.value.code == 2


class _FakeRunner:
    """最小 runner 替身:run() 立即结束(空 async gen),不卡 checkpoint。"""

    def __init__(self) -> None:
        self.state = SimpleNamespace(status="done")
        self.debug = True

    def clear_debug(self) -> None:
        pass

    def missing_upstream(self, nodes: set[str]) -> list[str]:
        return []

    def resume(self) -> None:
        pass

    def retry(self, run_id: str) -> None:
        pass

    def abort(self) -> None:
        pass

    async def run(self, nodes=None, break_before=None):
        if False:  # pragma: no cover
            yield


def test_view_auto_port_when_8765_occupied(monkeypatch, tmp_path: Path, capsys) -> None:
    """8765 被占时 run_html_view 用 port=0 让 OS 分配可用端口,不抛 OSError。"""
    # 占住 8765
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    blocker.bind(("127.0.0.1", 8765))
    blocker.listen(1)
    try:
        # 不开真浏览器,Runner 用替身让 run() 立即结束
        monkeypatch.setattr(
            htmlview_mod, "webbrowser", SimpleNamespace(open=lambda *a, **k: None)
        )
        monkeypatch.setattr(
            htmlview_mod.Runner, "load", staticmethod(lambda *a, **k: _FakeRunner())
        )

        from esflow.htmlview import run_html_view

        rc = asyncio.run(run_html_view(str(tmp_path), debug=True, only={"x"}))
        assert rc == 0
        out = capsys.readouterr().out
        assert "http://127.0.0.1:" in out
        assert "127.0.0.1:8765" not in out
    finally:
        blocker.close()
