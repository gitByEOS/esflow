"""cli new 命令测试:生成 skill 模板且 demo 可加载执行。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import easyflow.runner as runner_mod
from easyflow.cli import cmd_debug, cmd_new, main
from easyflow.runner import Runner


def _debug_root(monkeypatch, tmp_path: Path) -> Path:
    root = tmp_path / "debug_root"
    monkeypatch.setattr(runner_mod, "DEBUG_OUTPUT_ROOT", root)
    return root


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
    assert runner.artifacts["report"]["report"] == "共 3 项,合计 6"


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
    assert "easyflow debug" in err
