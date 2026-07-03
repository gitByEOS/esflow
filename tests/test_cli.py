"""cli new 命令测试:生成 skill 模板且 demo 可加载执行。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import easyflow.runner as runner_mod
from easyflow.cli import _handle_checkpoint_command, cmd_debug, cmd_new, main
from easyflow.runner import Runner


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

    def retry(self, step_id: str) -> None:
        self.calls.append(("retry", step_id))

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
    assert "easyflow debug" in err


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
    assert "easyflow run" in err
