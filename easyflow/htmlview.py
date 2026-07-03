"""纯标准库 HTML 调试界面。

    easyflow view ./my_flow

启动后开浏览器看:
- 上:DAG 拓扑图,节点按状态着色(● idle 灰 / running 黄 / paused 蓝 / done 绿 / error 红)
- 中:节点列表 + artifact JSON
- 下:事件流时间线(SSE 实时推送)
- checkpoint 时按钮亮起:resume / retry / abort

零外部依赖,纯 asyncio + http。runner 跑后台 task,事件经 SSE 推前端,
按钮 POST /control 调 runner.resume/retry/abort。
"""

from __future__ import annotations

import asyncio
import json
import sys
import textwrap
import webbrowser
from typing import Any

from .event import WorkflowJobEvent
from .runner import Runner
from .state import StepState

_STATUS_COLOR = {
    "idle": "#8b949e",
    "queued": "#38bdf8",
    "running": "#38bdf8",
    "paused": "#6b8afb",
    "done": "#468062",
    "error": "#f87171",
    "skipped": "#6b7280",
}


def _layered_layout(
    node_ids: list[str], edges: list[tuple[str, str]]
) -> dict[str, tuple[int, int]]:
    """Kahn 分层布局:节点按最长路径分层,同层水平排列。返回 {id: (col, row)}。"""
    indeg: dict[str, int] = {n: 0 for n in node_ids}
    adj: dict[str, list[str]] = {n: [] for n in node_ids}
    for f, t in edges:
        if f not in indeg or t not in indeg:
            continue  # 端点不在节点集(动态扩图中间态),跳过
        adj[f].append(t)
        indeg[t] += 1
    # 最长路径分层:拓扑序累加深度
    depth: dict[str, int] = {}
    queue = [n for n in node_ids if indeg[n] == 0]
    for n in queue:
        depth[n] = 0
    while queue:
        n = queue.pop(0)
        for m in adj[n]:
            depth[m] = max(depth.get(m, 0), depth[n] + 1)
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    # 同层节点按出现顺序排
    by_layer: dict[int, list[str]] = {}
    for n, d in depth.items():
        by_layer.setdefault(d, []).append(n)
    pos: dict[str, tuple[int, int]] = {}
    for layer, nodes in by_layer.items():
        for row, n in enumerate(nodes):
            pos[n] = (layer, row)
    return pos


def _dag_node_ids(runner: Runner) -> list[str]:
    """渲染用节点集:steps + edges 端点(未扩图的 dynamic 模板节点补为 idle)。"""
    node_ids = list(runner.steps.keys())
    seen = set(node_ids)
    for e in runner.flow.edges:
        for sid in (e.from_, e.to):
            if sid not in seen:
                node_ids.append(sid)
                seen.add(sid)
    return node_ids


def _template_title(runner: Runner, sid: str) -> str:
    """未扩图的 dynamic 模板节点:从 node_classes 取 title。"""
    cls = runner.node_classes.get(sid)
    return getattr(cls, "title", None) or getattr(cls, "id", sid) or sid


def _artifact_files(runner: Runner, sid: str) -> list[tuple[str, str]]:
    """从节点 artifact dict 里提取产物 (文件名, 完整路径),值是 output_dir 下的路径。"""
    if sid not in runner.steps:
        return []
    st = runner.state.steps.get(sid)
    if not st or not isinstance(st.artifact, dict):
        return []
    output_dir = str(runner.steps[sid].node.output_dir)
    return [
        (v.rsplit("/", 1)[-1], v)
        for v in st.artifact.values()
        if isinstance(v, str) and v.startswith(output_dir)
    ]


def _svg_dag(runner: Runner) -> str:
    """渲染 DAG 为 SVG,节点矩形 + 箭头 + 状态色块;有产物的节点下方挂产物文件名。"""
    node_ids = _dag_node_ids(runner)
    edges = [(e.from_, e.to) for e in runner.flow.edges]
    pos = _layered_layout(node_ids, edges)
    node_w, node_h, gap_x, gap_y, pad = 180, 44, 60, 30, 40
    # 计算每层节点数定画布
    max_row = max((r for _, r in pos.values()), default=0)
    max_col = max((c for c, _ in pos.values()), default=0)
    width = pad * 2 + (max_col + 1) * (node_w + gap_x)
    height = pad * 2 + (max_row + 1) * (node_h + gap_y)

    def xy(sid: str) -> tuple[int, int]:
        c, r = pos[sid]
        return pad + c * (node_w + gap_x), pad + r * (node_h + gap_y)

    parts = [f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">']
    # 先画边:贝塞尔曲线止于离目标节点 14px 处,手动画箭头三角填补,尖端贴节点边缘 2px
    ARROW_LEN = 12
    ARROW_GAP = 14  # 线终点离节点左边缘的距离
    TIP_GAP = 2     # 箭头尖端离节点左边缘的距离
    for f, t in edges:
        if f not in pos or t not in pos:
            continue
        x1, y1 = xy(f)
        x2, y2 = xy(t)
        x1 += node_w + 2
        y1 += node_h // 2
        node_left = x2
        line_end = node_left - ARROW_GAP
        y2 += node_h // 2
        dx = line_end - x1
        off = max(40, abs(dx) * 0.5)
        cx1, cx2 = x1 + off, line_end - off
        parts.append(
            f'<path d="M{x1},{y1} C{cx1},{y1} {cx2},{y2} {line_end},{y2}" '
            f'stroke="#4b5563" stroke-width="1.5" fill="none"/>'
        )
        # 箭头三角:尾在线终点,尖端在 node_left - TIP_GAP
        tip_x = node_left - TIP_GAP
        parts.append(
            f'<polygon points="{line_end},{y2 - 5} {line_end},{y2 + 5} {tip_x},{y2}" '
            f'fill="#4b5563"/>'
        )
    # 再画节点
    for sid in node_ids:
        if sid in runner.steps:
            st: StepState = runner.state.steps.get(sid, StepState(step_id=sid))
            color = _STATUS_COLOR.get(st.status, "#ffffff")
            title = runner.steps[sid].title
        else:
            # 未扩图的 dynamic 模板节点:渲染为 idle
            st = StepState(step_id=sid)
            color = _STATUS_COLOR["idle"]
            title = _template_title(runner, sid)
        x, y = xy(sid)
        parts.append(
            f'<g class="node" data-id="{sid}">'
            f'<rect x="{x}" y="{y}" width="{node_w}" height="{node_h}" rx="6" '
            f'fill="{color}" stroke="#1f2937" stroke-width="1.5"/>'
            f'<text x="{x + 8}" y="{y + 18}" fill="#fff" font-size="13" font-weight="600">{sid}</text>'
            f'<text x="{x + 8}" y="{y + 34}" fill="#e5e7eb" font-size="11">{st.status} · {title}</text>'
            f'</g>'
        )
        # 产物文件名挂在节点矩形下方,SVG file 图标 + 文件名,整组可点击复制完整路径
        files = _artifact_files(runner, sid)
        fx = x + 4
        icon_y = y + node_h + 4
        for fname, fpath in files:
            parts.append(
                f'<g class="artifact-link" data-path="{fpath}" '
                f'transform="translate({fx},{icon_y})">'
                f'<path d="M1,1 h6 l2,2 v8 h-8 z M7,1 v2 h2" '
                f'stroke="#fcd34d" fill="none" stroke-width="1" stroke-linejoin="round"/>'
                f'<text x="13" y="9" fill="#fcd34d" font-size="10" font-family="monospace">{fname}</text>'
                f'</g>'
            )
            fx += len(fname) * 6 + 30
    parts.append("</svg>")
    return "\n".join(parts)


_HTML = textwrap.dedent("""\
<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>easyflow view</title>
<style>
  :root {
    --bg: #0d1117;
    --panel: #161b22;
    --panel-2: #1c2230;
    --border: #2a3140;
    --text: #e6edf3;
    --muted: #8b949e;
    --accent: #2dd4bf;
    --accent-2: #38bdf8;
    --danger: #f87171;
    --ok: #4ade80;
    --warn: #fbbf24;
    --mono: "SF Mono", "JetBrains Mono", "Menlo", Consolas, monospace;
    --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", sans-serif;
  }
  *, *::before, *::after { box-sizing: border-box; }
  html, body { height: 100vh; margin: 0; overflow: hidden; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); -webkit-font-smoothing: antialiased; }
  header {
    padding: 10px 20px; background: var(--panel); border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 16px; height: 52px;
  }
  header h1 { margin: 0; font-size: 15px; font-weight: 600; letter-spacing: -0.01em; }
  header h1 .dot { color: var(--accent); }
  header .status {
    font-size: 12px; font-family: var(--mono); color: var(--muted);
    padding: 2px 8px; border: 1px solid var(--border); border-radius: 6px;
  }
  header .controls { margin-left: auto; display: flex; gap: 8px; }
  header button {
    padding: 6px 14px; border: 1px solid var(--border); border-radius: 6px;
    font-size: 13px; font-family: var(--mono); cursor: pointer;
    background: transparent; color: var(--text); transition: all .15s;
  }
  header button:hover:not(:disabled) { border-color: var(--accent); color: var(--accent); }
  header button:disabled { opacity: .3; cursor: not-allowed; }
  #start  { border-color: var(--accent); color: var(--accent); }
  #start:hover:not(:disabled) { background: var(--accent); color: var(--bg); }
  #resume { border-color: var(--ok); color: var(--ok); }
  #resume:hover:not(:disabled) { background: var(--ok); color: var(--bg); }
  #retry  { border-color: var(--warn); color: var(--warn); }
  #retry:hover:not(:disabled) { background: var(--warn); color: var(--bg); }
  #abort { border-color: var(--danger); color: var(--danger); }
  #abort:hover:not(:disabled) { background: var(--danger); color: var(--bg); }
  main {
    display: grid; grid-template-columns: 1fr 360px; gap: 1px;
    background: var(--border); height: calc(100vh - 52px);
  }
  .panel { background: var(--bg); overflow: auto; padding: 16px; }
  #dag { position: relative; overflow: hidden; cursor: grab; }
  #dag.dragging { cursor: grabbing; }
  #dag-canvas { position: absolute; inset: 0; }
  #dag-canvas svg { transform-origin: 0 0; }
  #dag .legend {
    position: absolute; left: 50%; bottom: 12px; transform: translateX(-50%);
    background: rgba(13, 17, 23, .85); border: 1px solid var(--border);
    border-radius: 8px; padding: 8px 14px; pointer-events: none;
    display: flex; align-items: center; gap: 14px; backdrop-filter: blur(6px);
  }
  #dag .legend .item { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; }
  #dag .legend .dot { width: 9px; height: 9px; border-radius: 50%; flex: none; }
  #dag .legend h4 { display: none; }
  #side { display: flex; flex-direction: column; gap: 14px; }
  #side h3 {
    margin: 0; font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: .8px; font-weight: 600;
  }
  #nodes-list { font-size: 13px; }
  #nodes-list .row {
    display: flex; align-items: center; gap: 8px; padding: 5px 8px;
    border-radius: 6px; cursor: pointer; transition: background .12s;
  }
  #nodes-list .row:hover { background: var(--panel); }
  #nodes-list .row.active { background: var(--panel-2); }
  #nodes-list .dot { width: 9px; height: 9px; border-radius: 50%; flex: none; }
  #nodes-list .id { font-family: var(--mono); font-size: 12px; }
  #nodes-list .title { color: var(--muted); margin-left: auto; font-size: 11px; }
  #nodes-list .files { color: #fcd34d; font-family: var(--mono); font-size: 10px; margin-left: 8px; cursor: pointer; }
  #dag-canvas .artifact-link { cursor: pointer; }
  #toast {
    position: fixed; top: 24px; left: 50%; transform: translateX(-50%) translateY(-8px);
    background: #161b22; color: #2dd4bf;
    padding: 9px 20px; border-radius: 8px; font-size: 13px; font-weight: 600;
    border: 1px solid #2dd4bf;
    opacity: 0; transition: opacity .2s, transform .2s; pointer-events: none; z-index: 100;
    box-shadow: 0 4px 12px rgba(0,0,0,.3);
  }
  #toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
  #artifact {
    font-family: var(--mono); font-size: 12px; white-space: pre-wrap;
    background: var(--panel); padding: 10px; border-radius: 8px;
    border: 1px solid var(--border); max-height: 220px; overflow: auto;
    color: var(--text); line-height: 1.5;
  }
  #log { font-family: var(--mono); font-size: 12px; line-height: 1.6; flex: 1; overflow: auto; }
  #log .line { padding: 2px 6px; border-radius: 4px; }
  #log .line.ck { background: rgba(45, 212, 191, .08); color: var(--accent); }
  #log .line.err { color: var(--danger); }
  #log .line.end { color: var(--ok); }
</style>
</head>
<body>
<div id="toast"></div>
<header>
  <h1>easyflow <span class="dot">·</span> <span id="flow-id">-</span></h1>
  <span class="status" id="job-status">idle</span>
  <div class="controls">
    <button id="start">start</button>
    <button id="resume" disabled>resume</button>
    <button id="retry"  disabled>retry</button>
    <button id="abort" disabled>abort</button>
  </div>
</header>
<main>
  <section class="panel" id="dag">
    <div id="dag-canvas"></div>
    <div class="legend">
      <div class="item"><span class="dot" style="background:#8b949e"></span>idle</div>
      <div class="item"><span class="dot" style="background:#38bdf8"></span>running</div>
      <div class="item"><span class="dot" style="background:#6b8afb"></span>paused</div>
      <div class="item"><span class="dot" style="background:#f87171"></span>error</div>
      <div class="item"><span class="dot" style="background:#468062"></span>done</div>
      <div class="item"><span class="dot" style="background:#6b7280"></span>skipped</div>
    </div>
  </section>
  <aside class="panel" id="side">
    <h3>节点</h3>
    <div id="nodes-list"></div>
    <h3>artifact</h3>
    <div id="artifact">(无产物)</div>
    <h3>事件流</h3>
    <div id="log"></div>
  </aside>
</main>
<script>
const es = new EventSource('/events');
let current = null, paused = null, lastDag = '';
es.onmessage = async (e) => {
  const data = JSON.parse(e.data);
  if (data.dag && data.dag !== lastDag) { lastDag = data.dag; document.getElementById('dag-canvas').innerHTML = data.dag; bindNodes(); }
  document.getElementById('flow-id').textContent = data.flow_id;
  document.getElementById('job-status').textContent = data.status;
  document.getElementById('nodes-list').innerHTML = data.nodes.map(n => {
    const files = n.file_paths ? Object.entries(n.file_paths).map(([fname, fpath]) =>
      `<span class="files" data-path="${fpath}" title="点击复制完整路径"><svg width="11" height="11" viewBox="0 0 11 11" style="vertical-align:-1px;margin-right:2px"><path d="M1,1 h6 l2,2 v8 h-8 z M7,1 v2 h2" stroke="#fcd34d" fill="none" stroke-width="1" stroke-linejoin="round"/></svg>${fname}</span>`
    ).join('') : '';
    return `<div class="row${n.id===current?' active':''}" data-id="${n.id}"><span class="dot" style="background:${n.color}"></span><span class="id">${n.id}</span><span class="title">${n.title}</span>${files}</div>`;
  }).join('');
  if (current && data.artifacts[current] !== undefined) {
    document.getElementById('artifact').textContent = JSON.stringify(data.artifacts[current], null, 2);
  } else if (paused) {
    document.getElementById('artifact').textContent = JSON.stringify(data.artifacts[paused] || '(无)', null, 2);
  } else {
    const ids = Object.keys(data.artifacts);
    const last = ids.length ? ids[ids.length - 1] : null;
    document.getElementById('artifact').textContent = last
      ? JSON.stringify(data.artifacts[last], null, 2)
      : '(无产物)';
  }
  const log = document.getElementById('log');
  log.innerHTML = data.events.map(l => `<div class="line ${l.cls}">${l.text}</div>`).join('');
  log.scrollTop = log.scrollHeight;
  paused = data.paused;
  document.getElementById('resume').disabled = !paused;
  document.getElementById('retry').disabled = !paused;
  document.getElementById('start').disabled = data.started;
  const abortable = data.started && (data.status === 'running' || data.status === 'paused');
  document.getElementById('abort').disabled = !abortable;
};
function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => t.classList.remove('show'), 1500);
}
function bindNodes() {
  document.querySelectorAll('#dag-canvas .node').forEach(g => {
    g.onclick = () => { current = g.dataset.id; es.onmessage({data: es.lastEventId}); };
  });
  document.querySelectorAll('#nodes-list .row').forEach(r => {
    r.onclick = (e) => { if (e.target.closest('.files')) return; current = r.dataset.id; };
  });
  // 产物文件名点击复制完整路径(DAG + 侧边栏统一),toast 提示
  document.querySelectorAll('#dag-canvas .artifact-link, #nodes-list .files').forEach(el => {
    el.onclick = (e) => {
      e.stopPropagation();
      const path = el.dataset.path;
      navigator.clipboard.writeText(path).then(() => showToast('路径已复制到剪切板'));
    };
  });
  applyView();
}
// DAG 拖动 + 滚轮缩放
const dag = document.getElementById('dag');
const dagCanvas = document.getElementById('dag-canvas');
let view = {x: 0, y: 0, s: 1};
function applyView() {
  const svg = dagCanvas.querySelector('svg');
  if (svg) svg.style.transform = `translate(${view.x}px, ${view.y}px) scale(${view.s})`;
}
let dragging = false, lastX = 0, lastY = 0;
dag.addEventListener('mousedown', (e) => {
  if (e.target.closest('.node') || e.target.closest('.legend')) return;
  dragging = true; lastX = e.clientX; lastY = e.clientY;
  dag.classList.add('dragging');
});
window.addEventListener('mousemove', (e) => {
  if (!dragging) return;
  view.x += e.clientX - lastX; view.y += e.clientY - lastY;
  lastX = e.clientX; lastY = e.clientY; applyView();
});
window.addEventListener('mouseup', () => { dragging = false; dag.classList.remove('dragging'); });
dag.addEventListener('wheel', (e) => {
  e.preventDefault();
  const delta = e.deltaY > 0 ? 0.9 : 1.1;
  view.s = Math.min(3, Math.max(0.3, view.s * delta));
  applyView();
}, {passive: false});
document.getElementById('start').onclick  = () => post({action: 'start'});
document.getElementById('resume').onclick = () => post({action: 'resume'});
document.getElementById('retry').onclick  = () => post({action: 'retry', from_step: paused});
document.getElementById('abort').onclick  = () => post({action: 'abort'});
document.addEventListener('keydown', (e) => {
  if (e.key === 'r') document.getElementById('resume').click();
  else if (e.key === 'e') document.getElementById('retry').click();
  else if (e.key === 'a') document.getElementById('abort').click();
});
async function post(body) {
  await fetch('/control', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
}
</script>
</body>
</html>
""")


def _artifact_summary(artifact: Any) -> str:
    """final 事件 artifact 摘要:字符串原样,dict JSON 截断到 200 字符。"""
    if artifact is None:
        return "(无)"
    if isinstance(artifact, str):
        return artifact if len(artifact) <= 200 else artifact[:200] + "…"
    s = json.dumps(artifact, ensure_ascii=False, default=str)
    return s if len(s) <= 200 else s[:200] + "…"


def _state_snapshot(
    runner: Runner, events: list[WorkflowJobEvent], started: bool = False
) -> dict[str, Any]:
    """构造推给前端的 state 快照。"""
    nodes = []
    for sid in _dag_node_ids(runner):
        if sid in runner.steps:
            st = runner.state.steps.get(sid, StepState(step_id=sid))
            status, title = st.status, runner.steps[sid].title
        else:
            status, title = "idle", _template_title(runner, sid)
        nodes.append(
            {
                "id": sid,
                "status": status,
                "color": _STATUS_COLOR.get(status, "#fff"),
                "title": title,
                "files": [f[0] for f in _artifact_files(runner, sid)],
                "file_paths": dict(_artifact_files(runner, sid)),
            }
        )
    paused = next(
        (sid for sid, s in runner.state.steps.items() if s.status == "paused"),
        None,
    )
    log_lines = []
    for ev in events[-200:]:
        cls = ""
        text = ev.type
        if ev.step_id:
            text += f" [{ev.step_id}]"
        if ev.status:
            text += f" {ev.status}"
        if ev.detail:
            text += f" {ev.detail}"
        if ev.message:
            text += f" {ev.message}"
        if ev.type == "checkpoint":
            cls, text = "ck", text + " ⏸"
        elif ev.type == "error":
            cls = "err"
        elif ev.type == "final":
            cls = "end"
            text += " → " + _artifact_summary(ev.artifact)
        elif ev.type == "end":
            cls = "end"
        log_lines.append({"cls": cls, "text": text})
    return {
        "flow_id": runner.flow.id,
        "status": runner.state.status if started else "idle",
        "started": started,
        "nodes": nodes,
        "dag": _svg_dag(runner),
        "artifacts": {
            sid: s.artifact
            for sid, s in runner.state.steps.items()
            if s.artifact is not None
        },
        "paused": paused,
        "events": log_lines,
    }


async def _read_request(reader: asyncio.StreamReader) -> tuple[str, str, dict, bytes]:
    """读一个 HTTP 请求,返回 (method, path, headers, body)。"""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    method, path, _ = lines[0].split(" ", 2)
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    body = b""
    cl = headers.get("content-length")
    if cl:
        body = await reader.readexactly(int(cl))
    return method, path, headers, body


def _http(resp: bytes, ctype: str = "text/html; charset=utf-8", status: int = 200) -> bytes:
    return (
        f"HTTP/1.1 {status} OK\r\n"
        f"Content-Type: {ctype}\r\n"
        f"Content-Length: {len(resp)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode("latin-1") + resp


async def run_html_view(
    flow_dir: str,
    host: str = "127.0.0.1",
    port: int = 8765,
    debug: bool = False,
    only: set[str] | None = None,
    clear: bool = False,
) -> int:
    """启动 HTML 调试界面:HTTP server + runner。

    debug 模式产物固定目录 + 持久化复用;clear 先清空 debug 目录;
    only 指定单调试节点,break_before=only 让 runner 跑完上游后暂停在指定节点前,
    等 resume 信号才执行该节点。非 only 模式等用户点 start 才开跑。
    """
    runner = Runner.load(flow_dir, debug=debug)
    if clear:
        runner.clear_debug()
    # debug 单点调试:上游产物缺失则命令行提示并退出,不开 view
    if debug and only:
        missing = runner.missing_upstream(set(only))
        if missing:
            print(
                f"单调试 {', '.join(only)} 缺上游产物:{', '.join(missing)}",
                file=sys.stderr,
            )
            print(
                f"先全跑落产物:easyflow debug {flow_dir}",
                file=sys.stderr,
            )
            return 1
    events: list[WorkflowJobEvent] = []
    sse_queues: list[asyncio.Queue] = []
    finished = {"v": False}
    started = {"v": bool(only)}
    start_event = asyncio.Event()
    break_before = set(only) if only else None

    async def drive() -> None:
        # only 模式:自动开跑,上游从磁盘复用,跑到指定节点前暂停等 resume
        # 非 only 模式:等用户点 start
        if not only:
            await start_event.wait()
        async for ev in runner.run(scope=only, break_before=break_before):
            events.append(ev)
            for q in sse_queues:
                await q.put(1)
        finished["v"] = True
        for q in sse_queues:
            await q.put(None)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            method, path, _, body = await _read_request(reader)
        except Exception:
            writer.close()
            return
        if method == "GET" and path == "/":
            writer.write(_http(_HTML.encode("utf-8")))
        elif method == "GET" and path == "/events":
            q: asyncio.Queue = asyncio.Queue()
            sse_queues.append(q)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
            )
            await writer.drain()
            # 先推一次快照
            snap = _state_snapshot(runner, events, started["v"])
            writer.write(f"data: {json.dumps(snap, ensure_ascii=False, default=str)}\n\n".encode("utf-8"))
            await writer.drain()
            while not finished["v"]:
                ev = await q.get()
                if ev is None:
                    break
                snap = _state_snapshot(runner, events, started["v"])
                writer.write(f"data: {json.dumps(snap, ensure_ascii=False, default=str)}\n\n".encode("utf-8"))
                await writer.drain()
            # 推最终态
            snap = _state_snapshot(runner, events, started["v"])
            writer.write(f"data: {json.dumps(snap, ensure_ascii=False, default=str)}\n\n".encode("utf-8"))
            await writer.drain()
            if q in sse_queues:
                sse_queues.remove(q)
        elif method == "POST" and path == "/control":
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {}
            action = payload.get("action")
            if action == "start":
                started["v"] = True
                start_event.set()
            elif action == "resume":
                runner.resume()
            elif action == "retry":
                runner.retry(payload.get("from_step") or "")
            elif action == "abort":
                runner.abort()
            # 每次控制后推一次快照
            for q in sse_queues:
                await q.put(1)
            writer.write(_http(b'{"ok":true}', "application/json"))
        else:
            writer.write(_http(b"not found", status=404))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, host, port)
    url = f"http://{host}:{port}"
    mode = "debug" if debug else "run"
    print(f"easyflow view ({mode}) → {url}")
    if only:
        print(f"单调试节点:{' '.join(only)}(上游自动跑完,在节点前暂停等 resume)")
    else:
        print("浏览器打开后点 start 开始,checkpoint 时按钮控制 resume/retry/abort")
    print("job 跑完后自动退出")
    webbrowser.open(url)

    async with server:
        await drive()
        # 推最终态给在线 SSE 客户端
        for q in sse_queues:
            await q.put(1)
        await asyncio.sleep(1.0)
        server.close()
        await server.wait_closed()
    return 0 if runner.state.status != "error" else 1
