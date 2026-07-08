# Next Plan:框架元数据隔离到 `.esflow/`

## 目标

把框架自己的状态文件(`artifact.json` / `_break_to_agent.json` / `_flow_dir.txt`)从节点产物目录挪到 `<job_dir>/.esflow/` 隐藏子目录,让 `<job_dir>/<run_id>/` 只装节点业务产物。消除"框架实现细节混入用户验收目录"的边界混乱。挪入 `.esflow/` 后文件名去掉下划线前缀(目录已是隐藏边界,前缀冗余)。

## 结构对比

### 当前

```text
<job_dir>/
  <run_id>/
    artifact.json          # 框架元数据,混在业务产物里
    result.txt             # 用户业务产物
    subtitle.srt
  _break_to_agent.json     # 框架状态,job 级
  _flow_dir.txt            # 框架状态,job 级
```

### 目标

```text
<job_dir>/
  .esflow/
    <run_id>/
      artifact.json        # 框架元数据,per-node
    break_to_agent.json   # job 级状态
    flow_dir.txt          # resume 找回 flow
  <run_id>/
    result.txt             # 纯用户业务产物
    subtitle.srt
```

`<run_id>` 目录树镜像两份:业务一份(用户看)、框架一份(隐藏)。用户视角只有业务那棵。

## 改动边界

### 路径常量

`esflow/runner.py` 顶部加一个常量,集中框架元数据根:

```python
ESFLOW_META_DIR = ".esflow"
```

### 逐函数改动

| 函数 | 文件:行 | 当前路径 | 目标路径 |
|---|---|---|---|
| `_persist_artifact` | runner.py:319 | `job_dir/<rid>/artifact.json` | `job_dir/.esflow/<rid>/artifact.json` |
| `_load_persisted_artifacts` | runner.py:336 | `job_dir/<rid>/artifact.json` | `job_dir/.esflow/<rid>/artifact.json` |
| `_invalidate_runs` | runner.py:358 | `rmtree(job_dir/<rid>)` | `rmtree(job_dir/.esflow/<rid>)` + `rmtree(job_dir/<rid>)`(见决策 1) |
| `_break_to_agent_path` | runner.py:361 | `job_dir/_break_to_agent.json` | `job_dir/.esflow/break_to_agent.json` |
| `_clear_break_to_agent` | runner.py:381 | 同上 | 同上 |
| retry 清磁盘 | runner.py:716 | `job_dir/<s2>/artifact.json` | `job_dir/.esflow/<s2>/artifact.json` |
| `missing_upstream` | runner.py:286 | `job_dir/<rid>/artifact.json` | `job_dir/.esflow/<rid>/artifact.json` |
| `clear_debug` | runner.py:272 | `rmtree(job_dir)` | 不变(debug 目录是框架专属,整体删) |

### `esflow/cli.py`

| 函数 | 文件:行 | 改动 |
|---|---|---|
| `_lookup_flow_dir` | cli.py:113 | `job_dir/_flow_dir.txt` → `job_dir/.esflow/flow_dir.txt` |
| `_record_flow_dir` | cli.py:125 | 同上 |
| `cmd_run` 防误跑检查 | cli.py:152 | `(out_path/_break_to_agent.json).exists()` → `(out_path/.esflow/break_to_agent.json).exists()` |

### TO_AGENT 扫文件构造 artifact

`_run_one` TO_AGENT 分支(runner.py:596-599)扫 `node.output_dir` 下文件,当前排除 `artifact.json` 和 `.` 开头文件。改完后 `artifact.json` 不在 `output_dir` 了,排除规则简化为只排除 `.` 开头(保留无害,防用户业务目录里有其他隐藏文件)。

## 不变的(用户 API 零影响)

- `Node.output_dir` 注入语义不变,仍指业务产物目录(`job_dir/<rid>` 或节点自定义)
- `_expand_fanout`(runner.py:545)注入 `node.output_dir = job_dir/rid` 不变
- `_run_one` 普通/TO_AGENT 分支的 `output_dir` fallback 逻辑不变
- 节点子类代码零改动,`self.output_dir / "xxx"` 写法照旧
- `Runner` 公共 API(`load`/`run`/`run_to_break`/`resume`/`retry`/`abort` 等)签名不变
- CLI flag 行为不变

## 决策点(已定)

### 决策 1:`_invalidate_runs` 删什么

**定:同时删 `.esflow/<rid>/`(框架元数据)和 `<rid>/`(业务产物)。**

理由:`invalidate_all` / `from_node` / `retry` 都是用户**主动要重跑**节点,旧业务产物留着会和新产物混淆。当前行为就是 `rmtree(job_dir/rid)` 连业务一起删,改动不改变这个语义,只额外删 `.esflow/<rid>/` 保持框架状态一致。破坏半径可控。

### 决策 2:`clear_debug` 删什么

**定:仍 `rmtree(job_dir)` 整个删。**

debug 模式 `job_dir = /tmp/esflow/debug/<flow_id>/`,整个目录都是框架管的,无用户业务产物担忧。简单。

### 决策 3:`--out` 模式下用户业务产物的命运

**定:框架只删自己管的 `.esflow/`,但 `_invalidate_runs` 按决策 1 仍删业务目录。**

这两个看起来矛盾,实际不矛盾:`_invalidate_runs` 是"重跑节点"场景,删业务产物合理;框架**不会**在别的地方主动删用户业务文件。`clear_debug` 是 debug 专属场景,见决策 2。

### 决策 4:是否给 `.esflow/` 加 `.gitignore` 提示

**定:不加。** `/tmp` 默认根不在 git 工作区;`--out` 指到工作区时由用户自己决定是否 ignore。框架不替用户管 git。

## 测试验证清单

改完按顺序跑,每步验证:

1. **单元测试全绿**:`pytest tests/test_runner.py tests/test_skip.py` — 验证加载/续跑/TO_AGENT 三条链路
2. **默认 run 落盘验证**:
   ```bash
   esflow run examples/quickstart_flow
   ls /tmp/esflow/outputs/quickstart_flow/<job_id>/
   # 期望:看到 .esflow/ 和各 <run_id>/,<run_id>/ 里只有业务文件
   ls /tmp/esflow/outputs/quickstart_flow/<job_id>/.esflow/
   # 期望:各 <run_id>/artifact.json
   ```
3. **`--out` 续跑验证**:
   ```bash
   esflow run examples/quickstart_flow --out ./runs/a
   esflow run examples/quickstart_flow --out ./runs/a --from review
   # 期望:续跑成功,上游产物从 .esflow/ 加载
   ```
4. **TO_AGENT 链路验证**:
   ```bash
   esflow run examples/agent_flow --out ./runs/agent
   # 期望:exit 2,stderr 打 resume_hint,./runs/agent/.esflow/break_to_agent.json 存在
   # 手动写产物到 ./runs/agent/<to_agent 节点>/
   esflow run --resume ./runs/agent
   # 期望:扫业务目录构造 artifact,deliver 通过,跑下游
   ```
5. **debug 单调试验证**:
   ```bash
   esflow debug examples/quickstart_flow
   esflow debug examples/quickstart_flow --node review
   # 期望:上游产物从 /tmp/esflow/debug/quickstart_flow/.esflow/ 加载
   ```
6. **TO_AGENT 自定义 output_dir 验证**:节点在 `accept` 里设 `self.output_dir` 指向业务目录,确认框架元数据落 `.esflow/<rid>/`,业务产物落节点自定义目录,两者物理分离。

## 文档同步清单

改动落地后同步:

- [ ] `docs/artifacts.md` — 路径示例(第 29/37/45/70/76 行)、TO_AGENT 链路描述(第 116-132 行)
- [ ] `docs/debug.md` — 产物目录表格(第 16 行)
- [ ] `docs/ref/Runner.md` — `job_dir` 推导表、产物持久化段
- [ ] `docs/ref/Node.md` — `output_dir` 运行时字段说明(强调只装业务产物,框架元数据在 `.esflow/`)
- [ ] `docs/ref/Checkpoint.md` — TO_AGENT 用法路径描述
- [ ] `CHANGELOG.md` — 新增 0.1.3 条目

## 不在本计划内

- 路径扁平化(默认 run 的 `/tmp/esflow/outputs/<flow_id>/<job_id>/<run_id>/` 三层嵌套保持不变,是 /tmp 共享根的防冲突结构)
- `clear` 语义重构(现有 `clear_debug` / `from_node` invalidate 行为不变)
- artifact.json schema 变更
