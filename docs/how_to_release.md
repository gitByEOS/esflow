# 如何打包发布

本文档说明 EasyFlow 的本地打包、验证和发布流程。版本变化见 [CHANGELOG.md](../CHANGELOG.md)。

## 一键打包

在仓库根目录执行:

```bash
bash tools/build.sh
```

脚本会依次执行:

1. `bash tools/check.sh`
2. 清理旧的 `dist/`、`build/`、`esflow.egg-info/`
3. 构建 sdist 和 wheel
4. 创建临时 venv
5. 安装 wheel
6. 执行 `esflow --help` 验证 CLI 可用

成功后产物在:

```text
dist/
  esflow-<version>-py3-none-any.whl
  esflow-<version>.tar.gz
```

## 本地安装测试

如果改了代码但没改版本号,用强制重装:

```bash
pip install --force-reinstall dist/*.whl
```

建议用干净虚拟环境验证:

```bash
python3 -m venv /tmp/esflow-user-test
/tmp/esflow-user-test/bin/pip install --force-reinstall dist/*.whl
/tmp/esflow-user-test/bin/esflow new demo
/tmp/esflow-user-test/bin/python demo/scripts/run.py
```

## 发布前检查

发布前必须确认:

- `bash tools/build.sh` 通过
- `CHANGELOG.md` 已写清版本变化和已知限制
- `pyproject.toml` 版本号与 changelog 一致
- README 示例和 CLI 行为一致
- LICENSE、包元信息、已知限制齐全

## 发布步骤

1. 更新 `CHANGELOG.md`
2. 更新 `pyproject.toml` 版本号
3. 执行 `bash tools/build.sh`
4. 用临时虚拟环境安装 `dist/*.whl`
5. 创建 git tag,例如 `v0.1.0-alpha`
6. 上传 `dist/*` 到目标仓库

## 回滚策略

如果包已发布后发现问题:

- 不删除已发布版本,发布新的 patch 或 alpha 版本
- 在 `CHANGELOG.md` 标记修复内容
- 若问题影响 artifact 契约或 CLI 续跑语义,在 README 中补迁移说明
