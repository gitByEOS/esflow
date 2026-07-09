# Agent Guidelines

## Changelog

- 改动用户可见行为、CLI/API 语义、产物格式、发布版本时,必须同步更新 `CHANGELOG.md`。
- changelog 只写发布层面的变化,避免实现过程描述。
- 行为差异要写准确:库式 API、CLI、生成模板分别说明,不要混成一条。
- 破坏性或兼容风险必须显式标出,不要放进普通“改进”里。
- 每个版本条目保持精简,优先描述用户需要知道的影响。

## Check

- 完成代码修改后,优先使用 `bash tools/check.sh` 做统一验证。
- 只需要局部快速验证时,可先跑相关测试;收尾前仍以 `tools/check.sh` 为准。
