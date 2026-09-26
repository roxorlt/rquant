# Serving 模拟账户来源缺失时的边界

## 范围与资产

本次仅调整源码生成的正式 Serving publisher 配置，以及读取来源失败时的分类。资产是 Serving 数据代中各来源的真实性、`paper_accounts` 空表与不可用水位的一致性、稳定的数据代身份。模拟账户权威由 paper broker 发布；Serving 只读其不可变文档与当前指针。其余五个来源各有独立权威，不能因模拟账户缺席而改变读法。

## 失败路径与不变量

1. 模拟账户权威根目录或 `current.json` 尚不存在，或已发布版本在本次 `as_of` 尚不可见：权威 reader 明确抛出 `ServingSourceAuthorityUnavailableError`。正式 profile 将 `paper_accounts` 列为可选来源；assembler 发布空账户表与状态为 `UNAVAILABLE` 的水位，原因来自固定的分类错误。相同缺失原因在不同轮次产生相同来源 generation ID 与 epoch 水位，不因观察时钟反复发布。
2. 权威恢复并通过指针、文档及内容身份验证：读取真实账户及其原有水位。空表不能伪造账户，恢复也不能继续沿用缺失的 generation ID。
3. 已出现的指针/文档若格式、身份、内容、所有权、路径安全或历史链有问题，reader 抛出 `ServingSourceAuthorityIntegrityError`；assembler 必须拒绝整个轮次。未知读取异常也拒绝，不能当成“暂时缺失”。
4. `signals`、`runtime_health` 与 `reference_slow_authority` 仍是必需来源；任一失败，整轮拒绝。已有 `lab_jobs`、`promotions` 可选行为仅接受明确的权威不可用分类；损坏不能降级。

## 排除与验收

不更改生产已部署 profile、数据库、服务配置、切流或模拟交易服务；不补造模拟账户。验收使用本地合成权威：正式 profile 与 authority stage 配置一致；缺失时可生成真实其它来源加空模拟账户；跨轮身份稳定；恢复后读真实账户；损坏的已发布权威和必需来源失败均阻断。聚焦测试与 Ruff 通过后只提交候选，交由独立审查。
