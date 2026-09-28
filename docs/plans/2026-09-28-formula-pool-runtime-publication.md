# 公式池只读投影接入实际 Serving 构建器

## 范围与分级

现有公式池三表仅能由直接构造的 `DuckDBSignalPageProjectionSource` 生成；把显式的 `FormulaPoolServingConfig` 接入生产同款 notifier 的页面投影构建路径，使离线受信配置可以真正生成完整 Serving 数据代。本片只改运行时有类型设置及构建器接线，并用合成数据验证；生产 profile 默认不配置，systemd/nginx、线上目录和服务不改。高风险：这是共享 Serving/通知基础设施，错误接线可能让坏池子被发布或让旧代被误称可用。

## 验收

1. notifier 设置增加单个可选、完整且有类型的公式池只读来源。只有同时配置页面只读副本、Serving authority 和 PageControl 审计快照时才接受；七个受信路径由现有 `FormulaPoolServingConfig` 校验，不能从 `.env`、网页请求或默认目录猜测。未配置时旧运行时设置/生产 profile 的行为与数据代不变。
2. 构建器把该配置原样交给同一次 `DuckDBSignalPageProjectionSource`，复用既有三表同代及创建审计、封存任务、每日结果核验；不在 notifier 重算公式或读取浏览器输入。配置后定义为空应发布显式空组三表；目录、任务、审计、结果不完整时拒绝本轮新数据代，不降级为零池子，不修改旧规则池投影。现有 `SignalPageProjectionProducer.publish()` 在来源失败而已有旧投影时会重发旧表；因此本片还须在已配置公式池的分支禁用该兜底，保持未配置的旧兜底行为。
3. 以合成保存→逐日结果和 notifier 本地运行时构建路径验证三表确实进入一个实际 Serving generation，随后由公式池 Web 只读 API 在同代读取；覆盖未配置旧代、显式空、坏目录/错审计与部分三表拒绝。对共享通知路径做聚焦回归，证明该可选设置不会改变未配置的既有发送/发布行为。
4. 运行直接相关 pytest、Ruff、diff 自检，候选做一次独立高风险终审。真实约 10 GB 数据、Linux owner/权限与资源占用单独验收；生产 profile/部署配置、服务权限或切流须另取用户明确授权。

## 冻结失败模型

- 资产：已核验的公式池定义/逐日结果、PageControl 审计、Serving 数据代与通知运行时现有职责。
- 边界：显式受信运行时设置→notifier 页面投影源→单一 Serving 发布；私有定义/任务/结果与 PageControl 审计进入只读适配器。
- 失败路径：缺半套路径仍启动并发布空表；跳过审计或把旧结果称今天；三表与其他投影跨代拼接；坏公式池来源触发既有旧投影兜底，重发旧公式池表并错误推进新权威代；未配置生产 profile 受新分支影响。
- 不变量：公式池未配置等价于旧行为；配置后完整且可信才进入同一 Serving 代，失败不发新代或伪空；通知领域逻辑不依赖公式池结果。
- 排除：生产 profile 注入新路径、systemd ReadOnlyPaths/服务重启、Web 生产每日根、定时 worker、真实数据写入与 Streamlit 停服。
