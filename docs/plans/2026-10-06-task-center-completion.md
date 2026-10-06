# M13 任务中心剩余能力 SPEC

状态：SPEC 候选，待一次独立 SPEC 审查。本文及配套矩阵中的实现用例均未运行。

原生产品：Codex OpenAI。作者：`/root/strategy_template_impl`，角色 implementer，父任务 `/root`。
工作树：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-neutralization-react`。
分支：`cdx/20261006-task-center-completion`。基准：`7f9a12be583f60e97d3a9d99547f5f7eae65d7d7`。
Root 已核清洁并复用本树；作者未执行 Git，24 项原准备来源已逐字核对。旧 PP/C5 资料保留。
本片为高风险任务，原因是共用调度、受控 unit 执行、权限和故障恢复。用户已取消固定时长、补修次数限制。

## 1. 范围与原权威

落实 CC v2 §4.13 C13.1–4 和任务页原型的 CPU、立即运行、全局暂停/恢复、运行结果、日志。
保留原定时任务、运行服务、资源、研究任务四区，原单任务控制、进度、ETA、事件和下载。
原型的示意数字、2.5 秒成功 toast、任务列表和假日志不构成准入、数据或完成事实。
本轮只写 SPEC/威胁模型/矩阵/来源/最小写集提案，不写产品、测试、生成物或安装文件。

- 命令复用原 PageControl UUID journal、protected/private 准入、lookup、claim、effect、settle、targeted recovery。
- 研究复用原 Lab DB、command spool、singleton lease、claim spool、隔离 worker、报告和 finalizer。
- 运维复用签名 OpsInstallManifest、原固定采集、ops_status owner 和原 Serving publisher。
- 日志复用原 JournalLogReader、UnitLogService/daemon、distinct UID、精确清单、签名 cursor、访问审计。
- 总览复用 GET /api/v1/tasks/overview 的一次 Serving borrow；原单任务路由/日志路径保持。

不新建第二队列、worker、命令 journal、任意命令入口、unit prefix resolver 或替代日志物理服务。
不修改数学、broker、T+1、费用、私有实验额度、原 claim/source 签名和已接受 family 的准入合同。
生产 sudo/unit/policy/安装、数据库升级和部署不随本文授权，仍走项目已有的单独授权路径。

### 1.1 当前直接事实

原 OpsUnitEvidence 拒绝非空 last_result；active/Result/ExecMainStatus 无法归属一次触发。CPU 当前未知。
Lab command v1 只有五种带真实 job_id 的单任务命令，全局控制不能伪造 job UUID。
scheduler 在读 inbox 前恢复 source stage；暂停必须覆盖恢复/发布分支。
原 LabClaimSpool.admit_execution 在共享锁下持久执行准入；原隔离 ACK 才允许 adapter 开始。
原服务日志路由是 /api/v1/tasks/services/{unit}/logs；daily、backup 以外原 MESSAGE 默认隐藏。

接受来源 map SHA：945ac0bfd133d2e498f05622915984bbf6566455b7d2df91657a8c48fd295556。
旧 map 的 review pending 是当时事实。完整直接源及新增 Linux 输入见 source-freeze-spec.json。

## 2. 冻结不变量

完整威胁模型见 threat-model.md，逐案用例见 acceptance-matrix.json。下列 ID 保持到最终复核。

| ID | 必须保持的不变量 | 直接边界 |
|---|---|---|
| TSC-01 | 原可信身份、角色、CSRF，public 拒绝 owned kind | Web、private admission |
| TSC-02 | 严格正文/时钟/预算，容量满后原 UUID 仍先查 | DTO、journal、parser |
| TSC-03 | CPU 两计数同 host/boot/cgroup/额度/时钟代 | collector、投影 |
| TSC-04 | 完整分母、单一算法，缺项不补 0、不夹 100 | producer、独立手算 |
| TSC-05 | 精确 unit、运行排斥、执行前复核、默认关闭/时间窗 | policy、start leaf |
| TSC-06 | 原 UUID/持久 effect，不确定外部 start 永不重发 | journal、互斥、恢复 |
| TSC-07 | 同 invocation 开始/结束才形成结果和耗时 | job witness、journal |
| TSC-08 | 全局 desired/applied CAS 持久，旧 UUID 不回滚 | 原 DB/spool/lease |
| TSC-09 | 原执行屏障闭合，applied paused 后无新 adapter ACK | claim lock、worker |
| TSC-10 | 当前报告/清理/封存、单任务控制/各 family 保持 | 原 Lab 恢复链 |
| TSC-11 | 同 host/boot/unit/invocation，原日志预算/cursor/脱敏 | 原物理日志服务 |
| TSC-12 | owner/cutoff/generation/完整 graph 相符，四区一次读取 | Serving、Web |
| TSC-13 | 真实等待/未知、短中文/手机/键盘/权限失效/原 UUID | React、浏览器 |

缺条件禁止新效果，显示未知或暂不可用。unknown 不能改为成功、失败、0、未安装、已暂停或允许自动重发。
原已持久回执仍按当前授权身份读取；身份/文件代不符不能转去新文件。

## 3. CPU 固定材料和算法

新增内聚 task_cpu.py，使用 frozen Pydantic TaskCpuObservation/Pair/Evidence。
计数只取 cgroup v2 cpu.stat 的 usage_usec，CPUUsageNSec 只作诊断。
固定原五个 slice：rquant.slice 和 live/serving/research/maintenance，HTTP 不传路径。
每次完整观测绑定：

1. 实际 host、boot、签名 manifest digest、slice、systemd InvocationID。
2. 已验证 cgroup2 mount/root 身份、相对路径、目录 device/inode，读取前后稳定。
3. usage_usec、UTC、CLOCK_MONOTONIC/CLOCK_BOOTTIME 纳秒值。
4. 本组至真正 cgroup 根的全部祖先路径/身份/cpu.max、controllers、subtree_control。
5. online CPU、有效 cpuset；本组及后代全部实际线程 affinity 与 pid/tid/start ticks/归属。
6. 枚举前后成员/容量完整且稳定；没有错误、截断、错 PID 代或未解释的缺字段。

affinity 来自被计量组实际线程，不能用 collector 自身 sched_getaffinity(0)、宿主核数或单进程替代。
有效集合 = online ∩ 有效 cpuset ∩ union(实际线程 affinity)。空组无可验证 affinity 时未知。
分母是该组配置允许的 CPU 上限，不是保证分配、兄弟组余额、空闲核数或当前线程数量。

### 3.1 缺字段的精确含义

- 普通非根 cpu.max 缺失/不可读为未知，不能当 unlimited。
- 仅真正 cgroup2 根的 cpu.max 不存在可表示 root 无 quota；必须先核 mount/root 身份和 cpu controller。
  假根、读错路径、permission error 或非根缺文件不享此例外。
- 子组 cpuset.cpus.effective 不存在，只有完整 controllers/subtree_control 证明 cpuset 未委派，
  且最近可用祖先至根的有效集合/身份完整时，才继承最近祖先有效 cpuset。
- controller 本应启用却缺文件、链不完整、控制器变代、祖先缺值或任意 read error 均未知。
- root/non-root、继承依据和原错误类型是材料字段，不能用 catch FileNotFound 就统一回填。

原 collect() 返回和默认行为保持。新增具体 collect_tasks() 在最终 sampled_at 前收集完整 CPU/运行材料，
返回 TaskOpsSample(snapshot, evidence)；不能先 cutoff 后补未来事实。前后两次真实采集，不为页面 sleep。
前样本取原ops_status受信发布材料中的最近完整观测，核原authority pointer/payload、采集契约/代码代和全部identity。
每次发布携带当次观测（首样本可unknown但保留完整raw），下一次从原owner读取，不新建metrics DB/cache或后台采样器。
长驻对象可缓存同一受信pointer，不能把裸本地缓存当权威；新进程只在同采集代/boot的旧可信观测仍合法时组成pair。
首样本、坏pointer、契约/collector代变化、boot变化或旧观测过期保持未知；不得跨root/host读取别人的counter。

### 3.2 算式

C = min(有效 CPU 集合大小, 本组及全部祖先的每个有限 quota/period)。max 不增加限制，缺值不是 max。
CPU% = 100 × (usage2 - usage1) × 1000 / ((boottime_ns2 - boottime_ns1) × C)。
用有界整数/有理数到最终展示一步，显示 1 位小数。source 是唯一计算口径，API/React 不重算。
超过 100 为 capacity_mismatch 未知，保留原材料，不夹值；完整有效 pair 的零 delta 才是 0。
总 rquant 值用其自身 counter，不平均或相加四组百分比。

独立手算：2 秒/C=0.5/delta=400000 usec→40.0%；2 秒/C=0.3/delta=570000 usec→95.0%。
自身 max/祖先0.5/允许2核→C=0.5；cpuset2核/实际affinity1核→C=1；完整有效零delta→0.0%。
参考不得 import producer 充当 expected；真实 inherited cpuset/root 无quota和错误缺字段分别验。

有效间隔 [0.5,120] 秒，两 clock delta 都正且相差≤5ms，否则 suspend/clock_discontinuity 未知。
UTC 回退/未来、host/boot/manifest/cgroup/InvocationID/成员/容量变化、counter 回退均无效。
所有材料≤owner cutoff；原 ops 120 秒 TTL，等于到期即未知。cgroup 重建不能新减旧。

数字文本先限19位、非负int64；quota/period正且有界；CPU list≤4KiB/ID<4096，祖先≤16，线程≤1024，
完整 raw pair≤128KiB。超过预算整对未知，不部分求分母。固定目录 nofollow/稳定 identity。
新增采集占用原整轮20秒 deadline，原单条1秒/4KiB预算保持；超时只使新能力不可用，不延长 deadline。

### 3.3 真实 Linux 准备的证据界线

旧样本间隔2.070766945秒，缺cpuset/祖先/affinity，CPU未知。新02样本间隔2.041665972秒，
父/根cpuset=0-3，父未委派cpuset，root cpu.max不存在，live/serving有稳定PID affinity。
此材料允许冻结具体继承语义，不能泛化缺文件为无限。02只有PID级affinity，缺完整线程/start-ticks/产品采集，
research/maintenance无成员，仍不能宣称产品CPU已验或用空affinity补0。两套原件均保留。
Root的03附件补齐五组真实线程、startticks/实际cgroup/affinity/前后与跨样本稳定身份；总组250线程，
live185/serving65，两个空组仍unknown。独立诊断示范26.0375/19.7507/80.0153，仅用于原材料算式核对，
不是产品producer/签名/cutoff/UI/安装验收。750103字节冗余原件仅作附件，不提高128KiB产品pair预算。
产品用typed thread/identity/mask表与group精确引用紧凑绑定全部事实，不复制重复JSON，也不丢线程凑预算。

## 4. 精确 unit 策略和受控 leaf

新增 task_unit_control.py：具体 TaskUnitRunPolicy 和固定 SystemdUnitRunExecutor。
目录权威是原签名 OpsInstallManifest；另受信签名 policy 引用其 digest/host，只收有限精确 service≤32项。
template unit必须列完整已安装实例，不按rquant-/@前缀构造。客户端不能声明readonly或传命令/args/env/path。
每项绑定service、政策版本、readonly|writer、启用位、固定执行契约v1；缺项/签名错/换host/清单变化拒绝fresh。
unit run全局默认关闭，writer逐项默认关闭，private backend/listener默认None。
unit操作员和整个Lab调度管理员为独立精确allowlist各≤16；原job操作权限不等于全局权限。
身份复用原trusted ingress，private transport原distinct UID、peer、0710/0660、组及锚核验。
原普通HTTP PageControl parser/loopback旧gateway拒绝新protected kinds。

只读任务使用确认框。writer必须服务端prepare后独立确认：不是一个confirmed=true字段。
PrepareUnitRun在原journal持久5分钟challenge，绑定prepare UUID、run UUID、完整draft hash、actor、
host/boot/unit、manifest/policy digest、原source context、server accepted_at/expires_at；本身不start。
RequestUnitRun带原完整draft+challenge；一次消费，跨actor/unit/boot/body/UUID及过期拒绝。
prepare掉回复查原prepare UUID；取消确认无run效果。已接受run retry先lookup，不重验过期challenge后重start。

真实服务端上海时间：周一至周五[09:15:00,15:10:00]只放行明确readonly。确认/等待到效果/恢复到start前均复核。
浏览器requested_at/时区或节假日不能放宽；其他时段writer仍需启用和两步。
fresh顺序：原lookup→角色/正文→可信context/policy→运行排斥→challenge→持久接受。
副作用前在同exact-unit持久互斥内再核host/boot/清单/policy/时钟/LoadState/InvocationID/ListJobs。
active/activating/reloading/deactivating、已有start job、未明旧attempt或身份变化都不调用start。
线程锁不足以代替原journal/effect及跨进程持久排斥。

唯一外部start：系统D-Bus org.freedesktop.systemd1.Manager.StartUnit(exact_service,"fail")。
固定/usr/bin/busctl --system argv的typed窄封装，deadline≤5秒/输出≤16KiB，所有leaf进程回收。
不shell/sudo/任意callable/StartTransientUnit，不以systemctl fallback重试。
调用前建立此unit的bounded job/Invocation见证，绑定返回job path/id、JobNew/JobRemoved、新的InvocationID、实际开始。
timer/external coalescing、错过job窗口、外部启动归属不明或watcher不能给唯一关联，只有unknown。
系统版本/权限/见证无法闭合时保持不可用，Root实际补证才开启。active、exit0、最新InvocationID或时间接近均不足。

## 5. 原命令、效果和结果

新增task_control_commands.py public drafts及三个具体owned kinds：OwnedPrepareUnitRun、OwnedRequestUnitRun、
OwnedSetLabSchedulingPaused。public正文禁止owner/actor/权限/command line/路径/任意metadata。
只在原可信private admission编译owned，绑定actor与原journal/queue/unitauthority身份和原完整body/hash。
接受时保存server时刻、原私有path/device/inode/instance；requested_at只属请求身份。同UUID改body/kind/actor冲突。
lookup在freshhead/policy/时段/4096容量/enabled前。旧请求不重编译，不写新head；当前权限仍核，撤销后不泄露。
只关闭fresh功能不阻断仍授权的原查询。journal/queue/effect代替换、损坏或不匹配拒绝，不转新文件/自动修复。

### 5.1 unit效果顺序

原journal为权威。exact-unit index仅原控制目录中的有限派生互斥/效果材料（≤32），引用原UUID/hash/identity，
不存待执行任务列表。每请求最多一次start attempt。

| 阶段 | 先持久什么 | 动作/恢复 |
|---|---|---|
| accepted | 原owned/body/UUID、server接受时刻、challenge | 同原请求查询/原journal继续 |
| prepared | unit排斥与effect，未外部attempt | 复核后继续；条件失效明确拒绝 |
| start_intent | fsync effect、challenge消费和unit排斥 | 从此禁止重发start，即使调用没执行 |
| acknowledged | 实际job path/id和host/boot/unit | 只观察此job/invocation |
| started | 唯一关联invocation、原request、实际start | 才显示已开始 |
| completed | 同次可信end/exit/result/duration | 才形成成功/失败，幂等返回原回执 |
| unknown | intent后超时/掉回复/外部归属不明/boot变化 | 原lookup/观察，不自动换UUID重试 |

intent→syscall crash window允许漏一次，不允许第二次。没有日志/现在inactive不能证明未运行。
收到可信同次证据可推进原记录；无job关联不可认领最新调用。journal→index→effect→complete逐窗口原targeted recovery。
index缺原接受记录、hash/identity错则闭合。未明效果同boot排斥新UUID；真实boot换代可持久fence旧attempt，
允许当前boot新请求，旧UUID仍历史unknown、不能迁移start/假称完成；host改变不自动转移目录。
challenge/index单项≤4KiB，任务控制历史≤4096/effect≤32KiB，原journal总限额保持。满后旧UUID先查，新请求无效果拒绝。

### 5.2 真实结果

TaskUnitRunEvidence包含origin=manual|timer|external、host/boot/unit/InvocationID、manual必填request UUID、
jobwitness、UTC/单调start/end、Result/ExecMainStatus、材料digest。start/end/show/journal身份和退出码一致。
end≥start、全部≤cutoff；耗时取同boot单调差，UTC作关联/展示，时钟回退或缺单调身份时未知。
manual成功必须真实同次end且success/exit0；失败必须明确非零/失败end。timeout不是业务失败，active不冒充完成。
旧OpsUnitEvidence.last_result validator保持，新owned投影只在完整归属时用于页面，不model_copy/any绕过。
定时“上次触发”仍原timer；“最近运行结果”单列origin，手动结束不冒充timer成功。
timer成功同样须可信触发/job→invocation回执；无法证明时未知，并列真实安装缺口。

## 6. 原Lab全局调度控制

新增lab_scheduling_control.py typedcommands/state/projectionvalidator，不创建DB。原v1五种job命令/receipt保持。
同一个原command spool加入schema v2 scheduler-scope sibling，只有PauseSchedulingCommand/ResumeSchedulingCommand。
target_scope=scheduler，原queuebinding、expected_version、requestUUID/hash/server接受时刻，无假job_id。
scheduler receipt带queueidentity、desiredversion、accept/reject，不能cast成v1jobreceipt。
新protected kind经原concrete Facade.submit_scheduling_control写原inbox，原no-clobber/先lookup/ACK保持。

### 6.1 持久CAS与迁移

本地显式schema16→17，加lab_scheduler_control单行和有界commandreceipt表，不复用claim_job_cursor行。
原claims/attempt/lease/输入/plan/签名/索引保持。绑定原DBstore/queue/pathidentity/schema/fence，替换库拒绝。
原integrity+事务迁移，中断只能完整16或完整17；不downgrade/修坏表/重置pause。旧16无新能力兼容。
16→17只在原受控维护/lease下、无activeclaim/nonterminalpublication/未知ACK/待接收报告或finalizer意图时执行。
不以升级改写旧QueueBinding/schema/frozenplan；未闭合的16效果保持原代恢复，先闭合才升级。排队job/旧receipt/
已封存结果字节保持，原C5/PP/M8接受任务重开直接兼容测试必须成立；若原冻结验证不允许此迁移，停在明确
schema接线阻断并交Root最小提案，不能宽松接受旧schema/fence或为求绿重签旧任务。
已有control却未加载capability，newdispatch failclosed，报告/清理仍可运行；开启前整组manifest/worker代码同代。
production升级/整组安装另授权，不能混跑不识别屏障的旧worker。
默认legacy初始化不偷偷升级16；只有受信task-center control profile显式启用且通过上述原维护条件才升级。
已升级/存在屏障的库必须识别其状态；缺profile不意味着忽略pause，newdispatch关闭但原维护可继续。

state包含desired_version/paused/request_id/accepted_at，applied_version/paused/applied_at，scheduler_fence、
barrieridentity、draining_count/observed_at。版本单调int64，服务端时刻。全局控制作用原整个queue，不是每用户独立pause。
原lease持有者原事务先lookupreceipt再CAS expected_version。旧UUID返回原receipt不再CAS；并发同版本仅一个胜出。
desired表示意愿；applied paused只在真实屏障和drain闭合后写。操作者/UUID仅private本人receipt，不进普通页面正文。

### 6.2 原claim锁的屏障

屏障在原LabClaimSpool管理目录及原exclusive_lock中，是DB/lease派生投影，不是第二权威。
≤8KiB，绑定queue/store、desired version/hash、原fence、有限精确drain tokens。
存在屏障/pending transition必核；defaultNone只兼容从无新状态的旧路径，不能忽略已暂停事实。
固定锁顺序claim-spool→原DB事务；DB→spool步骤先结束事务，再取锁重读fence/state。worker不写原DB。
barrier只允许原concrete scheduler fence写，不能新增任意callback或伪造currentauthority。
原v2 source scheduler的claim_spool=None、claim_worker_ids=()和无worker/spool publication权限保持。
以具体LabSchedulingBarrierPort仅开放global metadata transition/read、原锁和当前fence核验；不暴露publish/
consume/revoke任意claim、worker runtime或签名器。v1可从原已有claimspool装配同port，v2仅从受信原宿主的
控制metadata capability装配。new capability默认None，缺原安装身份/端口权限拒绝启用；不能为了globalpause
给source scheduler整个claimspool或旧协议没有的worker权威。具体端口/宿主最小delta须在实现授权内落地并直接验。

暂停持久顺序：

1. 原PageControl接受原UUID→原spool；scheduler先lookup原receipt。
2. 原claim锁下fsync transition_pending，关闭新admission/ACK，绑定command/expectedversion/fence。
3. 原DB lease/CAS提交desiredpaused及同UUIDreceipt。
4. 同锁持久closedbarrier；drain仅包含关闭前已有原admit_execution的精确当前claims，数量沿原worker上限。
5. unadmitted/HELD/未发布claim沿原park/revoke/recovery处理，不提交adapter ACK。
6. drain currentshards可完成原报告/封存；全部terminal，或原撤销/fence及child收尾已证明后，才提交appliedpaused。
   lease到期不能单独证明未知ACK/child/report消失，不能凭到期清drain后称已暂停。

DB前后/barrier前后/receiptACK前后crash由原receipt/DB/fence固定恢复。pending先闭合，不能自动删文件解锁。
有原DBreceipt就完成同transition；没接受证明时核原expectedversion+lease恢复此前屏障，不凭marker接受命令。
坏marker/DB身份/旧fence保持closed，不自动修复。恢复CAS先持久DB新version，再原子fsync openbarrier，最后复核才放行。
中断保持closed，旧pauseUUIDretry不关闭较新resume；全局resume不改任何job自己的paused/cancelled/deadline。

worker原admit_execution和原隔离ACK都检查屏障。ACK前同claim锁核current/revoked/highwater/drain/fence，
与原stopgate和resource/sessionrecheck兼容。原admitted记录旁先持久ACK start_intent，再发送原_IsolationStartAck。
ACK掉回复计为未知drain，不重复ACK后假称无执行。READY helper不是adapterstart，可收掉未ACKchild。
关闭前已准入且计入drain的当前shard可完成；ACK已提交不因globalpause被杀。
pause/ACK并发只允许先ACK计入drain，或先close拒绝ACK；appliedpaused后不能有迟到ACK/adapterstart。

### 6.3 全部分支

scheduler继续commands/lease/heartbeat/reports/finalizer/artifactcleanup，不用开头return跳过维护。
先恢复globalstate，再做任何source-stage恢复/publication；最终检查在实际效果边界：

- claim_next_shard / claim_next_source_stage原事务内，最终选择/commit前核desired。
- HELD_SOURCE→SOURCE_QUEUED→READY_TO_PUBLISH→PUBLISHED及takeover/replay，暂停不发布新permit。
- legacy _reconcile_claim_authority activeclaim重放，unadmitted不升级新执行许可。
- worker selected/consumed/resource-deferred/READY/ACK迟到：同claim锁最终屏障。
- scheduler/workerrestart、旧fenced scheduler、spool回执丢失、pendingreport和unknownchild：原身份恢复。

source已完成材料可验证/留存，暂停不升级新claim；不新造sourcecache/配额。版本、claimgeneration、attempt、
输入/plan/authority签名和已接受结果原值不变。无法证明drain结束显示等待当前分片，原事故/取消恢复保持。
原v2关闭前已持久emit permit的数据准备可按原operation/attempt收尾，计入精确source drain；未发permit的HELD
停住。appliedpaused还须证明已发sourcepermits都已完成或由原协议fence/收尾，不能留queued source operation
然后称无后续dispatch。READY材料留存，globalresume后才可继续原workerpublication；不改sourcebroker协议/权限。
若已准入source无法闭合，就如实等待，并在Tip说明仍在收尾分片/数据准备，不自动清除或无限宣称已暂停。

## 7. 原日志服务

原daemon物理服务、manifest/pubkey锚、distinctUID、audit-before-read、并发gate和预算保持。
原/services/log-capabilities及/services/{unit}/logs保持，在原typedquery/request加optional invocation_id（32hex）。
未传兼容旧currentboot查询；传入则固定argv精确匹配，并重验kernel_SYSTEMD_INVOCATION_ID。
host实际主机、boot前后实际boot；旧boot结果不拼当前boot日志，关联缺失暂不可用。
since服务端最近7天、level原枚举、pagesize1..498、原实际请求≤500含sentinel；response256KiB、message4KiB、
5秒、request8KiB、cursor4096预算保持。新cursor版本绑定unit/host/boot/manifest/invocation/since/level/journal/key。
旧cursor只兼容无invocation查询。改过滤拒绝；rotation/vacuum/anchor消失409，刷新丢旧页。
错unit/boot/invocation、时间乱序/未来、重复cursor、超限/未知JSON闭合，事实≤servercutoff。

closed lifecycle文案和deny-by-default保持，token/key/password/URL参数等原始内容都隐藏，不开放自由MESSAGE。
daily/backup run_with_lifecycle的实际start/success/failure要真实journal收录+kernelunit/boot/invocation；send无错或stub
不算收录。emit失败不改业务结果/异常，只显示日志缺口。其他unit保留原隐藏文案，不扩散所有CLI/productionunit。
Root在获准Linux环境补实际发射/失败/轮转/权限/清理；zero rows只说明范围内无记录，不证明服务缺失。

## 8. Serving和Web装配

ops_status新增精确两表ops_task_cpu（五slicetyped事实/结果）、ops_task_runs（最多32已核service最新结果摘要/引用）。
OpsStatusPayload新增optional TaskOpsEvidence；旧snapshot三表/validator保持。新两表全部出现或全部缺失，
unknownCPU仍五行明确理由。rawCPU≤128KiB/run摘要全表≤64KiB，原完整ops source仍≤512KiB。
完整run材料留原privateeffect/journal，projection只引用已提交原UUID/hash/material，不扫孤儿。
lab_jobs新增精确一行lab_scheduler_control，原完整queue/store/schema/fence、desired/applied/cutoff。
原LabJobsServingSourceReader两次读取同时核完整control没变化，job/event/control同authority代；
lab_jobs_state_identity包含globalcontrol，控制改变真正发布，不借jobupdated_at伪造发布事实。
旧库无control表时新能力不可用，原sharedowner7MiB/privateexperiment独立8MiB及原减账保持。
ownedvalidator校验完整typedgraph/固定主键/材料关系/时点/owner；不只比较两次自产hash。
缺新表不破坏旧内存，坏新表能力unknown/closed，不拼别代。一次overview borrow读取scheduled/services/resources/research。
运行摘要只有host/boot/unit/manifest及当前已核InvocationID与同次结果一致才覆盖该次运行；新Invocation出现后，
旧manual结果仅作明确历史记录，不能把它显示为当前新调用成功。四区保持同borrow的外层generation，
各owner内部材料同时核其自己的cutoff/identity，不能因为同一Serving文件就跳过owner校验。

### 8.1 新接口（全部/api/v1前缀）

| 方法/路径 | 正文/cap | 原效果 |
|---|---|---|
| GET /tasks/control-capabilities | 无body，≤32unit | 只读/defaultclosed |
| POST /tasks/units/{unit}/run/prepare | PrepareUnitRun≤4KiB | 原challenge |
| POST /tasks/units/{unit}/run | RequestUnitRun≤4KiB | 原journal/effect，一次start |
| POST /tasks/scheduling/commands | SetLabSchedulingPaused≤1KiB | 原globalspool/CAS |
| POST /tasks/controls/lookup | 原3public命令union≤4KiB | 原lookup，不compile/start/CAS |
| POST /tasks/controls/resume | 同原union≤4KiB | targetedrecovery，intent后不重发 |

保留overview、jobs/events、日志和jobcommands，不增重复资源/定时总览读取。均原current_user/proxy/ingress、角色、CSRF。
POST前置bodycap；extra/duplicatekey/巨大数字/非法time/URLunit拒绝。path/bodyunit一致，精确签名目录无prefix。
lookup在新head/容量/开关前；能力响应不能代替副作用前复核。
401/403清当前数据/能力；409明确冲突刷新，422明确拒绝允许改输入；429/503或掉回复无法证明未受理时保留原UUID/body，
显示结果待确认，只查原请求。lookup/resume也private身份+CSRF，不转旧publicbody入口。

拟定create_app(task_control_gateway: TaskControlGateway|None=None)/Context同名字段，具体privateUnixgateway。
WebSettings默认task_control_enabled=False、task_unit_run_users=()、task_scheduling_admin_users=()、
task_control_socket_path/service_uid/web_group_gid=None；开启需完整组合/原ingressready/不同peerUID/签名policy。
Root独占实际OpenAPI/schema/dist/identityinventory/生成脚本；实际DTO短冻结后Root生成，不手写公共TS。

### 8.2 原宿主启动入口

新增owned task_center_runtime.py提供TaskCenterControlProfile和固定构造器，无任意import/callback。
原cli.py只接原lab-scheduler/lab-worker的optional受信profile路径（默认None），验证原runtime/code/queue/root/
角色身份再装配concretecontrolport。原v2source角色不获得worker或publish权限，原shutdown/lease/child收尾保持。
原PageControl service builder显式optional task backend/admission defaultNone，原journal仍唯一命令权威。
CPU入口为固定CLI ops-task-snapshot：只读签名安装清单/原authority、真实内核采集、原collect-and-publish；
manifest/pubkey/authorityroot/producercommit来自本地受信安装参数，不来自HTTP。命令调用可在Root批准环境验证，
不自动创建timer、改productionpolicy或装服务。不接受任意unit/execargs。如此保证有实际宿主入口，不能只导出配置。

## 9. 页面

沿/app/#/tasks与相对API；e2e origin取实际baseURL，不硬编码port/假设SPA fallback。
原OverviewSections/LabJobControls/ServiceLogDrawer和Tip/ConfirmDialog/SideDrawer复用，不新控件库。
CPU/内存真值，unknown用—，Tip说明具体短理由；原120秒deadline到期不能继续显示旧数值。
定时表最近运行结果/耗时标origin，立即运行/运行日志依能力；确认展示任务名/真实影响，writer第二步由prepare驱动。
已受理/等待开始/已开始/已完成/结果待确认/明确失败对应真实server事实，不靠toast模拟成功。
全局pause受理显示等待当前分片，applied才显示研究调度已暂停；resume不改job自己的暂停，报告封存仍回读。
缺安装、无权限、源过期、emptylogs各自真实文案；empty说这个范围内暂无日志，不说服务缺失。
原五种主服务状态保持；控制状态独立动作/回执信息，不冒充正常/成功。
短中文/80%简单句；unit/UUID/hash/boot/代/算法/完整时间只详情或Tip。
Tip hover/focus/tap与checkbox/button默认动作分离。1440和390px无横向溢出；Tab/Enter/Escape和关闭焦点回实际入口，
入口消失回当前区标题。换人/换代取消请求/清选择logs；迟回执核viewer/operation/generation/原UUID。
unknown原请求只存当前viewer内存+完整body，不localStorage敏感正文；明确422不锁unknown，真正掉回复只恢复原UUID。

## 10. 实现顺序和验证

1. 冻结SPEC/威胁模型/逐案矩阵/beforeSHA写集；Root一次独立SPEC审查后授权实现。
2. CPU严格模型/独立手算/真实不完整材料、unit精确policy/UUID效果状态机先红测，不启动外部leaf。
3. 原PageControl/private typed接线；原Labschema/命令/CAS/claim锁/ACK屏障逐实际crash窗口红绿。
4. source恢复/legacydispatch/ACK竞态，原单任务/报告/finalizer相关兼容，不无关全库数学。
5. 原ops/labpublisher同代投影、日志cursor、Web默认closed/CSRF/bodycaps；实际DTO给Root生成。
6. 完整React/严格fixture，Root最小native/Linux/1440+390browser；环境阻止不计PASS。
7. 完整候选/证据freeze，一次独立集中终审；原作者只修阻断与直接回归，不每模块审查。

具体文件及用例名见proposed-write-set.json/matrix，当前提案不构成写权；矩阵case全NOT_RUN。
Python为原ROOT环境3.13.12，-I-B/ownsrc-root优先/dotenv关闭/dummytoken≥32、四私有DATA_DIR/DUCKDB_PATH/
PARQUET_DIR/LOG_DIR和TMP/BLAS1。禁止network/凭据/prod/Git/socket/GUI/background/子委派。
原命令/stdout/stderr/源摘要/手算/收尾入本片证据，Rootnative前短freeze；join/reap/socket/child/tmp逐一核。
旧M8/PP/C5仅复用未受delta影响的分支，新CPU/unit/globalbarrier不能借旧PASS。

## 11. 必要真实证明与安装边界

| gate | 最小真实证明 | 当前状态 |
|---|---|---|
| CPU Linux | 完整线程/祖先/继承cpuset pair，重置/陈旧，原source发布 | 03完整材料仍是准备，未产品验 |
| unit Linux | 批准精确测试unit StartUnitACK→invocation→end，掉回复/外部start不重发 | 未装配/权限，未验 |
| Labnative | 原journal/spool/lease/claim/isolatedACK→drain/applied，无后续ACK，restart | 新用例未运行 |
| journalLinux | 原服务真实成功/失败/发射丢失/轮转/empty | 旧zero行仅准备 |
| privateUID | 原socket/目录/实际peer；注入需标distinctOSUID=false | 新能力未运行 |
| browser | 1440/390确认/unknown/pause/logs/expiry/Tip/keyboard/closefocus | 新用例未运行 |

真实systemd版本/字段/权限不可猜，无法证明触发归属时保留unknown和实际阻断，不降低验收。
Root最后补证，生产安装另授权；本文不改deploy/policy/sudo。原日志/样本/失败不删除。

## 12. 冻结入口

证据目录data/verification/task-scheduling-completion-20261006，配套spec-author-start.json、scope-document-before-spec.md、
threat-model.md、acceptance-matrix.json、proposed-write-set.json、source-freeze-spec.json、SPEC-HANDOFF.md及最终metadata。
最终metadata锁具体SPEC/配套/源码SHA，不含自指hash；作者冻结后停资料写入。
Root后续index/commit/审查启动，本轮无产品/测试执行或后台进程。
审查只限TSC-01..13、最小共享接线和直接依赖，不重审旧数学/全仓/已接受PP/M8。
