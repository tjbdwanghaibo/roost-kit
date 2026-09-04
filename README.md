# roost-service

roost 框架的**通用服务层**：与玩法无关的公共服务，作为库提供给业务仓装配。

每个服务是一个包，内含请求/响应类型、`Client` 接口与 bus 实现、服务端实现、
存储契约与后端实现、admin 命令注册。业务仓装配它们；`app.Service` 进程壳留在业务仓，
因为"哪些服务同进程部署"是部署决策，不是库的决策。

## 设计约束

这些约束来自对一个生产业务仓公共服务层的逐文件审计（62 项发现，收敛到六个反复出现
的模式）。它们不是风格偏好，每一条都对应一类已确认的缺陷。完整依据见
[roost-core docs/ROOST_SERVICE_EXTRACTION.md](https://github.com/tjbdwanghaibo/roost-core/blob/main/docs/ROOST_SERVICE_EXTRACTION.md)。

1. **状态变更只有版本化路径。** 共享状态走 `roost-kit/versionstore`：契约里没有
   无条件写，因此非 CAS 实现无法存在。不自己手写"读-改-写"。
2. **生产包不保留内存版 source of truth。** 测试替身放 `_test.go` 或 `servicetest`。
3. **每个服务定义自己的 errcode 段**，纯客户端错误不返回 `CodeInternal`。
   实现方式是把 sentinel 本身变成 `errcode.Define(Code…, …)`，于是
   `errcode.ClientError` 能穿透任意层 `fmt.Errorf` 找到 code，**不需要一张
   逐 sentinel 的映射表**——那种表是第二份清单，新增的错误会从上面静默掉下去
   （`chat` 与 `global` 原本各有一张，已删）。反过来，**未分类的错误诚实地报
   `CodeInternal`**：本包没有"存储失败"这类兜底码，因为那是把猜测当成诊断。
   九个包各有一条测试，把「段内连续、个数精确、外部 sentinel 显式映射、
   未分类即 Internal」钉住。
4. **至少一次路径必须幂等**，幂等键由服务端生成或经 ledger 预留，不采信客户端。
5. **身份与权限不来自请求体。** 信任只能来自传输层身份。
6. **每个服务必须有 metrics**：队列深度、CAS 冲突率、丢弃计数。静默路径之所以能长期
   存在，就是因为没有信号。上报走 `servicemetrics`：**全仓一套词汇、一个 `Reporter`
   接口**，方法按事件命名（`Accepted`/`Refused`/`Replayed`/`Dropped`/`Conflict`/`Depth`）
   而不是字符串键的 `Count`，写错事件名是编译错误而不是一条没人看的指标。
   `nil` 是合法的，含义是"不上报"，且永远不会让操作失败——因此每个调用点都可以是
   无条件的，不存在"某个分支忘了报"。
   这一条本身也验证过：每个包都有一条测试断言上报点**真的被走到**，并经"去掉上报即
   变红"确认。约束不能只写在 README 里——本仓存在的理由就是"靠人记住的约束会失效"。
7. **任何列表接口都有上界**，且上界不能被 `0` 绕过。
8. **测试用求值型替身**（`roost-kit/mongo/mongotest`），并发不变量必须有并发测试。
9. **每个修掉的缺陷都有一条经"回退修复即变红"验证过的回归测试。**
10. **求值型替身也有边界，要说清楚。** 用 Go 重新实现 Lua 脚本语义的替身，无法发现
    脚本文本自身的缺陷——改脚本字符串对它没有影响。这部分由 `//go:build integration`
    的真实 Redis 测试覆盖，并因此把脚本写得尽量小。`rank` 的并发缺陷正是真实 Redis
    抓到、替身抓不到的。

## 六个反复出现的缺陷模式

十条约束不是凭空定的。对一个生产业务仓公共服务层做逐文件审计得到 62 项发现，它们
收敛到六个模式——每一个都在**多个互不相关的服务里被不同作者各自重新犯了一次**。
这解释了为什么约束写在契约层面而不是写成规范：靠人记住的东西，六次里失效了六次。

| 模式 | 独立出现处 |
| --- | --- |
| CAS/版本是可选的或假的 | 账号服务比较整个 JSON blob，且默认 store 根本没实现 CAS 接口；跨服服务 4 个 store 的 DAO 变体读后无条件写；匹配服务用进程内计数器当版本；榜单服务版本为 0 时守卫被整个跳过 |
| 静默丢弃却报告成功 | 取消操作回 OK 而存储里仍是 matched；归档首个短页即截断而元数据照写完整计数；重读不足时返回成功而队列已截断 |
| 至少一次路径无幂等 | 榜单重投累加 2–20 次；聊天发布无幂等键而传输是 `max_deliver 5`；充值发货自身无状态 |
| 身份/权限来自客户端 | 聊天的 `Trusted` 布尔；匹配的客户端提供 ticket ID；登录仅凭 `{channel, open_id}` |
| 一个玩家包触发无界读 | 榜单 `Limit: 0` 读整榜；匹配入队 O(N) 持全局锁；建角色每次全表扫服务器列表 |
| 零可观测性 | 匹配、副本、平台三个包没有一行 slog 或 metric，因此上面所有"静默"路径在生产中不可发现 |

还有一条横切的：**多处测试把缺陷当期望行为钉死了**。所以第 9 条约束（每个修掉的缺陷
都要经"回退修复即变红"验证）不是形式主义——省掉它就是那些测试的来源。

完整依据见 [roost-core docs/ROOST_SERVICE_EXTRACTION.md](https://github.com/tjbdwanghaibo/roost-core/blob/main/docs/ROOST_SERVICE_EXTRACTION.md)。

## 包

| 包 | 职责 | 单测 | 变异验证 |
| --- | --- | --- | --- |
| `directory/` | 唯一键预留的两阶段提交原语（**注意：同 owner 幂等，因此不是互斥锁**——需要"只准一次尝试"用 `versionstore.Create`） | 16 | 6 |
| `rank/` | 榜单提交与查询。排名所需的一切编进 sorted-set 的 member，因此读一页一次往返、排名与分数不可能不一致 | 27 | 8 |
| `match/` | 匹配：入队、分组、原子成组提交、超时执行。整个队列状态在一个版本化条目里，成组提交是一次 CAS | 24 | 10 |
| `account/` | 账号与角色目录、角色会话令牌。`IdentityVerifier`/`PlayerIDAllocator`/`NameValidator` 必填且**无默认实现** | 23 | 11 |
| `global/` | 跨服路由绑定（epoch CAS 迁移）、游戏服租约（incarnation fence）、跨服活动协调（首个 notify 起算的宽限窗、先预留后应用的进度 ledger、拒绝即审计、带 ACK 令牌的结果投递） | 48 | 13 |
| `chat/` | 频道消息：发布/历史/保留。`PublishRequest` 里**没有**发送者或可信字段；系统消息只能经 `PublishSystem` + 服务端签发的 `SystemToken` | 31 | 8 |
| `mail/` | 邮件：信封、按玩家的已读/领取状态、把附件恰好交付一次的三段式领取。**claim token 由服务端生成且对同一封邮件恒定**——重试换不出新的幂等键 | 43 | 20 |
| `platform/` | 渠道边缘：凭证换会话、支付回调换恰好一次发货。**验签是必要而不充分的**，订单是仅插入的持久记录 | 28 | 13 |
| `session/` | 有界 run 原语（从副本服务中抽出）：幂等 Enter、每 owner 一个活 run、资源恰好释放一次、截止时间真的被读 | 29 | 13 |
| `servicemetrics/` | 全仓共享的上报 seam 与测试用 `Recorder` | 8 | 3 |
| `servicemods/` | 九个服务的 capability 名字表与各 Mod 共用的配置读取 | 7 | — |
| `integration/` | 跨服务的真实后端测试：九个包在一个活 Redis 上装起来跑通、key 命名空间不冲突、以及**整套 Mod 生命周期端到端** | 17 真实 Redis | 18 |

合计 329 条单测 + 25 条真实 Redis 集成测试，`-race` 全绿（单测与集成测试都跑过 `-race`）。
发布态 `GOWORK=off` 下 build / vet / test / -race 与集成测试同样全绿。

「变异验证」= 把修复逐条回退、确认对应测试变红的次数。不编译的变异不算——它什么都
没证明。

## 装配：每个服务一个 `app.Mod`

每个服务提供**业务逻辑包 + 一个 `app.Mod`**（`global` 注册两个 capability，因为路由
与活动协调不共享状态）。`app.Service` 进程壳留在业务仓——"哪些服务同进程部署"是部署
决策，不是库的决策。

Mod 只拥有**基建接线**：Redis 客户端来自 registry，key 前缀与各 TTL 来自配置。
凡是配置给不出的东西——`account` 的 `IdentityVerifier`、`platform` 的
`Verifier`/`PlayerResolver`/`Deliverer`、`session` 的 `Releaser`、`chat` 的
`ChannelPolicy`/`SystemAuthenticator`、`directory` 的 `Normalizer`——都是**构造参数、
必填、无默认**，`Init` 会点名拒绝。

这一条不是风格。被替换的实现里，`platform` 的鉴权**根本不存在**，而那个缺席看起来
就像一个默认值;`account` 的 playerID 分配器默认是每进程从 1 开始的计数器。**给这些
东西一个默认值，就是把漏洞装回去。** 所以每个 Mod 都有一条"缺协作者即拒绝启动"的
测试。

key 前缀同样**必填且无默认**：默认值在每个部署里都是同一个字符串，于是共用一个 Redis
的两套部署会静默共享状态。`integration/` 用 before/after 快照回读整个 keyspace，断言
每个服务的新 key 都落在配置的 root 下**且落在自己的子命名空间里**——后半句是必要的：
读错别人的 config key 仍然落在 root 下，只有按服务断言才抓得到（那条变异一度是绿的）。

## 存储：复用，不重写

七个包**没有一行自己的存储代码**。它们的状态就是 `versionstore.Store[K,V]`，而
`roost-kit` 已经提供了这个契约的生产实现 `versionstore.NewRedisStore`：CAS 走
`roost-core/redis.CompareAndSet`、带全抖动指数退避、仅插入的 `Create`、版本校验的
`Delete`。每个包只有一个薄构造器（`redis_store.go`，几十行，零存储逻辑）。

这不是省事，是第 3.1 节那条系统性缺陷的结构性答案：被替换的实现里每个关注点都有
**四个手写 store**——Redis 变体的 `Update` 用 CAS，DAO 变体的 `Update` 读后无条件写
——两者满足同一个接口，于是类型系统分不出它们，配了哪个就决定文档写的不变量成不成立。
现在只有一个实现，而它的契约里没有无条件写。

薄构造器唯一拥有的职责是 **key 命名空间**：多个 store 共用一个 prefix 且不能互撞。
这一条由 `integration/` 里的实测保证——把九个包都驱动一遍，然后 `SCAN` 回读活
keyspace，断言每个声明的命名空间都恰好收到了写。十条"故意撞前缀"的变异全部变红。

真正需要写后端的只有两处，都不是 versionstore 能表达的：`rank` 的 sorted set，和
`mail` 的 `EnvelopeStore`——信封写一次不再改，且读路径是**批量**（一次
`IRedis.MGet`），这正是那个契约有 `GetMany` 的全部理由。
