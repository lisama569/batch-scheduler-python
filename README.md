# 单节点批处理作业调度器（Python / FastAPI）

一个单节点批处理作业调度服务：用**进程内状态**模拟集群资源（多维资源向量 + 全局锁），
保证作业在并发提交、取消与完成交错时，**资源占用与排队结论始终一致、守恒且可解释**。

- 语言/运行时：Python 3.14，FastAPI，pytest
- 无外部存储：所有状态在进程内存中，`threading.RLock` 保证并发互斥
- 确定性：调度只依赖「提交序号 + 可注入/可手动推进的逻辑时钟」，相同输入序列可复现

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖
uv run uvicorn main:app --reload   # 启动服务（默认节点容量 CPU=16, 内存=32GB, GPU=4）
```

健康/资源总览：

```bash
curl -s http://127.0.0.1:8000/ | python -m json.tool
```

### 本地验证

```bash
uv run pytest                 # 全量单测（日志实时打印调度快照与判定依据）
uv run pytest -k concurrent   # 只看并发交错用例
uv run pytest --log-cli-level=INFO   # 与 addopts 等价，显式打开 INFO 日志
```

可选的端到端演示脚本（提交→排队→推进时间→完成→补位，打印每一步快照）：

```bash
uv run python examples/demo.py
```

---

## 2. 调度模型

### 2.1 资源

资源是一个固定维度的非负整数向量，例如 `(cpu, memory_gb, gpu)`。

- 节点容量 `capacity`：构造调度器时给定，任一时刻被授予资源之和**逐维不得超过容量**。
- 作业请求 `request`：每个维度必须 `>= 1`；含 0、负数、维度不匹配均拒绝；
  单作业任一维超过节点容量直接拒绝（永远无法调度，没必要排队）。
- 记账模型：`used = Σ 所有 RUNNING 作业的 request`；按提交方另维护一份
  `submitter_used`。两套账与 RUNNING 集合在每轮变更后做一致性断言（资源守恒）。

### 2.2 作业状态机

```
QUEUED ──调度授予资源──▶ RUNNING ──complete──▶ COMPLETED
   │                       │
   │                       ├────fail────────▶ FAILED
   └────cancel─────────────┴────cancel──────▶ CANCELLED
```

- 只有 `QUEUED → RUNNING` 这一条路径会授予资源；授予时记录 `scheduled_at`。
- `COMPLETED / FAILED / CANCELLED` 为**终态**：作业退出队列与 RUNNING 集合，
  占用资源恰好释放一次，之后可被后续作业复用。
- 重复的 `complete/fail/cancel` 通知按**幂等重试**处理：同一终态的重复通知原样返回，
  **不会重复释放资源**；冲突终态（如先 COMPLETED 再 fail）返回 `INVALID_JOB_STATE`。
- 取消一个 QUEUED 作业只把它移出队列、不触碰资源；取消 RUNNING 作业则先释放再调度。

### 2.3 调度决策（可解释、可复现）

每次提交、完成、失败、取消后都会在锁内跑一轮 `schedule()`；也可通过
`POST /schedule` 手动触发。一轮调度：

1. 对当前所有 QUEUED 作业计算
   **有效优先级 = 基础优先级 + aging_weight × 已等待 tick 数**；
2. 按 `(有效优先级 降序, 提交序号 升序)` 排序——同等有效优先级严格 FIFO；
3. 顺序扫描，对每个作业同时检查两个条件：
   - 节点条件：`request <= capacity - used`；
   - 配额条件：`request <= quota(submitter) - submitter_used(submitter)`；
4. 两者都满足才授予资源并把作业移出队列；任一不满足则**跳过并记录原因**，
   扫描继续——队首放不下的大作业不会阻塞后面放得下的小作业（补位）。

每个被扫描的作业都会产生一条 `QueuedDecision`（作业、提交方、基础优先级、等待时长、
有效优先级、ADMIT/SKIP、人类可读原因），随快照返回并在测试日志中打印，例如：

```
decisions:
  - [ADMIT] job-... submitter=b base_priority=1 waited=3 effective_priority=4
      request=(2,2) reason=授予资源 request=(2,2)；节点剩余 (8,8)，提交方配额剩余 (8,8)
  - [SKIP]  job-... submitter=a ... reason=提交方 a 配额不足: ... 剩余 (0,0)
```

决策只依赖提交序号与逻辑时钟，**没有随机数、没有系统时间**，
因此相同输入序列（含相同的 tick 推进）产出完全相同的状态与决策（见
`test_schedule_is_reproducible`）。

### 2.4 逻辑时钟与老化（anti-starvation）

- 调度器内置一个从 0 开始的 `ManualClock`：默认每次提交自动 +1，也可调用
  `sched.tick(n)` 或 `POST /tick` 手动推进。
- 测试可注入自定义 clock（如 `ManualClock`）来精确控制等待时长。
- 等待越久，有效优先级线性升高。**任何排队作业的有效优先级都会随时间无限增长**，
  而后来者从 0 等待开始，因此老作业终将在排序中领先新到达的高优先级作业；
  又因为补位扫描 + 每次释放都重新调度，容量一旦可用它就能进入。
  这保证长期队列中**没有任何提交方会被无限期饿死**（见
  `test_aging_prevents_starvation`、`test_fairness_no_submitter_starved`）。

> 说明：一个单作业请求就超过节点剩余容量时，物理上必须等容量腾挪，这不属于饿死；
> 老化机制保证它在「容量可用的那一刻」排序足够靠前。

---

## 3. 提交方配额与公平

- 默认配额 = 节点容量（即默认不额外限制）；可按提交方设置
  `set_quota(submitter, limit)` / `POST /quotas/{submitter}`。
- **超配额排队而非丢弃**：当节点有余量但提交方已用量 + 请求 > 配额时，
  作业保持 QUEUED，决策原因写明 `提交方 X 配额不足: 需要…，已用…，配额上限…，剩余…`。
- 配额只在**调度授予时点**校验，是**非抢占**的：事后下调配额不会杀掉运行中的作业，
  已授予用量可能临时高于新配额，待其结束后新配额自然生效。
- 公平性来自三处：有效优先级老化、平局 FIFO（提交序号）、跳过不阻塞（补位）。
- 配额资源同样按提交方逐维记账并参与资源守恒校验。

---

## 4. 状态与错误语义

所有业务错误都继承 `SchedulerError`，携带稳定的字符串错误码 `code`；
HTTP 层统一包装为 `{"error": {"code": ..., "message": ...}}`。

| code | HTTP | 含义 |
|---|---|---|
| `INVALID_REQUEST` | 422 | 请求结构/字段非法（如提交方为空） |
| `INVALID_RESOURCE` | 422 | 资源向量非法：维度不匹配、含 0 或负数 |
| `INVALID_PRIORITY` | 422 | 优先级不是整数或超出 `[priority_min, priority_max]`（默认 0..100） |
| `RESOURCE_EXCEEDS_CAPACITY` | 422 | 单作业请求任一维超过节点总容量（拒绝而非排队） |
| `IDEMPOTENCY_CONFLICT` | 409 | 同一幂等键已存在，但 submitter/request/priority 不一致 |
| `JOB_NOT_FOUND` | 404 | 作业 ID 不存在 |
| `INVALID_JOB_STATE` | 409 | 当前状态不允许该操作（如对 QUEUED 作业 complete、终态互斥） |

> 请求体的 JSON/Pydantic 结构错误也返回 422，但走框架校验结构（`detail` 字段）。

**幂等提交**：携带相同 `idempotency_key` 的重复提交不会创建新作业，
返回首个作业且 `duplicated=true`；同键不同参数报 `IDEMPOTENCY_CONFLICT`，
避免调用方误以为自己的新参数生效了。

**一致性保证（不变量，`Scheduler.assert_invariants()` 每用例强制校验）**

1. `used` 非负且逐维 `<= capacity`；
2. RUNNING 集合、QUEUE 集合与作业状态严格一致，作业不会同时出现在两处；
3. 终态作业不在队列、不占资源槽；
4. `used == Σ RUNNING 作业请求`，且与按提交方的记账之和一致（资源守恒）；
5. 资源授予/释放在全局锁内完成，同一作业不会被重复调度或重复释放。

---

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 服务与资源总览 |
| POST | `/jobs` | 提交作业 `{submitter, resources[], priority?, idempotency_key?}`，返回 `{job, duplicated}` |
| GET | `/jobs` | 列出全部作业 |
| GET | `/jobs/{id}` | 查询单个作业（404 `JOB_NOT_FOUND`） |
| POST | `/jobs/{id}/cancel` | 取消（QUEUED 出队 / RUNNING 释放并重新调度） |
| POST | `/jobs/{id}/complete` | 标记完成，释放资源并重新调度 |
| POST | `/jobs/{id}/fail` | 标记失败，释放资源并重新调度 |
| POST | `/quotas/{submitter}` | 设置/清除（body `{"limit": null}`）提交方配额 |
| GET | `/snapshot` | 当前资源/队列/提交方用量快照 |
| POST | `/schedule` | 手动跑一轮调度，返回快照 + 每个排队作业的判定依据 |
| POST | `/tick` | 手动推进逻辑时钟 `{"n": 1}` |

---

## 6. 代码结构

```
scheduler/
  models.py      领域模型：Resource / Job / JobState / Snapshot / Decision
  errors.py      可区分错误码体系
  scheduler.py   调度核心（无框架依赖）：提交、状态机、调度、配额、老化、不变量
  service.py     FastAPI 装配：路由、请求/响应模型、错误码→HTTP 状态映射
main.py          入口：uvicorn main:app
tests/
  test_scheduler.py  核心单测（含并发、配额、公平、守恒、可复现，打印快照）
  test_api.py        HTTP 层单测
examples/demo.py     端到端演示（可选）
```
