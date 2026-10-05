# 单节点批处理作业调度服务

在单进程内模拟一个单节点集群的批处理（batch）作业调度器：节点容量、提交方配额、
排队与资源占用全部是进程内线程安全状态。系统在**并发提交、取消与完成交错**的情况下，
保证资源守恒、调度结论确定且可解释。

## 1. 调度模型

### 资源

资源是一个三维向量 `Resources(cpu, memory_mb, gpu)`，按整数单位记账。各维度相互独立，
只有三个维度都满足时才算“放得下”（`fits_in`）。

- 节点容量：全局上限，`main.py` 中配置为 `cpu=16, memory_mb=32768, gpu=2`。
- 任一时刻所有 `RUNNING` 作业的资源之和 **绝不超过节点容量**（授予时有断言保护）。

### 作业生命周期

```
                 依赖未就绪                 依赖全部成功
提交 ──有依赖──▶ BLOCKED ──────────────▶ PENDING ──授予资源──▶ RUNNING ──complete()──▶ COMPLETED
  │                │                        │                    │
  │                │ 上游失败/被取消         │                    ├────complete(failed=True)──▶ FAILED
  │                ▼                        │                    │
  │         DEPENDENCY_FAILED ◀─────────────┤                    │
  │                ▲                        ▼                    ▼
  │                └──────────── CANCELLED ◀──── cancel() ───────┘
  │
  └──无依赖 / 依赖已就绪 ──▶ PENDING（与上一版行为一致）
```

- `COMPLETED / FAILED / CANCELLED / DEPENDENCY_FAILED` 为终态。
- `BLOCKED`（等待依赖）：**不占节点容量、不占提交方配额、不进调度队列**，
  因此不影响其它作业的排队顺序；在快照的 `blocked_jobs` 视图中可见，
  附 `waiting_for`（尚未完成的上游）与可解释 `note`。
- `DEPENDENCY_FAILED`：某个（传递）上游失败或被取消后，下游被标记为该终态，
  永不调度、从不占资源；`scheduling_note` 指明是哪个上游、以何种终态导致。
- 每个作业**至多被调度一次**（`schedule_count ≤ 1`），资源**至多释放一次**
  （`release_count ≤ 1`）；重复完成 / 重复取消 / 对非运行作业完成都会得到
  `409 JOB_STATE_CONFLICT`，不会发生重复释放。
- `PENDING`/`BLOCKED` 取消不涉及资源（`release_count=0`）；`RUNNING` 取消释放资源。

### 调度触发与调度循环

调度是**事件驱动**的：每次 `提交 / 完成 / 失败 / 取消` 之后同步执行一轮 `tick()`，
不需要后台线程，因此对相同输入序列结果可复现。也可通过 `POST /scheduler/tick`
手动触发（例如外部执行器回报后）。

一轮 `tick()`：

1. 取当前全部 `PENDING` 作业；
2. 按**有效优先级**排序；
3. 依次做准入判定（容量 + 配额），满足则立即授予资源，不满足则记录阻塞原因并跳过；
4. 重复扫描直到一轮内没有新授予（充分利用刚释放出的碎片容量），队列顺序始终确定。

### 排队顺序（优先级 + 等待时间）

```
有效优先级 effective_priority = priority + 已等待秒数
排序键 = (-effective_priority, created_at_ns, job_id)
```

- `priority` 数值越大越优先；
- 等待时间每秒折算 1 个优先级档（`_AGE_NS_PER_PRIORITY_POINT = 1s`），
  等待越久分数越高；
- 完全同分时按到达时间、再按作业 ID，**无 map 迭代、无随机**，结论可复现。

## 2. 作业依赖

### 声明与就绪语义

- 提交时通过 `depends_on: [job_id, ...]` 声明依赖，可依赖多个作业，
  一个作业也可被多个作业依赖；依赖目标必须是**已存在**的作业 ID。
- 只有全部（传递）上游都 `COMPLETED`，作业才离开 `BLOCKED` 进入调度队列；
  进入队列后与无依赖作业一样参与“容量 + 配额”的准入判定——
  **配额或容量暂时不足时照常排队，不会被丢弃**。
- 上游成功：其直接下游中依赖已全部就绪者被唤醒（`BLOCKED -> PENDING`）并触发调度。
- 上游失败或被取消：沿依赖边**传递传播**，所有仍在等待的下游（含间接下游）
  被置为 `DEPENDENCY_FAILED` 终态，查询时可看到指明上游与原因的 `scheduling_note`，
  不会一直挂着不结束。
- 提交时上游已失败/已取消：作业立即以 `DEPENDENCY_FAILED` 落库（终态、不占资源）；
  提交时依赖已全部完成：直接进队列，与无依赖提交等价。
- 不变量：只有 `BLOCKED` 作业可能有未终态上游（依赖全部 `COMPLETED` 即被唤醒，
  而 `COMPLETED` 不可逆），因此传播只命中 `BLOCKED` 作业，
  绝不会影响已在排队或运行中的作业。

### 依赖校验（提交当下拒绝，原因可区分）

| 情形 | 拒绝原因 (`reason`) | HTTP |
|---|---|---|
| 依赖了不存在的作业 ID | `UNKNOWN_DEPENDENCY` | 422 |
| 依赖自己（作业 ID 按序生成，可预判） | `SELF_DEPENDENCY` | 422 |
| 依赖构成环（直接或间接） | `DEPENDENCY_CYCLE` | 422 |
| `depends_on` 含空串/非字符串 | `INVALID_REQUEST` | 422 |

说明：依赖只能指向已存在作业且边的集合不可变，API 层面天然不可能成环；
环检测（`_would_create_cycle`，纯函数）作为防御性校验保留并有独立单测。
被拒绝的提交**不落库、不占幂等键、不消耗作业 ID 序号**，修正后可原键重试。

### 与上一版行为的兼容范围

- 不声明 `depends_on` 的提交走**完全相同的代码路径**（提交即入队、
  同样的准入判定与排队顺序）。上一版的用例作为回归测试**原样保留**在
  `tests/test_scheduler.py / test_concurrency.py / test_api.py` 中，
  随整套单测一起运行（验证方式与可复现前提见第 7、9 节）。
- 既有状态（`PENDING/RUNNING/COMPLETED/FAILED/CANCELLED`）的语义、
  `schedule_count/release_count` 记账规则、幂等键行为、全部既有拒绝原因
  与 HTTP 状态码映射均不变。
- 新增内容只有：`BLOCKED`/`DEPENDENCY_FAILED` 两个状态、`depends_on` 字段、
  三个依赖相关拒绝原因、快照中的 `blocked_jobs` 视图。
- 唯一可观察的行为差异：作业 ID 序号现在只被**成功落库**的提交消耗
  （上一版中被拒提交会留下序号空洞）；ID 本身仍只是不透明标识。

## 3. 配额与公平规则

- 每个提交方（submitter）配置配额上限，配额是**占用量**上限而非提交量上限；
- 作业自身超过配额上限 → 直接拒绝（永远无法满足）；
- 作业本身合法、但提交方当前剩余配额暂时不足 → **排队而不是丢弃**，
  阻塞原因为“等待提交方配额释放”；
- 资源在作业完成 / 失败 / 取消后立刻释放回提交方与节点两级账本，立即可被复用；
- **反饿死**：有效优先级随等待时间无上界增长，任何低优先级作业在等待足够久后
  必然超过任意静态优先级的后来者，因此不存在提交方被无限期饿死的可能
  （见 `test_aging_bounds_waiting_time` 的构造性证明）。

## 4. 提交语义、状态与错误

### 请求校验

| 情形 | 拒绝原因 (`reason`) | HTTP |
|---|---|---|
| 字段为空 / 资源全为 0 或负数 / priority<0 / duration≤0 | `INVALID_REQUEST` | 422 |
| 单作业超过节点容量 | `EXCEEDS_NODE_CAPACITY` | 422 |
| 单作业超过自身配额上限 | `INVALID_REQUEST`（消息中注明配额） | 422 |
| 提交方未配置配额 | `UNKNOWN_SUBMITTER` | 422 |
| 幂等键重复 | `DUPLICATE_IDEMPOTENCY_KEY`（响应携带已存在作业） | 409 |
| 依赖相关（未知依赖 / 自依赖 / 依赖环） | 见第 2 节依赖校验表 | 422 |

### 幂等

- 以 `idempotency_key` 唯一标识一次提交，并发重复提交只有一个作业落库，
  其余拿到 `409` 与首次作业内容（先到为准）。

### 资源守恒不变量（测试中逐条断言）

1. `used == Σ RUNNING 作业资源 == Σ 各提交方占用`；
2. `used ≤ 节点容量`；各提交方占用 ≤ 各自配额；
3. 每个作业调度次数 ≤ 1、释放次数 ≤ 1，且已调度的终态作业二者配对；
4. 排队队列中不残留非 `PENDING` 作业。

### 可解释性

- 每个作业带 `scheduling_note`（最近一次判定原因）与 `history`（提交/授予/释放流水）；
- `GET /scheduler/snapshot` 与每轮 `tick` 返回快照：容量、已用、运行中作业、
  按有效优先级排序的队列（含等待时长）、各提交方占用，以及本轮每个作业的
  `GRANTED / BLOCKED` 判定、原因与判定时的剩余容量/剩余配额明细。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/jobs` | 提交作业（201）；拒绝见上表 |
| GET | `/jobs` | 全部作业 |
| GET | `/jobs/{id}` | 查询单个作业（404） |
| POST | `/jobs/{id}/cancel` | 取消（排队中/等待依赖/运行中均可，409 重复冲突） |
| POST | `/jobs/{id}/complete?failed=false` | 回报完成或失败，释放资源并触发调度 |
| POST | `/scheduler/tick` | 手动执行一轮调度，返回带判定依据的快照 |
| GET | `/scheduler/snapshot` | 当前调度快照 |

提交体示例：

```json
{
  "idempotency_key": "biz-20260923-001",
  "submitter": "team-a",
  "priority": 5,
  "resources": {"cpu": 4, "memory_mb": 8192, "gpu": 1},
  "duration_ms": 30000,
  "depends_on": ["job-000001", "job-000002"]
}
```

`depends_on` 可省略（等价于无依赖，行为与上一版完全一致）。

## 6. 代码结构

```
main.py                       # 入口：节点容量 + 提交方配额 + FastAPI
batch_scheduler/
  models.py                   # Resources / Job / JobState / RejectReason / SchedulingSnapshot
  errors.py                   # JobRejectedError / JobNotFoundError / JobStateConflictError
  scheduler.py                # 核心调度器：加锁记账、tick 调度循环、老化公平、依赖传播、快照
  service.py                  # FastAPI 路由与错误码映射
tests/
  conftest.py                 # 可推进时钟夹具、守恒不变量断言、快照日志打印
  test_scheduler.py           # 校验/拒绝原因/优先级/老化/配额/取消完成/反饿死
  test_dependencies.py        # 依赖：阻塞/放行/失败与取消传播/非法拒绝/并发交错/可复现
  test_concurrency.py         # 并发提交、并发幂等、取消与完成竞争、交错守恒、
                              # 交错结论跨轮次可复现、容量/额度不足边界（排队不丢弃）
  test_api.py                 # HTTP 端到端（含依赖流程与依赖拒绝）
```

## 7. 本地验证

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
# 安装依赖
uv sync

# 运行全部单测
uv run pytest -q

# 逐条列出覆盖了哪些业务用例、各用例是否通过
uv run pytest -v

# 再叠加 -s：打印每个场景的“调度快照 + 判定依据 + 终态摘要”日志
uv run pytest -v -s

# 只跑某一类场景
uv run pytest tests/test_dependencies.py  # 依赖能力用例
uv run pytest tests/test_concurrency.py   # 并发交错与容量/额度边界

# 可复现性验证（与本次结论的得出方式一致）：
# 1) 同一进程内反复跑整套单测，必须每次都全过
for i in $(seq 1 50); do uv run pytest -q -p no:cacheprovider | tail -1; done | sort | uniq -c
# 2) 换进程、换哈希种子跑，结论必须一致（测试不依赖 PYTHONHASHSEED）
for seed in 0 1 42 random; do PYTHONHASHSEED=$seed uv run pytest -q | tail -1; done

# 启动服务
uv run fastapi dev main.py
# 或
uv run uvicorn main:app --reload
```

手动体验：

```bash
curl -s localhost:8000/health
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"idempotency_key":"j1","submitter":"team-a","priority":1,
       "resources":{"cpu":4,"memory_mb":1024,"gpu":0}}'
curl -s localhost:8000/scheduler/snapshot
curl -s -X POST localhost:8000/scheduler/tick

# 依赖体验：下游等上游成功后才进入队列
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"idempotency_key":"j2","submitter":"team-a","priority":1,
       "resources":{"cpu":2,"memory_mb":512,"gpu":0},"depends_on":["job-000001"]}'
curl -s localhost:8000/scheduler/snapshot   # blocked_jobs 中可见 j2 及其等待的上游
curl -s -X POST localhost:8000/jobs/job-000001/complete   # 上游成功，j2 被唤醒并调度
curl -s localhost:8000/scheduler/snapshot
```

## 8. 设计说明与边界

- 本服务**模拟**集群：不真正拉起子进程执行作业，作业的完成由外部
  `POST /jobs/{id}/complete` 回报（或内部执行器接入）。调度状态即“资源是否已授予”。
- 时间源可注入（`clock_ns`），测试使用手动推进的假时钟，保证优先级/老化结论
  对相同输入序列逐位可复现；生产默认使用 `time.monotonic_ns()`。
- 所有状态变更都在同一把可重入锁内完成记账；调度器内部授予/释放路径带有
  “超配 / 超配额 / 重复调度 / 重复释放 / 用量为负”的断言，属于不可恢复的编程错误。

## 9. 并发测试的可复现性约定

并发交错场景（多线程同时提交、完成、取消）的测试遵循以下约定，保证
“同一份代码、反复跑、换进程跑，结论每次都一样”：

- **断言只表达交错不变量**：凡依赖线程交错顺序才可能成立的固定结果
  （例如“恰好 N 个在运行”），必须先给出与交错顺序无关的论证再断言。
  调度器每次操作都以 `tick()` 收尾、`tick()` 扫描到“整轮无新授予”才停，
  因此稳定态下“节点未满 ⇒ 每个还有排队作业的提交方配额必已用满”。
  据此可推出终态的运行/排队计数是确定的（论证写在
  `tests/test_concurrency.py` 的 `_CHURN_SUBMITTERS` 注释中），
  而不是某次运行的巧合。
- **测试代码不引入任何随机源**：不使用 `hash()`（字符串哈希随
  `PYTHONHASHSEED` 逐进程随机化）、不使用 `random`、不依赖墙钟时间；
  补提作业的提交方等选择一律按确定规则分配。曾经正是
  `hash(job_id) % 3` 让补提作业在提交方间的分布随进程漂移，
  分布偏斜时配额池填不满节点，导致“16 个在运行”的断言偶发失败、
  且换进程结论不一致——该写法已移除并有回归覆盖。
- **不变量逐用例校验**：每个用例结尾调用 `assert_conservation`
  （`used == ΣRUNNING == Σ各提交方占用`、不超容量/配额、调度/释放计数配对、
  队列无脏 id）；容量或额度暂时不足的场景断言“排队等待、不丢弃、
  不提前判终态、释放后被接住”（见
  `test_concurrent_quota_shortage_queues_then_fills`、
  `test_exact_fit_boundary_grant_vs_queue`）。
- **可复现性有专门用例兜底**：
  `test_interleaved_churn_reproducible_across_rounds` 把同一交错混沌脚本
  在 5 个全新调度器实例上各跑一遍（每轮线程交错都不同），
  要求终态摘要逐位相同；`test_same_input_sequence_reproduces_same_outcome`
  固定输入序列验证结论逐位可复现。

本次兼容性结论的验证方式：整套单测（58 条，含上一版保留的回归用例）
在同一进程内连续运行 50 次以上、并在多个 `PYTHONHASHSEED`（含 `random`）
下分别运行，结果全部通过；本地可用第 7 节的命令复现同样的结果。
结论可复现的前提：测试全部使用注入的假时钟、无随机源、无网络依赖；
若改动调度规则或配额/容量配置，需重新按上述方式验证。
