# Recovery Planner（恢复规划 HTTP API）

基于 FastAPI + SQLite 的恢复规划服务：批量登记数据库快照与日志片，针对给定 UTC 查询时刻，
计算最优恢复方案（快照 + 同格式日志片连续回放），返回快照、逐片实际回放区间与恢复终点序号。
还可以基于某一时刻的最佳方案**创建恢复作业**：作业冻结创建时的快照、逐片回放区间与终点，
在作业进行中持续保护其引用的记录不被清理，结束作业后才释放。

## 快速启动（Docker，单命令）

```bash
docker compose up --build
```

服务监听 **8000** 端口（宿主机 `8000` → 容器 `8000`）。镜像内已安装全部依赖，
并在构建时与服务启动时（幂等）初始化 SQLite schema；数据库文件位于容器内 `/data/recovery.db`，
通过命名卷 `recovery-data` 持久化。交互式 API 文档见 `http://localhost:8000/docs`。

本地开发（不用 Docker）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --port 8000     # DB_PATH 环境变量可指定数据库路径，默认 ./recovery.db
pytest                               # 运行测试
```

## 数据模型与规则

**快照 snapshot**：`id`（唯一，非空字符串）、`created_at`（UTC 生成时间，ISO 8601）、
`applied_seq`（全局非负整数序号）、`format`（格式代号，非空字符串）。

**日志片 log segment**：`id`（唯一）、`created_at`（UTC 生成时间）、`format`（格式代号）、
`first_seq..last_seq`（正整数闭区间，`1 <= first_seq <= last_seq`）。

**批量登记 `POST /batches`**：

- 请求体 `{"snapshots": [...], "log_segments": [...]}`，两表合计**至多 100 条**；
- `id` 在快照与日志片之间全局唯一（批内及与库中已有记录均不得重复）；
- 重复 id、字段非法（类型/取值/多余字段）或超过 100 条时，**整批拒绝、一条不入库**
  （字段/批内校验失败返回 `422`，与库中记录冲突返回 `409`）；
- 时间可带任意时区偏移（如 `+02:00`）或为朴素时间（视为 UTC），一律归一化为 UTC 存储。

**恢复规划 `GET /recovery-plan?at=<UTC 时刻>`**：

- 只使用 `created_at <= at` 的快照与日志片（含等于）；
- 从快照 `applied_seq + 1` 起，仅用**同格式**日志片连续回放：每片从下一未回放序号接至片尾，
  重叠部分不重放，遇缺口即停；
- 优化目标：最大化恢复终点序号；同分依次取 `applied_seq` 最大、`id` 字典序最小的快照，
  再取片数最少、回放 id 序列字典序最小的链；
- 无可用快照时返回 `404`，明确无恢复点。

**恢复作业（冻结方案 + 占用保护）**：

- `POST /recovery-jobs`，请求体 `{"at": "<UTC 时刻>"}`：按现有规划规则计算 `at` 时刻的最佳方案，
  把**选中的快照、逐片回放区间（id/from_seq/to_seq）与终点**原样冻结到作业中；返回服务生成的
  作业 `id`（UUID 十六进制）及 `status: "in_progress"`。无恢复点返回 `404` 且**不创建任何作业**；
  时刻非法或有多余字段返回 `422`。
- `GET /recovery-jobs/{id}`：返回作业冻结的方案（与 `/recovery-plan` 同构的 `snapshot`/`replay`/
  `endpoint`）、请求时刻 `at` 与 `status`；未知 id 返回 `404`。
- `POST /recovery-jobs/{id}/end`：结束作业，返回作业完整表示；重复结束幂等（始终 `200` 且状态
  保持 `finished`）；未知 id 返回 `404`。
- 作业创建后方案即固定：**后续登记不改变已存方案**（现场 `/recovery-plan` 仍按全量数据实时计算）。
- 进行中的作业**占用**其冻结方案用到的每一条记录（选中快照 + 每一片实际回放的日志片）；结束只
  释放本作业的占用，被多个作业共享的记录要等**所有占用者都结束**才解除保护。已结束作业不再占用
  任何记录，其冻结方案仍可读取（即使底层记录随后被删除）。
- 作业、方案与状态存入现有 SQLite，**重启保留**；服务不执行真实恢复，也不设自动过期。
- 创建、结束与清理均通过 `BEGIN IMMEDIATE` 写事务串行化：成功创建的进行中作业绝不会引用已删
  记录；结束一个作业也不会提前解除其他作业的保护。

**受保护时刻批量清理 `POST /cleanups`**：

- 请求体 `{"ids": [...], "protected_at": [...]}`：`ids` 为 **1～100 个互异**的已登记记录 id
  （快照、日志片可混填），`protected_at` 为 **1～8 个 UTC 查询时刻**；
- 请求内 id 重复、数量越界、字段非法或有多余字段返回 `422`；存在库中没有的 id 时**整批拒绝**
  （`404`），一条不删；
- **作业占用优先**：只要有任一 id 正被**进行中**的恢复作业占用（其冻结快照或任一回放片），立即
  `409` 整批拒绝、一条不删，返回 `conflicts`：每个冲突记录的 `id` 及占用它的作业 id 列表
  （`{"message": ..., "conflicts": [{"id": ..., "job_ids": [...]}]}`）。即使删除后存在等价恢复链、
  终点不下降，作业仍只认创建时选定的具体记录；共享记录须等所有占用作业结束；
- 否则按上面的规划规则，对每个时刻分别计算**删除前**与**整批删除后**的最佳恢复终点；
  无恢复点以 `null` 表示，低于任何序号；
- 只有**所有**时刻的删除后终点都不低于删除前（允许相等，允许 `null → null`）才执行删除；
  任一下降则 `409`，返回所有下降时刻及其前后终点，一条也不删；
- 两道检查与删除在同一个 SQLite 写事务（`BEGIN IMMEDIATE`）中完成，基于同一份数据库状态，
  失败不会留下部分删除；
- 成功返回 `200`：已删除 id 列表（按请求顺序）与各时刻保留的终点（`null` 表示无恢复点）。

## HTTP 示例（可直接执行）

### 1. 登记一批快照与日志片

```bash
curl -s -X POST http://localhost:8000/batches \
  -H 'Content-Type: application/json' \
  -d '{
    "snapshots": [
      {"id": "snap-1", "created_at": "2026-09-01T08:00:00Z", "applied_seq": 100, "format": "wal-v1"},
      {"id": "snap-2", "created_at": "2026-09-01T12:00:00Z", "applied_seq": 90, "format": "wal-v1"}
    ],
    "log_segments": [
      {"id": "seg-1", "created_at": "2026-09-01T09:00:00Z", "format": "wal-v1", "first_seq": 101, "last_seq": 150},
      {"id": "seg-2", "created_at": "2026-09-01T10:00:00Z", "format": "wal-v1", "first_seq": 140, "last_seq": 180},
      {"id": "seg-3", "created_at": "2026-09-01T11:00:00Z", "format": "wal-v1", "first_seq": 182, "last_seq": 200},
      {"id": "seg-9", "created_at": "2026-09-01T10:30:00Z", "format": "wal-v2", "first_seq": 101, "last_seq": 999}
    ]
  }'
# => 201  {"stored_snapshots": 2, "stored_log_segments": 4}
```

### 2. 查询恢复计划

```bash
curl -s 'http://localhost:8000/recovery-plan?at=2026-09-02T00:00:00Z'
```

响应（`snap-1` 起回放：`seg-1` 覆盖 101..150，`seg-2` 与其重叠、只回放 151..180；
`seg-3` 在 181 处出现缺口而停止；`seg-9` 格式不符被忽略；终点 180，而 `snap-2` 单独只到 160）：

```json
{
  "snapshot": {"id": "snap-1", "created_at": "2026-09-01T08:00:00+00:00", "applied_seq": 100, "format": "wal-v1"},
  "replay": [
    {"id": "seg-1", "from_seq": 101, "to_seq": 150},
    {"id": "seg-2", "from_seq": 151, "to_seq": 180}
  ],
  "endpoint": 180
}
```

### 3. 创建、读取、结束恢复作业

在某 UTC 时刻创建作业：服务按现有规则计算最佳方案并冻结，返回服务生成的作业 id，初始状态为
`in_progress`：

```bash
curl -s -X POST http://localhost:8000/recovery-jobs \
  -H 'Content-Type: application/json' \
  -d '{"at": "2026-09-02T00:00:00Z"}'
# => 201
```

```json
{
  "id": "6cdff6f411ee44cf9b3145a4ecd5b984",
  "at": "2026-09-02T00:00:00+00:00",
  "status": "in_progress",
  "snapshot": {"id": "snap-1", "created_at": "2026-09-01T08:00:00+00:00", "applied_seq": 100, "format": "wal-v1"},
  "replay": [
    {"id": "seg-1", "from_seq": 101, "to_seq": 150},
    {"id": "seg-2", "from_seq": 151, "to_seq": 180}
  ],
  "endpoint": 180
}
```

查询作业（返回冻结方案与当前状态）、结束作业（幂等，重复结束仍为 `finished`）：

```bash
curl -s http://localhost:8000/recovery-jobs/6cdff6f411ee44cf9b3145a4ecd5b984
curl -s -X POST http://localhost:8000/recovery-jobs/6cdff6f411ee44cf9b3145a4ecd5b984/end
# => 200  {"...": "...", "status": "finished"}
curl -s -X POST http://localhost:8000/recovery-jobs/6cdff6f411ee44cf9b3145a4ecd5b984/end
# => 200  仍为 finished；未知 id（读取或结束）=> 404
```

该时刻之前没有任何快照时，无恢复点，返回 `404` 且不创建作业：

```bash
curl -s -X POST http://localhost:8000/recovery-jobs \
  -H 'Content-Type: application/json' \
  -d '{"at": "2020-01-01T00:00:00Z"}'
# => 404  {"detail": "no recovery point: no snapshot created at or before 2020-01-01T00:00:00+00:00; job not created"}
```

作业进行中保护其引用的记录（`snap-1`、`seg-1`、`seg-2`）；之后即使登记了更好的快照，已存作业的
冻结方案也不变。`docker compose restart` 后作业、方案与状态仍保留。

### 4. 受保护时刻批量清理

清理两个对恢复终点没有贡献的 id（`seg-3` 因缺口从未被回放；`snap-2` 单独只到 160）。
同时给出两个受保护时刻：未来时刻要求终点仍为 180；2020 年时刻本就没有任何恢复点
（`null → null` 视为不下降），因此整批删除成功：

```bash
curl -s -X POST http://localhost:8000/cleanups \
  -H 'Content-Type: application/json' \
  -d '{
    "ids": ["seg-3", "snap-2"],
    "protected_at": ["2026-09-02T00:00:00Z", "2020-01-01T00:00:00Z"]
  }'
# => 200
```

```json
{
  "deleted_ids": ["seg-3", "snap-2"],
  "endpoints": [
    {"at": "2026-09-02T00:00:00+00:00", "endpoint": 180},
    {"at": "2020-01-01T00:00:00+00:00", "endpoint": null}
  ]
}
```

删除会令某时刻终点下降时（如下例删掉 `seg-2`，终点从 180 降到 150；另一时刻 `null → null`
不下降）返回 `409`，并列出**所有**下降时刻及前后终点，**一条也不删**：

```bash
curl -s -X POST http://localhost:8000/cleanups \
  -H 'Content-Type: application/json' \
  -d '{
    "ids": ["seg-2"],
    "protected_at": ["2026-09-02T00:00:00Z", "2020-01-01T00:00:00Z"]
  }'
# => 409
```

```json
{
  "detail": {
    "message": "recovery endpoint would decrease at protected moment(s); nothing deleted",
    "degraded_moments": [
      {"at": "2026-09-02T00:00:00+00:00", "before": 180, "after": 150}
    ]
  }
}
```

不存在的 id 整批拒绝（`404`）；请求内 id 重复、id/时刻数量越界（1～100 个互异 id、
1～8 个时刻）或字段非法返回 `422`：

```bash
curl -s -X POST http://localhost:8000/cleanups \
  -H 'Content-Type: application/json' \
  -d '{"ids": ["ghost"], "protected_at": ["2026-09-02T00:00:00Z"]}'
# => 404  {"detail": "id(s) not registered: ['ghost']; cleanup rejected"}
```

**进行中作业占用优先**：先创建一个使用 `snap-1`、`seg-1`、`seg-2` 的作业，然后清理 `seg-2`。
即使存在删除后仍能到达 180 的等价恢复链（如 `snap-2` 同链回放），也因作业占用而整批 `409`、
一条不删，并返回冲突记录及占用它的作业 id：

```bash
JOB=$(curl -s -X POST http://localhost:8000/recovery-jobs \
  -H 'Content-Type: application/json' \
  -d '{"at": "2026-09-02T00:00:00Z"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

curl -s -X POST http://localhost:8000/cleanups \
  -H 'Content-Type: application/json' \
  -d '{"ids": ["seg-2"], "protected_at": ["2026-09-02T00:00:00Z"]}'
# => 409
```

```json
{
  "detail": {
    "message": "record(s) are used by in-progress recovery job(s); nothing deleted",
    "conflicts": [
      {"id": "seg-2", "job_ids": ["6cdff6f411ee44cf9b3145a4ecd5b984"]}
    ]
  }
}
```

记录被多个进行中作业共享时，`job_ids` 列出全部占用者；`POST /recovery-jobs/{id}/end` 结束本作业
只释放它自己的占用，须等所有占用者结束后，该记录才回到受 `protected_at` 规则管理的可清理状态。
作业结束后其冻结方案仍可 `GET` 读取，哪怕底层记录随后已被删除。

### 5. 无恢复点（查询时刻之前没有任何快照）

```bash
curl -s 'http://localhost:8000/recovery-plan?at=2020-01-01T00:00:00Z'
# => 404  {"detail": "no recovery point: no snapshot created at or before 2020-01-01T00:00:00+00:00"}
```

### 6. 登记接口整批拒绝示例

```bash
# 重复 id（与库中已有记录冲突）：整批不入库
curl -s -X POST http://localhost:8000/batches \
  -H 'Content-Type: application/json' \
  -d '{"snapshots": [{"id": "snap-1", "created_at": "2026-09-03T00:00:00Z", "applied_seq": 5, "format": "wal-v1"}]}'
# => 409  {"detail": "id(s) already registered: ['snap-1']; batch rejected"}

# 非法字段（first_seq > last_seq）：422，整批不入库
curl -s -X POST http://localhost:8000/batches \
  -H 'Content-Type: application/json' \
  -d '{"log_segments": [{"id": "bad", "created_at": "2026-09-03T00:00:00Z", "format": "wal-v1", "first_seq": 10, "last_seq": 3}]}'
# => 422  {"detail": [... "last_seq (3) must be >= first_seq (10)" ...]}
```

### 7. 健康检查

```bash
curl -s http://localhost:8000/health    # => {"status": "ok"}
```

## 项目结构

```
app/
  main.py      # FastAPI 应用与端点（启动时初始化 SQLite；作业/清理在单写事务内完成）
  models.py    # 请求校验（pydantic，严格类型）
  planner.py   # 回放链规划：DAG 动态规划，实现全部平局裁决规则
  db.py        # SQLite 连接、建表、批量原子写入、删除与恢复作业持久化
tests/test_api.py
tests/test_jobs.py
Dockerfile / docker-compose.yml
```
