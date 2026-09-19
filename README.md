# Sequential Webhook Delivery API

一个让下游系统**稳定、按顺序**接收事件的 Webhook 投递服务。客户端注册回调地址后提交事件（`source` / `sequence` / `event_id`），服务为**每个订阅 × 每个 source** 维护严格顺序：

- **严格顺序投递** —— 同一订阅同一 `source` 严格按 `sequence` 升序投递；乱序到达的事件先缓冲，等缺号补齐。
- **幂等入库** —— `event_id` 全局唯一，重复提交返回 `200` 且 `duplicate: true`，不会重复投递。
- **失败不越过** —— 队头事件投递失败时指数退避重试（`1s, 2s, 4s…`，上限可配），后续 `sequence` 一律被挡住。
- **死信队列** —— 重试达到上限后进入死信，游标前移，后续序列继续投递；提供死信查询和**手动重放**接口。
- 内置 HMAC-SHA256 签名、订阅暂停/恢复、积压/统计查询。
- Python 3.11 + FastAPI + SQLite（WAL），零外部依赖、无需任何第三方账号。

---

## 一条命令启动

```bash
docker compose up --build
```

启动后服务监听 **http://localhost:8080**：

- API 文档（Swagger UI）：http://localhost:8080/docs
- 健康检查：http://localhost:8080/health

数据持久化在 Docker 卷 `webhook-data`（容器内 `/data/webhooks.db`）。数据库表在应用启动时自动创建（`CREATE TABLE IF NOT EXISTS`），无需手动初始化。

停止：`Ctrl+C`；后台运行：`docker compose up --build -d`。

---

## 快速上手

### 1. 注册回调地址

```bash
curl -X POST http://localhost:8080/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://your-downstream.example.com/hook", "secret": "optional-hmac-secret"}'
# -> 201 {"id": 1, "url": "...", "active": 1, ...}
```

订阅创建后，对它注册**之前**已经存在的事件也会自动补投（从各 source 的最小序号开始）。

### 2. 提交事件（乱序也没关系）

```bash
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' -d '{
  "source": "orders",
  "sequence": 3,
  "event_id": "evt-3",
  "payload": {"order_id": 42, "status": "shipped"}
}'
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' -d '{
  "source": "orders", "sequence": 1, "event_id": "evt-1",
  "payload": {"order_id": 42, "status": "created"}
}'
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' -d '{
  "source": "orders", "sequence": 2, "event_id": "evt-2",
  "payload": {"order_id": 42, "status": "paid"}
}'
```

即使按 3 → 1 → 2 到达，回调端也只会按 **1 → 2 → 3** 收到。

重复提交相同 `event_id` 幂等返回：

```json
{"event": { ... }, "duplicate": true, "status": "duplicate"}
```

### 3. 下游收到的请求

`POST <callback url>`，请求头：

| Header | 说明 |
| --- | --- |
| `X-Webhook-Event` | `event_id` |
| `X-Webhook-Source` | `source` |
| `X-Webhook-Sequence` | `sequence` |
| `X-Webhook-Delivery` | 内部投递记录 ID |
| `X-Webhook-Attempt` | 第几次尝试（从 1 开始） |
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(secret, raw-body)>`（仅在注册时提供了 `secret`） |

请求体（始终携带排序元数据的信封）：

```json
{
  "event_id": "evt-1",
  "source": "orders",
  "sequence": 1,
  "payload": {"order_id": 42, "status": "created"}
}
```

回调返回 **2xx** 视为成功；其他状态码、连接错误、超时均视为失败并触发退避重试。建议下游按 `X-Webhook-Event` 自行去重。

---

## 投递语义

- 顺序单元：`(subscription_id, source)`。不同订阅之间、同一订阅的不同 source 之间互不阻塞。
- 游标 `next_sequence` 指向该顺序单元**下一个待处理序号**。
  - 队头投递成功 → 游标 +1；
  - 队头重试中 → 游标不动，后续序号保持缓冲（积压接口中 `buffered: true`）；
  - 队头重试耗尽进入死信 → 游标 +1，后续序号继续投递；
  - 死信手动重放成功后变为 `delivered`（不改变游标，仅补齐该条）。
- 退避：第 `n` 次失败后等待 `min(RETRY_BASE_DELAY_SECONDS × 2^(n-1), RETRY_MAX_DELAY_SECONDS)`，默认 `1, 2, 4, 8, 16s`。
- 顺序保证由 SQL 队头判定 + 每个 `(subscription, source)` 同一时刻最多一个在途投递共同实现。
- 调度器运行在应用进程内（单 uvicorn worker），因此**每个数据库只运行一个实例**——`docker compose up` 默认即此拓扑；请勿对同一 SQLite 卷启动多个副本。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/subscriptions` | 注册回调 `{url, secret?}` → 201 |
| `GET`  | `/subscriptions` | 列出订阅（`limit`/`offset`） |
| `GET`  | `/subscriptions/{id}` | 订阅详情 |
| `POST` | `/subscriptions/{id}/activate` `/deactivate` | 恢复 / 暂停投递 |
| `POST` | `/events` | 提交事件 `{source, sequence, event_id, payload?}` |
| `GET`  | `/events/{event_id}` | 按 event_id 查询事件 |
| `GET`  | `/backlog` | 积压查询（可按 `subscription_id`、`source` 过滤），含 `buffered`、`awaiting_sequence`、`attempts`、`next_attempt_at` |
| `GET`  | `/dead-letters` | 死信查询（同上过滤） |
| `GET`  | `/deliveries/{id}` | 单条投递记录（错误原因、尝试次数等） |
| `POST` | `/dead-letters/{id}/replay` | 手动重放死信 → `{"status":"queued"}`；非死信返回 409 |
| `GET`  | `/stats` | 订阅/事件/各状态投递计数 |
| `GET`  | `/health` | 健康检查 |

典型运维操作：

```bash
# 查看某 source 的死信
curl "http://localhost:8080/dead-letters?source=orders"

# 修好下游后重放（用返回的 delivery id）
curl -X POST http://localhost:8080/dead-letters/12/replay
```

---

## 配置（环境变量）

全部有默认值，均为可选项，在 `docker-compose.yml` 的 `environment` 中调整：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/webhooks.db` | SQLite 文件路径 |
| `MAX_ATTEMPTS` | `5` | 最大尝试次数（首次 + 重试），达到后进入死信 |
| `RETRY_BASE_DELAY_SECONDS` | `1` | 首次重试等待，其后指数翻倍 |
| `RETRY_MAX_DELAY_SECONDS` | `300` | 单次退避上限（秒） |
| `DELIVERY_TIMEOUT_SECONDS` | `10` | 调用下游的 HTTP 超时 |
| `DISPATCH_INTERVAL_SECONDS` | `0.2` | 调度器轮询间隔 |
| `DISPATCH_CONCURRENCY` | `16` | 不同顺序单元之间的并发投递线程数 |
| `PORT` | `8080` | 服务监听端口 |

> 想让重试更快/更慢只需改 `RETRY_BASE_DELAY_SECONDS` 与 `MAX_ATTEMPTS`，重启容器生效。

---

## 本地开发（不使用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export DATABASE_PATH=./data/webhooks.db
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

运行测试（自带一个可配置失败行为的 HTTP 接收器，覆盖乱序缓冲、幂等、退避阻塞、死信与重放、source 隔离、HMAC 签名等 11 个用例）：

```bash
pip install -r requirements.txt pytest
PYTHONPATH=. pytest -v
```

## 项目结构

```
app/
  config.py      # 环境变量配置
  db.py          # SQLite 建表、入库、游标/死信/积压查询
  dispatcher.py  # 后台调度：队头选取、HTTP 投递、指数退避
  main.py        # FastAPI 路由
Dockerfile
docker-compose.yml
requirements.txt
tests/           # 端到端测试
```
