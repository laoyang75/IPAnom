# RB20 v2.5 可视化监控平台 (WebUI)

> Pipeline 协同调度看板 + 数据微观探查界面

## 功能模块

| 模块 | 说明 |
|------|------|
| 📊 协同看板 | KPI 仪表盘、65 Shard 热力图、网络规模分布、QA 断言结果、守恒公式验证 |
| 🔍 数据探查 | IP 溯源链路（Source → Natural Block → Profile → Final Block → H/E/F 分类） |
| ✅ QA 面板 | 逐条断言详情卡片、守恒差值验证 |

## 技术栈

- **后端**: FastAPI + SQLAlchemy (async) + asyncpg
- **前端**: Vue3 (CDN) + Vanilla CSS (暗色专业主题)
- **数据库**: PostgreSQL `rb20_v2_5` schema

## 目录结构

```
05_webui/
├── main.py              # FastAPI 入口，启动服务
├── config.py            # 数据库连接 & 应用配置
├── requirements.txt     # Python 依赖
├── api/
│   ├── dashboard.py     # 看板 API (8 个端点)
│   └── explorer.py      # IP 溯源 API (4 个端点)
├── models/
│   ├── database.py      # 异步连接池 & SQL 帮助函数
│   └── schemas.py       # Pydantic 数据模型
├── services/            # 业务逻辑层 (预留)
├── static/
│   ├── index.html       # Vue3 SPA 主页面
│   └── assets/style.css # 暗色主题样式
├── templates/           # Jinja2 模板 (预留)
└── ws/                  # WebSocket (预留)
```

## API 端点清单

### 看板 API (`/api/dashboard/`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/runs` | 列出所有运行批次 |
| GET | `/runs/{run_id}/overview` | 批次总览统计 (H/E/F/Keep/Drop/QA) |
| GET | `/runs/{run_id}/shards` | 65 Shard 状态矩阵 |
| GET | `/runs/{run_id}/qa` | QA 断言结果 |
| GET | `/runs/{run_id}/step-stats` | 各步骤执行耗时 |
| GET | `/runs/{run_id}/network-tier-distribution` | 网络规模分布 |
| GET | `/runs/{run_id}/classification-summary` | H/E/F 分类汇总 |

### 数据探查 API (`/api/explore/`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/ip/{ip_address}` | 按 IP 地址溯源 |
| GET | `/ip-long/{ip_long}` | 按 ip_long 整数溯源 |
| GET | `/block/{block_id}` | 块详情 |
| GET | `/blocks` | Shard 内块列表（分页） |

## 配置

通过环境变量配置数据库连接（默认值见 `config.py`）：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DB_HOST` | `192.168.200.217` | PostgreSQL 主机 |
| `DB_PORT` | `5432` | 端口 |
| `DB_USER` | `postgres` | 用户名 |
| `DB_PASSWORD` | `123456` | 密码 |
| `DB_NAME` | `ip_loc2` | 数据库名 |

## 快速启动

详见 [QUICKSTART.md](./QUICKSTART.md)
