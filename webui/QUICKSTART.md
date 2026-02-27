# 快速启动指南

## 环境要求

- Python ≥ 3.8
- PostgreSQL 数据库（已有 `rb20_v2_5` schema 数据）
- 网络可达数据库主机（默认 `192.168.200.217:5432`）

## 1. 安装依赖

```bash
cd webui
pip install -r requirements.txt
```

依赖清单：
- `fastapi` — Web 框架
- `uvicorn` — ASGI 服务器
- `sqlalchemy[asyncio]` — 异步 ORM
- `asyncpg` — PostgreSQL 异步驱动
- `psycopg2-binary` — PostgreSQL 同步驱动
- `pydantic` — 数据验证
- `python-dotenv` — 环境变量加载

## 2. 配置数据库连接

创建 `.env` 文件（可选，也可直接使用默认值）：

```bash
cat > .env << 'EOF'
DB_HOST=192.168.200.217
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=123456
DB_NAME=ip_loc2
EOF
```

## 3. 启动服务

```bash
# 开发模式（热重载）
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 或直接执行
python3 main.py
```

## 4. 访问界面

| 地址 | 说明 |
|------|------|
| `http://localhost:8000` | 主界面（看板/探查/QA） |
| `http://localhost:8000/docs` | Swagger API 文档（仅 DEBUG 模式） |
| `http://localhost:8000/health` | 健康检查 |

## 5. 使用说明

### 协同看板
- 页面加载后自动拉取最新 run_id 的数据
- 右上角下拉框可切换不同运行批次
- Shard 热力图悬浮查看详情，点击可查看 Shard 详细统计
- 底部 QA 断言表显示所有 Phase 99 校验结果

### 数据探查
- 切换到「数据探查」Tab
- 输入 IP 地址（如 `1.24.117.116`）或 ip_long 整数
- 查看完整溯源链路：Source → Natural Block → Profile → Final Block → 分类结果
- 自然块和最终块的画像详情（密度、Valid Count、网络规模）

## 常见问题

### Q: 启动报错 `TypeError: unsupported operand type(s) for |`
Python 3.9 及以下版本不支持 `X | Y` 类型语法，请确认已使用修复后的 `Optional[dict]` 写法。

### Q: Shard 数据加载很慢
首次加载 65 个 Shard 的多表 JOIN 统计约需 30 秒。后续可通过 PostgreSQL 物化视图优化（参见 `05_docs/04_online_migration_strategy.md`）。

### Q: 无法连接数据库
请确认：
1. 数据库主机网络可达（可能需要 VPN/SSH 隧道）
2. `.env` 或环境变量配置正确
3. PostgreSQL 已开放远程连接
