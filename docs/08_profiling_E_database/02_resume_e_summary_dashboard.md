# E 库块级摘要看板：开发中断与恢复指南 (Resume Guide)

## 当前进度总结 (Status)
我们在构建 E 库摘要看板（基于 CIDR 块/连打 `e_run_id` 的维度）时中断了进程。为了不影响前端响应速度，我们放弃了“实时关联宽表 `source_members` 计算”的方案，转为通过离线聚合脚本生成单表 `rb20_v2_5.e_runs_summary`。

目前各模块进度如下：
- ✅ **数据库设计**: 已完成聚合维度的 SQL 设计（涵盖 DAA/DNA上报数、制造商数、运营商等）。
- ✅ **构建脚本**: 已编写 `Y_IP_Codex_RB2_5/04_runbook/orchestrate_e_runs_summary.py`，负责并发聚合 64 个 Shard 写入新表。*(运行中被中断)*
- ✅ **Backend API**: 已在 `webui/api/research.py` 底部增加 `GET /runs/{run_id}/e/summary-blocks` 端点，支持分页和多维度排序。
- ⏳ **Frontend UI**: `.html` 侧已经注入了 Sub-Tab 的按钮 (`📝 块摘要`) 和部分 Vue 变量，但可视化组件（散点图、明细表）和 `loadESummary()` 取数逻辑还没有完全应用上。

## 下次启动时如何恢复工作 (How to Resume)

### 第一步：重新运行聚合数据脚本
由于刚才强制中断，你需要重新跑一遍生成专用提速表的脚本：
```bash
python3 Y_IP_Codex_RB2_5/04_runbook/orchestrate_e_runs_summary.py
```
*(该脚本将 Drop 并重构 `e_runs_summary`，运行约 15-20 分钟)*

### 第二步：恢复 Frontend 前端代码注入
将该提示词发给 AI：
> "请继续 E 库块级摘要统计的前端 `index.html` 注入工作。把散点分布图 (`chart-e-scatter`)、块明细表以及 `loadESummary()` 的取数逻辑补全。"

### 第三步：验证并启动 WebUI
当跑完数据脚本且前端补全后，启动 WebUI 测试：
```bash
python3 webui/launcher.py
```
进入浏览器 `http://localhost:8000`，切换至 **"🧪 库画像研究" -> "🟣 E 库" -> "📝 块摘要"** 面板验证效果。
