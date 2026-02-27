# CLI-Driven Agent Workflow (SOP)

> **生效时间**: 2026-02-26 起
> **核心原则**: CLI 下达指令 -> Agent 自治编码与调试 -> User Web 端验收

## 1. 核心分离模式 (Separation of Concerns)

未来的开发将全面采用 **"指令驱动 + 自动执行 + 黑盒验收"** 的异步协作模式，彻底分离“编码逻辑”和“视觉验收”。

### 👨‍💻 User (架构师 / 研究员)
- **唯一操作入口**: 通过 Cursor Chat 或自带终端等 CLI 界面，直接输入自然语言诉求或高层次指令。
- **唯一验收出口**: 在浏览器 (如 `http://localhost:8000`) 中查看图表渲染、点击交互操作，验证业务逻辑。
- **避免心智负担**: 不需要介入文件的修改、不需要关心前后端参数的对齐、不需要手动去重启 Uvicorn 进程。

### 🤖 Agent (AI 协同助手)
- **自治规划**: 接收指令后，自动检索相关文档，划分 Task Boundary，维护 `task.md`。
- **全栈编码**: 熟练使用 `multi_replace_file_content`、`write_to_file` 操作 Python / HTML / JS / CSS 源码。
- **自包含测试**: 编码后，**主动**使用终端命令(`run_command`)排查报错、运行 `curl` 验证接口、自动分析报错并原地修复。
- **交付终态**: 产生稳定的功能特性，通过 `notify_user` 输出简洁的完成报告及测试入口。

---

## 2. 标准迭代循环 (The Workflow Loop)

每次功能开发或 Bug 修复，严格遵守以下 4 步循环：

### 🔄 Step 1: 意图下达 (User)
* "按照 `03_continuation_plan.md`，实现 Phase 4 的研究会话留痕功能。"
* "图表渲染错位，`index.html` 里面的 `chart-scatter` 没有高度，去修复下样式。"
* "给 E 库增加一个新的 Issue 检测，逻辑是..."

### 🔄 Step 2: 方案与执行 (Agent)
1. **查阅资料**: 自动 `grep` 数据库字段或相关蓝图文件 (`codex.md` 及相关)。
2. **代码修改**: 对 `webui/api/*.py` 和 `webui/static/index.html` 进无缝修改。
3. **闭环自验**: 
   - Agent 后台启动 `uvicorn webui.main:app --reload`
   - Agent 跑 `curl` 拿到 JSON 数据，如果不符合预期，内部消化解决。

### 🔄 Step 3: 工作交接 (Agent -> User)
* 修改停止，Agent 生成本轮迭代的 `walkthrough.md`。
* Agent 返回一句话总结：“功能已完成，并在本地通过了 JSON 测试，请刷新页面验证 `http://localhost:8000`。”

### 🔄 Step 4: 结果签收 (User)
* User 在浏览器中刷新，观察新图表、新弹窗或刚提交的表单。
* **Pass** → 提出下一个 CLI 需求，进入新循环。
* **Fail** → 将浏览器的直观感受反馈回 CLI：“弹窗没出来，看下是不是 Vue 的双向绑定写错了”，退回 Step 1。

---

## 3. 目录与知识管理协议

为了保证这种基于上下文的自动化 Agent 修改能够持续生效，项目遵从以下**文档收敛机制**：

1. **宏观设计收敛到 `docs/`**
   - 新的项目蓝图、新增表的定义，由 User 或 Agent 梳理至 `docs/07_ip_profiling_visual_research/` 下。Agent 在接受宽泛指令时，首选看这里的文档。
2. **实施步骤收敛到 `task.md` / `walkthrough.md`**
   - Agent 在系统数据目录(`.gemini/antigravity/brain/...`) 动态维护当前 Checklist，作为临时记忆。
3. **架构极简原则**
   - 绝不引入 Node.js/Webpack 编译。前端全部集中于 `index.html` (使用 Vue3 Composition API CDN 版 + 原生 JS + ECharts)。
   - 后盾全在 FastAPI，统一使用 `models/database.py` 的 async DB 驱动。
   - 所有 Python 进程操作，必须指定 `PYTHONPATH=/Users/yangcongan/cursor/IP/webui`。
