# IP 库画像可视化研究平台 — 需求理解

## 核心目标

以 **单个 IP 库 (H/E/F)** 为研究单位，通过可视化方式还原 Pipeline 处理过程，发现问题后与 Agent 协作修正，最终形成可大规模执行的标准化处理规则。

## 工作分工

```
外部深度思考 Agent (Claude/Gemini) ── 02_agent_prompt.md
  │  理解全部项目上下文 → 输出方案设计
  │
  ↓ 方案
编码 Agent (本 Agent)
  │  结合方案 + 现有代码/数据库 → 实现可视化平台
  ↓
分析师
    使用平台研究 H/E/F 库 → 发现问题 → 记录规则 → 规模化执行
```

## 文件清单

| 文件 | 用途 |
|------|------|
| `00_requirements_understanding.md` | 本文件，需求概述 |
| `01_visualization_scheme.md` | 可视化方案初稿（待外部 Agent 优化） |
| `02_agent_prompt.md` | **给外部深度思考 Agent 的完整 Prompt**（自包含，含全部上下文） |
