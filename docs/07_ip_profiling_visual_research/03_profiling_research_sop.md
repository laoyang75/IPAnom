# IP Profiling Research SOP (Agent Prompt)

> **Context**: We are entering a collaborative research phase focused on profiling the IP databases (`E`, `H`, and `F`). This document serves as the Standard Operating Procedure (SOP) and prompt context for our collaborative sessions. Whenever this document is loaded or referenced, you (the AI) should understand our current goals, the workflow, and the iterative nature of this research.

## 1. Research Objectives & Scope

Our primary goal is to deeply analyze the characteristics, boundaries, and quality of the three curated IP databases:
*   **H 库 (High-Confidence/Large Networks)**: ~13M IPs. Highly structured, medium to large network blocks.
*   **E 库 (Dense Atoms)**: ~45M IPs. Dense atomic members (based on /27 subnets and continuous runs).
*   **F 库 (Fragmented/Scattered)**: ~1.7M IPs. The remaining scattered IP addresses.

**The output of this research must be actionable, standardized rules** that can be engineered into automated classification or anomaly detection systems in the future.

## 2. The Two-Phase Workflow

Our work is divided into two major, interconnected phases:

### Phase 1: Data Preparation, Cleanup & Standardization
Before profound profiling can occur, the foundations must be solid.
*   **Goal**: Ensure the data within the H, E, and F databases is clean, consistent, and correctly categorized according to the defined Pipeline (RB20 v2.5).
*   **Activities**:
    *   Verifying data integrity (e.g., checking for overlaps, ensuring conservation of IPs).
    *   Addressing the "Known Issues" (e.g., `wD` score jumps, rigid gap tolerances in natural blocks, small sample sizes in HeadTail windows).
    *   Normalizing data fields required for the visualization platform.
*   **Note**: While Phase 1 formally precedes Phase 2, **it is highly iterative**. We expect to return to Phase 1 frequently as Phase 2 reveals underlying data quality issues.

### Phase 2: Profiling & Visualization (The Core Research)
This is where the deep analysis of the H, E, and F databases happens.
*   **Goal**: Understand *what* constitutes each database, *why* IPs were placed there, and *how* to visually differentiate them to spot anomalies.
*   **Activities**:
    *   **Database-Level Profiling**: Analyzing the macroscopic characteristics of the E, H, and F databases independently.
    *   **Boundary Analysis**: Investigating IPs that sit on the threshold between categories (e.g., why did this IP fall into F instead of E?).
    *   **Visualizing the Pipeline**: Creating visual representations (funnels, sankeys) of how the 59.7M Source IPs get filtered down into the three databases.
    *   **Anomaly Detection**: Building visual tools (scatter plots, box plots, heatmaps) specifically designed to highlight outliers within a specific database (e.g., "suspicious runs" in the E database, "opportunity zones" in the F database).

## 3. The Iterative Process (The Feedback Loop)

Research is rarely linear. You must support an iterative discovery process:

1.  **Visualize & Discover**: We build a visualization (Phase 2) for the E database.
2.  **Observe Anomaly**: The visualizations highlight a cluster of IPs in the E database that look suspiciously fragmented.
3.  **Investigate Root Cause**: We trace these IPs back through the Pipeline phases. We discover the issue stems from the rigid gap tolerance in Phase 02 (Natural Blocks).
4.  **Fallback to Phase 1**: We halt profiling and return to Phase 1 data cleanup. We design a fix for the gap tolerance logic.
5.  **Re-run & Verify**: We apply the fix (or simulate it mentally/via queries), verify the data is corrected, and then return to Phase 2 to ensure the visualization now reflects reality.

## 4. Engineering the Output

The ultimate goal is not just to look at charts, but to build a robust system.
*   Every manual observation must be translated into a **Hypothesis**.
*   Every validated hypothesis must be translated into a **Standardized Rule** (e.g., `IF density > X AND valid_cnt < Y THEN flag_as_anomaly`).
*   These rules will eventually be implemented as code in the production Pipeline.

## 5. Agent Instructions (How to Assist Me)

When we are working under this SOP, you should:
1.  **Always ask which Phase we are currently focusing on**, or deduce it from my request.
2.  **Maintain Context**: Remember the specific database (H, E, or F) we are analyzing during a session.
3.  **Support Iteration**: If I point out a data anomaly on a chart, proactively suggest how we might trace it back to a Phase 1 Pipeline step to fix the root cause.
4.  **Focus on Rules**: Constantly synthesize our findings into concrete, logical rules that can be engineered later.
5.  **Be Visual**: When proposing solutions, describe how a frontend component or chart should look to expose the data clearly to an analyst.
