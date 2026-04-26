# CSG-2S-STPA

## 控制结构引导的两阶段大模型STPA方法 — AVP系统实验结果数据集

### 项目简介

本仓库发布论文《控制结构引导的两阶段大模型STPA方法》（投稿至《中国公路学报》）的实验结果数据，包括自动代客泊车（AVP）系统四类控制行为下，经人工专家审核后的不安全控制行为（UCA）识别结果与致因场景推演结果。

**论文方法概述：** CSG-2S-STPA 以控制结构为统一锚点，以单一控制行为为基本索引，将 UCA 识别和致因场景推演组织为前后衔接的两阶段结构化生成任务，并通过多维度质量门控、系统级语义归并和人工复核，形成从候选生成到可交付产物的闭环流程。

---

### 数据集说明

本仓库包含两类结果文件，共 32 个 JSON 文件（各 16 个）：

#### 阶段1：UCA 评测结果（`AVP_UCA_JUDGE_V8_FIX6_RESULTS/`）

针对 4 类控制行为 × 4 种生成方式（zero-shot、few-1-shot、few-3-shot、LoRA-SFT）的 UCA 候选条目评测结果，包含：
- 每条 UCA 的五维评分（A / B1 / B2 / B3 / B4）及评审理由
- 通过质量门控的可用条目（UC）
- 系统级语义归并后的保留条目（KC）
- 人工专家审核记录

**涉及控制行为：** 前进驱动（forward_drive）、车位确认（search_slot）、紧急制动（emergency_brake）、横向控制（lateral_control）

#### 阶段2：致因场景评测结果（`AVP_SCENARIO_JUDGE_STAGE2_RESULTS/`）

针对 4 类控制行为 × 4 种生成方式的致因场景候选条目评测结果，包含：
- 每条致因场景的五维评分（A / B1 / B2 / B3 / B4）及评审理由
- 通过质量门控的可用条目（UC）
- 系统级语义归并后的保留条目（KC）
- 人工专家审核记录

---

### 评价维度说明（表1）

| 任务阶段 | 维度 | 指标名称 | 含义说明 |
|---------|------|---------|---------|
| 阶段1（UCA） | A | 结构完整性 | 条目具备编号、类别、描述和危害链接，结构可解析 |
| 阶段1（UCA） | B1 | 上下文质量 | 描述与系统语境和控制行为语境一致 |
| 阶段1（UCA） | B2 | 控制行为忠实度 | 不安全形式明确围绕当前控制行为展开 |
| 阶段1（UCA） | B3 | 引导词一致性 | 描述可归入明确UCA语义族，并与类别标注一致 |
| 阶段1（UCA） | B4 | 危害链接一致性 | 危害链接来自已定义危害集合，且与描述内容一致 |
| 阶段2（致因场景） | A | 结构完整性 | 条目具备编号、描述与UCA链接，结构可解析 |
| 阶段2（致因场景） | B1 | 上下文质量 | 场景描述包含明确触发条件、状态或环境信息 |
| 阶段2（致因场景） | B2 | 因果链完整性 | 形成"触发条件—回路偏差—UCA形成"的基本链条 |
| 阶段2（致因场景） | B3 | 回路证据锚定 | 机制解释能够落在CA、PM或FB等控制回路证据 |
| 阶段2（致因场景） | B4 | UCA链接一致性 | UCA链接来自阶段1输出集合，且与场景机理一致 |

每个维度得分取值：0（不可用）、1（部分可用）、2（可用），满分10分。

---

### 主要实验结果

| 任务 | 方法 | UC（可用条目数） | KC（归并后保留数） | RR（保留率） |
|-----|------|--------------|----------------|------------|
| UCA生成 | LoRA-SFT | — | 最高（较次优+88.9%） | 最稳定 |
| 致因场景生成 | LoRA-SFT | — | 87条（较次优+93.3%） | 最稳定 |

人工专家审核：归并后282条条目整体保留率 **96.8%**，其中致因场景保留率 **100.0%**。

---

### 引用

如使用本数据集，请引用：

```
吕周杭, 吴晨辉, 贺胜, 梁茨. 控制结构引导的两阶段大模型STPA方法[J]. 中国公路学报（审稿中）.
```

---

### 基金支持

- 国家自然科学基金项目（52402493）
- 黑龙江省自然科学基金联合引导项目（LH2024E059）

---
---

## Control-Structure-Guided Two-Stage STPA Method Based on LLMs — AVP Experimental Results Dataset

### Overview

This repository releases the experimental result data from the paper *"Control-Structure-Guided Two-Stage STPA Method Based on Large Language Models"* (submitted to *China Journal of Highway and Transport*). The dataset covers four representative control actions of an Automated Valet Parking (AVP) system, providing post-review Unsafe Control Action (UCA) identification results and causal scenario reasoning results after manual expert review.

**Method summary:** CSG-2S-STPA takes the control structure as the unified anchor and a single control action as the basic analysis index. It organizes UCA identification and causal scenario reasoning into two sequential structured generation tasks, and forms a closed-loop pipeline from candidate generation to deliverable products through multidimensional quality gating, system-level semantic merging, and manual expert review.

---

### Dataset Description

This repository contains two sets of result files, totaling **32 JSON files** (16 per stage):

#### Stage 1: UCA Evaluation Results (`AVP_UCA_JUDGE_V8_FIX6_RESULTS/`)

Evaluation results for UCA candidates across 4 control actions × 4 generation methods (zero-shot, few-1-shot, few-3-shot, LoRA-SFT), including:
- Five-dimensional scores (A / B1 / B2 / B3 / B4) with rationales for each UCA
- Usable items after quality gating (UC)
- Retained items after system-level semantic merging (KC)
- Manual expert review records

**Control actions covered:** forward_drive, search_slot, emergency_brake, lateral_control

#### Stage 2: Causal Scenario Evaluation Results (`AVP_SCENARIO_JUDGE_STAGE2_RESULTS/`)

Evaluation results for causal scenario candidates across 4 control actions × 4 generation methods, including:
- Five-dimensional scores (A / B1 / B2 / B3 / B4) with rationales for each scenario
- Usable items after quality gating (UC)
- Retained items after system-level semantic merging (KC)
- Manual expert review records

---

### Evaluation Dimensions (Table 1)

| Task Stage | Dim. | Metric Name | Description |
|-----------|------|-------------|-------------|
| Stage 1 (UCA) | A | Structural Completeness | Item has ID, category, description, and hazard link; structure is parseable |
| Stage 1 (UCA) | B1 | Context Quality | Description is consistent with system context and control-action context |
| Stage 1 (UCA) | B2 | Control Action Fidelity | Unsafe form clearly revolves around the current control action |
| Stage 1 (UCA) | B3 | Guide-Word Consistency | Description maps to a clear UCA semantic category, consistent with category label |
| Stage 1 (UCA) | B4 | Hazard Link Consistency | Hazard link comes from the defined hazard set and is consistent with description |
| Stage 2 (Causal Scenario) | A | Structural Completeness | Item has ID, description, and UCA link; structure is parseable |
| Stage 2 (Causal Scenario) | B1 | Context Quality | Scenario description contains clear trigger conditions, state, or environmental info |
| Stage 2 (Causal Scenario) | B2 | Causal Chain Completeness | Forms the chain: "trigger condition → loop deviation → UCA formation" |
| Stage 2 (Causal Scenario) | B3 | Control Loop Evidence Anchoring | Mechanism explanation is grounded in CA, PM, or FB control loop evidence |
| Stage 2 (Causal Scenario) | B4 | UCA Link Consistency | UCA link comes from Stage 1 output set and is consistent with scenario mechanism |

Each dimension is scored 0 (unusable), 1 (partially usable), or 2 (usable). Maximum total score: 10.

---

### Key Results

| Task | Method | UC | KC | RR |
|------|--------|----|----|-----|
| UCA Generation | LoRA-SFT | — | Highest (+88.9% vs. 2nd-best) | Most stable |
| Causal Scenario Generation | LoRA-SFT | — | 87 items (+93.3% vs. 2nd-best) | Most stable |

Manual expert review: overall retention rate of 282 merged items = **96.8%**; causal scenario retention rate = **100.0%**.

---

### Citation

If you use this dataset, please cite:

```
LYU Zhouhang, WU Chenhui, HE Sheng, LIANG Ci. Control-Structure-Guided Two-Stage STPA Method Based on Large Language Models[J]. China Journal of Highway and Transport (under review).
```

---

### Funding

- National Natural Science Foundation of China (52402493)
- Heilongjiang Provincial Natural Science Foundation Joint Guidance Project (LH2024E059)
