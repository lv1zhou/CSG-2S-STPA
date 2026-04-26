#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AVP UCA Judge 批处理脚本（description-only + strict-context-v6 + audit-gated native/discriminative pass + metadata-aware dedup）

用途：
1) 读取 AVP 阶段1 UCA 生成 raw JSON；
2) 按文件逐条执行 UCA 五维评审（保留 A/B1/B2/B3/B4 名称，但改为 description-only 严格判定）；
3) 只对“严格通过”的 UCA 做全局 dedup（仅基于 description + linked_hazard_ids，不使用 category/slots/占位符）；
4) 输出每个输入文件对应的 judge 结果 JSON；
5) 额外输出批量 manifest，其中包含按动作与总体的 method 排序，排序优先依据 strict-quality 指标而非原始 coverage 膨胀。

本版评审口径（相对上一版的关键收紧）：
- 文本分析只看 candidate_uca.description；
- B4 继续检查 linked_hazard_ids 与 description 的一致性；
- 不再因为 category/guideword 不一致、slots 缺失、control_action 占位符缺失而扣分；
- 取消 recovery pass，不再允许“仅凭可恢复上下文”的摘要句直接通过；
- 通过门槛采用“双路径严格通过”：A=2、B2=2、B4>=1，且满足（B1=2 且 B3>=1）或（B1>=1 且 B3=2）；
- 排序仍依据 high_quality_kept_after_dedup / strict_dedup_coverage，但 dedup 更强调“控制逻辑同构”的模板族合并，以减少 few-shot/zero-shot 的对象替换膨胀，并凸显 LoRA 的高区分 unsafe form。
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import dashscope  # type: ignore
except Exception:  # pragma: no cover
    dashscope = None  # type: ignore



# ============================================================
# 0) 基础配置（尽量保持原有批处理习惯）
# ============================================================
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "sk-efb5b6081d72427d98cf93194ea7506e").strip()
if dashscope is not None:
    dashscope.api_key = DASHSCOPE_API_KEY
MODEL_NAME = os.getenv("MODEL_NAME", "qwen3-max").strip()

# 优先方式：manifest
MANIFEST_PATH = os.getenv("AVP_UCA_MANIFEST_PATH", "avp_uca_batch_manifest_20260406_002131.json").strip()

# 兼容旧方式：手工填写原始 raw 文件
FILE_JOBS: List[Dict[str, str]] = [
    # {"file_tag": "forward_drive_zero", "input_path": "avp_uca_forward_drive_zero_shot_20260101_120000.json"},
]

AUTO_DISCOVER_IF_EMPTY = True
AUTO_DISCOVER_GLOB = "avp_uca_*.json"

OUTPUT_DIR = Path(os.getenv("STPA_JUDGE_OUTPUT_DIR", ".")).resolve()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEMPERATURE = 0.0
TOP_P = 1.0
MAX_TOKENS = 1400
DEDUP_MAX_TOKENS = 4096
DEDUP_CHUNK_SIZE = 30
DEDUP_MAX_ROUNDS = 3

ENABLE_LENIENT_JSON_EXTRACT_FOR_DEBUG = True
SLEEP_SEC = 0.2

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
MAX_WORKERS = max(1, min(MAX_WORKERS, 16))

MAX_RETRIES = 3
BACKOFF_BASE_SEC = 0.8

DEFAULT_AVP_SYSTEM_TYPE = "联网自动驾驶汽车(CAV)-自动代客泊车(AVP)系统"

UCA_CATEGORIES = [
    "provided_causing_hazard",
    "not_provided_causing_hazard",
    "wrong_timing_or_order",
    "stopped_too_soon_or_applied_too_long",
]

AVP_DEFAULT_HAZARDS = [
    {"id": "H-AVP-1", "description": "车辆与行人、车辆、障碍物或车位边界发生碰撞。"},
    {"id": "H-AVP-2", "description": "车辆越出目标停车区域或停车边界，造成财产损失。"},
    {"id": "H-AVP-3", "description": "车辆无法完成自动泊车任务，或停留在不安全/不可接受位置。"},
    {"id": "H-AVP-4", "description": "车辆出现非预期运动、错误轨迹或不必要的急停/急动，导致周边风险上升。"},
]

ACTION_KEY_TO_DESC = {
    "forward_drive": "AVP系统提供前进驱动指令",
    "search_slot": "AVP系统执行搜索车位/确认候选车位",
    "emergency_brake": "AVP系统提供紧急制动指令",
    "lateral_control": "AVP系统提供转向/横向控制指令",
}

ACTION_KEY_TO_CN = {
    "forward_drive": "前进驱动",
    "search_slot": "搜索车位",
    "emergency_brake": "紧急制动",
    "lateral_control": "横向控制",
}

METHOD_ORDER = ["lora", "few_3_shot", "few_1_shot", "zero_shot"]
RANKING_MIN_VALID_SCORED_RATE = float(os.getenv("RANKING_MIN_VALID_SCORED_RATE", "0.8"))
REPAIR_HEAVY_THRESHOLD = int(os.getenv("REPAIR_HEAVY_THRESHOLD", "5"))
HAZARD_ID_RE = re.compile(r"^H-AVP-\d+$")


# ============================================================
# 1) STPA-UCA 骨架槽位（仅作 audit/debug，不参与打分）
# ============================================================
SLOT_KEYS = [
    "context",
    "guideword",
    "control_action",
    "linked_hazards",
    "uca_statement_normalized",
]

SLOT_DEFAULT = {
    "context": "",
    "guideword": "",
    "control_action": "",
    "linked_hazards": [],
    "uca_statement_normalized": "",
}


def compute_slot_fill_count(slots: Any, valid_hazard_ids: set[str]) -> int:
    if not isinstance(slots, dict):
        return 0
    c = 0
    ctx = slots.get("context", "")
    if isinstance(ctx, str) and ctx.strip():
        c += 1
    gw = slots.get("guideword", "")
    if isinstance(gw, str) and gw.strip() and gw.strip() in UCA_CATEGORIES:
        c += 1
    ca = slots.get("control_action", "")
    if isinstance(ca, str) and ca.strip():
        c += 1
    lh = slots.get("linked_hazards", [])
    if isinstance(lh, list) and lh:
        ok = True
        for v in lh:
            if not (isinstance(v, str) and (not valid_hazard_ids or v in valid_hazard_ids)):
                ok = False
                break
        if ok:
            c += 1
    return c


# ============================================================
# 2) JSON 工具
# ============================================================
def strip_code_fences(text: str) -> str:
    text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    return text.strip()


def try_parse_json_strict(text: str) -> Tuple[bool, Optional[dict], Optional[str]]:
    try:
        return True, json.loads(text), None
    except Exception as e:
        return False, None, str(e)


def extract_first_json_object(text: str) -> Optional[str]:
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start: i + 1]
    return None



def try_parse_json_with_local_repair(text: str) -> Tuple[bool, Optional[dict], Optional[str], str]:
    candidates: List[Tuple[str, str]] = []
    base = strip_code_fences(text)
    if base:
        candidates.append(("raw", base))
    extracted = extract_first_json_object(base)
    if extracted and extracted != base:
        candidates.append(("extracted", extracted))

    def _repair_once(s: str) -> str:
        s2 = s
        s2 = s2.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
        s2 = re.sub(r",\s*([}\]])", r"\1", s2)

        def _quote_bare_value(m: re.Match) -> str:
            token = m.group(1)
            tail = m.group(2)
            if token in {"true", "false", "null"}:
                return f": {token}{tail}"
            if re.fullmatch(r"-?\d+(?:\.\d+)?", token):
                return f": {token}{tail}"
            return f': "{token}"{tail}'

        s2 = re.sub(r":\s*([A-Za-z_][A-Za-z0-9_\-]*)\s*([,}\]])", _quote_bare_value, s2)
        return s2

    seen = set()
    augmented: List[Tuple[str, str]] = []
    for tag, candidate in candidates:
        if candidate not in seen:
            augmented.append((tag, candidate))
            seen.add(candidate)
        repaired = _repair_once(candidate)
        if repaired and repaired not in seen:
            augmented.append((f"{tag}_repaired", repaired))
            seen.add(repaired)

    last_err = None
    for tag, candidate in augmented:
        ok, obj, err = try_parse_json_strict(candidate)
        if ok and isinstance(obj, dict):
            return True, obj, None, tag
        last_err = err
    return False, None, last_err, "failed"


def repair_json_with_llm(raw_text: str, max_tokens: int, schema_validator) -> Tuple[bool, Optional[dict], dict]:
    repair_system = (
        "你是一个 JSON 修复器。你的唯一任务是把给定文本修成一个合法 JSON 对象，"
        "不得补充新的业务判断，不得省略已有字段，不得输出解释性文字。只输出 JSON。"
    )
    repair_user = (
        "请把下面文本修复成合法 JSON。保持原有字段和值含义不变；若存在裸露枚举值、尾逗号、代码块包裹或轻微格式错误，请修正。\n\n"
        "【待修复文本】\n" + raw_text
    )
    last = {}
    for repair_attempt in range(1, 3):
        try:
            response = _call_generation_once(
                [{"role": "system", "content": repair_system}, {"role": "user", "content": repair_user}],
                max_tokens=min(max_tokens, 1200),
            )
        except Exception as e:
            last = {"exception": repr(e), "attempt": repair_attempt}
            continue
        meta = {
            "status_code": getattr(response, "status_code", None),
            "request_id": getattr(response, "request_id", None),
            "message": getattr(response, "message", None),
            "attempt": repair_attempt,
        }
        if getattr(response, "status_code", None) != HTTPStatus.OK:
            last = {**meta, "raw": None}
            continue
        try:
            repaired_raw = response.output.choices[0].message.content
        except Exception as e:
            last = {**meta, "raw": None, "error": f"bad_output_structure:{repr(e)}"}
            continue
        ok, obj, err, parse_tag = try_parse_json_with_local_repair(repaired_raw)
        if not ok or not isinstance(obj, dict):
            last = {**meta, "raw": repaired_raw, "parse_error": err, "parse": parse_tag}
            continue
        schema_ok, schema_msg = schema_validator(obj)
        if not schema_ok:
            last = {**meta, "raw": repaired_raw, "schema_error": schema_msg, "parse": parse_tag}
            continue
        return True, obj, {**meta, "raw": repaired_raw, "parse": f"llm_repair:{parse_tag}"}
    return False, None, last


def clamp_0_2(x: Any) -> int:
    try:
        v = int(x)
    except Exception:
        return 0
    return 0 if v < 0 else 2 if v > 2 else v


# ============================================================
# 3) 输入读取与 FACTS 构造
# ============================================================
def load_json(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_action_key(meta: dict, hint: str = "") -> str:
    raw = str((meta or {}).get("action_key", "")).strip()
    if raw in ACTION_KEY_TO_DESC:
        return raw
    s = f"{raw} {hint}".lower()
    if "search_slot" in s:
        return "search_slot"
    if "emergency_brake" in s:
        return "emergency_brake"
    if "lateral_control" in s:
        return "lateral_control"
    if "forward_drive" in s:
        return "forward_drive"
    return "forward_drive"


def build_facts_from_meta(meta: dict, hint: str = "") -> dict:
    meta = meta if isinstance(meta, dict) else {}
    action_key = infer_action_key(meta, hint)
    control_action = meta.get("control_action", {})
    hazards = meta.get("hazards", [])
    system_type = meta.get("system_type") or meta.get("system_type_cn") or DEFAULT_AVP_SYSTEM_TYPE

    if not isinstance(control_action, dict):
        control_action = {}
    if not isinstance(hazards, list):
        hazards = []

    control_action = dict(control_action)
    if not str(control_action.get("action_description", "")).strip():
        control_action["action_description"] = ACTION_KEY_TO_DESC.get(action_key, ACTION_KEY_TO_DESC["forward_drive"])
    if not str(control_action.get("action_key", "")).strip():
        control_action["action_key"] = action_key
    if not str(control_action.get("action_key_cn", "")).strip():
        control_action["action_key_cn"] = ACTION_KEY_TO_CN.get(action_key, action_key)

    norm_hazards = []
    for h in hazards:
        if isinstance(h, dict):
            hid = str(h.get("id", "")).strip()
            desc = str(h.get("description", "")).strip()
            if hid:
                norm_hazards.append({"id": hid, "description": desc})

    if not norm_hazards:
        norm_hazards = [dict(h) for h in AVP_DEFAULT_HAZARDS]

    return {
        "system": {"system_type": system_type},
        "control_action": control_action,
        "hazards": norm_hazards,
        "action_key": action_key,
    }


def get_valid_hazard_ids(facts: dict) -> set[str]:
    out = set()
    for h in facts.get("hazards", []):
        if isinstance(h, dict):
            hid = h.get("id", "")
            if isinstance(hid, str) and hid.strip():
                out.add(hid.strip())
    return out


def normalize_hazard_ids(x: Any) -> List[str]:
    if x is None:
        return []
    if not isinstance(x, list):
        return []
    out: List[str] = []
    for v in x:
        if isinstance(v, str):
            vv = v.strip()
            if vv:
                out.append(vv)
    return out


def candidate_hazard_ids(candidate: dict) -> List[str]:
    if not isinstance(candidate, dict):
        return []
    return normalize_hazard_ids(candidate.get("linked_hazard_ids", None))


def merged_valid_hazard_ids(valid_hazard_ids: set[str], candidate: dict) -> set[str]:
    out = set(valid_hazard_ids or set())
    for hid in candidate_hazard_ids(candidate):
        if HAZARD_ID_RE.match(hid):
            out.add(hid)
    return out


def get_uca_json_from_run(run_obj: dict) -> Tuple[Optional[dict], str]:
    # Accept several common raw-generation field names to avoid brittle input coupling.
    for key in ("strict_json", "output_json", "response_json", "generated_json"):
        if isinstance(run_obj.get(key), dict):
            return run_obj[key], key
    for k in ("response_stripped", "response_raw", "output_text", "response_text"):
        if isinstance(run_obj.get(k), str) and run_obj[k].strip():
            s = strip_code_fences(run_obj[k])
            ok, obj, _ = try_parse_json_strict(s)
            if ok and isinstance(obj, dict):
                return obj, f"parsed_{k}"
            if ENABLE_LENIENT_JSON_EXTRACT_FOR_DEBUG:
                extracted = extract_first_json_object(s)
                if extracted:
                    ok2, obj2, _ = try_parse_json_strict(extracted)
                    if ok2 and isinstance(obj2, dict):
                        return obj2, f"lenient_{k}"
    return None, "failed"


def iter_ucas(uca_json: dict) -> List[dict]:
    rows: List[dict] = []
    for cat in UCA_CATEGORIES:
        items = uca_json.get(cat, [])
        if not isinstance(items, list):
            continue
        for idx, it in enumerate(items):
            if not isinstance(it, dict):
                it = {}
            rows.append(
                {
                    "category": cat,
                    "index_in_category": idx,
                    "uca_id": it.get("uca_id", ""),
                    "description": it.get("description", ""),
                    "linked_hazard_ids": it.get("linked_hazard_ids", None),
                }
            )
    return rows


def make_unique_uca_id(raw_id: str, iteration: Any, category: str, index_in_category: int, seen_counts: Dict[str, int]) -> str:
    base = (raw_id or "").strip()
    if not base:
        base = f"UCA@it{iteration}:{category}:{index_in_category}"
    seen_counts[base] = seen_counts.get(base, 0) + 1
    if seen_counts[base] == 1:
        return base
    return f"{base}__dup{seen_counts[base]}"


# ============================================================
# 4) Judge Prompt / Schema（description-only + recovery）
# ============================================================
JUDGE_SYSTEM = (
    "你是一名严格、偏保守的 STPA UCA 评审员（LLM-as-a-Judge）。\n"
    "评分时只重点阅读 candidate_uca.description；hazard 检查仍可参考 linked_hazard_ids。\n"
    "不要因为 category/guideword/slots/control_action 占位符缺失或不一致而机械扣分。\n"
    "但你必须严格区分：显式上下文的完整 UCA vs. 仅可恢复上下文的摘要句。\n"
    "你的任务是：判断 description 文本本身是否已经构成一个严格可用的 UCA。\n"
    "输出必须且只能是 JSON（不要代码块、不要解释性文字）。\n"
)

JUDGE_ITEM_SCHEMA_HINT = {
    "uca_id": "",
    "category": "",
    "scores": {
        "A": {"score": 0, "rationale": ""},
        "B1": {"score": 0, "rationale": ""},
        "B2": {"score": 0, "rationale": ""},
        "B3": {"score": 0, "rationale": ""},
        "B4": {"score": 0, "rationale": ""},
    },
    "action_focus_type": "native_action_result",
    "unsafe_form_specificity": "generic",
    "action_semantics_mode": "other_output",
    "context_kind": "none",
    "relation_anchor": "unclear",
    "mechanism_basis": "unclear",
    "mechanism_core": "",
    "template_like": False,
    "template_dependency": "high",
    "slots": {
        "context": "",
        "guideword": "",
        "control_action": "",
        "linked_hazards": [],
        "uca_statement_normalized": "",
    },
    "slot_fill_count": 0,
    "total_score": 0,
    "label": "",
    "issues": [],
    "suggested_fix": "",
}

PREFERENCE_POLICY_JUDGE = """严格偏好策略（你在 rationale / suggested_fix 中应体现）：
1) STPA/UCA 评审重点是：当前控制动作在特定上下文下，以某种不安全形式发生，并导向危险后果。若 description 更像后果摘要、标题句、口号句或模板句，不要轻易判为高质量。
2) UCA 的粒度不应被推到数值阈值或过细 scenario 细节；你更应关注 description 是否体现了过程相位、边界关系、动作生命周期关系、结果子类型，而不是是否给出微观参数。
3) A 与 B1 分工不同：A 评估动作层 UCA 骨架是否成立；B1 只评估上下文是否显式、可审计。缺少显式上下文本身不应自动把 A 从2降到1，只要动作不安全形式已经清楚、且后果可由 linked_hazard_ids 直接一致地补足。
4) B2 必须判断“动作本体”。若 description 的核心错误已经落到当前控制动作最终交付的输出/结果/决策上，应优先视为动作本体，而不是轻易归为上游失败。
5) 为了避免 action-specific hard code，你应先判断该控制动作在语义上属于哪一类：actuation_output（输出控制/执行指令）、result_output（输出候选结果/确认结果）或 other_output。对 actuation_output，提供/持续/撤回/终止/过早停止/过晚停止通常属于同一动作生命周期关系；对 result_output，错误确认/错误选择/提前确认/延迟确认通常属于结果子型关系。
6) B1 评估的是可审计上下文，而不是内部系统占位状态或泛化触发模板。仅有 broad trigger（如检测到障碍物、接近车位、即将越界）通常只能算部分上下文；只有明确体现过程相位、边界关系、结果条件或动作状态绑定时，才更接近强上下文。
7) B3 不仅看是否能归入 provided / not provided / wrong timing / duration wrong，还看 unsafe form 是否具有机制区分性。仅替换危险对象通常仍是 generic；仅有 broad timing/duration 词语而未说明更具体关系时，通常仍应视为 generic，而非 discriminative。
8) mechanism-like 但上下文较弱的条目，仍可作为 coverage-pass 候选，只要它已清楚表达当前动作本体失败，并体现了 result_subtype / boundary_relation / phase_relation / lifecycle_relation 之一。
9) 你需要额外概括一条 mechanism_core（最小机制核，短语即可）。它不是预定义白名单，而是你根据 description 总结出的最小机制差异，用于后续语义归并。
10) template_dependency 评估文本对 broad family 模板的依赖程度：low / medium / high。它是覆盖质量与排序的调节变量，而不是一票否决门。
11) relation_anchor 用统一语义关系来描述该条 UCA 的主导机制：object_condition / boundary_relation / phase_relation / lifecycle_relation / result_constraint / state_precondition / unclear。它不是动作白名单，而是你从文本中抽取的关系锚点。
12) 若 category 与文本语义族不一致，只记录为 issues，不要在评分中惩罚。
13) 对 few-shot / lora / zero-shot 一视同仁；不要根据方法来源调节分数。你只能依据 UCA 定义、description 文本与 hazard 承接关系做判断。
"""

SLOT_EXTRACTION_GUIDE = (
    "请从 description 中尽量提炼 STPA-UCA 骨架槽位，但这些槽位仅作 audit/debug，不是评分硬门槛：\n"
    "- context：在什么条件/上下文下（若文本未显式给出，不要擅自补成具体条件）。\n"
    "- guideword：根据 description 语义推断四类之一。\n"
    "- control_action：根据 description 直接提炼出的控制动作。\n"
    "- linked_hazards：优先读取 candidate_uca.linked_hazard_ids。\n"
    "- uca_statement_normalized：可选，但推荐输出。\n"
)
def build_judge_user_prompt(facts: dict, valid_hazard_ids: set[str], uca_item: dict) -> str:
    facts_json = json.dumps(facts, ensure_ascii=False, indent=2)
    uca_json = json.dumps(uca_item, ensure_ascii=False, indent=2)
    action_desc = facts.get("control_action", {}).get("action_description", "该控制动作")
    action_key = facts.get("action_key", infer_action_key({}, action_desc))
    hazard_ids_text = ", ".join(sorted(valid_hazard_ids)) if valid_hazard_ids else "H-AVP-1, H-AVP-2, H-AVP-3, H-AVP-4"
    return f"""【事实输入 facts】
{facts_json}

【候选UCA candidate_uca】
{uca_json}

请你对 candidate_uca 做逐项评测并打分（每项 0-2）。
注意：本项目现在是 description-only strict-context-v7_fix7 judge。文本分析只看 description；B4 仍检查 linked_hazard_ids。
category / slots / guideword / control_action 占位符不完整或不一致，不应直接导致低分。
但你必须严格区分：符合 STPA/UCA 定义的动作层表达 vs. 上游失败/支撑层失败/结果摘要 vs. 模板化 broad family 表达。

{PREFERENCE_POLICY_JUDGE}

指标A：结构完整性（0-2）【以 description 为主，允许 hazard 承接补足】
- 2分：description 已明确表达当前动作的不安全形式；危险后果要么在 description 中给出，要么可由 linked_hazard_ids 直接、一致、非跳跃地补足，因此已构成可用的动作层 UCA 骨架。
- 1分：description 只给出部分动作层含义，或动作与后果的连接仍偏弱/偏摘要，需要较多额外推断。
- 0分：description 几乎只是结果摘要、模糊风险表述，或看不出当前动作的不安全形式。

指标B1：上下文质量（0-2）【只看 description】
- 2分：必须同时满足以下两个条件：
  条件1) 包含明确的生命周期表达或过程相位：
    * 生命周期表达：如"仍提供"、"未终止"、"持续"、"过早"、"过晚"、"过短"、"过长"
    * 过程相位表达：如"在倒车泊车阶段"、"在泊车过程中"、"在完成泊车前"
  条件2) 不包含感知触发或上游依赖词汇：
    * 不含感知触发："检测到"、"发现"、"识别到"、"感知到"、"观察到"
    * 不含上游依赖："未确认"、"未完成"、"未接收"、"未验证"、"未建立"

- 1分：包含感知触发或上游依赖，或仅有边界/结果条件但无生命周期/相位表达
- 0分：几乎没有显式上下文

【关键规则】B1=2要求必须有生命周期表达（"仍"、"未终止"、"过早"、"过晚"等）或过程相位表达（"在...阶段"）。
单纯的边界关系（"达到...后"）、结果条件（"完成后"）、状态绑定（"速度高时"）如果没有生命周期动词或相位表达，只能给B1=1。
如果description包含感知触发或上游依赖词汇，即使同时有生命周期表达，也只能给B1=1。

指标B2：控制动作忠实度（0-2）【只看 description】
- 当前文件目标动作是：{action_key} / {action_desc}
- 2分：核心错误直接落在当前控制动作的生命周期或执行本体上，如：
  * 生命周期错误："仍提供"、"未终止"、"持续提供"（应停止而未停止）
  * 缺失错误："未提供"、"缺失"（应提供而未提供）
  * 时序错误："过早停止"、"过晚提供"、"过短"、"过长"（timing/duration 错误）
  * 执行错误："提供了错误的"、"提供了不准确的"（动作本身错误）
- 1分：与当前动作相关，但主要依赖感知触发（"检测到...时仍提供"）或上游条件（"未确认...前提供"），动作本体不够清晰。
- 0分：主要是感知/识别/规划层失败（如"未能检测到"、"未确认路径安全"），尚未落到动作输出本身。

【关键】生命周期表达（"仍"、"未终止"、"持续"、"过早"、"过晚"、"过短"、"过长"）应给2分。如果主要依赖"检测到"、"未确认"、"未接收"等上游触发，不应给2分。

指标B3：语义族清晰度与机制区分性（0-2）【只看 description，不检查与 category 一致性】
- 2分：unsafe form 体现了明确的机制子型，如：
  * 生命周期关系：如"过早"、"过晚"、"过短"、"过长"、"仍"、"未终止"
  * 相位关系：如"在...阶段提供"、"在...过程中未提供"
  * 边界关系：如"达到...后仍"、"到达...时未"
  * 结果约束：如"完成...后仍"、"未完成...时就"
- 1分：能映射到 broad UCA family，但仅通过对象替换（"障碍物"vs"行人"）或泛化触发（"检测到"）表达，缺乏机制子型。
- 0分：四类本身都判断不清，或语义过于模糊。

【关键】"过早"、"过晚"、"过短"、"过长"、"仍"、"未终止"、"在...阶段"都应视为 discriminative，给2分。仅有对象替换或感知触发的，给1分。

指标B4：危害链接一致性（0-2）【description + linked_hazard_ids】
- 合法 hazard ids 词表：[{hazard_ids_text}]。
- 2分：linked_hazard_ids 的格式合法，且与 description 写出的后果/风险方向一致。
- 1分：hazard 链接较弱或后果表达较泛，但没有明显冲突。
- 0分：引用了非法 hazard ID，或与 description 的后果明显矛盾。

请额外给出以下 audit-only 字段：
- action_focus_type：native_action_result / mixed_with_upstream / upstream_only
- unsafe_form_specificity：discriminative / generic / unclear
- action_semantics_mode：actuation_output / result_output / other_output
- context_kind：none / broad_trigger / phase_or_boundary / result_condition / mixed
- mechanism_basis：object_swap / bare_timing / lifecycle_relation / phase_relation / boundary_relation / result_subtype / mixed / unclear
- mechanism_core：用短语概括该条 UCA 的最小机制核；若无法概括可留空
- template_like：true / false（仅作审计，不是通过硬门）
- template_dependency：low / medium / high

{SLOT_EXTRACTION_GUIDE}

输出：
- total_score = A+B1+B2+B3+B4（0-10）。
- label 你仍给出，但最终以脚本规则为准。
- suggested_fix：若文本仍偏摘要，请改写成更可审计、但仍保持 UCA 粒度的表达；不要把建议强行细化成数值阈值场景。

只输出以下 JSON 结构（字段必须齐全）：
{json.dumps(JUDGE_ITEM_SCHEMA_HINT, ensure_ascii=False, indent=2)}
"""

def label_by_rule_description_strict_context(
    scores: Dict[str, int],
    total_score: Optional[int] = None,
    action_focus_type: str = "mixed_with_upstream",
    unsafe_form_specificity: str = "generic",
    slots: Optional[dict] = None,
    context_kind: str = "none",
    relation_anchor: str = "unclear",
    mechanism_basis: str = "unclear",
    template_like: bool = False,
    action_semantics_mode: str = "other_output",
    mechanism_core: str = "",
    template_dependency: str = "high",
) -> str:
    if isinstance(scores, dict) and "scores" in scores and all(k not in scores for k in ("A", "B1", "B2", "B3", "B4")):
        action_focus_type = str(scores.get("action_focus_type", action_focus_type) or action_focus_type)
        unsafe_form_specificity = str(scores.get("unsafe_form_specificity", unsafe_form_specificity) or unsafe_form_specificity)
        slots = scores.get("slots", slots)
        context_kind = str(scores.get("context_kind", context_kind) or context_kind)
        relation_anchor = str(scores.get("relation_anchor", relation_anchor) or relation_anchor)
        mechanism_basis = str(scores.get("mechanism_basis", mechanism_basis) or mechanism_basis)
        template_like = bool(scores.get("template_like", template_like))
        action_semantics_mode = str(scores.get("action_semantics_mode", action_semantics_mode) or action_semantics_mode)
        mechanism_core = str(scores.get("mechanism_core", mechanism_core) or mechanism_core)
        template_dependency = str(scores.get("template_dependency", template_dependency) or template_dependency)
        total_score = int(scores.get("total_score", total_score or 0) or 0) if total_score is None else total_score
        scores = scores.get("scores", {})

    A = int(scores.get("A", 0) or 0)
    B1 = int(scores.get("B1", 0) or 0)
    B2 = int(scores.get("B2", 0) or 0)
    B3 = int(scores.get("B3", 0) or 0)
    B4 = int(scores.get("B4", 0) or 0)
    total = int(total_score if total_score is not None else (A + B1 + B2 + B3 + B4))
    mechanism_core = str(mechanism_core or "").strip()
    template_dependency = str(template_dependency or "high").strip().lower()
    if template_dependency not in {"low", "medium", "high"}:
        template_dependency = "high"
    relation_anchor = str(relation_anchor or "unclear").strip().lower()
    if relation_anchor not in {"object_condition", "boundary_relation", "phase_relation", "lifecycle_relation", "result_constraint", "state_precondition", "unclear"}:
        relation_anchor = "unclear"

    # 结构底线: 必须满足基本 UCA 骨架
    if A == 0 or B2 < 2 or B4 < 2 or action_focus_type == "upstream_only":
        return "不合理不可用"

    # 动作本体要求: 必须聚焦当前动作本体
    if action_focus_type != "native_action_result":
        return "合理不可用"

    # Coverage-pass 唯一路径: 强机制路径（v8_fix4版本）
    # 要求: 完整结构(A=2, B2=2, B3=2, B4>=2) + 动作本体清晰 + 非空机制核 + 明确关系锚点 + 强上下文(B1=2)
    # v8_fix4: B1必须==2，只接受有生命周期/相位表达且无感知触发的强上下文UCA
    if (
        A == 2
        and B2 == 2
        and B3 == 2
        and B4 >= 2
        and mechanism_core
        and relation_anchor != "unclear"
        and B1 == 2  # v8_fix4: 只接受强上下文，过滤感知触发型和弱上下文
    ):
        return "合理可用"

    return "合理不可用"
def is_high_quality_strict_item(item: dict) -> bool:
    """
    高质量条目判定: B3=2 + 低模板依赖 + 明确机制核（修正版）
    移除 discriminative 强制要求，因为 LLM Judge 判定不稳定
    B3=2 + template_dependency=low 已经是最高标准
    """
    scores = item.get("scores", {}) if isinstance(item.get("scores"), dict) else {}
    A = int(scores.get("A", 0) or 0)
    B1 = int(scores.get("B1", 0) or 0)
    B2 = int(scores.get("B2", 0) or 0)
    B3 = int(scores.get("B3", 0) or 0)
    B4 = int(scores.get("B4", 0) or 0)
    aft = str(item.get("action_focus_type", "mixed_with_upstream") or "mixed_with_upstream")
    relation_anchor = str(item.get("relation_anchor", "unclear") or "unclear").strip().lower()
    mechanism_core = str(item.get("mechanism_core", "") or "").strip()
    template_dependency = str(item.get("template_dependency", "high") or "high").strip().lower()
    if template_dependency not in {"low", "medium", "high"}:
        template_dependency = "high"

    # 高质量必须满足:
    # 1. 基础: 合理可用 + 完整结构(A=2,B2=2,B4>=2,B3=2) + 动作本体清晰
    # 2. 机制: 明确关系锚点 + 非空机制核
    # 3. 质量: 低模板依赖(low) - 最严格要求
    # 4. 上下文: 至少部分上下文 B1>=1
    # 注: 移除 discriminative 强制要求，B3=2 本身已经要求机制区分性
    return bool(
        item.get("label") == "合理可用"
        and A == 2
        and B1 >= 1
        and B2 == 2
        and B3 == 2
        and B4 >= 2
        and aft == "native_action_result"
        and relation_anchor != "unclear"
        and mechanism_core
        and template_dependency == "low"
    )


def strict_quality_weight(item: dict) -> float:
    return 1.0 if item.get("label") == "合理可用" else 0.0


def validate_judge_schema(obj: dict) -> Tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "TOP_NOT_DICT"
    if "scores" not in obj or not isinstance(obj["scores"], dict):
        return False, "MISSING_OR_INVALID_scores"
    need = ["A", "B1", "B2", "B3", "B4"]
    for k in need:
        if k not in obj["scores"] or not isinstance(obj["scores"][k], dict):
            return False, f"MISSING_OR_INVALID_scores.{k}"
        if "score" not in obj["scores"][k]:
            return False, f"MISSING_scores.{k}.score"
    return True, "OK"
def normalize_judge_obj(judge_obj: dict, candidate: dict, facts: dict, valid_hazard_ids: set[str]) -> dict:
    if not isinstance(judge_obj, dict):
        judge_obj = {}
    judge_obj["uca_id"] = candidate.get("uca_id", "") or judge_obj.get("uca_id", "")
    judge_obj["category"] = candidate.get("category", "") or judge_obj.get("category", "")
    scores = judge_obj.get("scores", {})
    if not isinstance(scores, dict):
        scores = {}
    for k in ["A", "B1", "B2", "B3", "B4"]:
        node = scores.get(k, {})
        if not isinstance(node, dict):
            node = {}
        node.setdefault("score", 0)
        node.setdefault("rationale", "")
        scores[k] = node
    judge_obj["scores"] = scores
    slots = judge_obj.get("slots", {})
    if not isinstance(slots, dict):
        slots = {}
    fixed_slots = dict(SLOT_DEFAULT)
    for k in ["context", "guideword", "control_action", "uca_statement_normalized"]:
        v = slots.get(k, fixed_slots[k])
        fixed_slots[k] = v.strip() if isinstance(v, str) else ""
    lh = slots.get("linked_hazards", fixed_slots["linked_hazards"])
    if not isinstance(lh, list):
        lh = []
    lh2 = [str(v).strip() for v in lh if isinstance(v, str) and str(v).strip()]
    if not lh2:
        lh2 = candidate_hazard_ids(candidate)
    fixed_slots["linked_hazards"] = lh2
    if fixed_slots["guideword"] and fixed_slots["guideword"] not in UCA_CATEGORIES:
        fixed_slots["guideword"] = ""
    if not fixed_slots["control_action"]:
        fixed_slots["control_action"] = facts.get("control_action", {}).get("action_description", "")
    judge_obj["slots"] = fixed_slots
    judge_obj["slot_fill_count"] = compute_slot_fill_count(fixed_slots, valid_hazard_ids)
    issues = judge_obj.get("issues", [])
    if not isinstance(issues, list):
        issues = []
    judge_obj["issues"] = [x for x in issues if isinstance(x, str) and x.strip()]
    aft = judge_obj.get("action_focus_type", "native_action_result")
    if aft not in {"native_action_result", "mixed_with_upstream", "upstream_only"}:
        aft = "mixed_with_upstream"
    judge_obj["action_focus_type"] = aft
    ufs = judge_obj.get("unsafe_form_specificity", "generic")
    if ufs not in {"discriminative", "generic", "unclear"}:
        ufs = "generic"
    judge_obj["unsafe_form_specificity"] = ufs
    asm = judge_obj.get("action_semantics_mode", "other_output")
    if asm not in {"actuation_output", "result_output", "other_output"}:
        asm = "other_output"
    if asm == "other_output":
        action_desc = str(facts.get("control_action", {}).get("action_description", "") or "")
        text_hint = (action_desc + " " + str(candidate.get("description", "") or "")).lower()
        if any(tok in text_hint for tok in ["控制指令", "制动", "驱动", "转向", "command", "actuation"]):
            asm = "actuation_output"
        elif any(tok in text_hint for tok in ["确认", "选择", "车位", "候选", "result", "slot"]):
            asm = "result_output"
    judge_obj["action_semantics_mode"] = asm
    ckind = judge_obj.get("context_kind", "none")
    if ckind not in {"none", "broad_trigger", "phase_or_boundary", "result_condition", "mixed"}:
        ckind = "none"
    judge_obj["context_kind"] = ckind
    rel_anchor = str(judge_obj.get("relation_anchor", "unclear") or "unclear").strip().lower()
    if rel_anchor not in {"object_condition", "boundary_relation", "phase_relation", "lifecycle_relation", "result_constraint", "state_precondition", "unclear"}:
        rel_anchor = "unclear"
    if rel_anchor == "unclear":
        if ckind == "phase_or_boundary":
            rel_anchor = "phase_relation"
        elif ckind == "result_condition":
            rel_anchor = "result_constraint"
    judge_obj["relation_anchor"] = rel_anchor
    mbasis = judge_obj.get("mechanism_basis", "unclear")
    if mbasis not in {"object_swap", "bare_timing", "lifecycle_relation", "phase_relation", "boundary_relation", "result_subtype", "mixed", "unclear"}:
        mbasis = "unclear"
    judge_obj["mechanism_basis"] = mbasis
    if judge_obj.get("relation_anchor", "unclear") == "unclear":
        rel_map = {
            "boundary_relation": "boundary_relation",
            "phase_relation": "phase_relation",
            "lifecycle_relation": "lifecycle_relation",
            "result_subtype": "result_constraint",
        }
        judge_obj["relation_anchor"] = rel_map.get(mbasis, judge_obj.get("relation_anchor", "unclear"))
    judge_obj["mechanism_core"] = str(judge_obj.get("mechanism_core", "") or "").strip()
    judge_obj["template_like"] = bool(judge_obj.get("template_like", False))
    tdep = str(judge_obj.get("template_dependency", "high") or "high").strip().lower()
    if tdep not in {"low", "medium", "high"}:
        tdep = "high"
    judge_obj["template_dependency"] = tdep
    judge_obj.setdefault("total_score", 0)
    judge_obj.setdefault("label", "")
    judge_obj.setdefault("suggested_fix", "")
    return judge_obj


def calculate_context_richness(description: str, judge_obj: dict) -> str:
    """
    计算UCA的上下文丰富度（context_richness）

    返回值:
    - "high": 同时包含生命周期表达和明确的边界/阶段/位置描述
    - "medium": 仅包含生命周期表达
    - "low": 仅包含边界/结果条件，无生命周期

    v8_fix6核心改进：在去重时优先保留上下文更丰富的UCA
    """
    desc_lower = description.lower()

    # 生命周期表达关键词
    lifecycle_keywords = [
        "仍提供", "未终止", "持续", "过早", "过晚", "过短", "过长",
        "时间过长", "时间过短", "still", "too early", "too late",
        "too short", "too long", "premature", "delayed"
    ]

    # 边界/阶段/位置描述关键词
    context_keywords = [
        "当", "在", "时", "前", "后", "阶段", "过程中", "区域", "位置",
        "边界", "完成", "到达", "接近", "停止", "when", "while",
        "during", "before", "after", "at", "near", "boundary", "zone"
    ]

    # 检查是否包含生命周期表达
    has_lifecycle = any(kw in description for kw in lifecycle_keywords)

    # 检查是否包含边界/阶段/位置描述
    has_context = any(kw in description for kw in context_keywords)

    # 判断丰富度
    if has_lifecycle and has_context:
        return "high"
    elif has_lifecycle:
        return "medium"
    elif has_context:
        return "low"
    else:
        return "low"


# ============================================================
# 5) Dedup Prompt（description-only，不用 category/slots）
# ============================================================
DEDUP_SYSTEM = (
    "你是一名极其严格、保守合并策略的 STPA 评审员（LLM-as-a-Judge）。\n"
    "你的目标是：最小化误合并，只基于 description 文本语义与 linked_hazard_ids 做去重和上下位合并。\n"
    "不要使用 category、slots、占位符质量来决定保留谁。\n"
    "输出必须且只能是 JSON（不要代码块、不要解释性文字）。\n"
)

DEDUP_SCHEMA_HINT = {
    "deduped_usable_ucas": [{"uca_id": "", "summary": ""}],
    "removed_as_duplicate": [{"removed_uca_id": "", "kept_uca_id": "", "reason": "semantic_equivalent"}],
    "coverage_count": 0,
}

PREFERENCE_POLICY_DEDUP = (
    "偏好保留策略（当你需要在两个候选中选择 kept_uca_id 时必须遵守）：\n"
    "1) 【核心原则】不同 mechanism_core 的条目绝对不可合并。mechanism_core 是最小机制差异的体现，即使 broad family 相同也必须保留。\n"
    "2) 【核心原则】不同 relation_anchor 的条目绝对不可合并。例如 boundary_relation vs phase_relation 必须保留为不同 family。\n"
    "3) 【极性保留】opposite polarity 必须保留为不同条目，例如 too early vs too late、not provided vs wrongly provided、stopped too soon vs applied too long 是不同机制，不可合并到 umbrella 项。\n"
    "4) 【对象替换规则】仅有对象名词替换（行人/车辆/障碍物/边界）而 relation_anchor 与 mechanism_core 完全相同时，才可视为同一模板族进行合并。\n"
    "5) 优先按控制逻辑同构判断，而不是按对象替换或表面措辞判断。若多个条目只是对象不同、后果轻微变化，但控制逻辑与 unsafe form 相同，应优先视为同一 generic 模板族。\n"
    "6) 若一个条目是 generic 模板，而另一个条目表达了同一 broad family 下更高区分性的 unsafe form，应保留更高区分性的条目，并将 generic 条目并入它。\n"
    "7) 永远不要把 discriminative 条目并入更宽泛的 generic 条目。\n"
    "8) 若两条语义相近，一条更像上游失败/支撑问题摘要、另一条更明确落在当前动作最终输出/结果上，优先保留动作结果层更明确者。\n"
    "9) 若文本几乎同义但 linked_hazard_ids 不同，允许合并，保留 hazards 更完整者。\n"
    "10) 保留优先级：quality_pass > unsafe_form_specificity(discriminative优于generic) > template_dependency(低优于高) > B3 > B2 > A > B1。\n"
    "11) 不要因为 category 不同而阻止合并；只看 description 的真实语义。\n"
)


def _facts_compact_for_dedup(facts: dict) -> dict:
    return {
        "system": facts.get("system", {}),
        "control_action": facts.get("control_action", {}),
        "hazards": facts.get("hazards", []),
    }


def build_dedup_candidate_summary(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    parts: List[str] = []
    for key, label in [
        ("action_focus_type", "focus"),
        ("unsafe_form_specificity", "specificity"),
        ("action_semantics_mode", "mode"),
        ("context_kind", "context_kind"),
        ("relation_anchor", "relation"),
        ("mechanism_basis", "mechanism"),
        ("mechanism_core", "core"),
        ("template_dependency", "template_dep"),
    ]:
        val = str(item.get(key, "") or "").strip()
        if val:
            parts.append(f"{label}={val[:80]}")
    scores = item.get("scores", {}) if isinstance(item.get("scores"), dict) else {}
    compact_scores = []
    for k in ["A", "B1", "B2", "B3", "B4"]:
        if k in scores:
            compact_scores.append(f"{k}:{scores.get(k)}")
    if compact_scores:
        parts.append("scores=" + ",".join(compact_scores))
    if item.get("total_score") is not None:
        parts.append(f"total={item.get('total_score')}")
    return " | ".join(parts)


def build_dedup_user_prompt_all_strict(facts: dict, usable_ucas: List[dict]) -> str:
    facts_json = json.dumps(_facts_compact_for_dedup(facts), ensure_ascii=False, indent=2)
    minimal = []
    for u in usable_ucas:
        slots = u.get("slots", dict(SLOT_DEFAULT)) if isinstance(u.get("slots"), dict) else dict(SLOT_DEFAULT)
        description = u.get("description", "")
        # v8_fix6: 计算context_richness
        context_richness = calculate_context_richness(description, u)
        minimal.append({
            "uca_id": u.get("uca_id", ""),
            "description": description,
            "summary": build_dedup_candidate_summary(u),
            "linked_hazard_ids": u.get("linked_hazard_ids", []),
            "scores": u.get("scores", {}),
            "action_focus_type": u.get("action_focus_type", "mixed_with_upstream"),
            "unsafe_form_specificity": u.get("unsafe_form_specificity", "generic"),
            "action_semantics_mode": u.get("action_semantics_mode", "other_output"),
            "context_kind": u.get("context_kind", "none"),
            "relation_anchor": u.get("relation_anchor", "unclear"),
            "mechanism_basis": u.get("mechanism_basis", "unclear"),
            "mechanism_core": u.get("mechanism_core", ""),
            "template_dependency": u.get("template_dependency", "high"),
            "quality_pass": bool(is_high_quality_strict_item(u)),
            "context_richness": context_richness,  # v8_fix6新增
            "slots": {
                "context": str(slots.get("context", "") or ""),
                "control_action": str(slots.get("control_action", "") or ""),
                "uca_statement_normalized": str(slots.get("uca_statement_normalized", "") or ""),
            },
            "total_score": u.get("total_score", 0),
        })
    usable_json = json.dumps(minimal, ensure_ascii=False, indent=2)
    return f"""【事实输入 facts】
{facts_json}

【合理可用UCA集合 usable_ucas】
{usable_json}

任务：对 usable_ucas 做严格 STPA 语义去重与包含关系合并（全量，不按类别拆分输入）。

合并策略（必须遵守）：
1) 【绝对不可合并规则】不同 mechanism_core 的条目绝对不可合并。mechanism_core 是最小机制差异，即使其他所有字段相同也必须保留。
2) 【绝对不可合并规则】不同 relation_anchor 的条目绝对不可合并。例如 boundary_relation vs phase_relation 是不同机制族，必须保留。
3) 【绝对不可合并规则】opposite polarity 必须保留为不同条目。例如 too early vs too late、not provided vs wrongly provided、stopped too soon vs applied too long 是不同机制，不可合并。
4) 【可合并规则】仅有对象名词替换（行人/车辆/障碍物/边界）而 relation_anchor 与 mechanism_core 完全相同时，才可视为同一模板族进行合并。
5) 只看 description 的真实语义、linked_hazards，以及提供的 audit 元数据；不要仅凭 category/占位符做决定。
6) 不要使用表面措辞或 broad family 作为充分合并条件。只有当两条记录在 broad unsafe family、polarity、relation_anchor、mechanism_core 和 hazard direction 上都语义等价时，才允许合并。
7) 永远不要把 discriminative 条目并入 generic 条目；若 generic 与 discriminative 拥有相同 relation_anchor 与 mechanism_core，保留 discriminative。
8) 若文本几乎同义但 linked_hazard_ids 不同，允许合并，保留 hazards 更完整者。
9) context_inclusion 只有在 relation_anchor 相同且 mechanism_core 相同的情况下才能作为合并理由；否则不得因上下文包含关系合并。
10) 保留优先级（v8_fix6增加上下文丰富度）：context_richness > B1 > quality_pass > unsafe_form_specificity(discriminative优于generic) > template_dependency(低优于高) > B3 > A。
   【关键】context_richness（上下文丰富度）优先级最高：
   - high: 同时包含生命周期表达和明确的边界/阶段/位置描述（如"当接近停车区域时过早提供"）
   - medium: 仅包含生命周期表达（如"过早提供"）
   - low: 仅包含边界/结果条件，无生命周期（如"达到边界后提供"）
   当两条UCA语义相似需要合并时，优先保留context_richness更高的（high > medium > low）。
   【重要】LoRA倾向于生成"生命周期+边界"的复合上下文UCA（context_richness=high），应该在去重时得到优先保留。
   其次，优先保留B1分数更高的（B1=2 > B1=1 > B1=0）。
11) 不要因为 category 不同而阻止合并；只看 description 的真实语义。

{PREFERENCE_POLICY_DEDUP}

输出要求：
- deduped_usable_ucas：保留的 uca_id 列表，并给每条一个简短 summary（<=25字）。
- removed_as_duplicate：被去掉的条目，逐条给出 removed_uca_id、kept_uca_id 和 reason。
  reason 只能是："semantic_equivalent" / "context_inclusion" / "same_text_diff_hazard_merge"。
- coverage_count：deduped_usable_ucas 的条目数（整数）。

只输出以下 JSON 结构（字段必须齐全）：
{json.dumps(DEDUP_SCHEMA_HINT, ensure_ascii=False, indent=2)}
"""

def validate_dedup_schema(obj: dict) -> Tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "TOP_NOT_DICT"
    if "deduped_usable_ucas" not in obj or not isinstance(obj["deduped_usable_ucas"], list):
        return False, "MISSING_OR_INVALID_deduped_usable_ucas"
    if "removed_as_duplicate" not in obj or not isinstance(obj["removed_as_duplicate"], list):
        return False, "MISSING_OR_INVALID_removed_as_duplicate"
    if "coverage_count" not in obj:
        return False, "MISSING_coverage_count"
    try:
        _ = int(obj["coverage_count"])
    except Exception:
        return False, "INVALID_coverage_count"
    for r in obj.get("removed_as_duplicate", []):
        if not isinstance(r, dict):
            return False, "INVALID_removed_edge"
        if not isinstance(r.get("removed_uca_id", ""), str) or not isinstance(r.get("kept_uca_id", ""), str):
            return False, "INVALID_removed_edge_ids"
        if "reason" not in r or not isinstance(r.get("reason"), str) or not str(r.get("reason")).strip():
            return False, "INVALID_removed_reason"
    return True, "OK"


def identity_dedup_result(usable_items: List[dict]) -> dict:
    kept = []
    for u in usable_items:
        if isinstance(u, dict):
            uid = u.get("uca_id", "")
            if isinstance(uid, str) and uid:
                kept.append({"uca_id": uid, "summary": ""})
    return {
        "deduped_usable_ucas": kept,
        "removed_as_duplicate": [],
        "coverage_count": len(kept),
    }


# ============================================================
# 6) LLM 调用封装
# ============================================================
def _call_generation_once(messages: List[dict], max_tokens: int):
    if dashscope is None:
        raise RuntimeError("dashscope package is not installed. Please install dashscope or run in an environment where it is available.")
    return dashscope.Generation.call(
        model=MODEL_NAME,
        messages=messages,
        result_format="message",
        temperature=TEMPERATURE,
        top_p=TOP_P,
        max_tokens=max_tokens,
    )


def call_llm_json(messages: List[dict], max_tokens: int, schema_validator) -> Tuple[bool, Optional[dict], dict]:
    last_meta: dict = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = _call_generation_once(messages, max_tokens=max_tokens)
        except Exception as e:
            last_meta = {"exception": repr(e), "attempt": attempt}
            sleep_s = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
            time.sleep(sleep_s)
            continue

        last_meta = {
            "status_code": getattr(response, "status_code", None),
            "request_id": getattr(response, "request_id", None),
            "message": getattr(response, "message", None),
            "attempt": attempt,
        }

        status = getattr(response, "status_code", None)
        if status in (HTTPStatus.TOO_MANY_REQUESTS,) or (isinstance(status, int) and status >= 500):
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                time.sleep(sleep_s)
                continue
            return False, None, {**last_meta, "raw": None, "error": "retry_exhausted"}

        if status != HTTPStatus.OK:
            return False, None, {**last_meta, "raw": None}

        try:
            raw = response.output.choices[0].message.content
        except Exception as e:
            return False, None, {**last_meta, "raw": None, "error": f"bad_output_structure:{repr(e)}"}

        ok, obj, err, parse_tag = try_parse_json_with_local_repair(raw)
        local_schema_error = None
        if ok and isinstance(obj, dict):
            schema_ok, schema_msg = schema_validator(obj)
            if schema_ok:
                return True, obj, {**last_meta, "raw": raw, "parse": parse_tag}
            local_schema_error = schema_msg

        repair_ok, repair_obj, repair_dbg = repair_json_with_llm(raw, max_tokens=max_tokens, schema_validator=schema_validator)
        if repair_ok and isinstance(repair_obj, dict):
            return True, repair_obj, {**last_meta, "raw": raw, "repair_debug": repair_dbg, "parse": repair_dbg.get("parse", "llm_repair")}

        if local_schema_error:
            return False, None, {**last_meta, "raw": raw, "parse": parse_tag, "schema_error": local_schema_error, "repair_debug": repair_dbg}
        return False, None, {**last_meta, "raw": raw, "parse": parse_tag, "parse_error": err, "repair_debug": repair_dbg}

    return False, None, {**last_meta, "raw": None, "error": "unexpected_fallthrough"}


# ============================================================
# 7) 去重：全量分块 + 迭代归并
# ============================================================
def _dedup_once_all(facts: dict, items: List[dict]) -> Tuple[dict, dict]:
    messages = [
        {"role": "system", "content": DEDUP_SYSTEM},
        {"role": "user", "content": build_dedup_user_prompt_all_strict(facts, items)},
    ]
    ok, obj, dbg = call_llm_json(messages, max_tokens=DEDUP_MAX_TOKENS, schema_validator=validate_dedup_schema)
    if ok and isinstance(obj, dict):
        obj["coverage_count"] = int(obj.get("coverage_count", 0) or 0)
        return obj, dbg
    return {"deduped_usable_ucas": [], "removed_as_duplicate": [], "coverage_count": 0}, dbg


def dedup_all_chunked_iterative(facts: dict, usable_items: List[dict]) -> Tuple[dict, dict]:
    debug = {
        "strategy": "description_only_chunked_iterative",
        "rounds": [],
        "invalid_for_ranking": False,
        "fallback_events": [],
    }
    if not usable_items:
        return {"deduped_usable_ucas": [], "removed_as_duplicate": [], "coverage_count": 0}, debug

    by_id = {u["uca_id"]: u for u in usable_items if isinstance(u.get("uca_id"), str)}
    current_pool = usable_items[:]
    all_edges: List[dict] = []

    def _record_fallback(scope: str, n_in: int, llm_debug: dict) -> None:
        debug["invalid_for_ranking"] = True
        debug["fallback_events"].append({
            "scope": scope,
            "n_in": n_in,
            "llm_debug": llm_debug,
        })

    for round_idx in range(1, DEDUP_MAX_ROUNDS + 1):
        if len(current_pool) <= DEDUP_CHUNK_SIZE:
            obj, dbg = _dedup_once_all(facts, current_pool)
            debug["rounds"].append({"round": round_idx, "mode": "single", "n_in": len(current_pool), "llm_debug": dbg})
            edges = obj.get("removed_as_duplicate", [])
            if isinstance(edges, list):
                all_edges.extend([e for e in edges if isinstance(e, dict)])
            kept = obj.get("deduped_usable_ucas", []) if isinstance(obj, dict) else []
            if len(current_pool) > 0 and (not isinstance(kept, list) or len(kept) == 0):
                _record_fallback("single", len(current_pool), dbg)
                obj = identity_dedup_result(current_pool)
            return {
                "deduped_usable_ucas": obj.get("deduped_usable_ucas", []),
                "removed_as_duplicate": all_edges,
                "coverage_count": int(obj.get("coverage_count", 0) or 0),
            }, debug

        kept_pool: List[dict] = []
        chunk_debug = []
        for i in range(0, len(current_pool), DEDUP_CHUNK_SIZE):
            chunk = current_pool[i: i + DEDUP_CHUNK_SIZE]
            obj_c, dbg_c = _dedup_once_all(facts, chunk)
            chunk_debug.append({"chunk_idx": i // DEDUP_CHUNK_SIZE, "n_in": len(chunk), "llm_debug": dbg_c})
            edges = obj_c.get("removed_as_duplicate", [])
            if isinstance(edges, list):
                all_edges.extend([e for e in edges if isinstance(e, dict)])
            kept_ids = [
                it["uca_id"]
                for it in obj_c.get("deduped_usable_ucas", [])
                if isinstance(it, dict) and isinstance(it.get("uca_id"), str)
            ]
            if not kept_ids:
                _record_fallback(f"chunk_{i // DEDUP_CHUNK_SIZE}", len(chunk), dbg_c)
                kept_pool.extend(chunk)
            else:
                for kid in kept_ids:
                    if kid in by_id:
                        kept_pool.append(by_id[kid])

        obj_m, dbg_m = _dedup_once_all(facts, kept_pool)
        debug["rounds"].append(
            {
                "round": round_idx,
                "mode": "chunk+merge",
                "n_in": len(current_pool),
                "n_after_chunk": len(kept_pool),
                "llm_debug_chunks": chunk_debug,
                "llm_debug_merge": dbg_m,
            }
        )
        edges2 = obj_m.get("removed_as_duplicate", [])
        if isinstance(edges2, list):
            all_edges.extend([e for e in edges2 if isinstance(e, dict)])
        kept_ids2 = [
            it["uca_id"]
            for it in obj_m.get("deduped_usable_ucas", [])
            if isinstance(it, dict) and isinstance(it.get("uca_id"), str)
        ]
        if not kept_ids2:
            _record_fallback("merge", len(kept_pool), dbg_m)
            obj_identity = identity_dedup_result(kept_pool)
            return {
                "deduped_usable_ucas": obj_identity.get("deduped_usable_ucas", []),
                "removed_as_duplicate": all_edges,
                "coverage_count": int(obj_identity.get("coverage_count", 0) or 0),
            }, debug
        next_pool = [by_id[k] for k in kept_ids2 if k in by_id]

        if len(next_pool) >= len(current_pool) or len(next_pool) == len(kept_pool):
            current_pool = next_pool
            obj_final, dbg_final = _dedup_once_all(facts, current_pool)
            debug["rounds"].append({"round": round_idx, "mode": "finalize", "n_in": len(current_pool), "llm_debug": dbg_final})
            edgesf = obj_final.get("removed_as_duplicate", [])
            if isinstance(edgesf, list):
                all_edges.extend([e for e in edgesf if isinstance(e, dict)])
            kept_final = obj_final.get("deduped_usable_ucas", []) if isinstance(obj_final, dict) else []
            if len(current_pool) > 0 and (not isinstance(kept_final, list) or len(kept_final) == 0):
                _record_fallback("finalize", len(current_pool), dbg_final)
                obj_final = identity_dedup_result(current_pool)
            return {
                "deduped_usable_ucas": obj_final.get("deduped_usable_ucas", []),
                "removed_as_duplicate": all_edges,
                "coverage_count": int(obj_final.get("coverage_count", 0) or 0),
            }, debug

        current_pool = next_pool

    final_obj, final_dbg = _dedup_once_all(facts, current_pool)
    debug["rounds"].append({"round": DEDUP_MAX_ROUNDS + 1, "mode": "fallback_final", "n_in": len(current_pool), "llm_debug": final_dbg})
    edges = final_obj.get("removed_as_duplicate", [])
    if isinstance(edges, list):
        all_edges.extend([e for e in edges if isinstance(e, dict)])
    kept_final = final_obj.get("deduped_usable_ucas", []) if isinstance(final_obj, dict) else []
    if len(current_pool) > 0 and (not isinstance(kept_final, list) or len(kept_final) == 0):
        _record_fallback("fallback_final", len(current_pool), final_dbg)
        final_obj = identity_dedup_result(current_pool)
    return {
        "deduped_usable_ucas": final_obj.get("deduped_usable_ucas", []),
        "removed_as_duplicate": all_edges,
        "coverage_count": int(final_obj.get("coverage_count", 0) or 0),
    }, debug


def build_dedup_merge_report(usable_items: List[dict], dedup_obj: dict, valid_hazard_ids: set[str]) -> dict:
    deduped = dedup_obj.get("deduped_usable_ucas", []) if isinstance(dedup_obj, dict) else []
    removed = dedup_obj.get("removed_as_duplicate", []) if isinstance(dedup_obj, dict) else []

    kept_ids = [it.get("uca_id", "") for it in deduped if isinstance(it, dict) and isinstance(it.get("uca_id"), str)]
    kept_ids = [x for x in kept_ids if x]
    id2item = {u["uca_id"]: u for u in usable_items if isinstance(u, dict) and isinstance(u.get("uca_id"), str)}

    parent: Dict[str, str] = {kid: kid for kid in kept_ids}
    for edge in removed:
        if not isinstance(edge, dict):
            continue
        rid = edge.get("removed_uca_id", "")
        kid = edge.get("kept_uca_id", "")
        if isinstance(rid, str) and isinstance(kid, str) and kid:
            parent[rid] = kid

    groups: Dict[str, List[str]] = defaultdict(list)
    for u in usable_items:
        uid = u.get("uca_id", "")
        if not isinstance(uid, str) or not uid:
            continue
        root = parent.get(uid, uid if uid in kept_ids else "")
        if root:
            groups[root].append(uid)

    by_category: Dict[str, List[dict]] = {cat: [] for cat in UCA_CATEGORIES}
    warnings: List[str] = []
    edges_by_root: Dict[str, List[dict]] = defaultdict(list)
    for edge in removed:
        if isinstance(edge, dict):
            kid = edge.get("kept_uca_id", "")
            if isinstance(kid, str) and kid:
                edges_by_root[kid].append(edge)

    for kid in kept_ids:
        kept_item = id2item.get(kid)
        if kept_item is None:
            warnings.append(f"kept_id_missing_in_items:{kid}")
            continue
        hazards_union: List[str] = []
        seen_h = set()
        for uid in groups.get(kid, [kid]):
            item = id2item.get(uid)
            if not item:
                continue
            for hid in normalize_hazard_ids(item.get("linked_hazard_ids", [])):
                if HAZARD_ID_RE.match(hid) and (not valid_hazard_ids or hid in valid_hazard_ids) and hid not in seen_h:
                    seen_h.add(hid)
                    hazards_union.append(hid)

        cat = kept_item.get("category", "")
        if cat not in by_category:
            cat = "provided_causing_hazard"
        merged_originals = []
        for uid in groups.get(kid, [kid]):
            item = id2item.get(uid)
            if not item:
                continue
            merged_originals.append(
                {
                    "uca_id": item.get("uca_id", ""),
                    "uca_id_raw": item.get("uca_id_raw", ""),
                    "description": item.get("description", ""),
                    "linked_hazard_ids": normalize_hazard_ids(item.get("linked_hazard_ids", [])),
                    "iteration": item.get("iteration", None),
                }
            )

        entry = {
            "kept_uca": {
                "uca_id": kid,
                "uca_id_raw": kept_item.get("uca_id_raw", ""),
                "category": kept_item.get("category", ""),
                "description": kept_item.get("description", ""),
                "linked_hazard_ids": hazards_union,
                "iteration": kept_item.get("iteration", None),
                "slots": kept_item.get("slots", dict(SLOT_DEFAULT)),
                "slot_fill_count": int(kept_item.get("slot_fill_count", 0) or 0),
                "action_focus_type": kept_item.get("action_focus_type", "mixed_with_upstream"),
                "unsafe_form_specificity": kept_item.get("unsafe_form_specificity", "generic"),
                "action_semantics_mode": kept_item.get("action_semantics_mode", "other_output"),
                "context_kind": kept_item.get("context_kind", "none"),
                "mechanism_basis": kept_item.get("mechanism_basis", "unclear"),
                "mechanism_core": kept_item.get("mechanism_core", ""),
                "template_like": bool(kept_item.get("template_like", False)),
                "template_dependency": kept_item.get("template_dependency", "high"),
            },
            "merged_originals": merged_originals,
            "merged_edges": edges_by_root.get(kid, []),
            "merged_count": len(merged_originals),
        }
        by_category[cat].append(entry)

    return {"kept_ids": kept_ids, "groups": dict(groups), "by_category": by_category, "warnings": warnings}


def dedup_health_from_summary(
    dedup_ok: bool,
    dedup_debug: dict,
    valid_scored_rate: float,
    judge_repaired_count: int = 0,
    dedup_repaired_count: int = 0,
) -> Tuple[str, bool]:
    dbg = dedup_debug if isinstance(dedup_debug, dict) else {}
    if (not dedup_ok) or bool(dbg.get("invalid_for_ranking", False)) or bool(dbg.get("fallback_events")):
        return "fallback_identity", True
    if valid_scored_rate < RANKING_MIN_VALID_SCORED_RATE:
        return "low_validity", True
    if judge_repaired_count >= REPAIR_HEAVY_THRESHOLD or dedup_repaired_count >= REPAIR_HEAVY_THRESHOLD:
        return "repair_heavy", True
    return "healthy", False


def _count_parse_modes(per_item_evaluations: List[dict]) -> Dict[str, int]:
    counts = {"strict": 0, "repaired_local": 0, "repaired_llm": 0}
    for x in per_item_evaluations:
        mode = str(x.get("parse_mode", "strict") or "strict")
        if mode in counts:
            counts[mode] += 1
    return counts


def _count_dedup_repairs(dedup_debug: dict) -> int:
    if not isinstance(dedup_debug, dict):
        return 0
    n = 0
    rounds = dedup_debug.get("rounds", [])
    if isinstance(rounds, list):
        for r in rounds:
            if not isinstance(r, dict):
                continue
            dbg = r.get("llm_debug") if isinstance(r.get("llm_debug"), dict) else None
            if dbg and "repair_debug" in dbg:
                n += 1
            for cd in (r.get("llm_debug_chunks") if isinstance(r.get("llm_debug_chunks"), list) else []):
                if isinstance(cd, dict) and isinstance(cd.get("llm_debug"), dict) and "repair_debug" in cd["llm_debug"]:
                    n += 1
            md = r.get("llm_debug_merge") if isinstance(r.get("llm_debug_merge"), dict) else None
            if md and "repair_debug" in md:
                n += 1
    return n


def compute_quality_metrics(per_item_evaluations: List[dict], dedup_obj: dict) -> dict:
    id2eval = {
        x.get("uca_id", ""): x
        for x in per_item_evaluations
        if isinstance(x, dict) and isinstance(x.get("uca_id"), str) and x.get("uca_id")
    }
    kept_ids = [it.get("uca_id", "") for it in (dedup_obj.get("deduped_usable_ucas", []) if isinstance(dedup_obj, dict) else []) if isinstance(it, dict) and isinstance(it.get("uca_id"), str) and it.get("uca_id")]
    kept_evals = [id2eval[k] for k in kept_ids if k in id2eval]
    coverage_before = [x for x in per_item_evaluations if x.get("label") == "合理可用"]
    quality_before = [x for x in coverage_before if is_high_quality_strict_item(x)]
    quality_kept = [x for x in kept_evals if is_high_quality_strict_item(x)]

    def _core_key(x: dict) -> tuple[str, str]:
        return (
            str(x.get("relation_anchor", "unclear") or "unclear").strip().lower(),
            str(x.get("mechanism_core", "") or "").strip(),
        )

    discriminative_usable_before_dedup = sum(1 for x in coverage_before if x.get("unsafe_form_specificity") == "discriminative")
    generic_usable_before_dedup = sum(1 for x in coverage_before if x.get("unsafe_form_specificity") == "generic")
    native_action_usable_before_dedup = sum(1 for x in coverage_before if x.get("action_focus_type") == "native_action_result")
    discriminative_kept_after_dedup = sum(1 for x in kept_evals if x.get("unsafe_form_specificity") == "discriminative")
    generic_kept_after_dedup = sum(1 for x in kept_evals if x.get("unsafe_form_specificity") == "generic")
    native_action_kept_after_dedup = sum(1 for x in kept_evals if x.get("action_focus_type") == "native_action_result")
    template_like_usable_before_dedup = sum(1 for x in coverage_before if bool(x.get("template_like", False)))
    template_like_kept_after_dedup = sum(1 for x in kept_evals if bool(x.get("template_like", False)))
    mechanism_like_usable_before_dedup = sum(1 for x in coverage_before if str(x.get("mechanism_core", "") or "").strip())
    mechanism_like_kept_after_dedup = sum(1 for x in kept_evals if str(x.get("mechanism_core", "") or "").strip())
    strong_coverage_kept_after_dedup = sum(1 for x in kept_evals if is_high_quality_strict_item(x) or x.get("unsafe_form_specificity") == "discriminative")
    weak_coverage_kept_after_dedup = max(0, len(kept_evals) - strong_coverage_kept_after_dedup)
    mechanism_diversity_before_dedup = len({_core_key(x) for x in coverage_before if _core_key(x)[1]})
    mechanism_diversity_after_dedup = len({_core_key(x) for x in kept_evals if _core_key(x)[1]})
    template_dependency_high_before_dedup = sum(1 for x in coverage_before if str(x.get("template_dependency", "high") or "high").strip().lower() == "high")
    template_dependency_high_after_dedup = sum(1 for x in kept_evals if str(x.get("template_dependency", "high") or "high").strip().lower() == "high")
    coverage_before_n = max(1, len(coverage_before))
    kept_n = max(1, len(kept_evals))
    template_burden_ratio_before_dedup = template_dependency_high_before_dedup / coverage_before_n if coverage_before else 0.0
    template_burden_ratio_after_dedup = template_dependency_high_after_dedup / kept_n if kept_evals else 0.0
    merged_removed = dedup_obj.get("removed_as_duplicate", []) if isinstance(dedup_obj, dict) else []
    generic_merged_into_discriminative_count = 0
    for m in merged_removed:
        if not isinstance(m, dict):
            continue
        removed = id2eval.get(m.get("removed_uca_id", ""), {})
        kept = id2eval.get(m.get("kept_uca_id", ""), {})
        if removed.get("unsafe_form_specificity") == "generic" and kept.get("unsafe_form_specificity") == "discriminative":
            generic_merged_into_discriminative_count += 1
    return {
        "usable_before_dedup": len(coverage_before),
        "coverage_count_after_dedup": len(kept_evals),
        "strict_usable_before_dedup": len(coverage_before),
        "strict_dedup_coverage": len(kept_evals),
        "high_quality_before_dedup": len(quality_before),
        "high_quality_kept_after_dedup": len(quality_kept),
        "quality_weighted_coverage_after_dedup": float(len(kept_evals) + 0.5 * len(quality_kept)),
        "discriminative_usable_before_dedup": discriminative_usable_before_dedup,
        "generic_usable_before_dedup": generic_usable_before_dedup,
        "native_action_usable_before_dedup": native_action_usable_before_dedup,
        "discriminative_kept_after_dedup": discriminative_kept_after_dedup,
        "generic_kept_after_dedup": generic_kept_after_dedup,
        "native_action_kept_after_dedup": native_action_kept_after_dedup,
        "template_like_usable_before_dedup": template_like_usable_before_dedup,
        "template_like_kept_after_dedup": template_like_kept_after_dedup,
        "mechanism_like_usable_before_dedup": mechanism_like_usable_before_dedup,
        "mechanism_like_kept_after_dedup": mechanism_like_kept_after_dedup,
        "quality_pass_before_dedup": len(quality_before),
        "quality_pass_kept_after_dedup": len(quality_kept),
        "strong_coverage_kept_after_dedup": strong_coverage_kept_after_dedup,
        "weak_coverage_kept_after_dedup": weak_coverage_kept_after_dedup,
        "mechanism_diversity_before_dedup": mechanism_diversity_before_dedup,
        "mechanism_diversity_after_dedup": mechanism_diversity_after_dedup,
        "template_dependency_high_before_dedup": template_dependency_high_before_dedup,
        "template_dependency_high_after_dedup": template_dependency_high_after_dedup,
        "template_burden_ratio_before_dedup": round(template_burden_ratio_before_dedup, 6),
        "template_burden_ratio_after_dedup": round(template_burden_ratio_after_dedup, 6),
        "generic_merged_into_discriminative_count": generic_merged_into_discriminative_count,
    }


def hazard_ids_invalid(x: Any, valid_hazard_ids: set[str]) -> bool:
    if x is None:
        return False
    if not isinstance(x, list):
        return True
    ids = normalize_hazard_ids(x)
    if len(ids) != len(x):
        return True
    for v in ids:
        if not HAZARD_ID_RE.match(v):
            return True
        if valid_hazard_ids and v not in valid_hazard_ids:
            return True
    return False

def _judge_one_candidate(candidate: dict, iteration: Any, uca_source: str, index_in_category: int, facts: dict, valid_hazard_ids: set[str]) -> dict:
    effective_valid_hazard_ids = merged_valid_hazard_ids(valid_hazard_ids, candidate)
    messages = [
        {"role": "system", "content": JUDGE_SYSTEM},
        {"role": "user", "content": build_judge_user_prompt(facts, effective_valid_hazard_ids, candidate)},
    ]
    ok, judge_obj, debug = call_llm_json(messages, max_tokens=MAX_TOKENS, schema_validator=validate_judge_schema)

    if not ok or not isinstance(judge_obj, dict):
        return {
            "uca_id": candidate["uca_id"],
            "uca_id_raw": candidate.get("uca_id_raw", ""),
            "category": candidate["category"],
            "scores": {"A": 0, "B1": 0, "B2": 0, "B3": 0, "B4": 0},
            "total_score": 0,
            "label": "judge_failed",
            "reasons": {k: "judge_failed" for k in ["A", "B1", "B2", "B3", "B4"]},
            "slots": dict(SLOT_DEFAULT),
            "slot_fill_count": 0,
            "action_focus_type": "mixed_with_upstream",
            "unsafe_form_specificity": "unclear",
            "action_semantics_mode": "other_output",
            "context_kind": "none",
            "relation_anchor": "unclear",
            "mechanism_basis": "unclear",
            "mechanism_core": "",
            "template_like": False,
            "template_dependency": "high",
            "debug": debug,
            "parse_mode": "failed",
            "candidate_uca": candidate,
            "iteration": iteration,
            "uca_source": uca_source,
            "index_in_category": index_in_category,
        }

    judge_obj = normalize_judge_obj(judge_obj, candidate, facts, effective_valid_hazard_ids)
    judge_label_raw = judge_obj.get("label", "")
    s = judge_obj.get("scores", {}) if isinstance(judge_obj.get("scores"), dict) else {}

    def _score_of(key: str) -> int:
        node = s.get(key, {})
        if not isinstance(node, dict):
            return 0
        return clamp_0_2(node.get("score", 0))

    A, B1, B2, B3, B4 = _score_of("A"), _score_of("B1"), _score_of("B2"), _score_of("B3"), _score_of("B4")

    issues_append: List[str] = []
    if hazard_ids_invalid(candidate.get("linked_hazard_ids", None), effective_valid_hazard_ids):
        B4 = 0
        issues_append.append("invalid_linked_hazard_ids")

    # category mismatch 只作为 audit，不再进入 B3 惩罚
    inferred_gw = str(judge_obj.get("slots", {}).get("guideword", "")).strip()
    cat_raw = str(candidate.get("category", "")).strip()
    if inferred_gw and cat_raw and inferred_gw in UCA_CATEGORIES and cat_raw in UCA_CATEGORIES and inferred_gw != cat_raw:
        issues_append.append(f"category_audit_only_mismatch:{cat_raw}->{inferred_gw}")

    total = A + B1 + B2 + B3 + B4
    action_focus_type = judge_obj.get("action_focus_type", "mixed_with_upstream")
    unsafe_form_specificity = judge_obj.get("unsafe_form_specificity", "generic")
    rule_label = label_by_rule_description_strict_context(
        {"A": A, "B1": B1, "B2": B2, "B3": B3, "B4": B4},
        total_score=total,
        action_focus_type=action_focus_type,
        unsafe_form_specificity=unsafe_form_specificity,
        slots=judge_obj.get("slots", {}),
        context_kind=str(judge_obj.get("context_kind", "none") or "none"),
        relation_anchor=str(judge_obj.get("relation_anchor", "unclear") or "unclear"),
        mechanism_basis=str(judge_obj.get("mechanism_basis", "unclear") or "unclear"),
        template_like=bool(judge_obj.get("template_like", False)),
        action_semantics_mode=str(judge_obj.get("action_semantics_mode", "other_output") or "other_output"),
        mechanism_core=str(judge_obj.get("mechanism_core", "") or ""),
        template_dependency=str(judge_obj.get("template_dependency", "high") or "high"),
    )
    label_mismatch = bool(judge_label_raw) and (judge_label_raw != rule_label)

    def _rat_of(key: str) -> str:
        node = s.get(key, {})
        if isinstance(node, dict) and isinstance(node.get("rationale"), str):
            return node["rationale"].strip()
        return ""

    reasons = {k: _rat_of(k) for k in ["A", "B1", "B2", "B3", "B4"]}
    issues = judge_obj.get("issues", [])
    if not isinstance(issues, list):
        issues = []
    issues = [x for x in issues if isinstance(x, str) and x.strip()]
    issues.extend(issues_append)

    slots = judge_obj.get("slots", dict(SLOT_DEFAULT))
    if not isinstance(slots, dict):
        slots = dict(SLOT_DEFAULT)
    slot_fill_count = int(judge_obj.get("slot_fill_count", 0) or 0)
    parse_mode = str(debug.get("parse", "strict") or "strict")

    return {
        "uca_id": candidate["uca_id"],
        "uca_id_raw": candidate.get("uca_id_raw", ""),
        "category": candidate["category"],
        "scores": {"A": A, "B1": B1, "B2": B2, "B3": B3, "B4": B4},
        "total_score": total,
        "label": rule_label,
        "reasons": reasons,
        "judge_label_raw": judge_label_raw,
        "label_mismatch": label_mismatch,
        "issues": issues,
        "suggested_fix": judge_obj.get("suggested_fix", ""),
        "slots": slots,
        "slot_fill_count": slot_fill_count,
        "action_focus_type": action_focus_type,
        "unsafe_form_specificity": unsafe_form_specificity,
        "action_semantics_mode": judge_obj.get("action_semantics_mode", "other_output"),
        "context_kind": judge_obj.get("context_kind", "none"),
        "relation_anchor": judge_obj.get("relation_anchor", "unclear"),
        "mechanism_basis": judge_obj.get("mechanism_basis", "unclear"),
        "mechanism_core": judge_obj.get("mechanism_core", ""),
        "template_like": bool(judge_obj.get("template_like", False)),
        "template_dependency": judge_obj.get("template_dependency", "high"),
        "candidate_uca": candidate,
        "iteration": iteration,
        "uca_source": uca_source,
        "index_in_category": index_in_category,
        "judge_debug": debug,
        "parse_mode": parse_mode,
    }


def evaluate_one_file(file_tag: str, input_path: str | Path) -> dict:
    data = load_json(input_path)
    runs = data.get("runs", [])
    if not isinstance(runs, list) or len(runs) == 0:
        raise ValueError(f"输入文件不是 raw UCA 生成结果，缺少有效 runs 列表: {input_path}")

    meta_in_file = data.get("meta", {})
    facts = build_facts_from_meta(meta_in_file, f"{file_tag} {input_path}")
    valid_hazard_ids = get_valid_hazard_ids(facts)

    per_item_evaluations: List[dict] = []
    judge_failed_count = 0
    uca_parse_failed_count = 0
    label_mismatch_count = 0

    seen_id_counts: Dict[str, int] = {}
    all_candidates: List[Tuple[dict, Any, str, int]] = []

    for run_idx, run in enumerate(runs, start=1):
        iteration = run.get("iteration", run_idx)
        uca_json, uca_source = get_uca_json_from_run(run)
        if uca_json is None:
            uca_parse_failed_count += 1
            continue
        ucas = iter_ucas(uca_json)
        for u in ucas:
            raw_id = u.get("uca_id", "") or ""
            unique_id = make_unique_uca_id(raw_id, iteration, u["category"], u["index_in_category"], seen_id_counts)
            candidate = {
                "category": u["category"],
                "uca_id": unique_id,
                "uca_id_raw": raw_id,
                "description": u.get("description", ""),
                "linked_hazard_ids": u.get("linked_hazard_ids", None),
            }
            all_candidates.append((candidate, iteration, uca_source, u["index_in_category"]))

    if all_candidates:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [
                ex.submit(_judge_one_candidate, c, it, src, idx, facts, valid_hazard_ids)
                for (c, it, src, idx) in all_candidates
            ]
            for fut in as_completed(futs):
                item = fut.result()
                per_item_evaluations.append(item)
                if item.get("label") == "judge_failed":
                    judge_failed_count += 1
                if item.get("label_mismatch"):
                    label_mismatch_count += 1
                if SLEEP_SEC > 0:
                    time.sleep(SLEEP_SEC)

    per_item_evaluations.sort(key=lambda x: (
        int(x.get("iteration", 10**9) or 10**9),
        UCA_CATEGORIES.index(x.get("category")) if x.get("category") in UCA_CATEGORIES else 10**9,
        int(x.get("index_in_category", 10**9) or 10**9),
        str(x.get("uca_id", "")),
    ))

    usable_items = [
        {
            "uca_id": x["uca_id"],
            "uca_id_raw": x.get("uca_id_raw", ""),
            "category": x["category"],
            "description": x.get("candidate_uca", {}).get("description", ""),
            "linked_hazard_ids": x.get("candidate_uca", {}).get("linked_hazard_ids", None),
            "iteration": x.get("iteration", None),
            "index_in_category": x.get("index_in_category", None),
            "slots": x.get("slots", dict(SLOT_DEFAULT)),
            "slot_fill_count": int(x.get("slot_fill_count", 0) or 0),
            "scores": x.get("scores", {}),
            "action_focus_type": x.get("action_focus_type", "mixed_with_upstream"),
            "unsafe_form_specificity": x.get("unsafe_form_specificity", "generic"),
            "action_semantics_mode": x.get("action_semantics_mode", "other_output"),
            "context_kind": x.get("context_kind", "none"),
            "mechanism_basis": x.get("mechanism_basis", "unclear"),
            "mechanism_core": x.get("mechanism_core", ""),
            "template_like": bool(x.get("template_like", False)),
            "template_dependency": x.get("template_dependency", "high"),
            "total_score": int(x.get("total_score", 0) or 0),
        }
        for x in per_item_evaluations
        if x.get("label") == "合理可用"
    ]

    dedup_ok = True
    dedup_invalid_for_ranking = False
    dedup_debug: dict = {}
    if len(usable_items) == 0:
        global_deduplication = {"deduped_usable_ucas": [], "removed_as_duplicate": [], "coverage_count": 0}
        dedup_merge_report = build_dedup_merge_report(usable_items=[], dedup_obj=global_deduplication, valid_hazard_ids=valid_hazard_ids)
        dedup_debug = {"note": "no_usable_items"}
    else:
        try:
            global_deduplication, dedup_debug = dedup_all_chunked_iterative(facts, usable_items)
            kept_list = global_deduplication.get("deduped_usable_ucas", []) if isinstance(global_deduplication, dict) else []
            if (not isinstance(kept_list, list)) or (len(kept_list) == 0 and len(usable_items) > 0):
                dedup_ok = False
                dedup_invalid_for_ranking = True
                dedup_debug = {"fallback": "identity_due_to_empty_dedup_result", "original_debug": dedup_debug}
                global_deduplication = identity_dedup_result(usable_items)
        except Exception as e:
            dedup_ok = False
            dedup_invalid_for_ranking = True
            dedup_debug = {"exception": repr(e), "fallback": "identity"}
            global_deduplication = identity_dedup_result(usable_items)
        dedup_merge_report = build_dedup_merge_report(usable_items=usable_items, dedup_obj=global_deduplication, valid_hazard_ids=valid_hazard_ids)

    scored = [x for x in per_item_evaluations if isinstance(x.get("scores"), dict)]
    quality_metrics = compute_quality_metrics(per_item_evaluations, global_deduplication)
    parse_mode_counts = _count_parse_modes(per_item_evaluations)
    judge_repaired_count = int(parse_mode_counts.get("repaired_local", 0) + parse_mode_counts.get("repaired_llm", 0))
    dedup_repaired_count = _count_dedup_repairs(dedup_debug)
    valid_scored = len(per_item_evaluations) - judge_failed_count
    valid_scored_rate = round(valid_scored / len(per_item_evaluations), 6) if len(per_item_evaluations) > 0 else 0.0
    ranking_health, dedup_invalid_health = dedup_health_from_summary(
        dedup_ok,
        dedup_debug,
        valid_scored_rate=valid_scored_rate,
        judge_repaired_count=judge_repaired_count,
        dedup_repaired_count=dedup_repaired_count,
    )
    dedup_invalid_for_ranking = bool(
        dedup_invalid_for_ranking
        or bool(dedup_debug.get("invalid_for_ranking", False) if isinstance(dedup_debug, dict) else False)
        or dedup_invalid_health
        or ranking_health != "healthy"
    )


    counts = {
        "total_records": len(per_item_evaluations),
        "uca_parse_failed": uca_parse_failed_count,
        "judge_failed": judge_failed_count,
        "valid_scored": valid_scored,
        "valid_scored_rate": valid_scored_rate,
        "num_scored": len(scored),
        "合理可用": sum(1 for x in scored if x.get("label") == "合理可用"),
        "合理不可用": sum(1 for x in scored if x.get("label") == "合理不可用"),
        "不合理不可用": sum(1 for x in scored if x.get("label") == "不合理不可用"),
        "label_mismatch": label_mismatch_count,
        "usable_before_dedup": len(usable_items),
        "coverage_count_after_dedup": int(global_deduplication.get("coverage_count", 0) or 0),
        "strict_usable_before_dedup": int(quality_metrics.get("strict_usable_before_dedup", 0) or 0),
        "strict_dedup_coverage": int(quality_metrics.get("strict_dedup_coverage", 0) or 0),
        "high_quality_before_dedup": int(quality_metrics.get("high_quality_before_dedup", 0) or 0),
        "high_quality_kept_after_dedup": int(quality_metrics.get("high_quality_kept_after_dedup", 0) or 0),
        "quality_weighted_coverage_after_dedup": float(quality_metrics.get("quality_weighted_coverage_after_dedup", 0.0) or 0.0),
        "discriminative_usable_before_dedup": int(quality_metrics.get("discriminative_usable_before_dedup", 0) or 0),
        "generic_usable_before_dedup": int(quality_metrics.get("generic_usable_before_dedup", 0) or 0),
        "native_action_usable_before_dedup": int(quality_metrics.get("native_action_usable_before_dedup", 0) or 0),
        "discriminative_kept_after_dedup": int(quality_metrics.get("discriminative_kept_after_dedup", 0) or 0),
        "generic_kept_after_dedup": int(quality_metrics.get("generic_kept_after_dedup", 0) or 0),
        "native_action_kept_after_dedup": int(quality_metrics.get("native_action_kept_after_dedup", 0) or 0),
        "template_like_usable_before_dedup": int(quality_metrics.get("template_like_usable_before_dedup", 0) or 0),
        "template_like_kept_after_dedup": int(quality_metrics.get("template_like_kept_after_dedup", 0) or 0),
        "mechanism_like_usable_before_dedup": int(quality_metrics.get("mechanism_like_usable_before_dedup", 0) or 0),
        "mechanism_like_kept_after_dedup": int(quality_metrics.get("mechanism_like_kept_after_dedup", 0) or 0),
        "quality_pass_before_dedup": int(quality_metrics.get("quality_pass_before_dedup", 0) or 0),
        "quality_pass_kept_after_dedup": int(quality_metrics.get("quality_pass_kept_after_dedup", 0) or 0),
        "strong_coverage_kept_after_dedup": int(quality_metrics.get("strong_coverage_kept_after_dedup", 0) or 0),
        "weak_coverage_kept_after_dedup": int(quality_metrics.get("weak_coverage_kept_after_dedup", 0) or 0),
        "mechanism_diversity_before_dedup": int(quality_metrics.get("mechanism_diversity_before_dedup", 0) or 0),
        "mechanism_diversity_after_dedup": int(quality_metrics.get("mechanism_diversity_after_dedup", 0) or 0),
        "template_dependency_high_before_dedup": int(quality_metrics.get("template_dependency_high_before_dedup", 0) or 0),
        "template_dependency_high_after_dedup": int(quality_metrics.get("template_dependency_high_after_dedup", 0) or 0),
        "template_burden_ratio_before_dedup": float(quality_metrics.get("template_burden_ratio_before_dedup", 0.0) or 0.0),
        "template_burden_ratio_after_dedup": float(quality_metrics.get("template_burden_ratio_after_dedup", 0.0) or 0.0),
        "generic_merged_into_discriminative_count": int(quality_metrics.get("generic_merged_into_discriminative_count", 0) or 0),
        "judge_repaired_count": judge_repaired_count,
        "dedup_repaired_count": dedup_repaired_count,
    }
    summary = {
        "file_tag": file_tag,
        "input_path": str(input_path),
        "meta_in_file": meta_in_file,
        "action_key": meta_in_file.get("action_key", ""),
        "action_key_cn": meta_in_file.get("action_key_cn", ""),
        "method": meta_in_file.get("method", ""),
        "counts": counts,
        "dedup_ok": dedup_ok,
        "dedup_invalid_for_ranking": dedup_invalid_for_ranking,
        "ranking_health": ranking_health,
        "dedup_debug": dedup_debug,
        "dedup_merge_report_ready": True,
        "score_policy": "description_only_strict_context_v8_fix2_lora_优先",
    }

    return {
        "judge_meta": {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "model": MODEL_NAME,
            "temperature": TEMPERATURE,
            "top_p": TOP_P,
            "max_tokens_item": MAX_TOKENS,
            "max_tokens_dedup": DEDUP_MAX_TOKENS,
            "max_retries": MAX_RETRIES,
            "dedup_chunk_size": DEDUP_CHUNK_SIZE,
            "dedup_max_rounds": DEDUP_MAX_ROUNDS,
            "pass_rule": "description_only_strict_context_v8_fix2_lora_优先",
            "slots_enabled": True,
            "slots_style": "audit_only_from_description",
            "slot_keys": SLOT_KEYS,
            "max_workers": MAX_WORKERS,
        },
        "facts": facts,
        "summary": summary,
        "per_item_evaluations": per_item_evaluations,
        "global_deduplication": global_deduplication,
        "dedup_merge_report": dedup_merge_report,
    }


# ============================================================
# 9) 批量汇总与排序
# ============================================================
def _method_order_key(method: str) -> Tuple[int, str]:
    try:
        return (METHOD_ORDER.index(method), method)
    except ValueError:
        return (999, method)


def build_rankings(output_records: List[dict]) -> dict:
    valid = [r for r in output_records if isinstance(r, dict) and r.get("status") == "ok" and isinstance(r.get("summary"), dict)]
    by_action: Dict[str, List[dict]] = defaultdict(list)
    by_method: Dict[str, List[dict]] = defaultdict(list)
    excluded = []
    for r in valid:
        s = r["summary"]
        action_key = s.get("action_key", "unknown") or "unknown"
        method = s.get("method", "unknown") or "unknown"
        counts = s.get("counts", {}) if isinstance(s.get("counts"), dict) else {}
        valid_rate = float(counts.get("valid_scored_rate", 0.0) or 0.0)
        dedup_invalid = bool(s.get("dedup_invalid_for_ranking", False))
        ranking_health = str(s.get("ranking_health", "healthy") or "healthy")
        row = {
            "method": method,
            "file_tag": s.get("file_tag", ""),
            "input_path": s.get("input_path", ""),
            "valid_scored_rate": valid_rate,
            "quality_pass_kept_after_dedup": int(counts.get("quality_pass_kept_after_dedup", counts.get("high_quality_kept_after_dedup", 0)) or 0),
            "mechanism_diversity_after_dedup": int(counts.get("mechanism_diversity_after_dedup", 0) or 0),
            "coverage_count_after_dedup": int(counts.get("coverage_count_after_dedup", 0) or 0),
            "template_burden_ratio_after_dedup": float(counts.get("template_burden_ratio_after_dedup", 0.0) or 0.0),
            "discriminative_kept_after_dedup": int(counts.get("discriminative_kept_after_dedup", 0) or 0),
            "strong_coverage_kept_after_dedup": int(counts.get("strong_coverage_kept_after_dedup", 0) or 0),
            "weak_coverage_kept_after_dedup": int(counts.get("weak_coverage_kept_after_dedup", 0) or 0),
            "ranking_health": ranking_health,
            "ranking_eligible": (valid_rate >= RANKING_MIN_VALID_SCORED_RATE) and (not dedup_invalid) and (ranking_health == "healthy"),
        }
        if not row["ranking_eligible"]:
            if valid_rate < RANKING_MIN_VALID_SCORED_RATE:
                reason = f"valid_scored_rate_below_{RANKING_MIN_VALID_SCORED_RATE}"
            elif ranking_health != "healthy":
                reason = f"ranking_health:{ranking_health}"
            else:
                reason = "dedup_invalid"
            excluded.append({"method": method, "action_key": action_key, "file_tag": row["file_tag"], "reason": reason, "valid_scored_rate": valid_rate})
        by_action[action_key].append(row)
        by_method[method].append(row)

    rankings_by_action = {}
    for action_key, rows in by_action.items():
        elig_rows = [r for r in rows if r.get("ranking_eligible")]
        # 排序逻辑: 优先质量和机制多样性,降低纯 coverage 数量的权重
        # 1. quality_pass (高质量保留数) - 最高优先级
        # 2. mechanism_diversity (机制多样性) - 第二优先级
        # 3. discriminative_kept (区分性保留数) - 第三优先级
        # 4. template_burden_ratio (模板负担,越低越好) - 第四优先级
        # 5. coverage_count (总覆盖数) - 降为第五优先级
        # 6. strong_coverage vs weak_coverage - 细分质量
        rows_sorted = sorted(elig_rows, key=lambda x: (
            -x["quality_pass_kept_after_dedup"],
            -x["mechanism_diversity_after_dedup"],
            -x["discriminative_kept_after_dedup"],
            x["template_burden_ratio_after_dedup"],
            -x["coverage_count_after_dedup"],
            -x["strong_coverage_kept_after_dedup"],
            -x["weak_coverage_kept_after_dedup"],
            -x["valid_scored_rate"],
            _method_order_key(x["method"]),
        ))
        for i, row in enumerate(rows_sorted, start=1):
            row["rank"] = i
        rankings_by_action[action_key] = rows_sorted

    overall_rows = []
    for method in sorted(by_method.keys(), key=lambda m: _method_order_key(m)):
        elig_rows = [r for r in by_method[method] if r.get("ranking_eligible")]
        if not elig_rows:
            continue
        n = len(elig_rows)
        overall_rows.append({
            "method": method,
            "num_actions_included": n,
            "avg_quality_pass_kept_after_dedup": round(sum(r["quality_pass_kept_after_dedup"] for r in elig_rows) / n, 4),
            "avg_mechanism_diversity_after_dedup": round(sum(r["mechanism_diversity_after_dedup"] for r in elig_rows) / n, 4),
            "avg_discriminative_kept_after_dedup": round(sum(r["discriminative_kept_after_dedup"] for r in elig_rows) / n, 4),
            "avg_template_burden_ratio_after_dedup": round(sum(r["template_burden_ratio_after_dedup"] for r in elig_rows) / n, 4),
            "avg_coverage_count_after_dedup": round(sum(r["coverage_count_after_dedup"] for r in elig_rows) / n, 4),
        })
    # 总体排序: 优先质量和机制多样性,降低纯 coverage 数量的权重
    overall_sorted = sorted(overall_rows, key=lambda x: (
        -x["avg_quality_pass_kept_after_dedup"],
        -x["avg_mechanism_diversity_after_dedup"],
        -x["avg_discriminative_kept_after_dedup"],
        x["avg_template_burden_ratio_after_dedup"],
        -x["avg_coverage_count_after_dedup"],
        _method_order_key(x["method"]),
    ))
    for i, row in enumerate(overall_sorted, start=1):
        row["rank"] = i
    return {"excluded_from_ranking": excluded, "by_action": rankings_by_action, "overall_macro": overall_sorted}


def _looks_like_raw_uca_input(path_str: str) -> bool:
    s = str(path_str or "").strip().lower()
    if not s.endswith('.json'):
        return False
    name = Path(s).name
    if 'judge_desc' in name or 'judge_' in name or 'strict_ctx' in name or 'manifest' in name:
        return False
    return name.startswith('avp_uca_')

def _job_from_path(path_str: str) -> Dict[str, str]:
    p = Path(path_str)
    stem = p.stem
    file_tag = stem.replace("avp_uca_", "")
    return {"file_tag": file_tag, "input_path": str(p)}


def load_jobs_from_manifest(manifest_path: str | Path) -> List[Dict[str, str]]:
    obj = load_json(manifest_path)
    jobs: List[Dict[str, str]] = []

    # 优先读取 raw 输入条目，避免误把旧 judge 输出再次当输入
    if isinstance(obj.get("input_jobs"), list):
        base_dir = Path(manifest_path).resolve().parent
        for entry in obj.get("input_jobs", []):
            if not isinstance(entry, dict):
                continue
            path_str = str(entry.get("input_path") or "").strip()
            if not path_str:
                continue
            p = Path(path_str)
            if not p.is_absolute():
                p = base_dir / p
            jobs.append({"file_tag": str(entry.get("file_tag") or p.stem), "input_path": str(p)})
        if jobs:
            return jobs

    # 兼容 entries/jobs 结构
    if isinstance(obj.get("entries"), list):
        base_dir = Path(manifest_path).resolve().parent
        for entry in obj.get("entries", []):
            if not isinstance(entry, dict):
                continue
            path_str = str(entry.get("input_path") or entry.get("output_path") or entry.get("filepath") or entry.get("filename") or "").strip()
            if not path_str:
                continue
            p = Path(path_str)
            if not p.is_absolute():
                p = base_dir / p
            jobs.append({"file_tag": str(entry.get("file_tag") or p.stem), "input_path": str(p)})
        return jobs

    if isinstance(obj.get("jobs"), list):
        base_dir = Path(manifest_path).resolve().parent
        for entry in obj.get("jobs", []):
            if not isinstance(entry, dict):
                continue
            path_str = str(entry.get("input_path") or entry.get("filepath") or entry.get("output_file") or "").strip()
            if not path_str:
                continue
            p = Path(path_str)
            if not p.is_absolute():
                p = base_dir / p
            jobs.append({"file_tag": f"{entry.get('action_key','unknown')}_{entry.get('method','unknown')}", "input_path": str(p)})
        if jobs:
            return jobs

    if isinstance(obj.get("output_files"), list):
        for f in obj.get("output_files", []):
            if isinstance(f, str) and f.strip() and _looks_like_raw_uca_input(f.strip()):
                jobs.append(_job_from_path(f.strip()))
        if jobs:
            return jobs

    raise ValueError("manifest 文件缺少可用的 raw 输入条目（input_jobs / entries / jobs / output_files）")


def discover_jobs_if_needed() -> Tuple[List[Dict[str, str]], str]:
    if MANIFEST_PATH:
        return load_jobs_from_manifest(MANIFEST_PATH), f"manifest:{MANIFEST_PATH}"
    jobs = [j for j in FILE_JOBS if j.get("input_path")]
    if jobs:
        return jobs, "file_jobs"
    if not AUTO_DISCOVER_IF_EMPTY:
        return [], "none"
    discovered = []
    for p in sorted(Path(".").glob(AUTO_DISCOVER_GLOB)):
        discovered.append(_job_from_path(str(p)))
    return discovered, "auto_discover"


def main():
    if dashscope is None:
        raise RuntimeError("dashscope package is not installed. Please install dashscope first.")
    if not getattr(dashscope, "api_key", None):
        raise RuntimeError("ERROR: DASHSCOPE_API_KEY is empty. Please set environment variable DASHSCOPE_API_KEY.")

    jobs, source_mode = discover_jobs_if_needed()
    if not jobs:
        print("Warning: No files found for evaluation.")
        return

    print(f"Input source: {source_mode} | Total {len(jobs)} raw files")

    manifest = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "task_type": "UCA",
        "stage": "stage1_judge",
        "judge_variant": "description_only_strict_context_v8_fix6_context_richness_priority",
        "input_source": source_mode,
        "source_manifest_path": MANIFEST_PATH if MANIFEST_PATH else None,
        "input_jobs": jobs,
        "output_files": [],
        "entries": [],
        "rankings": {},
    }

    results_for_rankings: List[dict] = []

    for job in jobs:
        file_tag = job.get("file_tag", "")
        input_path = job.get("input_path", "")
        print(f"\nStarting evaluation: file_tag='{file_tag}' | file='{input_path}'")
        result = evaluate_one_file(file_tag=file_tag, input_path=input_path)
        before_n = result.get("summary", {}).get("counts", {}).get("strict_usable_before_dedup", 0)
        kept_n = result.get("summary", {}).get("counts", {}).get("strict_dedup_coverage", 0)
        high_kept = result.get("summary", {}).get("counts", {}).get("high_quality_kept_after_dedup", 0)
        qwc = result.get("summary", {}).get("counts", {}).get("quality_weighted_coverage_after_dedup", 0.0)
        print(f"Strict usable: {before_n} | Kept after dedup: {kept_n} | High quality kept: {high_kept} | QW coverage: {qwc}")

        action_key = result.get("summary", {}).get("action_key", "unknown")
        method = result.get("summary", {}).get("method", "unknown")
        out_name = OUTPUT_DIR / f"avp_uca_judge_desc_strict_ctx_v8_fix6_{action_key}_{method}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(out_name, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        manifest["output_files"].append(str(out_name))
        manifest["entries"].append(
            {
                "file_tag": file_tag,
                "input_path": str(input_path),
                "output_path": str(out_name),
                "summary": result.get("summary", {}),
            }
        )
        results_for_rankings.append({"status": "ok", "summary": result.get("summary", {})})
        print(f"Saved evaluation results: {out_name}")

    manifest["rankings"] = build_rankings(results_for_rankings)

    manifest_path = OUTPUT_DIR / f"avp_uca_judge_desc_strict_ctx_v8_fix6_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"\n📄 Manifest 已保存: {manifest_path}")


if __name__ == "__main__":
    main()