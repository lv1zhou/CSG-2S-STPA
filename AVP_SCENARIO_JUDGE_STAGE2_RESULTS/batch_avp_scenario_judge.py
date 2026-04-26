import os
import json
import re
import time
import random
import socket
import hashlib
import threading
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import dashscope


# ============================================================
# 0) 全局配置（速度 & 稳定性优先）
# ============================================================
# ============================================================
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "").strip() or "sk-95f5f1db1b2942578c66196757f59f23"
if DASHSCOPE_API_KEY:
    dashscope.api_key = DASHSCOPE_API_KEY

# ✅ DashScope Base URL（按需设置；支持自动 fallback）
DASHSCOPE_BASE_URL = os.getenv("DASHSCOPE_BASE_URL", "").strip()
if DASHSCOPE_BASE_URL:
    dashscope.base_http_api_url = DASHSCOPE_BASE_URL

MODEL_NAME = os.getenv("MODEL_NAME", "qwen3-max").strip()

OUTPUT_DIR = os.getenv("STPA_JUDGE_OUTPUT_DIR", ".").strip()
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 重试配置
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "1.2"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.02"))  # 降低并发尖峰
ENABLE_LENIENT_JSON_EXTRACT_FOR_DEBUG = os.getenv("ENABLE_LENIENT_JSON_EXTRACT_FOR_DEBUG", "1").strip() != "0"

# 并发
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
MAX_WORKERS = max(1, min(MAX_WORKERS, 16))

# token 上限（更小更快；可用环境变量覆盖）
MAX_TOKENS_ITEM = int(os.getenv("MAX_OUTPUT_TOKENS_ITEM", "900"))
MAX_TOKENS_DEDUP = int(os.getenv("MAX_OUTPUT_TOKENS_DEDUP", "3600"))

# dedup batching
DEDUP_BATCH_SIZE = int(os.getenv("DEDUP_BATCH_SIZE", "20"))
DEDUP_MAX_ITEMS_TOTAL = int(os.getenv("DEDUP_MAX_ITEMS_TOTAL", "300"))

# “合理可用”标签规则（绝对阈值，不做分位）
USABLE_MIN_TOTAL = int(os.getenv("USABLE_MIN_TOTAL", "9"))
USABLE_MIN_B2 = int(os.getenv("USABLE_MIN_B2", "2"))
USABLE_MIN_B3 = int(os.getenv("USABLE_MIN_B3", "1"))
USABLE_MIN_B4 = int(os.getenv("USABLE_MIN_B4", "1"))
USABLE_MIN_B1 = int(os.getenv("USABLE_MIN_B1", "1"))

# 你要评测的文件（两种入口都支持）
# 1) FILE_JOBS: 保留原始逐文件索引方式（可手工填写）
# 2) MANIFEST_PATH: 使用批量生成脚本输出的 manifest 作为索引
FILE_JOBS = [
    # 旧式逐文件入口示例（可继续使用）
    # {"file_tag": "zero_shot_forward_drive", "input_path": "scenario_zero_shot_forward_drive.json", "action_key": "forward_drive"},
    # {"file_tag": "few_1_shot_search_slot", "input_path": "scenario_few_1_shot_search_slot.json", "action_key": "search_slot"},
]
MANIFEST_PATH = r"C:\Users\32401\PycharmProjects\PythonProject\scenario_batch_manifest_20260407_113300.json"
PREFER_MANIFEST = True
WRITE_BATCH_SUMMARY = os.getenv("WRITE_BATCH_SUMMARY", "1").strip() != "0"
ALLOW_MISSING_INPUTS = os.getenv("ALLOW_MISSING_INPUTS", "1").strip() != "0"



# ============================================================
# 1) Facts（AVP 四个控制动作；Judge 仍只看到当前动作的 facts）
#    注意：评分逻辑/提示词不变，只把单动作 FACTS 扩成 action_key->facts 的索引
# ============================================================
AVP_HAZARDS = [
    {"id": "H-AVP-1", "description": "车辆与行人、车辆、障碍物或车位边界发生碰撞。"},
    {"id": "H-AVP-2", "description": "车辆越出目标停车区域或停车边界，造成财产损失。"},
    {"id": "H-AVP-3", "description": "车辆无法完成自动泊车任务，或停留在不安全/不可接受位置。"},
    {"id": "H-AVP-4", "description": "车辆出现非预期运动、错误轨迹或不必要的急停/急动，导致周边风险上升。"},
]

ACTION_FACTS: Dict[str, Dict[str, Any]] = {
    "forward_drive": {
        "system": {"system_type": "联网自动驾驶汽车(CAV) - 自动代客泊车(AVP)系统"},
        "control_action": {
            "action_description": "AVP系统提供前进驱动指令",
            "controller": "控制器-AVP泊车决策与控制模块",
            "controlled_process": "车辆纵向/横向泊车轨迹控制",
        },
        "uca_list": [
            {"uca_id": "UCA-1", "description": "前进驱动被不安全地提供，导致碰撞或驶入危险区域。"},
            {"uca_id": "UCA-2", "description": "前进驱动的边界控制或持续过程不当，导致越界或财产损失。"},
            {"uca_id": "UCA-3", "description": "应提供前进驱动时未提供，导致无法完成泊车或停留在不可接受位置。"},
            {"uca_id": "UCA-4", "description": "前进驱动的时机或持续时间不当，导致非预期运动或风险上升。"},
        ],
        "control_algorithm_description": [
            {"id": "CA-AVP-FD-1", "description": "路径有效、过程模型判断前方可通行且系统处于允许前进的泊车状态时，控制算法应发出前进驱动指令。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3", "UCA-4"]},
            {"id": "CA-AVP-FD-2", "description": "当车辆接近目标终点、边界阈值或出现越界趋势时，控制算法应停止继续下发前进驱动指令。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-4"]},
            {"id": "CA-AVP-FD-3", "description": "当前进驱动执行确认超时或回读不一致时，控制算法应停止前进驱动并进入降级/等待。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
        ],
        "process_model_description": [
            {"id": "PM-AVP-FD-1", "description": "环境位置信念：控制器相信其能正确获知车辆位置、障碍物距离与可通行空间。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-4"]},
            {"id": "PM-AVP-FD-2", "description": "任务与路径信念：控制器相信路径是最新且完整的，并相信模式、档位和路径阶段一致。", "linked_uca_ids": ["UCA-2", "UCA-3"]},
            {"id": "PM-AVP-FD-3", "description": "执行效果信念：控制器相信前进指令会产生期望位移，且可通过回读验证执行是否成功。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
        ],
        "feedback_path_description": [
            {"id": "FB-AVP-FD-1", "description": "超声波近距障碍物测距、置信度与异常丢失信息。", "linked_uca_ids": ["UCA-1", "UCA-2"]},
            {"id": "FB-AVP-FD-2", "description": "车辆CAN轮速、偏航、转角与档位回读，可能存在延迟或丢包。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "FB-AVP-FD-3", "description": "执行确认、扭矩、ACK与故障码反馈。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "FB-AVP-FD-4", "description": "上位机路径通信的序列号、时间戳、丢包与延迟状态。", "linked_uca_ids": ["UCA-3"]},
        ],
    },
    "search_slot": {
        "system": {"system_type": "联网自动驾驶汽车(CAV) - 自动代客泊车(AVP)系统"},
        "control_action": {
            "action_description": "AVP系统执行搜索车位/确认候选车位",
            "controller": "控制器-AVP感知与泊车决策模块",
            "controlled_process": "候选车位检测、确认与选择状态",
        },
        "uca_list": [
            {"uca_id": "UCA-1", "description": "系统错误确认或选择车位，导致后续泊车进入危险区域。"},
            {"uca_id": "UCA-2", "description": "系统应确认可用车位时未确认，导致无法完成泊车或任务中断。"},
            {"uca_id": "UCA-3", "description": "车位搜索或确认时机不当，导致错过目标车位或晚确认。"},
            {"uca_id": "UCA-4", "description": "搜索过程持续时间不当或过早终止，导致车位选择结果失真。"},
        ],
        "control_algorithm_description": [
            {"id": "CA-AVP-SS-1", "description": "APA激活且速度低于阈值时，系统基于侧向感知和视觉线索启动候选车位搜索。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3", "UCA-4"]},
            {"id": "CA-AVP-SS-2", "description": "仅当几何尺寸、可通行空间与边界约束共同满足时，系统才确认候选车位可用于泊车。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "CA-AVP-SS-3", "description": "若证据不一致、确认超时或车辆已越过候选区域，系统应取消确认并重新搜索。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3", "UCA-4"]},
        ],
        "process_model_description": [
            {"id": "PM-AVP-SS-1", "description": "车位几何与占用状态信念：系统相信其能正确识别车位边界、尺寸和占用情况。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "PM-AVP-SS-2", "description": "自车相对车位位置与已越过距离信念：系统相信其能正确判断车辆与候选车位的相对关系。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "PM-AVP-SS-3", "description": "候选车位确认置信度与搜索状态信念：系统相信当前搜索状态和确认置信度是可靠的。", "linked_uca_ids": ["UCA-1", "UCA-3", "UCA-4"]},
        ],
        "feedback_path_description": [
            {"id": "FB-AVP-SS-1", "description": "侧向超声波距离、异常回波与丢失信息。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "FB-AVP-SS-2", "description": "环视摄像头提供停车线、路缘与邻车轮廓信息。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "FB-AVP-SS-3", "description": "轮速里程计与自车位姿估计信息。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "FB-AVP-SS-4", "description": "APA模式、档位与HMI确认反馈。", "linked_uca_ids": ["UCA-3", "UCA-4"]},
        ],
    },
    "emergency_brake": {
        "system": {"system_type": "联网自动驾驶汽车(CAV) - 自动代客泊车(AVP)系统"},
        "control_action": {
            "action_description": "AVP系统提供紧急制动指令",
            "controller": "控制器-AVP安全监控与制动决策模块",
            "controlled_process": "车辆减速度、制动建立与停车状态",
        },
        "uca_list": [
            {"uca_id": "UCA-1", "description": "系统不必要地提供紧急制动，导致非预期急停或周边风险上升。"},
            {"uca_id": "UCA-2", "description": "系统应提供紧急制动时未提供，导致碰撞或风险未解除。"},
            {"uca_id": "UCA-3", "description": "紧急制动触发时机不当，导致过早或过晚制动。"},
            {"uca_id": "UCA-4", "description": "紧急制动解除或持续时间不当，导致次生风险或任务异常。"},
        ],
        "control_algorithm_description": [
            {"id": "CA-AVP-EB-1", "description": "检测到即将碰撞或碰撞时间低于阈值时，系统应立即触发紧急制动。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "CA-AVP-EB-2", "description": "在风险解除或车辆完全停止前，系统应保持制动状态。", "linked_uca_ids": ["UCA-3", "UCA-4"]},
            {"id": "CA-AVP-EB-3", "description": "若制动执行未确认或制动力异常，系统应进入安全停靠/告警模式。", "linked_uca_ids": ["UCA-2", "UCA-4"]},
        ],
        "process_model_description": [
            {"id": "PM-AVP-EB-1", "description": "障碍物距离与相对速度风险信念：系统相信其能正确判断碰撞风险水平。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "PM-AVP-EB-2", "description": "自车速度、制动能力与剩余停车距离信念：系统相信其能正确估计可停止能力。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "PM-AVP-EB-3", "description": "制动指令已执行且制动力建立成功的信念：系统相信执行链已按预期响应。", "linked_uca_ids": ["UCA-2", "UCA-4"]},
        ],
        "feedback_path_description": [
            {"id": "FB-AVP-EB-1", "description": "近距障碍物感知反馈（超声波/视觉）。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "FB-AVP-EB-2", "description": "轮速、减速度和IMU反馈。", "linked_uca_ids": ["UCA-2", "UCA-3", "UCA-4"]},
            {"id": "FB-AVP-EB-3", "description": "制动压力、执行ACK与故障码反馈。", "linked_uca_ids": ["UCA-2", "UCA-4"]},
            {"id": "FB-AVP-EB-4", "description": "CAN总线延迟、丢包与通信状态。", "linked_uca_ids": ["UCA-3", "UCA-4"]},
        ],
    },
    "lateral_control": {
        "system": {"system_type": "联网自动驾驶汽车(CAV) - 自动代客泊车(AVP)系统"},
        "control_action": {
            "action_description": "AVP系统提供转向/横向控制指令",
            "controller": "控制器-AVP横向规划与控制模块",
            "controlled_process": "车辆横向位置、航向角与轨迹跟踪状态",
        },
        "uca_list": [
            {"uca_id": "UCA-1", "description": "横向控制被不安全地提供，导致车辆擦碰边界或障碍物。"},
            {"uca_id": "UCA-2", "description": "应提供横向控制时未提供，导致无法对准车位或轨迹偏离。"},
            {"uca_id": "UCA-3", "description": "横向控制时机不当，导致轨迹偏差累积或调整过晚。"},
            {"uca_id": "UCA-4", "description": "横向控制持续时间不当，导致位置失准或非预期运动。"},
        ],
        "control_algorithm_description": [
            {"id": "CA-AVP-LAT-1", "description": "系统依据目标轨迹与位姿误差计算转向命令，并持续进行轨迹跟踪。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3", "UCA-4"]},
            {"id": "CA-AVP-LAT-2", "description": "接近边界或侧向障碍物时，系统应限制转角与横向推进，避免擦碰和越界。", "linked_uca_ids": ["UCA-1", "UCA-3", "UCA-4"]},
            {"id": "CA-AVP-LAT-3", "description": "若转向执行回读异常或路径状态不一致，系统应保持/停止横向控制并进入保守模式。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-4"]},
        ],
        "process_model_description": [
            {"id": "PM-AVP-LAT-1", "description": "自车横向位姿与航向误差信念：系统相信其能正确估计横向偏差与姿态。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "PM-AVP-LAT-2", "description": "目标轨迹曲率、边界位置与安全裕度信念：系统相信目标轨迹和边界约束是准确的。", "linked_uca_ids": ["UCA-1", "UCA-3", "UCA-4"]},
            {"id": "PM-AVP-LAT-3", "description": "转向执行效果已被正确跟踪的信念：系统相信执行链路与车身响应保持一致。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-4"]},
        ],
        "feedback_path_description": [
            {"id": "FB-AVP-LAT-1", "description": "环视摄像头提供停车线、边界与路缘识别信息。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "FB-AVP-LAT-2", "description": "转向角回读、执行ACK与故障状态反馈。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-4"]},
            {"id": "FB-AVP-LAT-3", "description": "偏航率、横向位移与位姿估计反馈。", "linked_uca_ids": ["UCA-1", "UCA-2", "UCA-3"]},
            {"id": "FB-AVP-LAT-4", "description": "侧向障碍物近距测距反馈。", "linked_uca_ids": ["UCA-1", "UCA-3", "UCA-4"]},
        ],
    },
}

DEFAULT_ACTION_KEY = "forward_drive"

def compact_facts(facts: dict) -> dict:
    def _pick(items: list, keys: List[str]) -> List[dict]:
        out = []
        if not isinstance(items, list):
            return out
        for it in items:
            if not isinstance(it, dict):
                continue
            d = {}
            for k in keys:
                if k in it:
                    d[k] = it[k]
            out.append(d)
        return out

    return {
        "system_type": (facts.get("system", {}) or {}).get("system_type", ""),
        "control_action": (facts.get("control_action", {}) or {}).get("action_description", ""),
        "controller": (facts.get("control_action", {}) or {}).get("controller", ""),
        "uca_list": _pick(facts.get("uca_list", []), ["uca_id", "description"]),
        "CA": _pick(facts.get("control_algorithm_description", []), ["id", "description"]),
        "PM": _pick(facts.get("process_model_description", []), ["id", "description"]),
        "FB": _pick(facts.get("feedback_path_description", []), ["id", "description"]),
        "note": "评分只看 candidate_scenario 的逻辑质量；facts 仅用于核对UCA/CA/PM/FB锚点是否具体且一致。",
    }

ACTION_FACTS_COMPACT: Dict[str, dict] = {k: compact_facts(v) for k, v in ACTION_FACTS.items()}
ACTION_VALID_UCA_IDS: Dict[str, set] = {
    k: {u["uca_id"] for u in (v.get("uca_list", []) or []) if isinstance(u, dict) and isinstance(u.get("uca_id"), str)}
    for k, v in ACTION_FACTS.items()
}

ANCHOR_ID_RE = re.compile(r"\b(?:CA|PM|FB)-[A-Z0-9\-]+\b")

def infer_action_key_from_name(name: str) -> str:
    s = (name or "").lower()
    if "search_slot" in s:
        return "search_slot"
    if "emergency_brake" in s:
        return "emergency_brake"
    if "lateral_control" in s:
        return "lateral_control"
    if "forward_drive" in s:
        return "forward_drive"
    return DEFAULT_ACTION_KEY

def get_action_bundle(action_key: Optional[str]) -> Tuple[str, dict, dict, set]:
    ak = (action_key or "").strip() or DEFAULT_ACTION_KEY
    if ak not in ACTION_FACTS:
        ak = infer_action_key_from_name(ak)
    if ak not in ACTION_FACTS:
        ak = DEFAULT_ACTION_KEY
    return ak, ACTION_FACTS[ak], ACTION_FACTS_COMPACT[ak], ACTION_VALID_UCA_IDS[ak]


def _manifest_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(MANIFEST_PATH)) if MANIFEST_PATH else os.getcwd()
    except Exception:
        return os.getcwd()

def _normalize_manifest_input_path(raw_path: str, manifest_dir: str) -> str:
    p = str(raw_path or "").strip()
    if not p:
        return ""
    if os.path.isabs(p):
        return p
    return os.path.normpath(os.path.join(manifest_dir, p))

def _append_manifest_entry_jobs(jobs: List[dict], entries: list, manifest_dir: str, debug: dict) -> None:
    debug["manifest_schema"] = "entries"
    debug["manifest_entries"] = len(entries)
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        raw_path = entry.get("filepath") or entry.get("output_file") or entry.get("output_path") or entry.get("input_path") or entry.get("filename") or ""
        input_path = _normalize_manifest_input_path(raw_path, manifest_dir)
        if not input_path:
            continue
        method_key = str(entry.get("method_key") or entry.get("method_name") or entry.get("method") or "untagged").strip()
        action_key = str(entry.get("action_key") or infer_action_key_from_name(input_path)).strip()
        file_tag = f"{method_key}_{action_key}" if method_key and action_key else (method_key or action_key or "untagged")
        jobs.append({
            "job_source": "manifest",
            "manifest_index": idx,
            "file_tag": file_tag,
            "input_path": input_path,
            "action_key": action_key or DEFAULT_ACTION_KEY,
            "filename": str(entry.get("filename") or os.path.basename(input_path)),
            "manifest_entry": entry,
        })

def _append_manifest_jobs_schema(jobs: List[dict], items: list, manifest_dir: str, debug: dict) -> None:
    debug["manifest_schema"] = "jobs"
    debug["manifest_jobs"] = len(items)
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        raw_path = item.get("output_file") or item.get("output_path") or item.get("filepath") or item.get("input_path") or item.get("filename") or ""
        input_path = _normalize_manifest_input_path(raw_path, manifest_dir)
        if not input_path:
            continue
        method_key = str(item.get("method") or item.get("method_key") or item.get("method_name") or "untagged").strip()
        action_key = str(item.get("action_key") or infer_action_key_from_name(input_path)).strip()
        file_tag = f"{method_key}_{action_key}" if method_key and action_key else (method_key or action_key or "untagged")
        jobs.append({
            "job_source": "manifest",
            "manifest_index": idx,
            "file_tag": file_tag,
            "input_path": input_path,
            "action_key": action_key or DEFAULT_ACTION_KEY,
            "filename": os.path.basename(input_path),
            "manifest_entry": item,
        })

def resolve_jobs() -> Tuple[List[dict], dict]:
    jobs: List[dict] = []
    debug: Dict[str, Any] = {
        "manifest_path": MANIFEST_PATH,
        "manifest_exists": bool(MANIFEST_PATH and os.path.exists(MANIFEST_PATH)),
        "manifest_loaded": False,
        "manual_jobs": len(FILE_JOBS),
    }

    if PREFER_MANIFEST and MANIFEST_PATH and os.path.exists(MANIFEST_PATH):
        try:
            manifest = load_json_any(MANIFEST_PATH)
            manifest_dir = _manifest_dir()

            if isinstance(manifest, dict):
                entries = manifest.get("entries", None)
                if isinstance(entries, list):
                    debug["manifest_loaded"] = True
                    _append_manifest_entry_jobs(jobs, entries, manifest_dir, debug)

                items = manifest.get("jobs", None)
                if isinstance(items, list):
                    debug["manifest_loaded"] = True
                    _append_manifest_jobs_schema(jobs, items, manifest_dir, debug)

                if not debug["manifest_loaded"]:
                    debug["manifest_top_keys"] = list(manifest.keys())

            elif isinstance(manifest, list):
                debug["manifest_loaded"] = True
                debug["manifest_schema"] = "top_list"
                _append_manifest_jobs_schema(jobs, manifest, manifest_dir, debug)

        except Exception as e:
            debug["manifest_error"] = repr(e)

    for idx, job in enumerate(FILE_JOBS):
        if not isinstance(job, dict):
            continue
        input_path = str(job.get("input_path", "")).strip()
        if not input_path:
            continue
        jobs.append({
            "job_source": "manual",
            "manual_index": idx,
            "file_tag": str(job.get("file_tag") or "untagged"),
            "input_path": input_path,
            "action_key": str(job.get("action_key") or infer_action_key_from_name(input_path)),
            "filename": os.path.basename(input_path),
            "manifest_entry": None,
        })

    deduped: List[dict] = []
    seen = set()
    for job in jobs:
        key = (os.path.abspath(job.get("input_path", "")), job.get("action_key", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(job)

    debug["resolved_jobs"] = len(deduped)
    return deduped, debug

DEFAULT_ACTION_KEY, FACTS, FACTS_COMPACT, VALID_UCA_IDS = get_action_bundle(DEFAULT_ACTION_KEY)



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
                return text[start : i + 1]
    return None

def clamp_0_2(x: Any) -> int:
    try:
        v = int(x)
    except Exception:
        return 0
    return 0 if v < 0 else 2 if v > 2 else v


# ============================================================
# 3) 输入文件读取：兼容 dict/runs 与 top-level list(lora)
# ============================================================
def load_json_any(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_runs(data: Any) -> Tuple[List[dict], Dict[str, Any]]:
    if isinstance(data, dict):
        runs = data.get("runs", [])
        meta = data.get("meta", {})
        if not isinstance(runs, list):
            runs = []
        return runs, meta if isinstance(meta, dict) else {}
    if isinstance(data, list):
        runs = [x for x in data if isinstance(x, dict)]
        meta = {"_meta_generated": True, "_source_format": "top_list"}
        return runs, meta
    return [], {"_meta_generated": True, "_source_format": "unknown"}

def get_scenario_json_from_run(run_obj: dict) -> Tuple[Optional[dict], str]:
    ar = run_obj.get("analysis_result")
    if isinstance(ar, dict) and isinstance(ar.get("causal_scenarios"), list):
        return ar, "analysis_result"

    if isinstance(run_obj.get("strict_json"), dict):
        return run_obj["strict_json"], "strict_json"

    for k in ("response_stripped", "response_raw", "response"):
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

def iter_scenarios(scenario_json: dict) -> List[dict]:
    rows: List[dict] = []
    items = scenario_json.get("causal_scenarios", [])
    if not isinstance(items, list):
        return rows
    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            it = {}
        rows.append(
            {
                "index_in_list": idx,
                "scenario_id": it.get("scenario_id", ""),
                "description": it.get("description", ""),
                "linked_uca_ids": it.get("linked_uca_ids", None),
            }
        )
    return rows

def make_unique_scenario_id(raw_id: str, iteration: Any, index_in_list: int, seen_counts: Dict[str, int]) -> str:
    base = (raw_id or "").strip()
    if not base:
        base = f"SC@it{iteration}:{index_in_list}"
    seen_counts[base] = seen_counts.get(base, 0) + 1
    return base if seen_counts[base] == 1 else f"{base}__dup{seen_counts[base]}"


# ============================================================
# 4) Judge Prompt / Schema（“正常打分”+ 强化B2/B3逻辑与细节）
# ============================================================
PROMPT_VERSION = "v2026-02-15.logic_quality_only"

JUDGE_SYSTEM = (
    "你是一名严格、一致性优先的 STPA 致因场景评审员（LLM-as-a-Judge）。\n"
    "你只基于【候选Scenario文本自身的逻辑质量】评分；facts 仅用于核对 UCA/CA/PM/FB 是否具体且一致。\n"
    "不要因为描述更长就扣分；只要逻辑自洽且更具体，应更高分。\n"
    "输出必须且只能是一个 JSON 对象（不要代码块、不要额外解释文字）。\n"
)

JUDGE_ITEM_SCHEMA_HINT = {
    "scenario_id": "",
    "scores": {
        "A": {"score": 0, "rationale": ""},
        "B1": {"score": 0, "rationale": ""},
        "B2": {"score": 0, "rationale": ""},
        "B3": {"score": 0, "rationale": ""},
        "B4": {"score": 0, "rationale": ""},
    },
    "total_score": 0,
    "label": "",
    "issues": [],
    "suggested_fix": "",
}

def build_judge_user_prompt(facts_compact: dict, scenario_item: dict) -> str:
    facts_json = json.dumps(facts_compact, ensure_ascii=False)
    sc_json = json.dumps(scenario_item, ensure_ascii=False)

    # 关键：把 0/1/2 的“可操作条件”写清楚，尤其 B2/B3
    return (
        f"【facts_compact】\n{facts_json}\n\n"
        f"【candidate_scenario】\n{sc_json}\n\n"
        "请对 candidate_scenario 逐项评分（每项 0/1/2），并给出最终标签。\n"
        "注意：\n"
        "- 评分只看 candidate_scenario 的逻辑质量（更具体、更有链条、更可验证 => 更高分）。\n"
        "- 泛化词（如“传感器故障/系统异常/环境复杂”）如果不指明是哪类反馈/过程模型/控制算法环节，不能拿高分。\n"
        "- facts 仅用于核对：UCA 是否有效；CA/PM/FB 的锚点是否具体且不矛盾。\n\n"
        "指标A：结构完整性（0-2）\n"
        "0：缺字段或 description 为空；\n"
        "1：字段齐，但 description 缺少关键要素；\n"
        "2：scenario_id 非空；linked_uca_ids 为非空列表且均为有效 UCA-*；description 至少包含：触发/上下文 + 因果机制 + 导致某些UCA。\n\n"
        "指标B1：上下文质量（0-2）\n"
        "0：几乎全是泛化描述；\n"
        "1：有一些具体上下文（如传感器类型、通信路径、时序/阈值等）但仍偏粗；\n"
        "2：上下文具体可检验（例如明确超声波/环视/CAN/ACK/上位机通信中的一种或多种，并描述触发条件/状态）。\n\n"
        "指标B2：因果链完整性（0-2）【重点：链条清晰就给2】\n"
        "0：基本没有因果链（只说“导致”）；\n"
        "1：只有 1 段因果跳跃，缺少中间环节；\n"
        "2：存在清晰链条，至少覆盖以下中的 3 个层级，并说明衔接：\n"
        "   (a) 触发/上下文(环境/传感器/通信/执行状态)\n"
        "   (b) 中间环节(反馈失真/延迟/丢包/误检/PM错误信念)\n"
        "   (c) 控制决策(错误触发/漏触发/持续下发/停止) 与 CA/控制逻辑相关\n"
        "   (d) 输出行为(发/不发/时序错误/持续过久)\n"
        "   (e) 明确导向某些 UCA（UCA-1/2/3）\n"
        "只要链条自洽且步骤明确，即使很短，也应给 2。\n\n"
        "指标B3：CA/PM/FB 证据锚定与回路一致性（0-2）【重点：具体锚点与解释】\n"
        "0：几乎不涉及控制回路要素，或与 facts 明显矛盾；\n"
        "1：提到回路要素但锚定弱（仅泛称“传感器/通信”）或解释不充分；\n"
        "2：明确锚定并解释其在链条中的作用：\n"
        "   - 直接引用 CA-*/PM-*/FB-* 之一或多个；或\n"
        "   - 至少点名具体证据通道（超声波/环视摄像头/CAN回读/ACK故障码/上位机路径通信），并说明它如何造成 PM/CA 的错误决策。\n"
        "只要锚定具体且不矛盾，应给 2。\n\n"
        "指标B4：UCA 链接一致性（0-2）\n"
        "0：linked_uca_ids 非列表/含无效ID/与描述明显矛盾；\n"
        "1：UCA 链接基本合理但较弱或不够明确；\n"
        "2：UCA 链接明确且与因果链一致。\n\n"
        "total_score = A+B1+B2+B3+B4（0-10）。\n\n"
        "标签规则（使用绝对阈值，不做分位）：\n"
        f"- 合理可用：A=2 且 total_score>={USABLE_MIN_TOTAL} 且 B1>={USABLE_MIN_B1} 且 B2>={USABLE_MIN_B2} 且 B3>={USABLE_MIN_B3} 且 B4>={USABLE_MIN_B4}\n"
        "- 不合理不可用：total_score<=5 或 A<2 或 B2=0 或 B4=0\n"
        "- 其他：合理不可用\n\n"
        "只输出以下 JSON 结构（字段必须齐全、不得新增字段）。rationale 请尽量短（<=30字）：\n"
        f"{json.dumps(JUDGE_ITEM_SCHEMA_HINT, ensure_ascii=False)}\n"
    )

def label_by_rule(scores: Dict[str, int]) -> str:
    A, B1, B2, B3, B4 = scores["A"], scores["B1"], scores["B2"], scores["B3"], scores["B4"]
    total = A + B1 + B2 + B3 + B4
    if (A == 2 and total >= USABLE_MIN_TOTAL and B1 >= USABLE_MIN_B1 and B2 >= USABLE_MIN_B2 and B3 >= USABLE_MIN_B3 and B4 >= USABLE_MIN_B4):
        return "合理可用"
    if total <= 5 or A < 2 or B2 == 0 or B4 == 0:
        return "不合理不可用"
    return "合理不可用"

def validate_judge_schema(obj: dict) -> Tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "TOP_NOT_DICT"
    if "scores" not in obj or not isinstance(obj["scores"], dict):
        return False, "MISSING_OR_INVALID_scores"
    for k in ["A", "B1", "B2", "B3", "B4"]:
        if k not in obj["scores"] or not isinstance(obj["scores"][k], dict):
            return False, f"MISSING_OR_INVALID_scores.{k}"
        if "score" not in obj["scores"][k]:
            return False, f"MISSING_scores.{k}.score"
    return True, "OK"


# ============================================================
# 5) Dedup Prompt / Schema（仍然只对“合理可用”做；强调“保留更完整/更具体”的版本）
# ============================================================
DEDUP_SYSTEM = (
    "你是一名严格、一致性优先的 STPA 评审员（LLM-as-a-Judge）。\n"
    "你只对【合理可用Scenario集合】做语义去重/合并：同机制同含义合并，不同机制绝不合并。\n"
    "当两条几乎等价时，优先保留：逻辑更完整/更具体/锚点更明确 的那条。\n"
    "输出必须且只能是一个 JSON 对象（不要代码块、不要解释性文字）。\n"
)

DEDUP_SCHEMA_HINT = {
    "deduped_usable_scenarios": [
        {
            "scenario_id": "",
            "summary": "",
            "mechanism_signature": "",
            "merged_linked_uca_ids": []
        }
    ],
    "removed_as_duplicate": [
        {
            "removed_scenario_id": "",
            "kept_scenario_id": "",
            "reason": "semantic_equivalent"
        }
    ],
    "coverage_count": 0,
}

def build_dedup_user_prompt(facts_compact: dict, usable_scenarios: List[dict]) -> str:
    facts_json = json.dumps(facts_compact, ensure_ascii=False)
    usable_json = json.dumps(usable_scenarios, ensure_ascii=False)

    return (
        f"【facts_compact】\n{facts_json}\n\n"
        f"【usable_scenarios】\n{usable_json}\n\n"
        "任务：对 usable_scenarios 做语义去重/包含关系合并，并给出每个保留条目的 mechanism_signature。\n\n"
        "合并原则（严格执行）：\n"
        "1) 机制/根因不同 => 绝不合并。\n"
        "2) 机制相同且语义等价/包含 => 允许合并。\n"
        "3) 若表述几乎相同但 linked_uca_ids 不同：允许合并，保留条目的 merged_linked_uca_ids 为并集，reason=same_text_diff_uca_merge。\n"
        "4) 当需要在等价条目中选择 kept：优先保留“更具体、更完整、锚点更明确”的版本。\n\n"
        "mechanism_signature 格式（必须稳定可读）：\n"
        "  <ROOTCAUSE_TYPE>|anchors=<sorted_ids_or_none>|key=<3~6字核心机制>\n"
        "ROOTCAUSE_TYPE 只能取：ultrasonic, camera, can_comm, algorithm, process_model, actuator, compute_hw, upstream_comm, other\n"
        "anchors 为 description 中明确提到或可严格对应的 CA-*/PM-*/FB-* 的ID集合（无则 anchors=none）。\n\n"
        "输出约束：\n"
        "- deduped_usable_scenarios[*].scenario_id 必须来自输入。\n"
        "- removed_as_duplicate[*].kept_scenario_id 必须出现在 deduped_usable_scenarios。\n"
        "- summary <= 25字；reason 只能是：semantic_equivalent / context_inclusion / same_text_diff_uca_merge\n\n"
        f"只输出以下 JSON 结构（字段必须齐全）：\n{json.dumps(DEDUP_SCHEMA_HINT, ensure_ascii=False)}\n"
    )

def validate_dedup_schema(obj: dict) -> Tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "TOP_NOT_DICT"
    if "deduped_usable_scenarios" not in obj or not isinstance(obj["deduped_usable_scenarios"], list):
        return False, "MISSING_OR_INVALID_deduped_usable_scenarios"
    if "removed_as_duplicate" not in obj or not isinstance(obj["removed_as_duplicate"], list):
        return False, "MISSING_OR_INVALID_removed_as_duplicate"
    if "coverage_count" not in obj:
        return False, "MISSING_coverage_count"
    try:
        int(obj["coverage_count"])
    except Exception:
        return False, "INVALID_coverage_count"
    for it in obj["deduped_usable_scenarios"]:
        if not isinstance(it, dict):
            return False, "INVALID_deduped_item_not_dict"
        if not isinstance(it.get("scenario_id", ""), str) or not it.get("scenario_id", "").strip():
            return False, "INVALID_deduped_item_missing_scenario_id"
        if "merged_linked_uca_ids" not in it or not isinstance(it["merged_linked_uca_ids"], list):
            return False, "INVALID_deduped_item_missing_merged_linked_uca_ids"
        if not isinstance(it.get("mechanism_signature", ""), str):
            return False, "INVALID_deduped_item_missing_mechanism_signature"
    for r in obj["removed_as_duplicate"]:
        if not isinstance(r, dict):
            return False, "INVALID_removed_item_not_dict"
        if r.get("reason") not in ("semantic_equivalent", "context_inclusion", "same_text_diff_uca_merge"):
            return False, "INVALID_removed_reason"
    return True, "OK"


# ============================================================
# 6) DashScope endpoint 探测与轮换（网络抖动更稳）
# ============================================================
DEFAULT_ENDPOINTS = [
    ("cn", "https://dashscope.aliyuncs.com/api/v1"),
    ("intl", "https://dashscope-intl.aliyuncs.com/api/v1"),
    ("us", "https://dashscope-us.aliyuncs.com/api/v1"),
]

def _host_from_base_url(base_url: str) -> str:
    m = re.match(r"^https?://([^/]+)", base_url.strip())
    return m.group(1) if m else ""

def can_resolve(host: str) -> bool:
    if not host:
        return False
    try:
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def ensure_working_base_url() -> Dict[str, Any]:
    if getattr(dashscope, "base_http_api_url", None):
        cur = dashscope.base_http_api_url
        host = _host_from_base_url(cur)
        if can_resolve(host):
            return {"using": cur, "fallback": False, "reason": "env_or_existing_ok"}
    for region, url in DEFAULT_ENDPOINTS:
        host = _host_from_base_url(url)
        if can_resolve(host):
            dashscope.base_http_api_url = url
            return {"using": url, "fallback": True, "reason": f"resolved_{region}"}
    return {"using": getattr(dashscope, "base_http_api_url", None), "fallback": False, "reason": "no_endpoint_resolvable"}

def rotate_endpoint(current_url: Optional[str]) -> str:
    urls = [u for _, u in DEFAULT_ENDPOINTS]
    if not urls:
        return current_url or ""
    if not current_url:
        return urls[0]
    try:
        i = urls.index(current_url)
        return urls[(i + 1) % len(urls)]
    except ValueError:
        return urls[0]


# ============================================================
# 7) LLM 调用（带 cache，提速且一致）
# ============================================================
_CACHE_LOCK = threading.Lock()
_LLM_CACHE: Dict[str, dict] = {}

def _cache_key(messages: List[dict], max_tokens: int) -> str:
    payload = {"model": MODEL_NAME, "max_tokens": max_tokens, "messages": messages}
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _call_generation_once(messages: List[dict], max_tokens: int):
    return dashscope.Generation.call(
        model=MODEL_NAME,
        messages=messages,
        result_format="message",
        temperature=0.0,
        top_p=1.0,
        max_tokens=max_tokens,
    )

def _is_tls_eof_like_exception(e: Exception) -> bool:
    s = repr(e)
    keywords = ["SSLEOFError", "EOF occurred in violation of protocol", "TLS", "CERTIFICATE_VERIFY_FAILED"]
    return any(k in s for k in keywords)

def call_llm_json(messages: List[dict], max_tokens: int, schema_validator) -> Tuple[bool, Optional[dict], dict]:
    ck = _cache_key(messages, max_tokens)
    with _CACHE_LOCK:
        if ck in _LLM_CACHE:
            return True, _LLM_CACHE[ck], {"cache_hit": True}

    last_meta: dict = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = _call_generation_once(messages, max_tokens=max_tokens)
        except Exception as e:
            if _is_tls_eof_like_exception(e):
                cur = getattr(dashscope, "base_http_api_url", None)
                dashscope.base_http_api_url = rotate_endpoint(cur)

            last_meta = {
                "exception": repr(e),
                "exception_type": type(e).__name__,
                "attempt": attempt,
                "base_url": getattr(dashscope, "base_http_api_url", None),
                "error": "exception",
            }
            sleep_s = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.35)
            time.sleep(sleep_s)
            continue

        last_meta = {
            "status_code": getattr(response, "status_code", None),
            "request_id": getattr(response, "request_id", None),
            "message": getattr(response, "message", None),
            "attempt": attempt,
            "base_url": getattr(dashscope, "base_http_api_url", None),
        }

        status = getattr(response, "status_code", None)

        if status in (HTTPStatus.TOO_MANY_REQUESTS,) or (isinstance(status, int) and status >= 500):
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.6)
                time.sleep(sleep_s)
                continue
            return False, None, {**last_meta, "raw": None, "error": "retry_exhausted_server_or_429"}

        if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            return False, None, {**last_meta, "raw": None, "error": f"auth_failed:{status}"}

        if status != HTTPStatus.OK:
            if attempt < MAX_RETRIES:
                cur = getattr(dashscope, "base_http_api_url", None)
                dashscope.base_http_api_url = rotate_endpoint(cur)
                sleep_s = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.35)
                time.sleep(sleep_s)
                continue
            return False, None, {**last_meta, "raw": None, "error": f"http_status_not_ok:{status}"}

        try:
            raw = response.output.choices[0].message.content
        except Exception as e:
            return False, None, {**last_meta, "raw": None, "error": f"bad_output_structure:{repr(e)}"}

        stripped = strip_code_fences(raw)
        ok, obj, err = try_parse_json_strict(stripped)

        if (not ok) and ENABLE_LENIENT_JSON_EXTRACT_FOR_DEBUG:
            extracted = extract_first_json_object(stripped)
            if extracted:
                ok2, obj2, err2 = try_parse_json_strict(extracted)
                if ok2 and isinstance(obj2, dict):
                    schema_ok, schema_msg = schema_validator(obj2)
                    if not schema_ok:
                        return False, None, {**last_meta, "raw": raw, "parse": "lenient", "schema_error": schema_msg}
                    with _CACHE_LOCK:
                        _LLM_CACHE[ck] = obj2
                    return True, obj2, {**last_meta, "raw": raw, "parse": "lenient"}
                return False, None, {**last_meta, "raw": raw, "parse_error": err2}

        if not ok or not isinstance(obj, dict):
            return False, None, {**last_meta, "raw": raw, "parse_error": err}

        schema_ok, schema_msg = schema_validator(obj)
        if not schema_ok:
            return False, None, {**last_meta, "raw": raw, "parse": "strict", "schema_error": schema_msg}

        with _CACHE_LOCK:
            _LLM_CACHE[ck] = obj
        return True, obj, {**last_meta, "raw": raw, "parse": "strict"}

    return False, None, {**last_meta, "raw": None, "error": "retry_exhausted_exception"}


# ============================================================
# 8) UCA 合法性（客观硬约束）
# ============================================================
def uca_ids_invalid(x: Any, valid_uca_ids: set) -> bool:
    if x is None:
        return True
    if not isinstance(x, list) or len(x) == 0:
        return True
    for v in x:
        if not isinstance(v, str) or v not in valid_uca_ids:
            return True
    return False



# ============================================================
# 9) Dedup batching（避免超上下文）
# ============================================================
def run_dedup_batched(usable_items: List[dict]) -> Tuple[dict, dict, bool]:
    dedup_debug: Dict[str, Any] = {"mode": "batched", "rounds": []}
    usable_items = usable_items[:DEDUP_MAX_ITEMS_TOTAL]

    if len(usable_items) <= DEDUP_BATCH_SIZE:
        msgs = [
            {"role": "system", "content": DEDUP_SYSTEM},
            {"role": "user", "content": build_dedup_user_prompt(FACTS_COMPACT, usable_items)},
        ]
        ok, obj, dbg = call_llm_json(msgs, max_tokens=MAX_TOKENS_DEDUP, schema_validator=validate_dedup_schema)
        dedup_debug["rounds"].append({"round": 1, "size": len(usable_items), "ok": ok, "debug": dbg})
        if not ok or not isinstance(obj, dict):
            return {"deduped_usable_scenarios": [], "removed_as_duplicate": [], "coverage_count": 0}, dedup_debug, False
        obj["coverage_count"] = int(obj.get("coverage_count", 0) or 0)
        return obj, dedup_debug, True

    # 多轮缩减：每轮先“批内去重”，再把 kept 汇总进入下一轮
    current = usable_items
    round_idx = 0
    while True:
        round_idx += 1
        kept_all: List[dict] = []
        removed_all: List[dict] = []
        round_info = {"round": round_idx, "input_size": len(current), "batches": []}

        for bi in range(0, len(current), DEDUP_BATCH_SIZE):
            batch = current[bi : bi + DEDUP_BATCH_SIZE]
            msgs = [
                {"role": "system", "content": DEDUP_SYSTEM},
                {"role": "user", "content": build_dedup_user_prompt(FACTS_COMPACT, batch)},
            ]
            ok, obj, dbg = call_llm_json(msgs, max_tokens=MAX_TOKENS_DEDUP, schema_validator=validate_dedup_schema)
            round_info["batches"].append({"batch_index": bi // DEDUP_BATCH_SIZE, "size": len(batch), "ok": ok, "debug": dbg})
            if not ok or not isinstance(obj, dict):
                return {"deduped_usable_scenarios": [], "removed_as_duplicate": [], "coverage_count": 0}, dedup_debug, False

            kept_ids = [x.get("scenario_id", "") for x in obj.get("deduped_usable_scenarios", []) if isinstance(x, dict)]
            kept_ids = [x for x in kept_ids if isinstance(x, str) and x.strip()]

            id2item = {x["scenario_id"]: x for x in batch if isinstance(x, dict) and isinstance(x.get("scenario_id"), str)}
            for kid in kept_ids:
                if kid in id2item:
                    kept_all.append(id2item[kid])

            if isinstance(obj.get("removed_as_duplicate"), list):
                removed_all.extend([r for r in obj["removed_as_duplicate"] if isinstance(r, dict)])

        dedup_debug["rounds"].append(round_info)

        # 收敛：如果本轮 kept 已经足够小或不再变化，则做一次全量终局 dedup
        if len(kept_all) <= DEDUP_BATCH_SIZE or len(kept_all) >= len(current):
            final_msgs = [
                {"role": "system", "content": DEDUP_SYSTEM},
                {"role": "user", "content": build_dedup_user_prompt(FACTS_COMPACT, kept_all[:DEDUP_MAX_ITEMS_TOTAL])},
            ]
            ok, final_obj, dbg = call_llm_json(final_msgs, max_tokens=MAX_TOKENS_DEDUP, schema_validator=validate_dedup_schema)
            dedup_debug["rounds"].append({"round": round_idx + 1, "final_size": len(kept_all), "ok": ok, "debug": dbg})
            if not ok or not isinstance(final_obj, dict):
                return {"deduped_usable_scenarios": [], "removed_as_duplicate": [], "coverage_count": 0}, dedup_debug, False
            final_obj["coverage_count"] = int(final_obj.get("coverage_count", 0) or 0)
            return final_obj, dedup_debug, True

        current = kept_all


# ============================================================
# 10) 主评测：并发逐条评分 + 去重
# ============================================================
def _judge_one_candidate(candidate: dict, iteration: Any, scenario_source: str, index_in_list: int, facts_compact: dict, valid_uca_ids: set) -> dict:
    messages = [
        {"role": "system", "content": JUDGE_SYSTEM},
        {"role": "user", "content": build_judge_user_prompt(facts_compact, candidate)},
    ]
    ok, judge_obj, debug = call_llm_json(messages, max_tokens=MAX_TOKENS_ITEM, schema_validator=validate_judge_schema)

    if not ok or not isinstance(judge_obj, dict):
        return {
            "scenario_id": candidate["scenario_id"],
            "scenario_id_raw": candidate.get("scenario_id_raw", ""),
            "scores": {"A": 0, "B1": 0, "B2": 0, "B3": 0, "B4": 0},
            "total_score": 0,
            "label": "judge_failed",
            "reasons": {k: "judge_failed" for k in ["A", "B1", "B2", "B3", "B4"]},
            "issues": ["judge_failed"],
            "suggested_fix": "",
            "debug": debug,
            "candidate_scenario": candidate,
            "iteration": iteration,
            "scenario_source": scenario_source,
            "index_in_list": index_in_list,
        }

    sc = judge_obj.get("scores", {}) if isinstance(judge_obj.get("scores"), dict) else {}

    def _score_of(key: str) -> int:
        node = sc.get(key, {})
        if not isinstance(node, dict):
            return 0
        return clamp_0_2(node.get("score", 0))

    def _rat_of(key: str) -> str:
        node = sc.get(key, {})
        if isinstance(node, dict) and isinstance(node.get("rationale"), str):
            return node["rationale"].strip()
        return ""

    A, B1, B2, B3, B4 = _score_of("A"), _score_of("B1"), _score_of("B2"), _score_of("B3"), _score_of("B4")

    issues: List[str] = []
    if uca_ids_invalid(candidate.get("linked_uca_ids", None), valid_uca_ids):
        A = 1 if A == 2 else A
        B4 = 0
        issues.append("invalid_linked_uca_ids")

    total = A + B1 + B2 + B3 + B4
    rule_label = label_by_rule({"A": A, "B1": B1, "B2": B2, "B3": B3, "B4": B4})

    model_label = judge_obj.get("label", "")
    label_mismatch = bool(model_label) and (model_label != rule_label)

    model_issues = judge_obj.get("issues", [])
    if isinstance(model_issues, list):
        for x in model_issues:
            if isinstance(x, str) and x.strip():
                issues.append(x.strip())

    return {
        "scenario_id": candidate["scenario_id"],
        "scenario_id_raw": candidate.get("scenario_id_raw", ""),
        "scores": {"A": A, "B1": B1, "B2": B2, "B3": B3, "B4": B4},
        "total_score": total,
        "label": rule_label,
        "reasons": {k: _rat_of(k) for k in ["A", "B1", "B2", "B3", "B4"]},
        "judge_label_raw": model_label,
        "label_mismatch": label_mismatch,
        "issues": issues,
        "suggested_fix": judge_obj.get("suggested_fix", ""),
        "candidate_scenario": candidate,
        "iteration": iteration,
        "scenario_source": scenario_source,
        "index_in_list": index_in_list,
        "judge_debug": debug,
        "prompt_version": PROMPT_VERSION,
    }


def evaluate_one_file(file_tag: str, input_path: str, action_key: Optional[str] = None, source_meta: Optional[dict] = None) -> dict:
    action_key, facts, facts_compact, valid_uca_ids = get_action_bundle(action_key)

    raw_data = load_json_any(input_path)
    runs, meta_in_file = normalize_runs(raw_data)

    file_format = {"top_type": "dict" if isinstance(raw_data, dict) else "list" if isinstance(raw_data, list) else str(type(raw_data))}
    if isinstance(raw_data, list):
        file_format["format"] = "lora_top_list"
    elif isinstance(raw_data, dict) and "runs" in raw_data:
        file_format["format"] = "runs_dict"
    else:
        file_format["format"] = "unknown"

    per_item_evaluations: List[dict] = []
    scenario_parse_failed_count = 0
    scenario_parse_failed_reasons: Dict[str, int] = defaultdict(int)
    seen_id_counts: Dict[str, int] = {}

    all_candidates: List[Tuple[dict, Any, str, int]] = []
    for run_idx, run in enumerate(runs, start=1):
        iteration = run.get("iteration", run_idx)
        scenario_json, scenario_source = get_scenario_json_from_run(run)
        if scenario_json is None:
            scenario_parse_failed_count += 1
            scenario_parse_failed_reasons[scenario_source] += 1
            continue

        scenarios = iter_scenarios(scenario_json)
        for s in scenarios:
            raw_id = s.get("scenario_id", "") or ""
            unique_id = make_unique_scenario_id(raw_id, iteration, s["index_in_list"], seen_id_counts)
            candidate = {
                "scenario_id": unique_id,
                "scenario_id_raw": raw_id,
                "description": s.get("description", ""),
                "linked_uca_ids": s.get("linked_uca_ids", None),
            }
            all_candidates.append((candidate, iteration, scenario_source, s["index_in_list"]))

    label_mismatch_count = 0
    judge_failed_count = 0

    if all_candidates:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(_judge_one_candidate, c, it, src, idx, facts_compact, valid_uca_ids) for (c, it, src, idx) in all_candidates]
            for fut in as_completed(futs):
                item = fut.result()
                per_item_evaluations.append(item)
                if item.get("label") == "judge_failed":
                    judge_failed_count += 1
                if item.get("label_mismatch"):
                    label_mismatch_count += 1
                if SLEEP_SEC > 0:
                    time.sleep(SLEEP_SEC)

    usable_items = [
        {
            "scenario_id": x["scenario_id"],
            "scenario_id_raw": x.get("scenario_id_raw", ""),
            "description": (x.get("candidate_scenario", {}) or {}).get("description", ""),
            "linked_uca_ids": (x.get("candidate_scenario", {}) or {}).get("linked_uca_ids", None),
            "iteration": x.get("iteration", None),
            "index_in_list": x.get("index_in_list", None),
        }
        for x in per_item_evaluations
        if x.get("label") == "合理可用"
    ]

    global_deduplication = {"deduped_usable_scenarios": [], "removed_as_duplicate": [], "coverage_count": 0}
    dedup_ok = True
    dedup_debug: dict = {}

    if len(usable_items) > 0:
        global_deduplication, dedup_debug, dedup_ok = run_dedup_batched(usable_items)
    global_deduplication["coverage_count"] = int(global_deduplication.get("coverage_count", 0) or 0)

    scored = [x for x in per_item_evaluations if isinstance(x.get("scores"), dict)]
    summary = {
        "file_tag": file_tag,
        "input_path": input_path,
        "filename": os.path.basename(input_path),
        "action_key": action_key,
        "file_format": file_format,
        "meta_in_file": meta_in_file if isinstance(meta_in_file, dict) else {},
        "source_meta": source_meta if isinstance(source_meta, dict) else {},
        "counts": {
            "total_runs_in_file": len(runs),
            "total_records": len(per_item_evaluations),
            "scenario_parse_failed": scenario_parse_failed_count,
            "scenario_parse_failed_reasons": dict(scenario_parse_failed_reasons),
            "judge_failed": judge_failed_count,
            "num_scored": len(scored),
            "合理可用": sum(1 for x in scored if x.get("label") == "合理可用"),
            "合理不可用": sum(1 for x in scored if x.get("label") == "合理不可用"),
            "不合理不可用": sum(1 for x in scored if x.get("label") == "不合理不可用"),
            "label_mismatch": label_mismatch_count,
            "usable_before_dedup": len(usable_items),
            "coverage_count_after_dedup": int(global_deduplication.get("coverage_count", 0)),
        },
        "dedup_ok": dedup_ok,
        "dedup_debug": dedup_debug,
        "perf": {
            "max_workers": MAX_WORKERS,
            "max_tokens_item": MAX_TOKENS_ITEM,
            "max_tokens_dedup": MAX_TOKENS_DEDUP,
        },
        "thresholds": {
            "USABLE_MIN_TOTAL": USABLE_MIN_TOTAL,
            "USABLE_MIN_B1": USABLE_MIN_B1,
            "USABLE_MIN_B2": USABLE_MIN_B2,
            "USABLE_MIN_B3": USABLE_MIN_B3,
            "USABLE_MIN_B4": USABLE_MIN_B4,
        },
        "prompt_version": PROMPT_VERSION,
    }

    return {
        "judge_meta": {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "model": MODEL_NAME,
            "temperature": 0.0,
            "top_p": 1.0,
            "max_tokens_item": MAX_TOKENS_ITEM,
            "max_tokens_dedup": MAX_TOKENS_DEDUP,
            "max_retries": MAX_RETRIES,
            "dedup_batch_size": DEDUP_BATCH_SIZE,
            "dedup_max_items_total": DEDUP_MAX_ITEMS_TOTAL,
            "dashscope_base_url": getattr(dashscope, "base_http_api_url", None),
        },
        "action_key": action_key,
        "facts": facts,
        "facts_compact": facts_compact,
        "summary": summary,
        "per_item_evaluations": per_item_evaluations,
        "global_deduplication": global_deduplication,
    }


def main():
    if not dashscope.api_key:
        raise RuntimeError("DASHSCOPE_API_KEY 为空：请先设置环境变量 DASHSCOPE_API_KEY（不要写在代码里）。")

    endpoint_info = ensure_working_base_url()
    print(f"DashScope endpoint: {endpoint_info}")
    if endpoint_info.get("reason") == "no_endpoint_resolvable":
        raise RuntimeError(
            "无法解析任何 DashScope endpoint 域名（cn/intl/us）。\n"
            "建议：检查 DNS/代理/公司网络 或设置 DASHSCOPE_BASE_URL。"
        )

    jobs, resolve_debug = resolve_jobs()
    if not jobs:
        print("未解析到任何可用输入文件。请填写 FILE_JOBS 或确保 MANIFEST_PATH 存在且可读。")
        return

    batch_records: List[dict] = []

    for job in jobs:
        file_tag = job.get("file_tag", "")
        input_path = job.get("input_path", "")
        action_key = job.get("action_key", DEFAULT_ACTION_KEY)
        if not input_path:
            continue
        if (not os.path.exists(input_path)) and ALLOW_MISSING_INPUTS:
            print(f"跳过不存在的输入文件: {input_path}")
            batch_records.append({
                "file_tag": file_tag,
                "input_path": input_path,
                "action_key": action_key,
                "job_source": job.get("job_source"),
                "status": "missing_input",
            })
            continue

        print(f"\n开始评测: file_tag='{file_tag}' | action='{action_key}' | file='{input_path}'")
        t0 = time.time()
        result = evaluate_one_file(file_tag=file_tag, input_path=input_path, action_key=action_key, source_meta=job)
        dt = time.time() - t0

        stem = os.path.splitext(os.path.basename(input_path))[0]
        out_name = os.path.join(
            OUTPUT_DIR,
            f"scenario_judge_{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        with open(out_name, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        c = result["summary"]["counts"]
        print(f"已保存评测结果: {out_name}")
        print(
            f"用时: {dt:.2f}s | "
            f"usable={c['合理可用']} unusable={c['合理不可用']} bad={c['不合理不可用']} "
            f"| dedup_coverage={c['coverage_count_after_dedup']} | judge_failed={c['judge_failed']}"
        )

        batch_records.append({
            "file_tag": file_tag,
            "input_path": input_path,
            "filename": os.path.basename(input_path),
            "action_key": action_key,
            "job_source": job.get("job_source"),
            "output_path": out_name,
            "status": "ok",
            "counts": c,
        })

    if WRITE_BATCH_SUMMARY:
        batch_out = os.path.join(
            OUTPUT_DIR,
            f"scenario_judge_batch_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        with open(batch_out, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "script": os.path.basename(__file__),
                "resolve_debug": resolve_debug,
                "entries": batch_records,
            }, f, ensure_ascii=False, indent=2)
        print(f"已保存批量索引: {batch_out}")


if __name__ == "__main__":
    main()
