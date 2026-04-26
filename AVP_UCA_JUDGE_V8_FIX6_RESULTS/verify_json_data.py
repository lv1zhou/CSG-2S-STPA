"""
验证JSON文件数据与图片数据的一致性
"""
import json
import os

# 定义16个JSON文件
json_files = {
    "forward_drive": {
        "zero": "avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_zero_shot_20260413_114557.json",
        "few_1": "avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_few_1_shot_20260413_114951.json",
        "few_3": "avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_few_3_shot_20260413_115345.json",
        "lora": "avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_lora_20260413_115743.json",
    },
    "search_slot": {
        "zero": "avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_zero_shot_20260413_120158.json",
        "few_1": "avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_few_1_shot_20260413_120555.json",
        "few_3": "avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_few_3_shot_20260413_120946.json",
        "lora": "avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_lora_20260413_121344.json",
    },
    "emergency_brake": {
        "zero": "avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_zero_shot_20260413_121750.json",
        "few_1": "avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_few_1_shot_20260413_122131.json",
        "few_3": "avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_few_3_shot_20260413_122534.json",
        "lora": "avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_lora_20260413_122925.json",
    },
    "lateral_control": {
        "zero": "avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_zero_shot_20260413_123339.json",
        "few_1": "avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_few_1_shot_20260413_123720.json",
        "few_3": "avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_few_3_shot_20260413_124121.json",
        "lora": "avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_lora_20260413_124512.json",
    }
}

# 从代码中提取的预期数据
expected_usable = {
    "forward_drive":   [6, 9, 10, 12],
    "search_slot":     [14, 13, 14, 3],
    "emergency_brake": [15, 4, 16, 10],
    "lateral_control": [8, 14, 21, 14],
}

expected_kept = {
    "forward_drive":   [2, 5, 6, 10],
    "search_slot":     [3, 4, 3, 3],
    "emergency_brake": [6, 4, 4, 8],
    "lateral_control": [4, 3, 5, 13],
}

def count_usable_kept(json_path):
    """统计JSON文件中的usable和kept数量"""
    if not os.path.exists(json_path):
        return None, None, f"文件不存在: {json_path}"

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 从summary.counts中获取统计数据
        usable_count = None
        kept_count = None

        if 'summary' in data and 'counts' in data['summary']:
            counts = data['summary']['counts']
            usable_count = counts.get('usable_before_dedup', 0)
            kept_count = counts.get('coverage_count_after_dedup', 0)

        if usable_count is None or kept_count is None:
            return None, None, "无法找到统计数据"

        return usable_count, kept_count, None
    except Exception as e:
        return None, None, str(e)

# 验证所有文件
print("=" * 80)
print("验证JSON文件数据与图片数据的一致性")
print("=" * 80)

methods_order = ["zero", "few_1", "few_3", "lora"]
methods_display = ["ZS", "FS-1", "FS-3", "SFT"]

all_match = True

for action in ["forward_drive", "search_slot", "emergency_brake", "lateral_control"]:
    print(f"\n【{action}】")
    print("-" * 80)

    for i, (method, display) in enumerate(zip(methods_order, methods_display)):
        json_file = json_files[action][method]
        usable, kept, error = count_usable_kept(json_file)

        expected_u = expected_usable[action][i]
        expected_k = expected_kept[action][i]

        if error:
            print(f"  {display}: 错误 - {error}")
            all_match = False
        elif usable is None:
            print(f"  {display}: 无法读取数据")
            all_match = False
        else:
            usable_match = "OK" if usable == expected_u else "FAIL"
            kept_match = "OK" if kept == expected_k else "FAIL"

            rr = (kept / usable * 100) if usable > 0 else 0

            print(f"  {display}:")
            print(f"    Usable: {usable} (预期: {expected_u}) {usable_match}")
            print(f"    Kept:   {kept} (预期: {expected_k}) {kept_match}")
            print(f"    RR:     {rr:.0f}%")

            if usable != expected_u or kept != expected_k:
                all_match = False

print("\n" + "=" * 80)
if all_match:
    print("OK - All data verified!")
else:
    print("FAIL - Data mismatch found!")
print("=" * 80)
