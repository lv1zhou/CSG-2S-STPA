"""
验证Scenario JSON文件数据与图片数据的一致性
"""
import json
import os

# 定义16个JSON文件
json_files = {
    "forward_drive": {
        "zero": "scenario_judge_scenario_zero_shot_forward_drive_20260407_104715_20260407_120216.json",
        "few_1": "scenario_judge_scenario_few_1_shot_forward_drive_20260407_105009_20260407_120638.json",
        "few_3": "scenario_judge_scenario_few_3_shot_forward_drive_20260407_105306_20260407_121545.json",
        "lora": "scenario_judge_scenario_lora_forward_drive_20260407_105759_20260407_122257.json",
    },
    "search_slot": {
        "zero": "scenario_judge_scenario_zero_shot_search_slot_20260407_105918_20260407_122441.json",
        "few_1": "scenario_judge_scenario_few_1_shot_search_slot_20260407_110136_20260407_122911.json",
        "few_3": "scenario_judge_scenario_few_3_shot_search_slot_20260407_110409_20260407_123359.json",
        "lora": "scenario_judge_scenario_lora_search_slot_20260407_110906_20260407_123843.json",
    },
    "emergency_brake": {
        "zero": "scenario_judge_scenario_zero_shot_emergency_brake_20260407_111030_20260407_124022.json",
        "few_1": "scenario_judge_scenario_few_1_shot_emergency_brake_20260407_111409_20260407_124424.json",
        "few_3": "scenario_judge_scenario_few_3_shot_emergency_brake_20260407_111654_20260407_125019.json",
        "lora": "scenario_judge_scenario_lora_emergency_brake_20260407_112138_20260407_125710.json",
    },
    "lateral_control": {
        "zero": "scenario_judge_scenario_zero_shot_lateral_control_20260407_112315_20260407_125959.json",
        "few_1": "scenario_judge_scenario_few_1_shot_lateral_control_20260407_112602_20260407_130442.json",
        "few_3": "scenario_judge_scenario_few_3_shot_lateral_control_20260407_112834_20260407_130859.json",
        "lora": "scenario_judge_scenario_lora_lateral_control_20260407_113300_20260407_131522.json",
    }
}

# 从代码中提取的预期数据 (JPC = usable, MEC = kept)
expected_jpc = {
    "forward_drive":   [21, 47, 50, 35],
    "search_slot":     [16, 47, 49, 40],
    "emergency_brake": [15, 43, 49, 39],
    "lateral_control": [32, 50, 49, 40],
}

expected_mec = {
    "forward_drive":   [6, 10, 15, 28],
    "search_slot":     [9, 10, 11, 19],
    "emergency_brake": [5, 9, 9, 22],
    "lateral_control": [8, 10, 10, 18],
}

def count_jpc_mec(json_path):
    """统计JSON文件中的JPC和MEC数量"""
    if not os.path.exists(json_path):
        return None, None, f"文件不存在: {json_path}"

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 从summary.counts中获取统计数据
        jpc_count = None
        mec_count = None

        if 'summary' in data and 'counts' in data['summary']:
            counts = data['summary']['counts']
            jpc_count = counts.get('usable_before_dedup', 0)
            mec_count = counts.get('coverage_count_after_dedup', 0)

        if jpc_count is None or mec_count is None:
            return None, None, "无法找到统计数据"

        return jpc_count, mec_count, None
    except Exception as e:
        return None, None, str(e)

# 验证所有文件
print("=" * 80)
print("验证Scenario JSON文件数据与图片数据的一致性")
print("=" * 80)

methods_order = ["zero", "few_1", "few_3", "lora"]
methods_display = ["ZS", "FS-1", "FS-3", "SFT"]

all_match = True

for action in ["forward_drive", "search_slot", "emergency_brake", "lateral_control"]:
    print(f"\n【{action}】")
    print("-" * 80)

    for i, (method, display) in enumerate(zip(methods_order, methods_display)):
        json_file = json_files[action][method]
        jpc, mec, error = count_jpc_mec(json_file)

        expected_j = expected_jpc[action][i]
        expected_m = expected_mec[action][i]

        if error:
            print(f"  {display}: 错误 - {error}")
            all_match = False
        elif jpc is None:
            print(f"  {display}: 无法读取数据")
            all_match = False
        else:
            jpc_match = "OK" if jpc == expected_j else "FAIL"
            mec_match = "OK" if mec == expected_m else "FAIL"

            crr = (mec / jpc * 100) if jpc > 0 else 0

            print(f"  {display}:")
            print(f"    JPC: {jpc} (预期: {expected_j}) {jpc_match}")
            print(f"    MEC: {mec} (预期: {expected_m}) {mec_match}")
            print(f"    CRR: {crr:.0f}%")

            if jpc != expected_j or mec != expected_m:
                all_match = False

print("\n" + "=" * 80)
if all_match:
    print("OK - All data verified!")
else:
    print("FAIL - Data mismatch found!")
print("=" * 80)
