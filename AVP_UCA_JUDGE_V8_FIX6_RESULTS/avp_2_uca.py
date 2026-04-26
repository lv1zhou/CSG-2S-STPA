"""
AVP UCA Judge Results Visualization - v8_fix6 Final Results

Data Source:
- forward_drive_lora: avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_lora_20260413_115743.json
- forward_drive_few_3: avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_few_3_shot_20260413_115345.json
- forward_drive_few_1: avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_few_1_shot_20260413_114951.json
- forward_drive_zero: avp_uca_judge_desc_strict_ctx_v8_fix6_forward_drive_zero_shot_20260413_114557.json
- search_slot_lora: avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_lora_20260413_121344.json
- search_slot_few_3: avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_few_3_shot_20260413_120946.json
- search_slot_few_1: avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_few_1_shot_20260413_120555.json
- search_slot_zero: avp_uca_judge_desc_strict_ctx_v8_fix6_search_slot_zero_shot_20260413_120158.json
- emergency_brake_lora: avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_lora_20260413_122925.json
- emergency_brake_few_3: avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_few_3_shot_20260413_122534.json
- emergency_brake_few_1: avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_few_1_shot_20260413_122131.json
- emergency_brake_zero: avp_uca_judge_desc_strict_ctx_v8_fix6_emergency_brake_zero_shot_20260413_121750.json
- lateral_control_lora: avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_lora_20260413_124512.json
- lateral_control_few_3: avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_few_3_shot_20260413_124121.json
- lateral_control_few_1: avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_few_1_shot_20260413_123720.json
- lateral_control_zero: avp_uca_judge_desc_strict_ctx_v8_fix6_lateral_control_zero_shot_20260413_123339.json

Judge Version: v8_fix6 (description_only_strict_context_v8_fix6_context_richness_priority)
Date: 2026-04-13

Key Results:
- forward_drive: LoRA(10) > Few-3(6) > Few-1(5) > Zero(2)
- search_slot: Few-1(4) > LoRA(3) = Few-3(3) = Zero(3)
- emergency_brake: LoRA(8) > Zero(6) > Few-3(4) = Few-1(4)
- lateral_control: LoRA(13) > Few-3(5) > Zero(4) > Few-1(3)

Overall: LoRA achieves 1st place in 3 out of 4 actions (75% success rate)
"""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
import matplotlib.patheffects as pe

# ======================================
# Data (v8_fix6 results)
# ======================================
actions = ["forward_drive", "search_slot", "emergency_brake", "lateral_control"]
action_titles = [
    "(a) Forward drive",
    "(b) Search slot",
    "(c) Emergency brake",
    "(d) Lateral control"
]

methods = ["ZS", "FS-1", "FS-3", "SFT"]

# Usable counts (before deduplication)
Usable = {
    "forward_drive":   [6, 9, 10, 12],    # [Zero, Few-1, Few-3, LoRA]
    "search_slot":     [14, 13, 14, 3],   # [Zero, Few-1, Few-3, LoRA]
    "emergency_brake": [15, 4, 16, 10],   # [Zero, Few-1, Few-3, LoRA]
    "lateral_control": [8, 14, 21, 14],   # [Zero, Few-1, Few-3, LoRA]
}

# Kept counts (after deduplication)
Kept = {
    "forward_drive":   [2, 5, 6, 10],     # [Zero, Few-1, Few-3, LoRA]
    "search_slot":     [3, 4, 3, 3],      # [Zero, Few-1, Few-3, LoRA]
    "emergency_brake": [6, 4, 4, 8],      # [Zero, Few-1, Few-3, LoRA]
    "lateral_control": [4, 3, 5, 13],     # [Zero, Few-1, Few-3, LoRA]
}

# Retention Rate (Kept/Usable * 100%)
RR = {
    k: (np.array(Kept[k]) / np.array(Usable[k]) * 100.0)
    for k in actions
}

# ======================================
# Global style
# ======================================
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 8.5,
    "ytick.labelsize": 8.5,
    "legend.fontsize": 9,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})

# Journal-like palette
usable_light = "#9BB7D4"
usable_dark  = "#3E6A9E"

kept_light = "#E9B56B"
kept_dark  = "#D8892B"

line_color = "#3D3D3D"
stem_color = "#B5B5B5"
sft_point_color = "#8C1D2C"

# light halo only
text_pe = [pe.withStroke(linewidth=1.6, foreground="white", alpha=0.95)]

# ======================================
# Layout
# ======================================
fig = plt.figure(figsize=(8.2, 6.2), facecolor="white")
gs = GridSpec(2, 4, height_ratios=[3.15, 1.75], hspace=0.30, wspace=0.24)

top_axes = [fig.add_subplot(gs[0, i]) for i in range(4)]
bot_axes = [fig.add_subplot(gs[1, i]) for i in range(4)]

x = np.arange(len(methods))
bar_w = 0.22  # narrower bars -> more whitespace

# ======================================
# Top row: grouped bars
# ======================================
for i, (ax, action, title) in enumerate(zip(top_axes, actions, action_titles)):
    u = np.array(Usable[action])
    k = np.array(Kept[action])

    ax.set_facecolor("white")

    u_colors = [usable_light, usable_light, usable_light, usable_dark]
    k_colors = [kept_light, kept_light, kept_light, kept_dark]

    # bars
    for j in range(len(methods)):
        lw = 1.1 if j == 3 else 0.8

        ax.bar(
            x[j] - bar_w/2, u[j], width=bar_w,
            color=u_colors[j], edgecolor="black", linewidth=lw,
            label="Usable" if (i == 0 and j == 0) else None,
            zorder=2
        )
        ax.bar(
            x[j] + bar_w/2, k[j], width=bar_w,
            color=k_colors[j], edgecolor="black", linewidth=lw,
            label="Kept" if (i == 0 and j == 0) else None,
            zorder=3
        )

    # Usable labels: centered above blue bars
    for xi, yi in zip(x - bar_w/2, u):
        txt = ax.annotate(
            f"{int(yi)}",
            xy=(xi, yi),
            xytext=(0, 6),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=8.4, color="#1F1F1F",
            clip_on=False
        )
        txt.set_path_effects(text_pe)

    # Kept labels: centered above orange bars
    for xi, yi in zip(x + bar_w/2, k):
        txt = ax.annotate(
            f"{int(yi)}",
            xy=(xi, yi),
            xytext=(3, 6),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=8.4, color="#1F1F1F",
            clip_on=False
        )
        txt.set_path_effects(text_pe)

    ax.set_title(title, pad=10)
    ax.set_ylim(0, 25)
    ax.set_yticks(np.arange(0, 26, 5))
    ax.set_xlim(-0.45, 3.45)
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.24, color="#B8B8B8")
    ax.set_axisbelow(True)

    ax.set_xticks(x)
    ax.set_xticklabels([])

    if i == 0:
        ax.set_ylabel("Count", labelpad=10)
    else:
        ax.set_yticklabels([])

    for spine in ax.spines.values():
        spine.set_linewidth(0.8)
        spine.set_color("#202020")

# ======================================
# Bottom row: Retention Rate line charts
# ======================================
for i, (ax, action) in enumerate(zip(bot_axes, actions)):
    r = np.array(RR[action])

    ax.set_facecolor("white")

    # stems
    for xi, yi in zip(x, r):
        ax.vlines(xi, 0, yi, color=stem_color, linewidth=0.95, zorder=1)

    # line + markers
    ax.plot(x, r, color=line_color, linewidth=1.25, zorder=2)
    ax.scatter(
        x[:-1], r[:-1], s=34,
        facecolor="white", edgecolor=line_color, linewidth=1.2, zorder=3
    )
    ax.scatter(
        x[-1], r[-1], s=50,
        facecolor=sft_point_color, edgecolor=line_color, linewidth=1.0, zorder=4
    )

    # percentage labels:
    # leftmost -> shift right
    # rightmost -> shift left
    # middle -> centered above
    for k, (xi, yi) in enumerate(zip(x, r)):
        if k == 0:
            xytext = (6, 6)
            ha = "left"
        elif k == len(x) - 1:
            xytext = (-6, 6)
            ha = "right"
        else:
            xytext = (0, 6)
            ha = "center"

        txt = ax.annotate(
            f"{yi:.0f}%",
            xy=(xi, yi),
            xytext=xytext,
            textcoords="offset points",
            ha=ha, va="bottom",
            fontsize=8.4, color="#1F1F1F",
            clip_on=False
        )
        txt.set_path_effects(text_pe)

    ax.set_ylim(0, 110)
    ax.set_yticks([0, 20, 40, 60, 80, 100])
    ax.set_xlim(-0.15, 3.15)
    ax.set_xticks(x)
    ax.set_xticklabels(methods)

    # emphasize SFT tick label
    xticklabels = ax.get_xticklabels()
    xticklabels[-1].set_fontweight("bold")
    xticklabels[-1].set_color(sft_point_color)

    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.24, color="#B8B8B8")
    ax.set_axisbelow(True)

    if i == 0:
        ax.set_ylabel("RR (%)", labelpad=10)
    else:
        ax.set_yticklabels([])

    for spine in ax.spines.values():
        spine.set_linewidth(0.8)
        spine.set_color("#202020")

# ======================================
# Legend
# ======================================
handles = [
    plt.Rectangle((0, 0), 1, 1, facecolor=usable_dark, edgecolor="black"),
    plt.Rectangle((0, 0), 1, 1, facecolor=kept_dark, edgecolor="black"),
    plt.Line2D([0], [0], color=line_color, marker='o', markersize=5.5,
               markerfacecolor='white', markeredgecolor=line_color, linewidth=1.25)
]
labels = ["Usable", "Kept", "RR (%)"]

fig.legend(
    handles, labels,
    loc="upper center", ncol=3, frameon=False,
    bbox_to_anchor=(0.5, 1.01)
)

# ======================================
# Bottom note
# ======================================
fig.text(
    0.5, 0.02,
    "ZS: Zero-shot; FS-1: Few-shot (1-shot); FS-3: Few-shot (3-shot); SFT: LoRA-SFT",
    ha="center", va="center", fontsize=8.2
)

fig.subplots_adjust(left=0.08, right=0.995, top=0.86, bottom=0.10)
fig.savefig("avp_uca_judge_v8_fix6_results.pdf", bbox_inches="tight")
fig.savefig("avp_uca_judge_v8_fix6_results.png", dpi=600, bbox_inches="tight")
plt.show()
