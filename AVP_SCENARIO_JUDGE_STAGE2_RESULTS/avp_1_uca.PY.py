import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
import matplotlib.patheffects as pe

# ======================================
# Data
# ======================================
actions = ["forward_drive", "search_slot", "emergency_brake", "lateral_control"]
action_titles = [
    "(a) Forward drive",
    "(b) Search slot",
    "(c) Emergency brake",
    "(d) Lateral control"
]

methods = ["ZS", "FS-1", "FS-3", "SFT"]

JPC = {
    "forward_drive":   [21, 47, 50, 35],
    "search_slot":     [16, 47, 49, 40],
    "emergency_brake": [15, 43, 49, 39],
    "lateral_control": [32, 50, 49, 40],
}

MEC = {
    "forward_drive":   [6, 10, 15, 28],
    "search_slot":     [9, 10, 11, 19],
    "emergency_brake": [5, 9, 9, 22],
    "lateral_control": [8, 10, 10, 18],
}

CRR = {
    k: (np.array(MEC[k]) / np.array(JPC[k]) * 100.0)
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
jpc_light = "#9BB7D4"
jpc_dark  = "#3E6A9E"

mec_light = "#E9B56B"
mec_dark  = "#D8892B"

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
    j = np.array(JPC[action])
    m = np.array(MEC[action])

    ax.set_facecolor("white")

    j_colors = [jpc_light, jpc_light, jpc_light, jpc_dark]
    m_colors = [mec_light, mec_light, mec_light, mec_dark]

    # bars
    for k in range(len(methods)):
        lw = 1.1 if k == 3 else 0.8

        ax.bar(
            x[k] - bar_w/2, j[k], width=bar_w,
            color=j_colors[k], edgecolor="black", linewidth=lw,
            label="JPC" if (i == 0 and k == 0) else None,
            zorder=2
        )
        ax.bar(
            x[k] + bar_w/2, m[k], width=bar_w,
            color=m_colors[k], edgecolor="black", linewidth=lw,
            label="MEC" if (i == 0 and k == 0) else None,
            zorder=3
        )

    # JPC labels: centered above blue bars
    for xi, yi in zip(x - bar_w/2, j):
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

    # MEC labels: centered above orange bars
    for xi, yi in zip(x + bar_w/2, m):
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
    ax.set_ylim(0, 60)
    ax.set_yticks(np.arange(0, 61, 10))
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
# Bottom row: CRR line charts
# ======================================
for i, (ax, action) in enumerate(zip(bot_axes, actions)):
    r = np.array(CRR[action])

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

    ax.set_ylim(0, 100)
    ax.set_yticks([0, 20, 40, 60, 80])
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
        ax.set_ylabel("CRR (%)", labelpad=10)
    else:
        ax.set_yticklabels([])

    for spine in ax.spines.values():
        spine.set_linewidth(0.8)
        spine.set_color("#202020")

# ======================================
# Legend
# ======================================
handles = [
    plt.Rectangle((0, 0), 1, 1, facecolor=jpc_dark, edgecolor="black"),
    plt.Rectangle((0, 0), 1, 1, facecolor=mec_dark, edgecolor="black"),
    plt.Line2D([0], [0], color=line_color, marker='o', markersize=5.5,
               markerfacecolor='white', markeredgecolor=line_color, linewidth=1.25)
]
labels = ["JPC", "MEC", "CRR (%)"]

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
fig.savefig("stage2_composite_figure_final_stable.pdf", bbox_inches="tight")
fig.savefig("stage2_composite_figure_final_stable.png", dpi=600, bbox_inches="tight")
plt.show()