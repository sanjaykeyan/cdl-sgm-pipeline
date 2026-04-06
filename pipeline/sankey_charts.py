"""
Sankey diagram generation.
Extracted from sankey_yn_transitions.ipynb.

Input:  ProcessedData/SGM3_SurveyResults_Processed_Panel.xlsx
Output: analysis_outputs/figures/sankey_yn_transitions.html
        analysis_outputs/figures/sankey_yn_transitions_ai.html
        analysis_outputs/figures/sankey_yn_transitions_climate.html
"""

import os

import pandas as pd
import plotly.graph_objects as go


_COLOR_MAP = {
    "Yes": "#2ecc71",
    "No": "#e74c3c",
    "DK": "#d4af37",
}
_CATEGORY_ORDER = ["DK", "No", "Yes"]


def _create_matched_pairs(df):
    df_yn = df[df["YN"].notna()].copy()
    pre = df_yn[df_yn["Pre/Post.1"] == "Pre SGM"].copy()
    post = df_yn[df_yn["Pre/Post.1"] == "Post SGM"].copy()

    matched = pre.merge(
        post,
        on=["SelectedMentor", "VentureName"],
        suffixes=("_Pre", "_Post"),
        how="inner",
    )
    matched = matched[["SelectedMentor", "VentureName", "SelectedStream_Pre", "YN_Pre", "YN_Post"]]
    matched = matched.rename(columns={"SelectedStream_Pre": "Stream"})
    return matched


def _apply_yn_labels(matched):
    sample = matched["YN_Pre"].iloc[0] if len(matched) > 0 else None
    if isinstance(sample, str):
        matched = matched.copy()
        matched["YN_Pre_Label"] = matched["YN_Pre"]
        matched["YN_Post_Label"] = matched["YN_Post"]
    else:
        yn_map = {1: "Yes", 2: "No", 3: "DK"}
        matched = matched.copy()
        matched["YN_Pre_Label"] = matched["YN_Pre"].map(yn_map)
        matched["YN_Post_Label"] = matched["YN_Post"].map(yn_map)
    return matched


def _build_sankey_fig(transition_df, title):
    """
    transition_df must have columns: source_category, target_category, count
    """
    pre_totals = transition_df.groupby("source_category")["count"].sum()
    post_totals = transition_df.groupby("target_category")["count"].sum()
    total_obs = transition_df["count"].sum()

    pre_categories = [c for c in _CATEGORY_ORDER if c in pre_totals.index]
    post_categories = [c for c in _CATEGORY_ORDER if c in post_totals.index]

    pre_labels = []
    for cat in pre_categories:
        count = int(pre_totals[cat])
        pct = pre_totals[cat] / total_obs * 100
        pre_labels.append(f"{cat} (Pre)<br>{count} ({pct:.1f}%)")

    post_labels = []
    for cat in post_categories:
        count = int(post_totals[cat])
        pct = post_totals[cat] / total_obs * 100
        post_labels.append(f"{cat} (Post)<br>{count} ({pct:.1f}%)")

    all_labels = pre_labels + post_labels

    pre_index_map = {cat: i for i, cat in enumerate(pre_categories)}
    post_index_map = {cat: i + len(pre_categories) for i, cat in enumerate(post_categories)}

    source_indices, target_indices, values, link_colors = [], [], [], []
    for _, row in transition_df.iterrows():
        src, tgt, cnt = row["source_category"], row["target_category"], row["count"]
        if src in pre_index_map and tgt in post_index_map:
            source_indices.append(pre_index_map[src])
            target_indices.append(post_index_map[tgt])
            values.append(cnt)
            base = _COLOR_MAP.get(src, "#95a5a6")
            r, g, b = int(base[1:3], 16), int(base[3:5], 16), int(base[5:7], 16)
            link_colors.append(f"rgba({r},{g},{b},0.4)")

    node_colors = [_COLOR_MAP.get(c, "#95a5a6") for c in pre_categories + post_categories]
    node_x = [0.01] * len(pre_categories) + [0.99] * len(post_categories)

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                textfont=dict(size=15, color="#000000", family="Arial Black"),
                node=dict(
                    pad=25,
                    thickness=30,
                    line=dict(color="white", width=2),
                    label=all_labels,
                    color=node_colors,
                    x=node_x,
                ),
                link=dict(
                    source=source_indices,
                    target=target_indices,
                    value=values,
                    color=link_colors,
                ),
            )
        ]
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color="#2c3e50"), x=0.5, xanchor="center"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=700,
        margin=dict(l=120, r=120, t=80, b=50),
    )
    return fig


def build_sankey_filtered(
    panel_path: str,
    stream: str | None = None,
    shift: str | None = None,
) -> go.Figure:
    """
    Build a Sankey figure filtered to a specific stream and/or shift.

    Parameters
    ----------
    panel_path : str
        Path to SGM3_SurveyResults_Processed_Panel.xlsx.
    stream : str or None
        Stream name to filter on (e.g. "AI", "Climate"). None = all streams.
    shift : str or None
        Shift label to filter on (e.g. "Shift 1"). None = all shifts.

    Returns
    -------
    go.Figure
    """
    df_panel = pd.read_excel(panel_path)

    matched = _create_matched_pairs(df_panel)
    if len(matched) == 0:
        raise ValueError("No matched Pre/Post pairs found in panel file.")
    matched = _apply_yn_labels(matched)

    if stream:
        matched = matched[matched["Stream"] == stream]
    if shift:
        # SelectedShift in panel looks like "Shift 1" — match against that via the Pre merge suffix
        # The matched df has Stream from SelectedStream_Pre; shift comes from SelectedShift_Pre
        # which was dropped during _create_matched_pairs. Re-filter via the original panel.
        pre_shifts = df_panel[df_panel["Pre/Post.1"] == "Pre SGM"][
            ["SelectedMentor", "VentureName", "SelectedShift"]
        ].rename(columns={"SelectedShift": "Shift_label"})
        matched = matched.merge(pre_shifts, on=["SelectedMentor", "VentureName"], how="left")
        matched = matched[matched["Shift_label"] == shift]

    if len(matched) == 0:
        raise ValueError(f"No data found for stream={stream!r}, shift={shift!r}.")

    parts = []
    if stream and shift:
        title = f"Willingness to Mentor: Pre → Post SGM ({stream} Stream, {shift})"
    elif stream:
        title = f"Willingness to Mentor: Pre → Post SGM ({stream} Stream)"
    elif shift:
        title = f"Willingness to Mentor: Pre → Post SGM ({shift})"
    else:
        title = "Willingness to Mentor: Pre → Post SGM (All)"

    transitions = (
        matched.groupby(["YN_Pre_Label", "YN_Post_Label"])
        .size()
        .reset_index(name="count")
        .rename(columns={"YN_Pre_Label": "source_category", "YN_Post_Label": "target_category"})
    )
    return _build_sankey_fig(transitions, title)


def run_sankey(panel_path: str, figures_dir: str) -> dict:
    """
    Generate three Sankey HTML files.

    Parameters
    ----------
    panel_path : str
        Path to SGM3_SurveyResults_Processed_Panel.xlsx.
    figures_dir : str
        Output directory for HTML files.

    Returns
    -------
    dict
        { 'overall': ..., 'ai': ..., 'climate': ... }
    """
    os.makedirs(figures_dir, exist_ok=True)

    df_panel = pd.read_excel(panel_path)

    matched = _create_matched_pairs(df_panel)
    if len(matched) == 0:
        raise ValueError(
            "No matched Pre/Post pairs found. "
            "Check that the panel file has both 'Pre SGM' and 'Post SGM' rows with YN values."
        )
    matched = _apply_yn_labels(matched)

    def _transitions(df):
        return (
            df.groupby(["YN_Pre_Label", "YN_Post_Label"])
            .size()
            .reset_index(name="count")
            .rename(columns={"YN_Pre_Label": "source_category", "YN_Post_Label": "target_category"})
        )

    # Overall
    overall_path = os.path.join(figures_dir, "sankey_yn_transitions.html")
    _build_sankey_fig(
        _transitions(matched), "Willingness to Mentor: Pre-SGM → Post-SGM"
    ).write_html(overall_path)

    # Per stream
    stream_paths = {}
    for stream in matched["Stream"].dropna().unique():
        stream_data = matched[matched["Stream"] == stream]
        fname = f"sankey_yn_transitions_{stream.lower()}.html"
        path = os.path.join(figures_dir, fname)
        _build_sankey_fig(
            _transitions(stream_data),
            f"Willingness to Mentor: Pre-SGM → Post-SGM ({stream} Stream)",
        ).write_html(path)
        stream_paths[stream.lower()] = path

    return {
        "overall": overall_path,
        "ai": stream_paths.get("ai", ""),
        "climate": stream_paths.get("climate", ""),
    }
