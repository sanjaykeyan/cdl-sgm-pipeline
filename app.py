"""
CDL SGM3 Pipeline — Streamlit app.

Two data sources: fetch from Qualtrics API, or upload a CSV directly.
After data is available, runs processing → room allocation → Sankey diagrams.
"""

import os

import streamlit as st

st.set_page_config(
    page_title="CDL SGM Pipeline",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@300;400;500&display=swap');

:root {
    --bg:           #faf9f6;
    --bg-surface:   #f3f1ec;
    --bg-card:      #edeae3;
    --navy:         #1a2744;
    --amber:        #c8860a;
    --amber-light:  #fef3dc;
    --amber-border: rgba(200,134,10,0.25);
    --text:         #111827;
    --text-dim:     #4b5563;
    --text-muted:   #9ca3af;
    --border:       #ddd9d0;
    --border-light: #e8e5de;
    --success:      #15803d;
    --error:        #b91c1c;
    --mono:         'IBM Plex Mono', monospace;
    --sans:         'Syne', sans-serif;
}

/* ── Reset & base ─────────────────────────────── */
.stApp {
    background: var(--bg) !important;
    font-family: var(--mono) !important;
}
#MainMenu, footer, header { visibility: hidden !important; }
.block-container {
    padding: 2.5rem 3.5rem 5rem !important;
    max-width: 1100px !important;
}

/* ── Typography ───────────────────────────────── */
h1, h2, h3, h4 {
    font-family: var(--sans) !important;
    color: var(--navy) !important;
    letter-spacing: -0.025em !important;
}
/* Apply mono only to markdown/text content, not widget internals */
.stMarkdown, .stMarkdown p, .stMarkdown span,
.stText, .stCaption,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span {
    font-family: var(--mono) !important;
}
.stMarkdown p {
    color: var(--text-dim);
    font-size: 0.83rem;
    line-height: 1.7;
}

/* ── Divider ──────────────────────────────────── */
hr {
    border: none !important;
    border-top: 1px solid var(--border) !important;
    margin: 2.5rem 0 !important;
}

/* ── Primary buttons ──────────────────────────── */
[data-testid="stButton"] > button,
.stButton > button {
    background: var(--navy) !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 3px !important;
    font-family: var(--sans) !important;
    font-weight: 700 !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.09em !important;
    text-transform: uppercase !important;
    padding: 0.7rem 2rem !important;
    transition: background 0.15s, box-shadow 0.15s, transform 0.1s !important;
    box-shadow: 0 1px 3px rgba(26,39,68,0.15) !important;
}
[data-testid="stButton"] > button:hover,
.stButton > button:hover {
    background: #233260 !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 16px rgba(26,39,68,0.2) !important;
}
[data-testid="stButton"] > button:active,
.stButton > button:active { transform: translateY(0) !important; }
[data-testid="stButton"] > button:disabled,
.stButton > button:disabled {
    background: var(--bg-card) !important;
    color: var(--text-muted) !important;
    box-shadow: none !important;
    transform: none !important;
}

/* ── Download button ──────────────────────────── */
[data-testid="stDownloadButton"] > button,
.stDownloadButton > button {
    background: transparent !important;
    color: var(--navy) !important;
    border: 1.5px solid var(--navy) !important;
    border-radius: 3px !important;
    font-family: var(--sans) !important;
    font-weight: 600 !important;
    font-size: 0.76rem !important;
    letter-spacing: 0.09em !important;
    text-transform: uppercase !important;
    padding: 0.65rem 1.6rem !important;
    transition: background 0.15s, box-shadow 0.15s !important;
}
[data-testid="stDownloadButton"] > button:hover,
.stDownloadButton > button:hover {
    background: var(--bg-surface) !important;
    box-shadow: 0 2px 10px rgba(26,39,68,0.12) !important;
}

/* ── Radio toggle ─────────────────────────────── */
[data-testid="stRadio"] > label { display: none !important; }
[data-testid="stRadio"] > div {
    display: flex !important;
    flex-direction: row !important;
    gap: 0 !important;
    background: var(--bg-surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 4px !important;
    padding: 3px !important;
    width: fit-content !important;
}
[data-testid="stRadio"] > div > label {
    display: flex !important;
    align-items: center !important;
    padding: 0.45rem 1.25rem !important;
    font-family: var(--sans) !important;
    font-size: 0.76rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.07em !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
    cursor: pointer !important;
    border-radius: 3px !important;
    transition: all 0.15s !important;
    user-select: none !important;
}
/* Hide the radio circle indicator (Streamlit's custom visual element) */
[data-testid="stRadio"] > div > label > div:first-child {
    display: none !important;
}
/* Hide native radio input */
[data-testid="stRadio"] input[type="radio"] {
    display: none !important;
}
/* Selected state — must force color on ALL nested elements */
[data-testid="stRadio"] > div > label:has(input:checked) {
    background: var(--navy) !important;
    box-shadow: 0 1px 4px rgba(26,39,68,0.2) !important;
}
[data-testid="stRadio"] > div > label:has(input:checked),
[data-testid="stRadio"] > div > label:has(input:checked) p,
[data-testid="stRadio"] > div > label:has(input:checked) span,
[data-testid="stRadio"] > div > label:has(input:checked) div {
    color: #ffffff !important;
}

/* ── File uploader ────────────────────────────── */
[data-testid="stFileUploader"] {
    background: #ffffff !important;
    border: 1.5px dashed var(--border) !important;
    border-radius: 6px !important;
    padding: 0.5rem !important;
    transition: border-color 0.2s !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: var(--navy) !important;
}
[data-testid="stFileUploaderDropzoneInstructions"],
[data-testid="stFileUploader"] label,
[data-testid="stFileUploader"] small {
    color: var(--text-dim) !important;
    font-family: var(--mono) !important;
    font-size: 0.82rem !important;
}
/* Browse files button inside the uploader */
[data-testid="stFileUploaderDropzone"] button {
    font-family: var(--sans) !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.03em !important;
    text-transform: none !important;
    background: var(--bg-surface) !important;
    color: var(--text) !important;
    border: 1px solid var(--border) !important;
    border-radius: 3px !important;
    box-shadow: none !important;
}

/* ── Selectbox ────────────────────────────────── */
[data-testid="stSelectbox"] label {
    font-family: var(--sans) !important;
    font-size: 0.71rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
}
[data-testid="stSelectbox"] > div > div {
    background: #ffffff !important;
    border: 1px solid var(--border) !important;
    border-radius: 3px !important;
    color: var(--text) !important;
    font-family: var(--mono) !important;
    font-size: 0.84rem !important;
}
[data-testid="stSelectbox"] > div > div:focus-within {
    border-color: var(--navy) !important;
    box-shadow: 0 0 0 2px rgba(26,39,68,0.1) !important;
}

/* ── Alerts ───────────────────────────────────── */
[data-testid="stAlert"] {
    background: var(--amber-light) !important;
    border: 1px solid var(--amber-border) !important;
    border-left: 3px solid var(--amber) !important;
    border-radius: 3px !important;
    font-size: 0.82rem !important;
}
[data-testid="stAlert"] p,
[data-testid="stAlert"] span,
[data-testid="stAlert"] code {
    color: var(--text-dim) !important;
    font-family: var(--mono) !important;
}
[data-testid="stAlert"] code {
    background: rgba(200,134,10,0.08) !important;
}

/* ── Plotly chart ─────────────────────────────── */
[data-testid="stPlotlyChart"] {
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    overflow: hidden !important;
    background: #ffffff !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05) !important;
}

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSV  = os.path.join(BASE_DIR, "RawData", "SGM3_SurveyResults.csv")

PANEL_PATH  = os.path.join(BASE_DIR, "ProcessedData", "SGM3_SurveyResults_Processed_Panel.xlsx")
FIGURES_DIR = os.path.join(BASE_DIR, "analysis_outputs", "figures")


def _check_credentials() -> bool:
    try:
        _ = st.secrets["QUALTRICS_API_TOKEN"]
        _ = st.secrets["QUALTRICS_SURVEY_ID"]
        _ = st.secrets["QUALTRICS_BASE_URL"]
        return True
    except Exception:
        return False


def _save_uploaded_csv(uploaded_file) -> None:
    os.makedirs(os.path.dirname(RAW_CSV), exist_ok=True)
    with open(RAW_CSV, "wb") as f:
        f.write(uploaded_file.getbuffer())


def _outputs_exist() -> bool:
    return (
        st.session_state.get("pipeline_complete", False)
        and os.path.exists(PANEL_PATH)
    )


def _run_pipeline(skip_fetch: bool) -> None:
    from pipeline.orchestrator import run_pipeline

    log_box   = st.empty()
    log_lines: list[str] = []

    def _render_log(lines, done=False, err=None):
        if done:
            status_color = "#15803d"
            status_label = "COMPLETE"
        elif err:
            status_color = "#b91c1c"
            status_label = "ERROR"
        else:
            status_color = "#c8860a"
            status_label = "RUNNING"

        rows = "".join(
            f'<div class="tline"><span class="tprompt">›</span>{ln}</div>'
            for ln in lines
        )
        if err:
            rows += f'<div class="tline terror"><span class="tprompt">✗</span>{err}</div>'

        log_box.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700&family=IBM+Plex+Mono:wght@400&display=swap');
.t-wrap {{
    background: #f9f8f5;
    border: 1px solid #ddd9d0;
    border-radius: 6px;
    overflow: hidden;
    margin: 1.2rem 0;
    font-family: 'IBM Plex Mono', monospace;
}}
.t-bar {{
    display: flex;
    align-items: center;
    gap: 8px;
    background: #f0ede6;
    border-bottom: 1px solid #ddd9d0;
    padding: 0.5rem 1rem;
}}
.t-dot {{
    width: 9px; height: 9px;
    border-radius: 50%;
    background: #d0cdc6;
}}
.t-title {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #9ca3af;
    flex: 1;
    margin-left: 4px;
}}
.t-status {{
    font-family: 'Syne', sans-serif;
    font-size: 0.66rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    color: {status_color};
}}
.t-body {{
    padding: 0.9rem 1.1rem;
    min-height: 2.5rem;
}}
.tline {{
    font-size: 0.8rem;
    color: #4b5563;
    line-height: 2;
    display: flex;
    gap: 0.65rem;
    font-family: 'IBM Plex Mono', monospace;
}}
.tprompt {{
    color: #c8860a;
    user-select: none;
    font-family: 'IBM Plex Mono', monospace;
}}
.terror {{ color: #b91c1c !important; }}
.terror .tprompt {{ color: #b91c1c !important; }}
</style>
<div class="t-wrap">
  <div class="t-bar">
    <div class="t-dot"></div><div class="t-dot"></div><div class="t-dot"></div>
    <span class="t-title">pipeline.log</span>
    <span class="t-status">{status_label}</span>
  </div>
  <div class="t-body">{rows}</div>
</div>
""", unsafe_allow_html=True)

    try:
        for message, result in run_pipeline(skip_fetch=skip_fetch):
            log_lines.append(message)
            _render_log(log_lines)
            if result is not None:
                st.session_state["last_outputs"] = result
        st.session_state["pipeline_complete"] = True
        _render_log(log_lines, done=True)
        st.rerun()
    except Exception as exc:
        st.session_state["pipeline_complete"] = False
        _render_log(log_lines, err=str(exc))


# ---------------------------------------------------------------------------
# Section label helper
# ---------------------------------------------------------------------------

def _section(number: str, title: str, subtitle: str = "") -> None:
    sub = f'<p class="sec-sub">{subtitle}</p>' if subtitle else ""
    st.markdown(f"""
<style>
.sec-wrap {{ margin-bottom: 1.4rem; }}
.sec-row {{
    display: flex;
    align-items: baseline;
    gap: 0.7rem;
    margin-bottom: 0.3rem;
}}
.sec-num {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    color: #c8860a;
    letter-spacing: 0.1em;
    flex-shrink: 0;
    padding-top: 2px;
}}
.sec-title {{
    font-family: 'Syne', sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
    color: #1a2744;
    letter-spacing: -0.02em;
}}
.sec-rule {{
    flex: 1;
    height: 1px;
    background: #ddd9d0;
    margin-bottom: 3px;
}}
.sec-sub {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    color: #9ca3af;
    margin: 0 0 0 2.2rem !important;
    line-height: 1.6;
}}
</style>
<div class="sec-wrap">
  <div class="sec-row">
    <span class="sec-num">{number}</span>
    <span class="sec-title">{title}</span>
    <div class="sec-rule"></div>
  </div>
  {sub}
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=IBM+Plex+Mono:wght@300;400&display=swap');
.cdl-header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    padding-bottom: 2rem;
    border-bottom: 2px solid #1a2744;
    margin-bottom: 2.8rem;
}
.cdl-left { display: flex; flex-direction: column; gap: 0.15rem; }
.cdl-eyebrow {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #c8860a;
}
.cdl-title {
    font-family: 'Syne', sans-serif;
    font-size: 2.4rem;
    font-weight: 800;
    color: #1a2744;
    letter-spacing: -0.04em;
    line-height: 1;
}
</style>
<div class="cdl-header">
  <div class="cdl-left">
    <span class="cdl-eyebrow">Creative Destruction Lab</span>
    <div class="cdl-title">SGM Pipeline</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# 01 — Data source
# ---------------------------------------------------------------------------

has_credentials = _check_credentials()

_section("01", "Data Source", "Choose how survey data enters the pipeline.")

source = st.radio(
    "Data source",
    options=["Fetch from Qualtrics", "Upload CSV"],
    horizontal=True,
    label_visibility="collapsed",
)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

if source == "Fetch from Qualtrics":
    if not has_credentials:
        st.warning(
            "**Qualtrics credentials not configured.**\n\n"
            "Add to `.streamlit/secrets.toml`:\n"
            "```toml\n"
            'QUALTRICS_API_TOKEN = "your_token"\n'
            'QUALTRICS_SURVEY_ID = "SV_xxxxxxxx"\n'
            'QUALTRICS_BASE_URL  = "https://yul1.qualtrics.com"\n'
            "```"
        )
    if st.button("▶  Run Pipeline", type="primary", disabled=not has_credentials, key="btn_qualtrics"):
        _run_pipeline(skip_fetch=False)

else:
    uploaded = st.file_uploader(
        "Drop your Qualtrics CSV export here",
        type="csv",
        label_visibility="visible",
    )
    if uploaded is not None:
        if st.button("▶  Process CSV", type="primary", key="btn_upload"):
            _save_uploaded_csv(uploaded)
            _run_pipeline(skip_fetch=True)


# ---------------------------------------------------------------------------
# 02 — Results
# ---------------------------------------------------------------------------

if _outputs_exist():
    import pandas as pd
    from pipeline.sankey_charts import build_sankey_filtered

    st.markdown("<hr>", unsafe_allow_html=True)
    _section("02", "Analysis", "Willingness to mentor — Pre → Post SGM transitions by stream and shift.")

    df_panel   = pd.read_excel(PANEL_PATH)
    all_streams = sorted(df_panel["SelectedStream"].dropna().unique().tolist())
    all_shifts  = sorted(df_panel["SelectedShift"].dropna().unique().tolist())

    def _shifts_for_stream(stream_choice):
        if stream_choice == "All":
            return ["All"] + all_shifts
        available = sorted(
            df_panel[df_panel["SelectedStream"] == stream_choice]["SelectedShift"]
            .dropna().unique().tolist()
        )
        return ["All"] + available

    col1, col2, _pad = st.columns([1, 1, 3])
    with col1:
        stream_choice = st.selectbox("Stream", ["All"] + all_streams)
    with col2:
        shift_choice = st.selectbox("Shift", _shifts_for_stream(stream_choice))

    stream_arg = None if stream_choice == "All" else stream_choice
    shift_arg  = None if shift_choice == "All" else shift_choice

    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)

    try:
        fig = build_sankey_filtered(PANEL_PATH, stream=stream_arg, shift=shift_arg)
        fig.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            font=dict(family="IBM Plex Mono, monospace", color="#4b5563", size=12),
            margin=dict(l=24, r=24, t=40, b=24),
        )
        st.plotly_chart(fig, use_container_width=True)
    except ValueError as e:
        st.warning(str(e))

    st.markdown("<hr>", unsafe_allow_html=True)
    _section("03", "Export")

    if os.path.exists(PANEL_PATH):
        with open(PANEL_PATH, "rb") as f:
            st.download_button(
                label="⬇  Download Panel Dataset",
                data=f.read(),
                file_name="SGM3_SurveyResults_Processed_Panel.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

else:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@600&family=IBM+Plex+Mono:wght@400&display=swap');
.idle-box {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    margin-top: 2.5rem;
    padding: 1.8rem 2rem;
    background: #f3f1ec;
    border: 1px solid #ddd9d0;
    border-radius: 6px;
}
.idle-icon {
    font-size: 1.6rem;
    color: #9ca3af;
    flex-shrink: 0;
    line-height: 1;
}
.idle-text {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    color: #9ca3af;
    line-height: 1.8;
}
.idle-text strong {
    font-family: 'Syne', sans-serif;
    color: #4b5563;
    font-weight: 600;
}
</style>
<div class="idle-box">
  <div class="idle-icon">⬡</div>
  <div class="idle-text">
    Results will appear here once the pipeline has run.<br>
    Select a data source above and click <strong>Run Pipeline</strong> to begin.
  </div>
</div>
""", unsafe_allow_html=True)
