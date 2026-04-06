"""
CDL SGM3 Pipeline — Streamlit app.

Two data sources: fetch from Qualtrics API, or upload a CSV directly.
After data is available, runs processing → room allocation → Sankey diagrams.
"""

import os

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="CDL SGM3 Pipeline",
    page_icon="🔬",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSV = os.path.join(BASE_DIR, "RawData", "SGM3_SurveyResults.csv")

PANEL_PATH = os.path.join(BASE_DIR, "ProcessedData", "SGM3_SurveyResults_Processed_Panel.xlsx")
FIGURES_DIR = os.path.join(BASE_DIR, "analysis_outputs", "figures")

SANKEY_PATHS = {
    "Overall": os.path.join(FIGURES_DIR, "sankey_yn_transitions.html"),
    "AI Stream": os.path.join(FIGURES_DIR, "sankey_yn_transitions_ai.html"),
    "Climate Stream": os.path.join(FIGURES_DIR, "sankey_yn_transitions_climate.html"),
}


def _check_credentials() -> bool:
    try:
        _ = st.secrets["QUALTRICS_API_TOKEN"]
        _ = st.secrets["QUALTRICS_SURVEY_ID"]
        _ = st.secrets["QUALTRICS_BASE_URL"]
        return True
    except Exception:
        return False


def _read_html(path: str) -> str | None:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None


def _outputs_exist() -> bool:
    """Only true if the pipeline was run in this session AND files are on disk."""
    return (
        st.session_state.get("pipeline_complete", False)
        and os.path.exists(PANEL_PATH)
    )


def _save_uploaded_csv(uploaded_file) -> None:
    os.makedirs(os.path.dirname(RAW_CSV), exist_ok=True)
    with open(RAW_CSV, "wb") as f:
        f.write(uploaded_file.getbuffer())


def _run_pipeline(skip_fetch: bool) -> None:
    from pipeline.orchestrator import run_pipeline

    status_box = st.empty()
    log_lines: list[str] = []

    try:
        for message, result in run_pipeline(skip_fetch=skip_fetch):
            log_lines.append(message)
            status_box.info("\n\n".join(log_lines))
            if result is not None:
                st.session_state["last_outputs"] = result
        st.session_state["pipeline_complete"] = True
        status_box.success("\n\n".join(log_lines))
        st.rerun()
    except Exception as exc:
        st.session_state["pipeline_complete"] = False
        st.error(f"Pipeline failed: {exc}")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("CDL SGM3 Pipeline")
st.caption("Fetch survey data, run processing, and explore results — all in one click.")

# ---------------------------------------------------------------------------
# Data source selection
# ---------------------------------------------------------------------------

st.divider()

has_credentials = _check_credentials()

source = st.radio(
    "Data source",
    options=["Fetch from Qualtrics", "Upload CSV"],
    horizontal=True,
    label_visibility="collapsed",
)

st.write("")  # spacing

if source == "Fetch from Qualtrics":
    if not has_credentials:
        st.warning(
            "**Qualtrics credentials not configured.**\n\n"
            "Create `.streamlit/secrets.toml` with:\n"
            "```toml\n"
            'QUALTRICS_API_TOKEN = "your_token"\n'
            'QUALTRICS_SURVEY_ID = "SV_xxxxxxxx"\n'
            'QUALTRICS_BASE_URL  = "https://yul1.qualtrics.com"\n'
            "```"
        )
    if st.button(
        "▶ Fetch & Process Data",
        type="primary",
        disabled=not has_credentials,
        use_container_width=False,
    ):
        _run_pipeline(skip_fetch=False)

else:  # Upload CSV
    uploaded = st.file_uploader(
        "Upload the Qualtrics CSV export (same format as SGM3_SurveyResults.csv)",
        type="csv",
        label_visibility="visible",
    )
    if uploaded is not None:
        if st.button("▶ Process Uploaded CSV", type="primary", use_container_width=False):
            _save_uploaded_csv(uploaded)
            _run_pipeline(skip_fetch=True)

# ---------------------------------------------------------------------------
# Results area (shown when outputs exist, even from a previous run)
# ---------------------------------------------------------------------------

if _outputs_exist():
    import pandas as pd
    from pipeline.sankey_charts import build_sankey_filtered  # noqa: E402

    st.divider()

    # Sankey diagrams
    st.subheader("Sankey Diagrams — Willingness to Mentor (Pre → Post SGM)")

    # Load available streams and shifts from the panel
    df_panel = pd.read_excel(PANEL_PATH)
    all_streams = sorted(df_panel["SelectedStream"].dropna().unique().tolist())
    all_shifts = sorted(df_panel["SelectedShift"].dropna().unique().tolist())

    # Compute which shifts are valid for the chosen stream
    def _shifts_for_stream(stream_choice):
        if stream_choice == "All":
            return ["All"] + all_shifts
        available = sorted(
            df_panel[df_panel["SelectedStream"] == stream_choice]["SelectedShift"]
            .dropna().unique().tolist()
        )
        return ["All"] + available

    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        stream_choice = st.selectbox("Stream", ["All"] + all_streams)
    with filter_col2:
        shift_choice = st.selectbox("Shift", _shifts_for_stream(stream_choice))

    stream_arg = None if stream_choice == "All" else stream_choice
    shift_arg = None if shift_choice == "All" else shift_choice

    try:
        fig = build_sankey_filtered(PANEL_PATH, stream=stream_arg, shift=shift_arg)
        st.plotly_chart(fig, use_container_width=True)
    except ValueError as e:
        st.warning(str(e))

    st.divider()

    # Downloads
    st.subheader("Download Datasets")

    if os.path.exists(PANEL_PATH):
        with open(PANEL_PATH, "rb") as f:
            st.download_button(
                label="⬇ Panel Dataset (with Room Allocation)",
                data=f.read(),
                file_name="SGM3_SurveyResults_Processed_Panel.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
else:
    st.info("Click **▶ Fetch & Process Data** to run the pipeline and see results here.")
