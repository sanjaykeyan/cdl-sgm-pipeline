# CDL SGM Pipeline — Claude Onboarding

## What this is
A Streamlit app that processes CDL SGM (Science to Market) survey data and displays results. Users either fetch live data from Qualtrics or upload a CSV export, and the app runs a processing pipeline and shows interactive Sankey diagrams.

## Repo structure
```
app.py                        # Streamlit entry point — run with: streamlit run app.py
pipeline/
  orchestrator.py             # Runs all steps in sequence, yields progress messages
  qualtrics_client.py         # Fetches survey CSV from Qualtrics API
  survey_processing.py        # Raw CSV → Panel Excel + Complete Dataset Excel
  room_allocation.py          # Assigns mentors to rooms based on venture sequences
  sankey_charts.py            # Builds Plotly Sankey figures (filterable by stream/shift)
.streamlit/
  secrets.toml.example        # Template — copy to secrets.toml and fill in credentials
requirements.txt
```

## How the pipeline works
1. **Data in**: Qualtrics API fetch OR user uploads a CSV (same format as Qualtrics export)
2. **survey_processing.py**: Wide → long reshape, builds complete mentor roster, merges participation
3. **room_allocation.py**: Groups mentors by venture sequence, assigns room names
4. **sankey_charts.py**: Matches Pre/Post SGM pairs on YN column, builds transition diagrams
5. **Outputs**: `ProcessedData/` and `analysis_outputs/figures/` (generated at runtime, gitignored)

## Key data facts
- Survey CSV columns: `venture1`–`venture5`, `SelectedMentor`, `SelectedStream`, `SelectedShift`, `Pre/Post.1`, `YN_1`–`YN_5`, `Slider_*`, `Comparison_*`, `Finished`
- Streams: AI, Climate — Shifts: Shift 1, Shift 2 (not all stream/shift combos exist)
- CSV may have **extra columns or different column order** — pipeline selects by name only
- Qualtrics exports have 2 extra metadata header rows — survey_processing.py auto-detects and skips them
- The matching CSV (`SGM-SGM Matching-Feb 23.csv`) is a separate static input, NOT uploaded by users

## App behaviour
- Results (graphs, downloads) only appear **after the pipeline runs in the current session** — controlled via `st.session_state["pipeline_complete"]`
- Sankey chart has Stream + Shift dropdowns; Shift options are filtered based on selected Stream
- Download button exposes `SGM3_Complete_Dataset_with_Participation.xlsx`
- Qualtrics credentials go in `.streamlit/secrets.toml` (gitignored); app shows a warning but still works without them if CSV upload is used instead

## Local dev
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Credentials (never commit)
```toml
# .streamlit/secrets.toml
QUALTRICS_API_TOKEN = "..."
QUALTRICS_SURVEY_ID = "SV_..."
QUALTRICS_BASE_URL  = "https://yul1.qualtrics.com"
```

## Deployment
Hosted on Streamlit Community Cloud — private repo `sanjaykeyan/cdl-sgm-pipeline`.
Secrets are configured in the Streamlit Cloud dashboard (App settings → Secrets).
Filesystem is ephemeral on Streamlit Cloud — all output files are generated fresh each session.
