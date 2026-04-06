"""
Pipeline orchestrator.
Runs all steps in sequence and yields status messages for Streamlit progress display.
"""

import os
from typing import Generator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_CSV = os.path.join(BASE_DIR, "RawData", "SGM3_SurveyResults.csv")
PROCESSED_DIR = os.path.join(BASE_DIR, "ProcessedData")
FIGURES_DIR = os.path.join(BASE_DIR, "analysis_outputs", "figures")

PANEL_PATH = os.path.join(PROCESSED_DIR, "SGM3_SurveyResults_Processed_Panel.xlsx")
ROSTER_PATH = os.path.join(PROCESSED_DIR, "SGM3_Mentor_Roster.xlsx")
ALLOCATION_PATH = os.path.join(PROCESSED_DIR, "SGM3_Room_Allocation.xlsx")


def run_pipeline(skip_fetch: bool = False) -> Generator[tuple[str, dict | None], None, None]:
    """
    Run the full pipeline, yielding (status_message, result_or_None) tuples.

    Each yield is either a progress message (result=None) or the final outputs dict.
    The last yielded value always has result set to the outputs dict on success.

    Parameters
    ----------
    skip_fetch : bool
        If True, skip the Qualtrics download and use the existing raw CSV.
        Useful for re-running processing without re-fetching data.
    """
    from pipeline.qualtrics_client import fetch_survey
    from pipeline.room_allocation import run_room_allocation
    from pipeline.sankey_charts import run_sankey
    from pipeline.survey_processing import run_processing

    # Step 1: Fetch data from Qualtrics
    if not skip_fetch:
        yield "Fetching data from Qualtrics...", None
        os.makedirs(os.path.join(BASE_DIR, "RawData"), exist_ok=True)
        fetch_survey(RAW_CSV)
        yield "✓ Qualtrics data downloaded", None
    else:
        if not os.path.exists(RAW_CSV):
            raise FileNotFoundError(
                f"Raw CSV not found at {RAW_CSV}. "
                "Uncheck 'Skip fetch' or ensure the file exists."
            )
        yield "✓ Using existing raw data (fetch skipped)", None

    # Step 2: Process survey
    yield "Processing survey responses...", None
    run_processing(RAW_CSV, PROCESSED_DIR)
    yield "✓ Survey responses processed", None

    # Step 3: Room allocation
    yield "Assigning rooms...", None
    run_room_allocation(ROSTER_PATH, PROCESSED_DIR, panel_path=PANEL_PATH)
    yield "✓ Room allocation complete", None

    # Step 4: Sankey diagrams
    yield "Generating Sankey diagrams...", None
    sankey_paths = run_sankey(PANEL_PATH, FIGURES_DIR)
    yield "✓ Sankey diagrams generated", None

    yield "Pipeline complete!", {
        "panel": PANEL_PATH,
        "allocation": ALLOCATION_PATH,
        "sankey": sankey_paths,
    }
