"""
Survey processing pipeline.
Extracted from survey3_processing.ipynb.

Input:  RawData/SGM3_SurveyResults.csv
Output: ProcessedData/SGM3_SurveyResults_Processed_Panel.xlsx
        ProcessedData/SGM3_Mentor_Roster.xlsx
"""

import os

import pandas as pd


# Column mapping: each venture slot uses different raw column names
_VENTURE_COLS_MAP = {
    1: [
        "Slider_1_9", "Slider_1_10", "Slider_1_11", "Slider_1_12", "Slider_1_13",
        "Comparison_1_4", "Comparison_1_5", "Comparison_1_6", "Comparison_1_7",
        "Comparison_1_8", "Comparison_1_9", "YN_1",
    ],
    2: [
        "Slider_2_4", "Slider_2_5", "Slider_2_6", "Slider_2_7", "Slider_2_8",
        "Comparison_2_4", "Comparison_2_5", "Comparison_2_6", "Comparison_2_7",
        "Comparison_2_8", "Comparison_2_9", "YN_2",
    ],
    3: [
        "Slider_3_4", "Slider_3_5", "Slider_3_6", "Slider_3_7", "Slider_3_8",
        "Comparison_3_4", "Comparison_3_5", "Comparison_3_6", "Comparison_3_7",
        "Comparison_3_8", "Comparison_3_9", "YN_3",
    ],
    4: [
        "Slider_4_4", "Slider_4_5", "Slider_4_6", "Slider_4_7", "Slider_4_8",
        "Comparison_4_4", "Comparison_4_5", "Comparison_4_6", "Comparison_4_7",
        "Comparison_4_8", "Comparison_4_9", "YN_4",
    ],
    5: [
        "Slider_5_4", "Slider_5_5", "Slider_5_6", "Slider_5_7", "Slider_5_8",
        "Comparison_5_4", "Comparison_5_5", "Comparison_5_6", "Comparison_5_7",
        "Comparison_5_8", "Comparison_5_9", "YN_5",
    ],
}

_STD_COLS = [
    "Slider_1", "Slider_2", "Slider_3", "Slider_4", "Slider_5",
    "Comparison_4", "Comparison_5", "Comparison_6", "Comparison_7",
    "Comparison_8", "Comparison_9", "YN",
]

_META_COLS = ["SelectedMentor", "SelectedStream", "SelectedShift", "Pre/Post.1"]

_DROP_COLS = [
    "EndDate", "Status", "IPAddress", "RecipientLastName", "RecipientFirstName",
    "RecipientEmail", "ExternalReference", "DistributionChannel", "UserLanguage",
    "Q28", "Q29", "Pre/Post", "Q19", "Timing",
]


def run_processing(raw_csv: str, output_dir: str) -> dict:
    """
    Run the full survey processing pipeline.

    Parameters
    ----------
    raw_csv : str
        Path to the raw Qualtrics CSV (e.g. RawData/SGM3_SurveyResults.csv).
    output_dir : str
        Directory where processed files will be saved.

    Returns
    -------
    dict
        Paths to the two output Excel files:
        { 'panel': ..., 'roster': ... }
    """
    os.makedirs(output_dir, exist_ok=True)

    # -----------------------------------------------------------------
    # 1. Load and clean raw survey data
    # -----------------------------------------------------------------
    # Qualtrics CSV exports include 2 extra header rows after the column names:
    #   row 1 (index 0): column names  ← kept as header by pandas
    #   row 2 (index 1): question text (e.g. "Please select your name")
    #   row 3 (index 2): Qualtrics import IDs (e.g. '{"ImportId":"QID50"}')
    #   row 4+: actual response data
    # Detect these by checking if the "Finished" column's first two rows
    # are non-numeric (metadata) and skip them if so.
    df = pd.read_csv(raw_csv)
    if "Finished" in df.columns:
        try:
            pd.to_numeric(df["Finished"].iloc[0])
        except (ValueError, TypeError):
            # First data row is metadata — skip the first two rows
            df = pd.read_csv(raw_csv, skiprows=[1, 2])

    existing_drop = [c for c in _DROP_COLS if c in df.columns]
    df.drop(columns=existing_drop, inplace=True)

    # Validate required venture columns are present
    venture_name_cols = ["venture1", "venture2", "venture3", "venture4", "venture5"]
    missing = [c for c in venture_name_cols if c not in df.columns]
    if missing:
        found = [c for c in df.columns if "venture" in c.lower() or c.startswith("Q")][:10]
        raise ValueError(
            f"Expected columns {venture_name_cols} not found in the uploaded CSV.\n"
            f"Columns found (sample): {df.columns.tolist()[:20]}\n\n"
            "Make sure you are uploading the raw Qualtrics export with the correct export tags."
        )

    # Keep rows that have at least one venture
    df = df.dropna(subset=venture_name_cols, how="all")

    # -----------------------------------------------------------------
    # 2. Wide → long reshape (one row per mentor × venture)
    # -----------------------------------------------------------------
    dfs = []
    for i in range(1, 6):
        venture_name_col = f"venture{i}"
        raw_cols = _VENTURE_COLS_MAP[i]

        available_raw = [c for c in raw_cols if c in df.columns]
        available_std = _STD_COLS[: len(available_raw)]

        sub = df[_META_COLS + [venture_name_col] + available_raw].copy()
        sub = sub.rename(
            columns=dict(zip([venture_name_col] + available_raw, ["VentureName"] + available_std))
        )
        sub["VentureSlot"] = i
        dfs.append(sub)

    df_long = pd.concat(dfs, ignore_index=True)
    df_long = df_long.dropna(subset=["VentureName"])

    # Recode YN: 1→Yes, 2→No, 3→DK
    if "YN" in df_long.columns:
        df_long["YN"] = df_long["YN"].replace({1: "Yes", 2: "No", 3: "DK"})

    panel_path = os.path.join(output_dir, "SGM3_SurveyResults_Processed_Panel.xlsx")
    df_long.to_excel(panel_path, index=False)

    # -----------------------------------------------------------------
    # 3. Build mentor roster from survey data (for room allocation)
    # -----------------------------------------------------------------
    roster_cols = ["SelectedMentor", "SelectedStream", "SelectedShift"] + venture_name_cols
    mentor_roster = df[roster_cols].rename(columns={
        "SelectedMentor": "Mentor",
        "SelectedStream": "Stream",
        "SelectedShift": "Shift",
    }).copy()
    mentor_roster["Shift"] = (
        mentor_roster["Shift"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    )
    mentor_roster = mentor_roster.dropna(subset=["Mentor", venture_name_cols[0]])
    mentor_roster = mentor_roster.drop_duplicates(subset=["Mentor", "Stream", "Shift"])

    roster_path = os.path.join(output_dir, "SGM3_Mentor_Roster.xlsx")
    mentor_roster.to_excel(roster_path, index=False)

    return {"panel": panel_path, "roster": roster_path}
