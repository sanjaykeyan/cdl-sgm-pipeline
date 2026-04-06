"""
Survey processing pipeline.
Extracted from survey3_processing.ipynb.

Input:  RawData/SGM3_SurveyResults.csv
        RawData/SGM-SGM Matching-Feb 23.csv
Output: ProcessedData/SGM3_SurveyResults_Processed_Panel.xlsx
        ProcessedData/SGM3_Complete_Dataset_with_Participation.xlsx
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


def run_processing(raw_csv: str, matching_csv: str, output_dir: str) -> dict:
    """
    Run the full survey processing pipeline.

    Parameters
    ----------
    raw_csv : str
        Path to the raw Qualtrics CSV (e.g. RawData/SGM3_SurveyResults.csv).
    matching_csv : str
        Path to the SGM Matching CSV (e.g. RawData/SGM-SGM Matching-Feb 23.csv).
    output_dir : str
        Directory where processed files will be saved.

    Returns
    -------
    dict
        Paths to the two output Excel files:
        { 'panel': ..., 'complete': ... }
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
    # 3. Build complete mentor roster from SGM Matching file
    # -----------------------------------------------------------------
    sgm_matching = pd.read_csv(matching_csv)

    mentor_roster = []
    for _, row in sgm_matching.iterrows():
        room = str(row["Room Name"])
        parts = room.split("|")
        stream = parts[0].strip()
        shift = int(parts[1].strip().split()[1])
        mentors = [m.strip() for m in str(row["[People] Mentor Group"]).split(",") if m.strip()]
        for mentor in mentors:
            mentor_roster.append({"Mentor": mentor, "Stream": stream, "Shift": shift})

    mentor_roster_df = pd.DataFrame(mentor_roster).drop_duplicates()

    # Each mentor × shift × pre/post
    complete_roster = []
    for _, row in mentor_roster_df.iterrows():
        for prepost in ["Pre SGM", "Post SGM"]:
            complete_roster.append(
                {"Mentor": row["Mentor"], "Stream": row["Stream"], "Shift": row["Shift"], "PrePost": prepost}
            )
    complete_roster_df = pd.DataFrame(complete_roster)

    # -----------------------------------------------------------------
    # 4. Merge survey responses onto complete roster
    # -----------------------------------------------------------------
    # Only completed surveys
    survey_data = df[df["Finished"] == 1].copy()

    survey_data = survey_data.rename(
        columns={
            "SelectedMentor": "Mentor",
            "SelectedStream": "Stream",
            "SelectedShift": "Shift",
            "Pre/Post.1": "PrePost",
        }
    )

    # "Shift 1" → 1
    survey_data["Shift"] = survey_data["Shift"].astype(str).str.extract(r"(\d+)").astype(int)
    survey_data["Participated"] = True

    complete_dataset = complete_roster_df.merge(
        survey_data, on=["Mentor", "Stream", "Shift", "PrePost"], how="left"
    )
    # Drop low-value metadata columns kept from Qualtrics
    for col in ["Progress", "Finished", "RecordedDate", "LocationLatitude", "LocationLongitude"]:
        if col in complete_dataset.columns:
            complete_dataset.drop(columns=[col], inplace=True)

    # Participated is True for matched rows, NaN for unmatched — convert to 0/1
    complete_dataset["Participated"] = complete_dataset["Participated"].notna().astype(int)

    # Re-order: key columns first
    key_cols = ["Mentor", "Stream", "Shift", "PrePost", "Participated"]
    other_cols = [c for c in complete_dataset.columns if c not in key_cols]
    complete_dataset = complete_dataset[key_cols + other_cols]

    complete_path = os.path.join(output_dir, "SGM3_Complete_Dataset_with_Participation.xlsx")
    complete_dataset.to_excel(complete_path, index=False)

    return {"panel": panel_path, "complete": complete_path}
