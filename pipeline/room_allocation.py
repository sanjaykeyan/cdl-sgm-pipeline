"""
Room allocation pipeline.
Extracted from sgm3_room_allocation.ipynb.

Input:  ProcessedData/SGM3_Mentor_Roster.xlsx
Output: ProcessedData/SGM3_Room_Allocation.xlsx
"""

import os

import pandas as pd


_VENTURE_COLS = ["venture1", "venture2", "venture3", "venture4", "venture5"]


def _build_venture_sequence(row):
    return tuple(str(row[col]).strip() for col in _VENTURE_COLS if pd.notna(row[col]))


def _assign_rooms(group_df):
    stream = group_df["Stream"].iloc[0]
    shift = group_df["Shift"].iloc[0]
    room_map = {}
    room_counter = 1
    room_numbers = []
    room_names = []

    for seq in group_df["venture_sequence"]:
        if seq not in room_map:
            room_map[seq] = room_counter
            room_counter += 1
        num = room_map[seq]
        room_numbers.append(num)
        room_names.append(f"S{shift}R{num} - {stream}")

    result = group_df.copy()
    result["Room"] = room_numbers
    result["Room Name"] = room_names
    return result


def run_room_allocation(roster_path: str, output_dir: str, panel_path: str | None = None) -> dict:
    """
    Assign rooms to mentors and write output file.
    Optionally merges Room Name back into the panel dataset.

    Parameters
    ----------
    roster_path : str
        Path to SGM3_Mentor_Roster.xlsx.
    output_dir : str
        Directory for SGM3_Room_Allocation.xlsx.
    panel_path : str, optional
        Path to the panel Excel file to update with Room Name column.

    Returns
    -------
    dict
        { 'allocation': path_to_room_allocation }
    """
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_excel(roster_path)

    # One canonical row per mentor × Stream × Shift (prefer rows with venture data)
    venture_col = _VENTURE_COLS[0]
    if venture_col not in df.columns:
        raise ValueError(f"Column '{venture_col}' not found. Check input file columns.")

    df_mentors = (
        df.dropna(subset=[venture_col])
        .drop_duplicates(subset=["Mentor", "Stream", "Shift"])
        .copy()
    )

    df_mentors["venture_sequence"] = df_mentors.apply(_build_venture_sequence, axis=1)

    # Assign rooms within each Stream × Shift
    parts = []
    for (stream, shift), group_df in df_mentors.groupby(["Stream", "Shift"]):
        parts.append(_assign_rooms(group_df))
    df_rooms = (
        pd.concat(parts, ignore_index=True)
        .sort_values(["Stream", "Shift", "Room", "Mentor"])
        .reset_index(drop=True)
    )

    # Room summary: one row per room
    summary_rows = []
    for (stream, shift, room), grp in df_rooms.groupby(["Stream", "Shift", "Room"]):
        venture_seq = grp["venture_sequence"].iloc[0]
        room_name = grp["Room Name"].iloc[0]
        mentors = sorted(grp["Mentor"].tolist())
        summary_rows.append(
            {
                "Stream": stream,
                "Shift": shift,
                "Room Name": room_name,
                "Ventures (in order)": " → ".join(venture_seq),
                "Mentors": ", ".join(mentors),
                "Mentor Count": len(mentors),
            }
        )
    df_summary = pd.DataFrame(summary_rows)

    # Flat allocation table: one row per mentor
    alloc_cols = ["Stream", "Shift", "Room Name", "Mentor"] + _VENTURE_COLS
    existing_alloc_cols = [c for c in alloc_cols if c in df_rooms.columns]
    df_allocation = (
        df_rooms[existing_alloc_cols]
        .sort_values(["Stream", "Shift", "Room Name", "Mentor"])
        .reset_index(drop=True)
    )

    allocation_path = os.path.join(output_dir, "SGM3_Room_Allocation.xlsx")
    with pd.ExcelWriter(allocation_path, engine="openpyxl") as writer:
        df_allocation.to_excel(writer, sheet_name="Room_Allocation", index=False)
        df_summary.to_excel(writer, sheet_name="Room_Summary", index=False)

    # Merge Room Name back into the panel dataset
    if panel_path and os.path.exists(panel_path):
        room_lookup = df_rooms[["Mentor", "Stream", "Shift", "Room Name"]].drop_duplicates()

        df_panel = pd.read_excel(panel_path)
        # Panel uses original column names and "Shift 1" string format — normalise for join
        df_panel["_ShiftInt"] = (
            df_panel["SelectedShift"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        )
        df_panel = df_panel.merge(
            room_lookup.rename(columns={"Mentor": "SelectedMentor", "Stream": "SelectedStream", "Shift": "_ShiftInt"}),
            on=["SelectedMentor", "SelectedStream", "_ShiftInt"],
            how="left",
        ).drop(columns=["_ShiftInt"])

        # Move Room Name after SelectedShift
        cols = df_panel.columns.tolist()
        if "Room Name" in cols:
            cols.remove("Room Name")
            shift_idx = cols.index("SelectedShift") if "SelectedShift" in cols else 3
            cols.insert(shift_idx + 1, "Room Name")
            df_panel = df_panel[cols]

        df_panel.to_excel(panel_path, index=False)

    return {"allocation": allocation_path}
