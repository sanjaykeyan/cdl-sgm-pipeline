"""
Room allocation pipeline.
Extracted from sgm3_room_allocation.ipynb.

Input:  ProcessedData/SGM3_Complete_Dataset_with_Participation.xlsx
Output: ProcessedData/SGM3_Room_Allocation.xlsx
        ProcessedData/SGM3_Complete_Dataset_with_Participation.xlsx  (updated with Room Name)
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


def run_room_allocation(complete_path: str, output_dir: str) -> dict:
    """
    Assign rooms to mentors and write two output files.

    Parameters
    ----------
    complete_path : str
        Path to SGM3_Complete_Dataset_with_Participation.xlsx.
    output_dir : str
        Directory for SGM3_Room_Allocation.xlsx.

    Returns
    -------
    dict
        { 'allocation': path_to_room_allocation, 'complete': updated_complete_path }
    """
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_excel(complete_path)

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

    # Merge Room Name back into the complete dataset
    room_lookup = df_rooms[["Mentor", "Stream", "Shift", "Room Name"]].drop_duplicates()

    if "Room Name" in df.columns:
        df = df.drop(columns=["Room Name"])

    df_merged = df.merge(room_lookup, on=["Mentor", "Stream", "Shift"], how="left")

    # Move Room Name to position 3 (after Shift)
    cols = df_merged.columns.tolist()
    if "Room Name" in cols:
        cols.remove("Room Name")
        cols.insert(3, "Room Name")
        df_merged = df_merged[cols]

    df_merged.to_excel(complete_path, index=False)

    return {"allocation": allocation_path, "complete": complete_path}
