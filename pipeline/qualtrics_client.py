"""
Qualtrics API client.
Downloads survey responses and saves them as a CSV to RawData/.
"""

import io
import os
import time
import zipfile

import requests
import streamlit as st


def _get_secrets():
    try:
        token = st.secrets["QUALTRICS_API_TOKEN"]
        survey_id = st.secrets["QUALTRICS_SURVEY_ID"]
        base_url = st.secrets["QUALTRICS_BASE_URL"].rstrip("/")
    except Exception:
        raise RuntimeError(
            "Missing Qualtrics credentials. "
            "Add QUALTRICS_API_TOKEN, QUALTRICS_SURVEY_ID, and QUALTRICS_BASE_URL "
            "to .streamlit/secrets.toml (local) or the Streamlit Cloud secrets dashboard."
        )
    return token, survey_id, base_url


def fetch_survey(output_path: str, poll_interval: float = 3.0, timeout: float = 300.0) -> str:
    """
    Export responses from Qualtrics and save as CSV.

    Parameters
    ----------
    output_path : str
        Where to save the downloaded CSV file.
    poll_interval : float
        Seconds between status checks.
    timeout : float
        Maximum seconds to wait for the export to complete.

    Returns
    -------
    str
        Path to the saved CSV file.
    """
    token, survey_id, base_url = _get_secrets()

    headers = {
        "X-API-TOKEN": token,
        "Content-Type": "application/json",
    }

    # 1. Start export
    export_url = f"{base_url}/API/v3/surveys/{survey_id}/export-responses"
    payload = {"format": "csv"}
    resp = requests.post(export_url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    progress_id = resp.json()["result"]["progressId"]

    # 2. Poll until complete
    status_url = f"{export_url}/{progress_id}"
    deadline = time.time() + timeout
    while True:
        if time.time() > deadline:
            raise TimeoutError("Qualtrics export did not complete within the timeout period.")
        time.sleep(poll_interval)
        status_resp = requests.get(status_url, headers=headers, timeout=30)
        status_resp.raise_for_status()
        data = status_resp.json()["result"]
        status = data.get("status")
        if status == "complete":
            file_id = data["fileId"]
            break
        if status == "failed":
            raise RuntimeError(f"Qualtrics export failed: {data}")

    # 3. Download ZIP
    file_url = f"{export_url}/{file_id}/file"
    file_resp = requests.get(file_url, headers=headers, timeout=60)
    file_resp.raise_for_status()

    # 4. Extract CSV from ZIP
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(file_resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV found in Qualtrics export ZIP.")
        with zf.open(csv_names[0]) as csv_file, open(output_path, "wb") as out_file:
            out_file.write(csv_file.read())

    return output_path
