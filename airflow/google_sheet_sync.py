import natsort
import pandas as pd
import rainbowbatch.kfio as kfio

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from rainbowbatch.external.google_sheets import make_sheets_client

# TODO: Also write back to sheet.
# TODO: Handle Links in cells!

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1dYbaOXvCVSgb89enbXKgmorXHwxmGPIuSHqaoHYoLXw'

BRIGHT_SPOT_RANGE = 'bright_spots!A1:D'
OOC_DROP_RANGE = 'ooc_drops!A1:C'

COLUMN_REMAP = {
    "Human Reviewed?": "reviewed",
    "Ep #": 'episode_number',
    "host": "host",
    "Bright Spot": "brightSpot",
    "OOC Drop": "ooc_drop",
}


def download_bright_spots():
    sheet = make_sheets_client()
    fields = ""
    result = sheet.get(
        spreadsheetId=SPREADSHEET_ID,
        ranges=[BRIGHT_SPOT_RANGE],
        includeGridData=True
    ).execute()

    grid_data = result['sheets'][0]['data'][0]['rowData']

    # Get column headers from the first row
    headers = [cell.get('formattedValue', '')
               for cell in grid_data[0]['values']]

    # Prepare remapped headers
    mapped_headers = [COLUMN_REMAP.get(h, h) for h in headers]

    rows = []
    for row in grid_data[1:]:
        values = []
        for header, cell in zip(headers, row.get('values', [])):
            text = cell.get('formattedValue', '')
            hyperlink = cell.get('hyperlink')

            if hyperlink and header == 'Bright Spot':
                # Convert to MediaWiki format
                text = f"[{hyperlink} {text}]"

            values.append(text)

        # pad missing columns
        while len(values) < len(headers):
            values.append('')
        rows.append(values)

    df = pd.DataFrame(rows, columns=mapped_headers)

    reviewed_df = df[df.reviewed == "Yes"]

    # Group and aggregate
    grouped = (
        reviewed_df
        .sort_values(by=["episode_number", "host"])  # ensure stable order
        .groupby("episode_number")
        .apply(lambda g: ", ".join(
            f"{host}: {', '.join(bs for bs in g[g.host == host]['brightSpot'])}"
            for host in sorted(g['host'].unique())
        ).replace(".,", ".").replace("!,", "!"))
        .reset_index(name="brightSpot")
    )[['episode_number', 'brightSpot']]

    existing_data = kfio.load('data/final.json')
    existing_data = existing_data[~existing_data.brightSpot.isna()][[
        'episode_number', 'brightSpot']]

    grouped = existing_data.merge(grouped, on='episode_number', how='outer')

    # Prefer 'brightSpot_y' (from grouped), but fall back to 'brightSpot_x' (from existing_data)
    grouped['brightSpot'] = grouped['brightSpot_y'].combine_first(
        grouped['brightSpot_x'])

    # Optionally drop the old columns
    grouped = grouped.drop(columns=['brightSpot_x', 'brightSpot_y'])

    grouped = grouped.sort_values(
        by=['episode_number'], key=natsort.natsort_keygen())

    # Show the final output
    print(grouped)

    kfio.save(grouped, 'data/bright_spots.json')


def download_ooc_drops():
    sheet = make_sheets_client()

    fields = ""
    result = sheet.get(
        spreadsheetId=SPREADSHEET_ID,
        ranges=[OOC_DROP_RANGE],
        includeGridData=True
    ).execute()

    grid_data = result['sheets'][0]['data'][0]['rowData']

    # Get column headers from the first row
    headers = [cell.get('formattedValue', '')
               for cell in grid_data[0]['values']]

    # Prepare remapped headers
    mapped_headers = [COLUMN_REMAP.get(h, h) for h in headers]

    rows = []
    for row in grid_data[1:]:
        values = []
        for header, cell in zip(headers, row.get('values', [])):
            text = cell.get('formattedValue', '')
            values.append(text)

        # pad missing columns
        while len(values) < len(headers):
            values.append('')
        rows.append(values)
    df = pd.DataFrame(rows, columns=mapped_headers)

    reviewed_df = df[df.reviewed == "Yes"][['episode_number', 'ooc_drop']]

    existing_data = kfio.load('data/final.json')
    existing_data = existing_data[~existing_data.ooc_drop.isna()][[
        'episode_number', 'ooc_drop']]

    reviewed_df = existing_data.merge(
        reviewed_df, on='episode_number', how='outer')

    reviewed_df = reviewed_df.sort_values(
        by=['episode_number'], key=natsort.natsort_keygen())

    # Prefer 'ooc_drop_y' (from grouped), but fall back to 'ooc_drop_x' (from existing_data)
    reviewed_df['ooc_drop'] = reviewed_df['ooc_drop_y'].combine_first(
        reviewed_df['ooc_drop_x'])

    # Optionally drop the old columns
    reviewed_df = reviewed_df.drop(columns=['ooc_drop_x', 'ooc_drop_y'])

    # TODO: FIX THINGS.

    # Show the final output
    print(reviewed_df)

    kfio.save(reviewed_df, 'data/ooc_drops.json')


with DAG(
    "google_sheet_sync",
    start_date=datetime(2025, 6, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    bright_spot_task = PythonOperator(
        task_id='bright_spot_downloader',
        python_callable=download_bright_spots,
    )

    ooc_drop_task = PythonOperator(
        task_id='ooc_drop_downloader',
        python_callable=download_ooc_drops,
    )
