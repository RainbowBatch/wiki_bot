from __future__ import print_function

import os.path
import kfio
import natsort

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from pprint import pprint

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1dYbaOXvCVSgb89enbXKgmorXHwxmGPIuSHqaoHYoLXw'
SAMPLE_RANGE_NAME = 'data!A1:D'

COLUMN_REMAP = {
    "Human Reviewed?": "reviewed",
    "Ep #": 'episode_number',
    "host": "host",
    "Bright Spot": "brightSpot",
}

def download_bright_spots():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        fields = ""
        result = sheet.get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            ranges=[SAMPLE_RANGE_NAME],
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
        )

        grouped = grouped.sort_values(by=['episode_number'], key=natsort.natsort_keygen())

        # Show the final output
        print(grouped)

        kfio.save(grouped, 'data/bright_spots.json')


    except HttpError as err:
        print(err)


if __name__ == '__main__':
    download_bright_spots()