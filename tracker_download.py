from __future__ import print_function

import os.path

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
SAMPLE_SPREADSHEET_ID = '1p2u4MH5ywwsIHZvpdBbpzhPqAQedFR_xvbs1jbiAVOk'
SAMPLE_RANGE_NAME = 'Sheet1!A1:L'

COLUMN_REMAP = {
    'Episode Number': 'episode_number',
    'Air Date': 'release_date',
    'Coverage Start Date': 'coverage_start_date',
    'Coverage End Date': 'coverage_end_date',
    'Type of Episode': 'episode_type',
    'Novelty Beverage': 'beverage',
    'Refereneced people/ Guests ': 'people',
    'Books/ Primary sources': 'sources',
    'Themes': 'themes',
    'Out Of Context': 'ooc_drop',
    'Noteable Drops or Bits': 'notable_bits',
    'Episode Description': 'description',
}

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
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        df = pd.DataFrame(values[1:], columns = [
            COLUMN_REMAP[original_column_name]
            for original_column_name in values[0]
        ])

        pprint(
            df.to_dict(orient='records')
        )

        with open("tracker.csv", "w", encoding='utf-8') as csv_file:
            csv_file.write(df.to_csv(index=False, line_terminator='\n'))


except HttpError as err:
    print(err)