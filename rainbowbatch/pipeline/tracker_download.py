import os.path
import rainbowbatch.kfio as kfio

import pandas as pd
from rainbowbatch.external.google_sheets import make_sheets_client


# The ID and range of a sample spreadsheet.
# This is the original, but there are mistakes. '1p2u4MH5ywwsIHZvpdBbpzhPqAQedFR_xvbs1jbiAVOk'
SAMPLE_SPREADSHEET_ID = '1HfPRsxjGFFqQWXaY_NKSX1ej7bRz5cuKCWp1T6Y1fI8'
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


sheet = make_sheets_client()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
values = result.get('values', [])

if not values:
    print('No data found.')
else:
    df = pd.DataFrame(values[1:], columns=[
        COLUMN_REMAP[original_column_name]
        for original_column_name in values[0]
    ])

    print("WRITING", df)

    kfio.save(df, 'data/tracker.json')
