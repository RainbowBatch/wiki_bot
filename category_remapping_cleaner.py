import pandas as pd
from pprint import pprint
from box import Box
import math

raw_category_remapping_df = pd.read_csv('categories_remapping_raw.csv')

print(raw_category_remapping_df)

RECORDS = raw_category_remapping_df.to_dict(orient='records')
NEW_RECORDS = []

ALL_CATEGORIES = set()
for raw_record in RECORDS:
    raw_record = Box(raw_record)
    pprint(raw_record)
    categories = []
    for key in ['category_1', 'category_2', 'category_3']:
        category = raw_record[key]
        if isinstance(category, float) and math.isnan(category):
            continue
        categories.append(category)

    categories = sorted(categories)

    people = []
    people_slug = raw_record.people
    if not (isinstance(people_slug, float) and math.isnan(people_slug)):
        people.extend(people_slug.split(';'))
    people = [person.strip() for person in people]

    ALL_CATEGORIES.update(categories)

    NEW_RECORDS.append({
        'original_category': raw_record.category,
        'new_categories': ';'.join(categories),
        'people': ';'.join(people)
    })

df = pd.DataFrame.from_records(NEW_RECORDS)

print(ALL_CATEGORIES)

with open("categories_remapping.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))