import rainbowbatch.kfio as kfio
from pick import pick
from pprint import pprint, pformat
from box import Box
from rainbowbatch.remap.wiki_cleaner import simple_format
from tqdm import tqdm

page_data = kfio.load('data/scraped_page_data.json')

categories = set()

for wiki_categories in page_data.wiki_categories.to_list():
    categories.update(wiki_categories)

categories = list(sorted(categories))
print(categories)

S = set([])
page_data = page_data[page_data.wiki_categories.map(S.issubset)]
page_data = page_data[page_data.wiki_categories.str.len() <= len(S)]


for page_record in tqdm(page_data.to_dict(orient='records')):
    page_record = Box(page_record)

    if page_record.redirect is not None:
        continue

    selected_categories = pick(
        [c for c in categories if c not in page_data.wiki_categories],
        pformat(page_record) + "[<space> select, <enter> proceed]",
        multiselect=True,
    )

    if len(selected_categories) == 0:
        continue

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    with open(fname, encoding='utf-8') as f:
        page_text = f.read()

    assert '#redirect' not in page_text.lower()

    page_text += ''.join([
        "\n\n[[Category:%s]]\n\n" % c
        for c, _ in selected_categories
    ])

    formatted_page_text = simple_format(page_text)

    if page_text.strip() != formatted_page_text.strip():
        with open(fname, "w", encoding='utf-8') as f:
            f.write(formatted_page_text)
