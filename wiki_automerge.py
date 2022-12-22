import re
from pprint import pprint
from box import Box
from wiki_cleaner import simple_format
import kfio

merge_conflict_pattern = re.compile(
    r'^<{7} (?P<branch1>[a-z0-9_-]+)(?P<content1>(?:\n(?!={7}\n).*)*)\n={7}(?P<content2>(?:\n(?!>{7} ).*)*)\n>{7} (?P<branch2>[a-z0-9_-]+)',
    re.MULTILINE | re.IGNORECASE,
)


SAMPLE = '''
<<<<<<< head-5
-Stacy
-Alexander

Marketing team
- Collins
- Linda
- Patricia
- Morgan
- Amanda
=======
zzzz
zzz
z
>>>>>>> master-2
'''


def get_merge_conflicts(text):
    conflicts = []
    for m in merge_conflict_pattern.finditer(text):
        conflicts.append(Box(m.groupdict()))
    return conflicts


#print(merge_conflict_pattern.search(SAMPLE))


def my_replace(match):
    match = match.group()
    return match + str(match.index('e'))


#string = "The quick @red fox jumps over the @lame brown dog."
#print(re.sub(r'@\w+', my_replace, string))

page_listing = kfio.load('kf_wiki_content/page_listing.json')

for page_record in page_listing.to_dict(orient='records'):
    page_record = Box(page_record)

    if 'Dreamy Creamy Summer' in page_record.title:
        continue

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    try:
        with open(fname, encoding='utf-8') as f:
            page_text = f.read()

        conflicts = get_merge_conflicts(page_text)

        if len(conflicts) > 0:
            print(len(conflicts), "conflicts in", fname)

        if len(conflicts) != 1:
            continue

        raw = conflicts[0].content2.strip() + '\n\n' + conflicts[0].content1.strip()
        pretty = simple_format(raw)

        with open(fname, mode="w", encoding="utf-8") as f:
            f.write(pretty)

    except:
        print("Problem with", fname)
