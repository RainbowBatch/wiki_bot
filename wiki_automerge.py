import re

merge_conflict_pattern = re.compile(
	r'^<{7} (?P<branch1>[a-z0-9_-]+)(?P<content1>(?:\n(?!={7}\n).*)*)\n={7}(?P<content2>(?:\n(?!>{7} ).*)*)\n>{7} (?P<branch2>[a-z0-9_-]+)',
	re.MULTILINE|re.IGNORECASE,
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


print(merge_conflict_pattern.search(SAMPLE))

def my_replace(match):
     match = match.group()
     return match + str(match.index('e'))

string = "The quick @red fox jumps over the @lame brown dog."
print(re.sub(r'@\w+', my_replace, string))
