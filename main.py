import pywikibot
from pywikibot import pagegenerators
site = pywikibot.Site()
cat = pywikibot.Category(site,'Category:Living people')
gen = pagegenerators.CategorizedPageGenerator(cat)
for page in gen:
    #Do something with the page object, for example:
    text = page.text
    print(text)