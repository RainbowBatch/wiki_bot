# TODO(woursler): Extract URLS directly from libsyn_details.csv

'''
LINKS = []

	ep_record['mediawiki_description'] = pandoc.write(
		pandoc.read(ep_record['details_html'], format="html-native_divs-native_spans"),
		format="mediawiki"
	)

	parsed_description = wikitextparser.parse(ep_record['mediawiki_description'])

	if len(parsed_description.external_links) > 0:
		print(ep_record['title'])
		print(parsed_description.external_links)
		LINKS.extend(parsed_description.external_links)

for link in LINKS:
	print(link.url)

'''

URLS = [
    "https://knowledgefight.com/research/2021/10/10/episode-604-october-5-2021",
    "https://knowledgefight.com/research/2021/10/12/episode-605-june-17-2003",
    "https://knowledgefight.com/research/2021/10/14/episode-606-june-18-19-2003",
    "https://knowledgefight.com/research/2021/10/24/episode-609-october-20-2021",
    "https://knowledgefight.com/research/2021/10/31/episode-611-oct-29-2021",
    "https://knowledgefight.com/research/2021/10/7/episode-603-october-3-4-2021",
    "https://knowledgefight.com/research/2021/11/11/episode-616-november-9-2021",
    "https://knowledgefight.com/research/2021/11/14/episode-617-june-27-2003",
    "https://knowledgefight.com/research/2021/11/16/episode-618-november-15-2021",
    "https://knowledgefight.com/research/2021/11/21/episode-619-november-16-2021",
    "https://knowledgefight.com/research/2021/11/25/episode-620-november-23-2021",
    "https://knowledgefight.com/research/2021/11/28/episode-621-reset-wars-episode-1",
    "https://knowledgefight.com/research/2021/11/3/episode-612-october-31-2021",
    "https://knowledgefight.com/research/2021/11/30/episode-622-june-30-july-1-2003",
    "https://knowledgefight.com/research/2021/11/4/episode-613-june-24-25-2003",
    "https://knowledgefight.com/research/2021/11/7/episode-614-the-purge-of-gates",
    "https://knowledgefight.com/research/2021/12/12/episode-626-december-9-2021",
    "https://knowledgefight.com/research/2021/12/16/episode-628-july-10-2003",
    "https://knowledgefight.com/research/2021/12/2/episode-623-july-2-3-2003",
    "https://knowledgefight.com/research/2021/12/29/episode-631-december-20-2021",
    "https://knowledgefight.com/research/2021/12/7/episode-624-november-30-2021",
    "https://knowledgefight.com/research/2021/12/9/episode-625-december-5-2021",
    "https://knowledgefight.com/research/2021/7/11/episode-576-july-9-2021",
    "https://knowledgefight.com/research/2021/7/15/episode-577-july-13-2021",
    "https://knowledgefight.com/research/2021/7/18/episode-578-a-little-side-track",
    "https://knowledgefight.com/research/2021/7/20/episode-579-may-30-2003",
    "https://knowledgefight.com/research/2021/7/22/episode-580-july-20-21-2021",
    "https://knowledgefight.com/research/2021/7/6/episode-574-may-28-29-2003",
    "https://knowledgefight.com/research/2021/7/8/episode-575-july-4-2021",
    "https://knowledgefight.com/research/2021/8/10/episode-585-august-8-2021",
    "https://knowledgefight.com/research/2021/8/15/episode-586-august-11-12-2021",
    "https://knowledgefight.com/research/2021/8/19/episode-588-august-15-2021",
    "https://knowledgefight.com/research/2021/8/22/episode-589-august-18-20-2021",
    "https://knowledgefight.com/research/2021/8/26/episode-590-august-24-2021",
    "https://knowledgefight.com/research/2021/8/29/episode-591-june-11-2003",
    "https://knowledgefight.com/research/2021/8/3/episode-583-august-1-2021",
    "https://knowledgefight.com/research/2021/8/8/episode-584-august-5-2021",
    "https://knowledgefight.com/research/2021/9/16/episode-597-september-14-2021",
    "https://knowledgefight.com/research/2021/9/19/episode-598-september-17-2021",
    "https://knowledgefight.com/research/2021/9/23/episode-599-june-13-2003",
    "https://knowledgefight.com/research/2021/9/26/episode-600-september-22-2021",
    "https://knowledgefight.com/research/2021/9/28/episode-601-june-16-2003",
    "https://knowledgefight.com/research/2021/9/9/episode-594-september-7-2021",
    "https://knowledgefight.com/research/2022/1/13/episode-636-april-27-2009",
    "https://knowledgefight.com/research/2022/1/16/episode-637-january-13-2022",
    "https://knowledgefight.com/research/2022/1/18/episode-638-see-you-at-the-cross-rhodes",
    "https://knowledgefight.com/research/2022/1/2/episode-633-december-30-2021",
    "https://knowledgefight.com/research/2022/1/20/episode-639-january-17-2022",
    "https://knowledgefight.com/research/2022/1/23/episode-640-january-22-2022",
    "https://knowledgefight.com/research/2022/1/28/episode-642-january-24-2022",
    "https://knowledgefight.com/research/2022/1/30/episode-643-january-28-2022",
    "https://knowledgefight.com/research/2022/1/6/episode-634-july-11-2003",
    "https://knowledgefight.com/research/2022/1/9/episode-635-january-6-7-2022",
    "https://knowledgefight.com/research/2022/2/11/episode-647-february-8-2022",
    "https://knowledgefight.com/research/2022/2/13/episode-648-february-11-2022",
    "https://knowledgefight.com/research/2022/2/15/episode-649-february-14-2022",
    "https://knowledgefight.com/research/2022/2/17/episode-650-july-17-2003",
    "https://knowledgefight.com/research/2022/2/20/episode-651-february-18-2022",
    "https://knowledgefight.com/research/2022/2/25/episode-652-february-21-2022",
    "https://knowledgefight.com/research/2022/2/27/episode-653-february-25-2022",
    "https://knowledgefight.com/research/2022/2/3/episode-644-february-2-2022",
    "https://knowledgefight.com/research/2022/2/9/episode-646-february-6-2022",
    "https://knowledgefight.com/research/2022/3/1/episode-654-february-27-2022",
    "https://knowledgefight.com/research/2022/3/3/episode-655-july-18-2003",
]

for url in URLS:
	print(url)
