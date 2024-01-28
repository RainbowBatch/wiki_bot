from langchain.llms import Ollama
ollama = Ollama(base_url='http://localhost:11434',
model="zephyr")
print(ollama("""
Instruction:

Annotate the text with a mediawiki link to the named page named (e.g. [[Link Text|Page Name]]) on the most closely matching text. Maintain the text of the page. Don't include possessives and other punctuation (e.g. link like [[Bob]]'s not [[Bob|Bob's]])

Examples:

Input: **Alex discusses Jayda Fransen
Entity: jayda fransen
Output: **Alex discusses [[Jayda Fransen]]

Input: Sandra Bullock is a Nazi
Entity: sandra bullock
Output:""", stop=["Input:"]))