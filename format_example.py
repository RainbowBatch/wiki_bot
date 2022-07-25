from pprint import pprint

SAMPLE = {'Air Date': '3/2/2022',
          'Books/ Primary sources': '',
          'Coverage End Date': '2/27/2022',
          'Coverage Start Date': '2/27/2022',
          'Episode Description': 'Today, Dan and Jordan continue to be astounded by '
          "Alex Jones' terrible coverage of Putin's invasion of "
          'Ukraine.  In this installment, Alex throws a '
          'tantrum, either because the pressure is getting to '
          'him, or because he just prepared too much for this '
          'episode.',
          'Episode Number': '654',
          'Noteable Drops or Bits': '',
          'Novelty Beverage': '',
          'Out Of Context': '',
          'Refereneced people/ Guests ': 'Vladimir Putin; George Soros; Alexander '
                                 'Lukashenko; Leo Zagami',
          'Themes': 'Heads up: Dan and Jordan on Behind the Bastards; Russia is being '
          'very restrained in this invasion; Invasion death tolls are '
          'underreported on both sides; The West is putting out more '
          'propaganda than Russia; Alex tries to reframe his past coverage '
          'of Putin; War is coming because Soros overthrew Ukraine; Review: '
          'Alex never claimed Putin bit off more than he could chew; Putin '
          "warns world is on brink of nuclear war; Alex won't cover the "
          "cancelled CNN doc on him; Russia has been invaded a lot, so it's "
          'ok for Putin to invade; Putin is invading for the same reasons as '
          'Alex: globalists and trans people; Slavs have always killed each '
          "other, but htis is Soros's fault; Ukraine attacked Russia first; "
          "Biden doesn't know what's going on; Russia always gets "
          "doublecrossed, just like Hitler and Stalin; We're running out of "
          'freedom... Alex is going to go home early; The world deserves to '
          "die; Alex mopes, says he's taking time off; Alex apologizes, he's "
          'crazy like Gnarls Barkley; Ales is going to start over; Alex '
          "doesn't have the Stockholm gene, so news stacks make him mad; "
          "Covid is a bioweapon that killed Alex's friends and family; Covid "
          'rage feeds Russian defense rant; Russia is the new America; Rant '
          'takes the wind out of Alex; Alex prepared for 15 hours; Alex '
          "won't apologize, god wants him to get upset; Alex plays Putin's "
          'war declaration, paraphrases subtitles; Bigotry Rorchach test; '
          'The globalist plan is to get Alex to freak out; Alex relates to '
          'Belarusian dictator Alexander Lukashenko; Guest: Leo Zagami, '
          'Russia-Ukraine expert; Leo knew Putin when he was mayor; Leo: '
          "Russian protestors are Soros backed; Leo explains Putin's plan: "
          'reform Soviet Union',
          'Type of Episode': 'Present Day'}

def splitlist(s):
  l1 = s.split(';')
  l2 = [s.strip() for s in l1]
  return [
    s
    for s in l2
    if len(s) > 0
  ]

print(SAMPLE)
pprint(splitlist(SAMPLE['Refereneced people/ Guests ']))
pprint(splitlist(SAMPLE['Themes']))