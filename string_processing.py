import pandas as pd
import re

def cleans(s):
    if pd.isna(s) or len(s.strip()) == 0:
        return None
    return s.strip()


def cleantitle(s):
    if pd.isna(s) or len(s.strip()) == 0:
        return None
    s = s.strip()
    if s[0] == '#':
        s = s[1:].strip()
    return s


def agressive_splits(s):
    if pd.isna(s) or len(s.strip()) == 0:
        return []

    l1 = re.split(',|;', s)
    l2 = [x.strip() for x in l1]
    return [x for x in l2 if len(x) > 0]


def splits(s):
    if pd.isna(s) or len(s.strip()) == 0:
        return []

    l1 = s.split(";")
    l2 = [x.strip() for x in l1]
    return [x for x in l2 if len(x) > 0]