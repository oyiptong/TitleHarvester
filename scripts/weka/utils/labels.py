#!/usr/bin/env python
import json
import csv
import sys
import bz2file

FIELD_SEP = '\t'
CATEGORY_SEP = ','

labels = set([])

with bz2file.open(sys.argv[1], 'rb') as tsvfile:
    try:
        for line in tsvfile:
            line = line.strip()
            url, title, keywords, categories = line.split(FIELD_SEP)
            categories = categories.split(CATEGORY_SEP)

            for cat in categories:
                labels.add(cat)
    except IOError:
        '''
        Error might be caught:
          IOError: invalid data stream
        '''
        pass

labels = list(labels)
with open('labels.json', 'w') as f:
    json.dump(labels, f)
