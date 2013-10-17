#!/usr/bin/env python
import json
import re
import csv
import sys
import bz2file
from unidecode import unidecode

FIELD_SEP = '\t'
CATEGORY_SEP = ','
NOT_WORD = re.compile(u'[^a-z0-9 ]+')
NO_LETTERS = re.compile(u'^[^a-z]+$')

labels = None
STOPWORDS = set()
URL_STOPWORDS = set()

with open('labels.json', 'r') as f:
    labels = json.load(f)

with open('stopwords.txt', 'r') as f:
    for line in f:
        STOPWORDS.add(line.rstrip())

with open('url_stopwords.txt', 'r') as f:
    for line in f:
        URL_STOPWORDS.add(line.rstrip())

line_no = 0
with bz2file.open(sys.argv[1], 'rb') as tsvfile:
    line_no += 1
    with open(sys.argv[2], 'w') as arfffile:
        csvwriter = csv.writer(arfffile, quoting=csv.QUOTE_ALL)

        arfffile.write("@relation 40-cat-training\n")
        """
        arfffile.write("@attribute url string\n")
        arfffile.write("@attribute title string\n")
        """
        arfffile.write("@attribute tokens string\n")
        arfffile.write("@attribute klass {{{0}}}\n".format(",".join(labels)))
        arfffile.write("@data\n")

        for line in tsvfile:
            line = line.strip()
            try:
                url, title, categories = line.split(FIELD_SEP)
            except ValueError, e:
                print "ERROR at line {0}: {1} ".format(line_no, line)
                continue

            url = url.lower()
            title = title.lower()

            url = NOT_WORD.sub(" ", url)
            title = NOT_WORD.sub(" ", title)

            url = url.split()
            title = title.split()

            out_tokens = []

            for token in url:
                match = NO_LETTERS.match(token)
                if (token not in URL_STOPWORDS) and match is None and len(token) > 3:
                    out_tokens.append(token)

            for token in title:
                match = NO_LETTERS.match(token)
                if (token not in STOPWORDS) and match is None and len(token) > 3:
                    out_tokens.append(token)

            out_str = " ".join(out_tokens)

            if out_str == "":
                continue

            categories = categories.split(CATEGORY_SEP)
            for cat in categories:
                csvwriter.writerow([out_str, cat])
                #csvwriter.writerow([url, title, cat])
                #csvwriter.writerow([title, cat])
