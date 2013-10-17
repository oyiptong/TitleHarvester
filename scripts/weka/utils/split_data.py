#!/usr/bin/env python
import json
import csv
import sys
import bz2file
import random

FIELD_SEP = '\t'
CATEGORY_SEP = ','

docs = {}

print "Reading input"
with bz2file.open(sys.argv[1], 'rb') as tsvfile:
    for line in tsvfile:
        line = line.strip()
        url, title, categories = line.split(FIELD_SEP)
        categories = categories.split(CATEGORY_SEP)

        for cat in categories:
            if docs.has_key(cat):
                docs[cat].append(line)
            else:
                docs[cat] = [line]

train = open("train.tsv", 'w')
validate = open("validate.tsv", 'w')
test = open("test.tsv", 'w')

print "Writing output"
for _, docs in docs.iteritems():
    random.shuffle(docs)
    num_docs = len(docs)

    num_train = int(.80 * num_docs)
    num_validate = int(.10 * num_docs)

    for doc in docs[:num_train]:
        print >> train, doc

    for doc in docs[num_train : num_train+num_validate]:
        print >> validate, doc

    for doc in docs[num_train+num_validate:]:
        print >> test, doc

train.close()
validate.close()
test.close()
