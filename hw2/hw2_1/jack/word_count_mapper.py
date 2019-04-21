#!/usr/bin/python3.5
import sys
from nltk.tokenize import RegexpTokenizer



tokenizer = RegexpTokenizer(r'\w+')

for line in sys.stdin:
    words = tokenizer.tokenize(line.strip())

    for word in words:
        print ('%s\t%s' % (word, 1))
