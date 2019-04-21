#!/usr/bin/python3.5
import sys
import jieba



for line in sys.stdin:
    words = jieba.cut(line.strip(), cut_all=False, HMM=False)

    for word in words:
        if len(word) > 1:
            print ('%s\t%s' % (word, 1))
