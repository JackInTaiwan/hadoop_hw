#!/usr/bin/python3.5
import sys



current_word, current_count = None, 0

for line in sys.stdin:
    word, count = line.strip().split('\t', 1)
    count = int(count)

    if current_word == word:
        current_count += count
    else:
        if current_count:
            print('%s\t%s' % (current_word, current_count))
        current_count, current_word = count, word
if current_count:
    print('%s\t%s' % (current_word, current_count))