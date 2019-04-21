# Dig Data Management Assignment 2

## Task2 - Word Count Task on a Serie of Novels Based on Hadoop MapReduce
In this task, we count the words in a series of Novels applying Hadoop MapReduce framework.


### How We Implement
In this task, we use Python to go through it. Hadoop provides a streaming method to make it possible to run MapReduce tasks for all programming languages.
Here shows the simplistic codes of mapper,
```python
#!/usr/bin/python3.5
# mapper.py
import sys
import jieba

for line in sys.stdin:
    words = jieba.cut(line.strip(), cut_all=False, HMM=False)

    for word in words:
        if len(word) > 1:
            print ('%s\t%s' % (word, 1))
```
It's quite simple and straightforward. We can design any parsing pattern to build up our key-value pairs. We use package 'Jieba' to tokenize  Chinese sentences.

```python
#!/usr/bin/python3.5
# reducer.py
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
```
We need to treat the key-value pairs as string and do customized parsing here, and then we do a simple counting task. Note that the standard input key-value pairs are sorted by key before passed into reducers, it helps simplify the logic of counting.

### Result of Word Count
Top 10
```
说道    13714
什么    12738
自己    10841
韦小宝  9920
一个    9361
咱们    7017
武功    6804
一声    6520
不是    6125
师父    6071
```

Last 10 (Chinese only)
```
一不怕苦       1
一下脸         1
一下手         1
一下子把       1
一万间         1
一万户         1
一万多         1
一万六千多     1
一万余         1
一万九千       1
```

### Observations
1. Kiled/Failed Tasks would be transfered to another nodes to run
![04747783.png](:storage/b18431a7-8de9-455d-b5b2-f3913edca2d4/04747783.png)
2. Error caused from lack of RAM (exit code=137)
We need to set `yarn-site.xml` appropriately to solve the problem over lack of RAM. `yarn.nodemanager.resource.memory-mb` is set to be `1600` to solve the error this time.
3. Some files and packages(libraries) are required to be allocated and installed on every single nodes. In this task, bugs did torture us for a while for we didn't install package 'Jieba' for Python on Slave1 and Slave2 nodes.
4. The command format matters and diverses in different verions when you use streaming method to run MapReduce tasks. The final legal format turned out to be something like 
```
hadoop jar /usr/local/hadoop/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar \
-D mapreduce.job.name="novel mapreduce" \
-mapper word_count_mapper.py -file ./word_count_mapper.py \
-reducer word_count_reducer.py -file ./word_count_reducer.py \
-input novel/ -output novel_output
```

### Reflections
1. This time, we're rather more familiar with the relation and involved items of 'NameNode', 'DataNode', 'SourceManager' and 'NodeManager.'
2. Trying to reset configuration such as `yarn-site.xml` to solve bugs help us be more into the structure of Hadoop.