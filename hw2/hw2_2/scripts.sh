hadoop jar /usr/local/hadoop/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar \
-D mapreduce.job.name="novel mapreduce" \
-mapper word_count_mapper.py -file ./word_count_mapper.py \
-reducer word_count_reducer.py -file ./word_count_reducer.py \
-input novel/ -output novel_output