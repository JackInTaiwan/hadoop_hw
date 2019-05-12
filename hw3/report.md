# Dig Data Management Assignment 3
<div align = right>信科四 郑元嘉 1800920541</div>



<br><br>
## Task1 - Hive
1. Create a table first.
`
$ create table titanic(PassengerId int, Survived int, Pclass int, Name string, Sex string, Age float, SibSp int, Parch int, Ticket string, Fare float, Cabin string, Embarked string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' location '/user/hive/warehouse/mydb.db/titanic';
`

2. Check out the creation.
`> DESCRIBE titanic;`
![6ec7d31d.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/e5837507.png)

3. Load csv file `TitanicData.csv` to Hive.
`$ load data local inpath '/media/jack/File/hadoopHw/hw3/Titanic/TitanicData.csv' overwrite into table titanic;`

4. Check out the loading.
`> SELECT * FROM titanic`
![71f06577.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/71f06577.png)

5. Query I: Find all data where the gender is female and she survived.
`> SELECT * FROM titanic WHERE sex == 'female' AND survived == 1;`
![d6844eef.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/d6844eef.png)

6. Query II: List the rearranged age labels and names.
```sql
> SELECT name,
> CASE WHEN age < 20 THEN 'young'
> WHEN 20 <= age AND age < 40 THEN 'adult'
> WHEN 40 <= age THEN 'senior'
> ELSE 'none'
> END AS age
> FROM titanic;
```
![036f16fd.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/036f16fd.png)




<br><br>
## Task2 - HBase
1. Create table named 'titanic' first.
`> create 'titanic', 'survived', 'pclass', 'name', 'sex', 'age', 'sibSp', 'parch', 'ticket', 'fare', 'cabin', 'embarked'`
Note that we don't pass the attribute 'passengerId', for it will ba used as 'HBASE_ROW_KEY' later on.

2. Check out the creation.
![712bc787.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/5f11eb4a.png)

3. Load csv file `TitanicData.csv` to HBase.
`$ hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,survived,pclass,name,sex,age,sibSp,parch,ticket,fare,cabin,embarked '-Dimporttsv.separator=,' titanic TitanicData.csv`
Note that the first column name must be 'HBASE_ROW_KEY' which indicates the indexes (ids) for the row data.

4. Error: caused by wrong column names
I was confronted by the error due to not completely identical columns the table holds and my loading command holds. The uppercase/lowercase requires the same as well.
![b8274db4.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/b8274db4.png)
\
![b7be0f58.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/b7be0f58.png)

5. Screenshot for success in loading task.
![98108760.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/98108760.png)

6. Check out the table 'titanic.'
`> count 'titanic'`
![49b2439c.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/49b2439c.png)

7. Query: Find the survials.
`scan 'titanic', {COLUMN => 'survived', FILTER => "ValueFilter(=, 'binary:1')"}`
![6b48b9b7.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/6b48b9b7.png)

8. (Extra) Drop the table.
If we attempt to drop, we need to 'disable' the table fisrt then drop it.
`> disable 'titanic'`
`> drop 'titanic'`




<br><br>
## Task3 - PIG
1. We don't need to create one table or load the csv file from local (if it has already been loaded to HDFS). The only thing to extract the csv data is to use `LOAD` command and corresponding package `org.apache.pig.piggybank.storage.CSVExcelStorage()` in PIG shell.
`> records = LOAD 'TitanicData.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (passengerIp:int, survived:int, pclass:chararray, name:chararray, sex:chararray, age:float, sibSp: chararray, parch:chararray, ticket: float, fare:float, cabin: chararray, embarked: chararray);`

2. Check out the loading by dumping `records.`
`> dump records;`
![9065fa31.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/9065fa31.png)

3. Query: List the names and genders of those who are male survivors.
`> male_survivor = FOREACH (FILTER records BY survived == 1 AND sex == 'male') GENERATE name, sex;`
`dump male_survivor;`
![d80a52c8.png](:storage/9a5e055a-2227-449d-9cd7-0840abffe014/d80a52c8.png)


<br><br>
## References
[Hive 导入CSV文件 - 小海的专栏 - CSDN博客](https://blog.csdn.net/duyuanhai/article/details/52840717)
[HBase shell 命令介绍 - 纯洁的微笑 - 博客园](https://www.cnblogs.com/ityouknow/p/7344001.html)
[Hbase 上传CSV文件 - mega-victor - CSDN博客](https://blog.csdn.net/u014469615/article/details/78533407)