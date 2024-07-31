print("We are learning spark core programming")
#parts (2,3,4 of this learning is not much important in our realtime environment now a days
#parts (1 and 5) is very very important
#1. How to create spark session object (leveraging all possible features/options) -Imporant (core/sql/streaming)
#2. How to create and manage (partitioning & Caching/persisting) RDDs (foreground)
#3. How to transform RDDs
#4. How to produce/store the result of the RDDs (transformed) into console/storage layers by performing some actions
#5. Spark Arch/terminologies, Performance Optimization, Job Submission, Spark Cluster concept (VV Important) - not used only for core, used in SQL & Streaming also

#def main(args): - Lets consider we are writing the further lines of code inside main method

print("IMPORTANT - 1. How to create spark session object (leveraging all possible features/options) -Imporant")
#How to create spark session object (leveraging all possible features) -Imporant
#Spark session object is the entry point for accessing the spark cluster
#lets learn in detail about the spark session object creation
from pyspark.sql.session import SparkSession
#builder is a factory pattern which is responsible for instantiating the sparksession class with more options like additional config, appname,
# master(local[2]), enableHiveSupport().. getOrCreate()
#getOrCreate() - Get an existing spark session object or create a new object if the spark session object is stopped/closed or not present
spark=SparkSession.builder.master("local[2]").appName("WD32_Spark_Core_App").enableHiveSupport().getOrCreate()
print(spark)
rdd1=spark.sparkContext.parallelize(range(1,100))
print(rdd1.getNumPartitions())
#spark.stop()
spark1=SparkSession.builder.master("local[4]").appName("WE43_Spark_Core_App").enableHiveSupport().getOrCreate()
print(spark1)

#To write spark core program follow all these.....
#a. import required libraries
from pyspark.sql.session import SparkSession

#b. Create spark session object (internally it will create spark context (sc), sqlContext/hiveContext)
spark1=SparkSession.builder.master("local[*]").appName("WE43_Spark_Core_App").enableHiveSupport().getOrCreate()

#c. Define the sparkcontext for writing rdd programs in the below way..
sc=spark1.sparkContext#define sc which is like an alias/renamed spark.sparkContext object

#d. Show only the error, dont display information or warning
sc.setLogLevel("ERROR")

#e. Start write the lines of code
rdd1=sc.parallelize(range(1,100))
print(rdd1.collect())
print(rdd1.getNumPartitions())

###############################################################################################
print("Not IMPORTANT - 2. How to create and manage RDDs (not important)")
#Not IMPORTANT - 2. How to create and manage RDDs (not important)
#What are the possible ways of Createing RDDs
#RDD -
# Resilient (Rebuild (fault tolerance) in case if the data in the memory is lost using the concept of lineage in DAG)
# Distributed (across multiple memory of different computers using concept of partitions)
# Dataset (using files/sources/programaticaly/other rdd/memory)
#1. RDDs can be created from some sources (FS/DB/any other sources)
#2. RDDs can be created from another RDD
#3. RDDs can be created from memory
#4. RDDs can be created programatically

#1. RDDs can be created from some sources (FS/DB/any other sources) - If we need to create resilient distributed dataset in memory for computation
file_rdd=sc.textFile("file:///home/hduser/hive/data/txns")
hadoop_rdd=sc.textFile("hdfs://127.0.0.1:54310/user/hduser/txns")
print(file_rdd.take(5))
#print(hadoop_rdd.take(5))

#2. RDDs can be created from another RDD (refreshed memory) (because the given RDD is IMMUTABLE)
#map_rdd created from file_rdd
map_rdd=file_rdd.map(lambda row:row.split(","))#Convert every row (string) of the data present in the file_rdd to list(values) - Higher Order Function
print(map_rdd.take(5))
#or
split_func=lambda row:row.split(",")
map_rdd=file_rdd.map(split_func)#Convert every row (string) of the data present in the file_rdd to list(values) - Higher Order Function
print(map_rdd.take(5))
#or
def split_met(row):
    return row.split(",")
map_rdd=file_rdd.map(split_met)#Convert every row (string) of the data present in the file_rdd to list(values) - Higher Order Function
print(map_rdd.take(5))

#3. RDDs can be created from memory (retained)
#Find the total sales happened?
#Lazy Transformations happens in the below lines
file_rdd=sc.textFile("file:///home/hduser/hive/data/txns")#Rdd created from file/disk/sources
map_rdd=file_rdd.map(lambda row:row.split(","))#RDD created from another RDD
map_rdd.cache()#Garbage collector pls don't clean the data from map_rdd, pls allow me to retain for later usage
#Action happens from the below line
print(map_rdd.count())
amt_rdd=map_rdd.map(lambda split_row:float(split_row[3]))#RDD can created from memory
print(amt_rdd.take(3))
city_rdd=map_rdd.map(lambda split_row:split_row[7])#RDD can created from memory
print(city_rdd.take(3))
print("Total sales is ",amt_rdd.sum())

#4. RDDs can be created programatically (learning & analysis) using the parallelize function
#RDDs are resilient distributed dataset
lst=list(range(1,101))
even_lst=[]
for i in lst:#map will run for loop
    if i%2==0:
        even_lst.append(i)
print(even_lst)
#[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]
rdd_lst=sc.parallelize(lst)
filter_even_rdd=rdd_lst.filter(lambda i:i%2==0)#Filter (select + where clause in sql) is a transformation that act like a map (for loop) to iterate on every element
# and apply any condition like a (if condition)
filter_even_rdd.collect()

#Use parallelize or programatic rdd creation to understand the datascruture and rdd transformations and actions concept easily..
lst1=["1,irfan,42","2,bala,35"]
for i in lst1:
    print(i.split(","))

print(sc.parallelize(lst1).glom().collect())
print(sc.parallelize(lst1).map(lambda x:x.split(",")).collect())
print(sc.parallelize(lst1).map(lambda x:x.split(",")[0]).collect())

print("3. How to Transform/process RDDs - Not much important")
#Interview Question: How to identify the given function is a transformation or an action?
#Ans: function return another RDD is a transformation, function return result/value is action
#some of the Transformation (function that returns another RDD) functions asked in interviews (rarely) -> map, filter, flatmap, reduceByKey, zipWithIndex, mappartition, join etc.,
#Interview Question: Transformation are of 2 categories -
# 1. Passive - If the output elements is equal to the input elements of an RDD
# 2. Active transformation - If the output elements is NOT equal to the input elements of an RDD

#Interview Question:
#Transformations Characteristics
# Paired RDD Transformations can be run on normal RDD or Paired RDD (paired RDD transformations requires data in a key,value pair)
# Eg. reduceByKey, join, groupByKey, combineByKey ...

#Interview Question:
#Transformations Dependency Characteristics (spark CM, job, task, stage)
# Narrow Dependent -
# Wide Dependent Transformations -
# Wide Dependent transformation example Eg. reduceByKey, join ...

#How to do you identify a given function is a transformation or action?
#In python A variable can hold values or programs(object) or functions(anonymous/lambda function) or data object (rdd)
lines_rdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")#rdd returned (lazily) of type list[string]
#map method (select statement) - Passive transformation
map_rdd=lines_rdd.map(lambda row_str:row_str.split(','))
formatted_rdd=map_rdd.map(lambda row_str_cols:(row_str_cols[0],row_str_cols[1],int(row_str_cols[2]),row_str_cols[3],float(row_str_cols[4])))
amt_rdd=formatted_rdd.map(lambda x:x[4])#no. of columns returned is 5th column
sum_of_amt=amt_rdd.sum()#Minimum one Action is must needed for every spark application

#Filter (select and where)- Active transformation
filter_rdd=map_rdd.filter(lambda x:x[1]=='chennai')#no. of columns returned is all
filter_rdd.collect()

city_rdd=map_rdd.map(lambda x:x[1]).filter(lambda x:x=='chennai')#better?yes (secondarily) projection pushdown
print("filter1 output")
print(city_rdd.collect())
city_rdd=map_rdd.filter(lambda x:x[1]=='chennai').map(lambda x:x[1])#better?yes (primarily) - predicate pushdown
print("filter2 output")

#flatMap -#flatmap is a higher order function that iterate on each row of the given dataset of an rdd as like a map transformation
#used to convert unstructured text data into structured data
# It will work like a nested for loop or it will work like select with explode function
# and then it will flatten the row further down into elements
#'hi team goodmorning','lets learn spark' -> map(lambda x:x.split(' ')) -> [[hi,team,goodmorning],[lets,learn,spark]]
#'hi team goodmorning','lets learn spark' -> flatMap(lambda x:x.split(' ')) -> [[hi,team,goodmorning],[lets,learn,spark]] -> [hi,team,goodmorning,lets,learn,spark]

#Interview Question: Difference between map and flatMap?
#Map will iterate at the row level in a given dataset of multiple rows
#flatMap will iterate at the row level (map) in a given dataset of multiple rows, then flatten at the every element of the row
for i in ['hi team goodmorning', 'lets learn spark']:
 for j in i.split(" "):
  print(j.upper())

#Interview Question: When do we use flatmap in realtime? Whether you know spark core programming? Yes
# Can you write word count program using spark core?
# Can you let me know the total occurance of the given keyword in an unstructured data?
#how many people shown interest in learning spark
'''
hadoop spark hadoop spark kafka datascience
spark hadoop spark datascience
informatica java aws gcp
gcp aws azure spark
gcp pyspark hadoop hadoop
'''
sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).filter(lambda x:x=='spark').count()

#zip, zipWithIndex - HOF that returns the zipped result of two rdds contains (same number of partition elements)
emp_basic_rdd = sc.textFile("file:/home/hduser/sparkdata/empbasicinfo.txt").map(lambda x:x.split(","))
emp_addon_rdd = sc.textFile("file:/home/hduser/sparkdata/empaddoninfoId.txt").map(lambda x:x.split(","))
#zip (joins rdds horizontally without any join conditions)-> Zips this RDD with another one, returning key-value pairs with the first element in each RDD second element in each RDD, etc. Assumes
# rules: The two RDDs have the same number of partitions and the same number of elements in each partition
print(emp_basic_rdd.zip(emp_addon_rdd).collect())

emp_basic_rdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt").map(lambda x:x.split(","))
emp_addon_rdd = sc.textFile("file:/home/hduser/sparkdata/empaddoninfo.txt").map(lambda x:x.split(","))
print(emp_basic_rdd.zip(emp_addon_rdd).map(lambda x:[x[0][0],x[0][1],x[0][2],x[1][0]]).collect())

#Zipwithindex - HOF returns the rdd output adding index value for every element of the given rdd
#Realtime usage of zipwithindex
#Interview question: How to do you remove the header from a given dataset?
emprdd = sc.textFile("file:/home/hduser/sparkdata/empdata_header.txt")
print('zipwithindexed original data',emprdd.zipWithIndex().collect())
print('zipwithindexed original data',emprdd.zipWithIndex().filter(lambda x:x[1]>0).collect())

header=emprdd.first()
print('using first action to filter header data',emprdd.filter(lambda x:x!=header).collect())

#union, subtract, intersection, distinct (set functions)
#union (merge rdds vertically without any join conditions) - For detail description refer /usr/local/spark/python/pyspark/rdd.py
# rules:
# Not a strict rule, but we have to ensure to apply this rule programatically
# All set operators will say - number of columns, datatype should be same between the 2 datasets going to be operated
#Union will return duplicates
#rdd1-1,a,10
#rdd2-2,b,20
#rdd3-rdd1+rdd2

emprdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
emprdd1 = sc.textFile("file:/home/hduser/sparkdata/empdata1.txt")
print(emprdd.count())
print(emprdd1.count())

#union includes duplicate also
union_rdd=emprdd.union(emprdd1) #union (will retain duplicates) in spark core is union all in sql databases
print(union_rdd.collect())
print(union_rdd.count())

#union will not place a strict rules of same number of columns and datatype in both rdds
emprdd2 = sc.textFile("file:/home/hduser/sparkdata/empdata2.txt")#4 columns data
union_rdd=emprdd.union(emprdd2) #will this work? it works because we are unioning list(str)+list(str)
print(union_rdd.collect())

#union will not place a strict rules of same number of columns and datatype in both rdds, but we have to apply the rules programatically by
#converting the given rdd into tuples
#Data strandardization
emprdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt").map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],int(x[2]),x[3],x[4]))
emprdd2 = sc.textFile("file:/home/hduser/sparkdata/empdata2.txt").map(lambda x:x.split(",")).map(lambda x:(x[0],'Unknown',int(x[1]),x[2],x[3]))
union_rdd=emprdd.union(emprdd2)
print(union_rdd.collect())
print(union_rdd.map(lambda x:x[1]).collect())

emprdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
emprdd1 = sc.textFile("file:/home/hduser/sparkdata/empdata1.txt")
intersect_rdd=emprdd.intersection(emprdd1)#intersection shows only the common data between the given rdds
#Show me the employees working in both regions?
print(intersect_rdd.collect())

subtract_rdd=emprdd.subtract(emprdd1)#subtract shows difference between rdd1 minus rdd2 or vice versa
print(subtract_rdd.collect())

#I want to deduplicate the duplicate data in the given RDD or I wanted to implement union (deduplicated) rather than union all?
union_rdd=emprdd.union(emprdd1)
print(union_rdd.collect())#union all
print(union_rdd.distinct().collect())#union (de duplicated)

#reduce (action)
#Calculate the sum of salary of the given employees working in chennai? select sum(sal) from table where city='chennai';
#lets write a map reduce program to achieve this
#map(collect, filter, convert) -> shuffle (data copy from mapper to reducer containers) -> reducer (merge, sort, group, aggregate) consolidation
emprdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt").map(lambda x:x.split(",")).filter(lambda x:x[1]=='chennai').map(lambda x:(int(x[4])))
#collect, conversion and filter
sum_fun=lambda brain_accumulator,finger_increment:brain_accumulator+finger_increment#school days mind and finger game
#[100000, 10000, 100000]
reducer=emprdd.reduce(sum_fun)

#python way of doing reduction
from functools import reduce
reduce(lambda x,y:x+y,[100000, 10000, 100000])

#paired RDD - If we can perform operations on the rdd data if it is in a key,value pair format, then it is a paired rdd
#select city,sum(sal) from table group by city;
#reduceByKey (paired RDD functions) - Important transformation as like map, filter, flatmap
#It will use hash partitioning to distribute the respective key in the respective reducers
#Calculate the city wise sum of salary of the given employees? chennai,210000, hyd,115000
emp_paired_rdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt").map(lambda x:x.split(","))\
    .map(lambda x:(x[1],int(x[4])))
reducer_by_key=emp_paired_rdd.reduceByKey(sum_fun)

#Can you write word count program using spark core? we can achieve using reduceByKey function
'''
hadoop spark hadoop spark kafka datascience
spark hadoop spark datascience
informatica java aws gcp
gcp aws azure spark
gcp pyspark hadoop hadoop
'''
#import libraries
#spark session #1. Spark session object creation
unstructured_rdd=sc.textFile("file:///home/hduser/mrdata/courses.log")#2. RDD Creation
structured_rdd=unstructured_rdd.flatMap(lambda x:x.split(" "))#3. Transformation
paired_rdd=structured_rdd.map(lambda course:(course,1))#3. Transformation
reduced_by_key_rdd=paired_rdd.reduceByKey(lambda x,y:x+y)#3. Transformation
print(reduced_by_key_rdd.collect())#4. Action

#join transformation (paired RDD functions) - joins are Not suggested literally using rdd functions
emp_paired_rdd = sc.textFile("file:/home/hduser/sparkdata/empdata_id.txt").map(lambda x:x.split(",")).map(lambda x:(int(x[6]),(int(x[0]),x[1],x[2])))
dept_paired_rdd = sc.textFile("file:/home/hduser/sparkdata/dept_id.txt").map(lambda x:x.split(",")).map(lambda x:(int(x[0]),x[1]))
print(emp_paired_rdd.join(dept_paired_rdd).collect())
print(emp_paired_rdd.leftOuterJoin(dept_paired_rdd).collect())

#Vignesh
#One basic doubt Irfan, In dataframe spark, the transformations will happen logical table view and physical rdd (background) -> sql (logical table)-> rdd trans -> rdd data
# and in RDD the transformations will happen on the physical rdd directly -> rdd trans -> rdd data

print("4. How to Materialize/Actions RDDs - Not much important")
#some of the Actions (function that returns the result) functions asked in interviews (rarely) -> map, filter, flatmap, reduceByKey, zipWithIndex, mappartition, join etc.,
#Interview Question:
#Actions Characteristics
# Paired RDD Actions can be run on normal RDD or Paired RDD (paired RDD actions requires data in a key,value pair)
# Eg. countByKey, combineByKey ...

#Spark core application to perform word count operation
#import libraries
from pyspark.sql.session import *
#spark session #1. Spark session object creation
#classes in a InitUpper case
#objects & functions in a camelCase
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
#sc.setLogLevel("INFO")
unstructured_rdd=sc.textFile("file:///home/hduser/mrdata/courses.log")#2. RDD Creation
structured_rdd=unstructured_rdd.flatMap(lambda x:x.split(" "))#3. Transformation
#transformation can have a characteristics of (active/passive, regular/pairedrdd, narrow/wide)
print(structured_rdd.collect())
paired_rdd=structured_rdd.map(lambda course:(course,1))#3. Transformation
print(paired_rdd.collect())#4. Action1 -> entire lineage executed (line# 8 to 10 will be executed)
reduced_by_key_rdd=paired_rdd.reduceByKey(lambda x,y:x+y)#3. Transformation
#reduceByKey transformation is a

print(reduced_by_key_rdd.collect())#4. Action2 -> entire lineage executed (line# 8 to 12 will be executed)
#collect action - Collect from different executor (container) memory and return all items to the driver memory
#Interview Question: Is it good to use collect in a spark core application? No
# collect is a top costly action, hence it should be causiously used for dev purpose or avoided using in an application until it is inevitable
#This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver's memory

print(reduced_by_key_rdd.count())#4. Action1 -> entire lineage executed (line# 8 to 12 will be executed)
#count action is preferred than collect
#count action - Count number of elements in the rdd partition from different executor (container) memory and return the total count to the driver memory
#Will not happen like this....>  collect all the data from the executors and Count number of elements in the driver memory

#quick usecase to understand all 4 parts of spark core?
#Develop a spark core application to identify the total number of errors and warnings (column 2) available in the hadoop-hduser-namenode-localhost.localdomain.log file
#/usr/local/hadoop/logs
from pyspark.sql import *
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
rdd=sc.textFile("/user/hduser/dir1/*")
split_rdd=rdd.map(lambda x:x.split(" "))
#Interview Question: If I give a data file with variable number of columns and you are supposed to consider 4 columns only, how do you handle it?
#answer: After splitting, i will filter the data contains less than 4 columns, then will go with further transformation or action
split_rdd_filtered_min3_cols=split_rdd.filter(lambda x:len(x)>2)
print("Total log entries in the given log file=",rdd.count())
err_rdd=split_rdd_filtered_min3_cols.filter(lambda x:x[2]=='ERROR')
print("Total error log entries in the given log file=",err_rdd.count())
warn_rdd=split_rdd_filtered_min3_cols.filter(lambda x:x[2]=='WARN')
print("Total warning log entries in the given log file=",warn_rdd.count())

print("running take,first, top, sample, countbyvalue,countbykey etc.,")
#Data sampling actions (preferred rather than using collect)
print(err_rdd.take(3))
print(err_rdd.first())
print(err_rdd.top(3))
print("running sample, countbyvalue,countbykey etc.,")

#Random sampling transformation
err_rdd.sample(True,.2).collect()#collect only 20% of overall data
reduced_by_key_rdd.sample(True,.3)
err_rdd.sample(True,.3,2)#param1:replace if duplicate output produced, param2: percent fraction of overall volume, param3: seed value to change the sample output
reduced_by_key_rdd.sample(True,.3,4)

print(reduced_by_key_rdd.sample(True,.3,4).collect())#since we are taking sample, not complete data, depends upon the volume, we can use collect

#countByValue action is used to do a count of the given value by itself produced as key value pair output (dictionary)
#reduced_by_key_rdd.countByValue()
print(split_rdd_filtered_min3_cols.map(lambda x:x[2]).countByValue())
#Try implement countByValue for the wordcount program? [hadoop,spark,hadoop] -> hadoop->2,spark->1

print("running sample, countbyvalue,countbykey etc.,")
#countByKey is a paired rdd action, since it requires the key,value pair rdd as an input
unstructured_rdd=sc.textFile("file:///home/hduser/mrdata/courses.log")
structured_rdd=unstructured_rdd.flatMap(lambda x:x.split(" "))
paired_rdd=structured_rdd.map(lambda course:(course,1))
paired_rdd.collect()
paired_rdd.reduceByKey(lambda x,y:x+(y+2))#we can write some custom functionalities on the values of the given key
print(paired_rdd.countByKey())#count how many keys are occured
print(split_rdd_filtered_min3_cols.map(lambda x:(x[2],'dummy value')).countByKey())#countbykey requires the data in a key,value pair as an input

#lookup is a paired rdd action, since it requires the key,value pair rdd as an input
#lookup will help us perform lookup and enrichment operation on the given rdd
unstructured_rdd=sc.textFile("file:///home/hduser/mrdata/courses.log")
structured_rdd=unstructured_rdd.flatMap(lambda x:x.split(" "))
paired_rdd=structured_rdd.map(lambda course:(course,1))
print("individual count of occurance",paired_rdd.lookup('spark'))

paired_rdd=structured_rdd.map(lambda course:(course,1)).reduceByKey(lambda x,y:x+y)
print("combined count of occurance",paired_rdd.lookup('spark'))

#reduce() action help us reduce the result in any way we needed
fun1=lambda x,y:x+y
fun2=lambda x,y:x if x>y else y
fun3=lambda x,y:x*y
fun4=lambda x,y:x-y
sal_lst=[10000,20000,15000,30000,40000]
sal_rdd1=sc.parallelize(sal_lst)
amt_lst_rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:float(x.split(",")[3]))
reduced_result=amt_lst_rdd1.reduce(fun1)
print("total sales happened",reduced_result)
reduced_result=amt_lst_rdd1.reduce(fun2)
print("max amount transacted",reduced_result)
reduced_result=amt_lst_rdd1.reduce(fun3)
print("multiplication of all amt",reduced_result)
reduced_result=amt_lst_rdd1.reduce(fun4)
print("subraction of all amount",reduced_result)

print(amt_lst_rdd1.sum())
print(amt_lst_rdd1.max())
print(amt_lst_rdd1.min())

#How the name lambda has been brought in?
#lambda is syntax/keyword in python or lambda is an architecture or lambda is a service in AWS cloud
#y


'''reduced_by_key_rdd.reduce()
reduced_by_key_rdd.sum()
reduced_by_key_rdd.max()
reduced_by_key_rdd.min()'''
reduced_by_key_rdd.saveAsTextFile("hdfs:///user/hduser/sparkappdata")
print("last line of the spark application")


#Can you write word count program in spark core?



#Important (Core/SQL/Stream)- 5. Spark Arch/terminologies (first cut), Performance Optimization, Job Submission, Spark Cluster concept (VV Important) - not used only for core, used in SQL & Streaming also
print("5. Spark Arch/terminologies, Performance Optimization, Job Submission, Spark Cluster concept (VV Important) - not used only for core, used in SQL & Streaming also")
#Performance Optimization (foundation concepts)- primary activity
#pluck the Low hanging fruits
#1. Remove all unwanted (costly) actions performed while doing the development like collect & print/take/count... and use it if it is really needed
#Removal of dead codes/dormant codes, removal of the unused rdds

#2. Partition management - When creating RDDs (naturally and customized), before/after transformation , before performing action
#What is Partitioning -
# Partitioning the Horizontal division of data
#Partitioning is used for defining the degree of parallelism and distribution
#Partition help us distribute the data (in terms of spark partition help us distribute the data across multiple nodes in memory, in a form of RDD partitions)

#by default while creating RDDs -
# random (number of rows in a partition) partitioning or range of size of data
# coalesce - range partitioning (range of values in a given partition) or data size is random/different in diff partitions
# repartition (internally coalesce(shuffle=True)) - round robin  partitioning

#Partition management - when creating RDDs (how by default the number of partitions are assigned)
#Applied based on size, number of rows, availability of resources & types of transformations/actions used
#If we create RDDs from Local FS - number of partition is calculated based on 2 criterias -
# 1. Per partition for 32mb of data
rdd1_part7=sc.textFile("file:///home/hduser/hive/data/txns_big1")
print(rdd1_part7.getNumPartitions())

# 2. If size is less than 32mb then default of 2 partitions
rdd1_part2=sc.textFile("file:///home/hduser/hive/data/txns")
print(rdd1_part2.getNumPartitions())

#If we create RDDs from HDFS - number of partition is calculated based on 2 criterias -
# 1. Per partition for 128mb of data (per block one partition)
rdd1_part7=sc.textFile("/user/hduser/txns_big1")
print(rdd1_part7.getNumPartitions())

# 2. If size is less than 128mb then default of 2 partitions
rdd1_part2=sc.textFile("/user/hduser/txns")
print(rdd1_part2.getNumPartitions())

#If we create RDDs programatically - number of partition is calculated based on number of cores defined in the sparksession-spark context creation
rdd_prog1_part4=sc.parallelize(range(1,1001))
rdd_prog1_part4.getNumPartitions()
spark.stop()
spark=SparkSession.builder.master("local[1]").getOrCreate()
rdd_prog1_part1=sc.parallelize(range(1,1001))
rdd_prog1_part1.getNumPartitions()

#If we create RDDs from another RDD - number of partition is calculated based on the parent RDD
spark.stop()
spark=SparkSession.builder.master("local[4]").getOrCreate()
rdd_prog1_part4=sc.parallelize(range(1,1001))
rdd_prog1_part4.getNumPartitions()
rdd2=rdd_prog1_part4.filter(lambda x:x%2==0)

#Partition management - when creating RDDs (how to change the partitions)
#Using the RDD creation functions parameter that it supports we can redefine the number of partitions (at the rdd creation time)
rdd1_part=sc.textFile("/user/hduser/txns_big1")#total partitions will be 2 only
rdd1_part=sc.textFile("/user/hduser/txns_big1",1)#total partitions will be 2 only since minimum 1 is needed, but 2 is available hence 2 is considered
rdd1_part=sc.textFile("/user/hduser/txns_big1",4)#total partitions will increase from 2 to 4
print(rdd1_part.getNumPartitions())

rdd_prog1_part4=sc.parallelize(range(1,1001))#default total cores will be considered
rdd_prog1_part4.getNumPartitions()
rdd_prog1_part10=sc.parallelize(range(1,1001),10)#default total cores will be considered, but can be overrided with num of slices
rdd_prog1_part10.getNumPartitions()

#How do we manage/handle/change the number of partitions before/after performing transformations
#Why we have change the number of partitions at the time of rdd creation or before/after transformation or before action?
#Number of partition = the parent rdd partition,

# but i wanted to change(increase/decrease) the number of partition based on volume of data

#Scenario1 (increase partitions) - We are having 1000 rows in an RDD with 4 partitions, any transformation is completing the task in 250 secs,
# I wanted to improve the performance to complete this work in 100 seconds, what we do? increase the partition to 10
rdd_prog1_part4=sc.parallelize(range(1,1001))
rdd_prog1_part4.getNumPartitions()
rdd2_transform1=rdd_prog1_part4.repartition(10)#repartition is used to increase the number of partitions
#Partitions increased from 4 to 10 to improve performance
# and increase degree of parallelism, hence work can be finished in 100 secs
rdd2_transform1.getNumPartitions()
rdd3=rdd2_transform1.filter(lambda x:x%2==0)#Partitions returned after performing filter is 10
rdd3.getNumPartitions()
print(rdd3.count())

#Scenario2 (reduce partition)- We are having less volume of 1000 rows in an RDD with more number of partitions 100,
# any transformation is completing the task in .5 secs,
# I wanted to improve the performance to complete this work faster, what we do? decrease the partition less than 100
rdd_prog1_part4=sc.parallelize(range(1,1001),100)
rdd_prog1_part4.getNumPartitions()
rdd2_transform1=rdd_prog1_part4.coalesce(2)#Coalesce is used to reduce the number of partitions
rdd2_transform1.getNumPartitions()
rdd3=rdd2_transform1.filter(lambda x:x%2==0)
print(rdd3.count())

# but i wanted to change(increase/decrease) the number of partition based on type of transformation going to perform or performed..

from pyspark.sql import *
spark=SparkSession.builder.master("local[4]").getOrCreate()
#Scenario3 (reducing partitions) - We are having 1000 rows in an RDD with 4 partitions, filter transformation is completing the task in 250 secs,
# I wanted to improve the performance to complete this work in 100 seconds, what we do? increase the partition to 10 before passing to filter
#map, reduceByKey
sc=spark.sparkContext
rdd_prog1_part4=sc.parallelize(range(1,1001))
rdd_prog1_part4.getNumPartitions()#1000 rows with 4 partitions
rdd2_transform1=rdd_prog1_part4.repartition(10)
#Partitions increased from 4 to 10 to improve performance and increase degree of parallelism, hence work can be finished in 100 secs
rdd2_transform1.getNumPartitions()
print("number of rows before filter",rdd2_transform1.count())
rdd3=rdd2_transform1.filter(lambda x:x%2==0)#Partitions returned after performing filter is 10 and performance is improved to 100 secs from 250 secs
print("number of rows after filter",rdd3.count())
print(rdd3.getNumPartitions())
#Do we really need same 10 partitions after filter is performed? no, we can reduce it to 5
rdd4=rdd3.coalesce(5)
print(rdd4.getNumPartitions())
rdd5=rdd4.map(lambda x:x+1)#We are completing the work in 100 secs still, despite number of partition is reduced
print(rdd5.count())

#Scenario4 (increasing partitions) - We are having 10 rows in an RDD with 2 partitions (right number of partitions),
# flatMap/union/join transformation is producing 100 rows with 2 partitions (not a right number of partitions),
# I wanted to improve the performance by increasing the number of partitions (to 20) after performing flatmap
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log")
print("file partitions",rdd1.getNumPartitions())
print("file count",rdd1.count())
rdd2=rdd1.flatMap(lambda x:x.split(" "))
print("flatmap output partitions",rdd2.getNumPartitions())
print("flatmap output count",rdd2.count())
rdd3=rdd2.repartition(20)
print("post flatmap repartitioned partitions",rdd3.getNumPartitions())
print("post flatmap repartitioned count",rdd3.count())

#Scenario5(increase/decrease partitions before performing actions) -
#How do we manage/handle/change the number of partitions before performing actions
#We are producing the result of the given RDD as an output in the console or in some storage layers

#In the Console:
#Wanted to change the number of partitions before performing actions (in the console) to understand the behavior of the partitions functions,
# to use it better while i produce the result in the storage layer
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")
print("number of partitions",rdd1.getNumPartitions())
print("number of rows",rdd1.count())

print("Originally created Partition wise rows count", "Range Distribution 32mb per partition")
result=rdd1.glom().collect()
for i in result:
 print(len(i))

#Interview Questions
#How the repartition and coalesce is working internally to distribute the partitions (range, round robin, random)
#When we have use repartition & coalesce, which is more preferred for reducing the number of partitions?
#How do you produce the output as a single file in spark?
#How to find the number of elements in a given rdd?
#How to find the number of elements in the each partition of a given rdd?

print("Increased Partition wise rows count using Repartition",
      "1. Repartition uses Round Robin Distribution, hence we will equal partition size "
      "2. Repartition will do shuffling (by default) or redistribution of data (hence costly function to use)"
      "3. Repartition is a costly function to use (use it causiously), only for increasing the number of partition use repartition, not for reducing")
result=rdd1.repartition(12).glom().collect()
for i in result:
 print(len(i))

print("Decreased Partition wise rows count using coalesce",
      "1. Coalesce uses Random Distribution (partitions will be merged), hence equal partition size cannot be assured"
      "2. Coalesce preferably will not do shuffling (but shuffling may happen at times) , hence coalaese is less cost"
      "3. Coalesce is a less cost function is supposed to use for reducing the number of partitions")
result=rdd1.coalesce(4).glom().collect()
for i in result:
 print(len(i))

#In the Output Storage layer, how do we manage partitions
rdd2=rdd1.filter(lambda x:"California" in x)#like statement in database
rdd2.coalesce(1).saveAsTextFile("file:///home/hduser/sparkout")

#Interview Question: How to do count the number of elements at the partition level
res=sc.parallelize(range(1,10001),4).glom().collect()
print(res)
for i in res:
    print(len(i))

for i in sc.parallelize(range(1,10001),4).coalesce(2).glom().collect():#equal distribution (despite coalesce is used)
    print(len(i))

for i in sc.parallelize(range(1,10001),4).coalesce(3).glom().collect():#in equal distribution (range partitioning)
    print(len(i))

print(sc.parallelize(range(1,101),4).coalesce(2).glom().collect())#random distribution (merged), with equal partitions

for i in sc.parallelize(range(1,10001),4).repartition(6).glom().collect():#assured equal distribution (round robin distribution)
 print(len(i))

print(sc.parallelize(range(1,101),1).repartition(4).glom().collect())#round robin distribution


#3. Memory Optimization - Performance Optimization using Cache/Persist once RDD is created
#Generally Caching is mechanism of bring/retaining/maintain/keeping/storing the data in memory
#Spark Caching is a mechanism of retaining the data in memory till it is needed temporarily
# (managing garbage collector programatically to clean/not clean the memory objects (we can't assure this))

#Few limitations/challenges in Caching:
#1. If the underlying data of the cached is changed, then we may not get expected results (happens on streaming applications)
# to overcome this issue, we can use checkpoint rather than cache
#2. Caching has to considered with other factors like volume of data, availability of resource, time taken for serialization/deserialization etc.
#3. Caching has to be used appropriately and cleaned appropriately
#4. Use cache appropriately before actions are performed on the parent or child rdds.
#5. Usage of the right type of cache in the name of persist is supposed to be considered.

#Interview Question: I have RAM less than the data in HDD, can we still process that data using Spark?
#Yes, Internally spark only bring the data into the RAM upto how much allocated, rest will be in disk and brought iteratively

from pyspark.sql.session import *
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
rdd1=sc.textFile("file:///home/hduser/hive/data/txns")#I will execute lazily (Irfan pls pick the remote, when i ask you to show)
rdd1.cache()#I will execute lazily or immediately? Lazy (Irfan after picking the remote, when i ask you to show pls request GC to not clean that remote)
print(rdd1.collect())#Action1 - bring the data using textFile into rdd1 memory, once collected and printed GC will delete the data in memory
print("number of rows",rdd1.count())#Action2 - bring the data using textFile into rdd1 memory, once counted and printed GC will delete the data in memory

result=rdd1.glom().collect()#Action3 - bring the data using textFile into rdd1 memory, once glom & collect and printed GC will delete the data in memory
for i in result:
 print(len(i))

result=rdd1.repartition(12).glom().collect()#Action4
for i in result:
 print(len(i))

#In the Output Storage layer, how do we manage partitions
rdd2=rdd1.filter(lambda x:"California" in x)#like statement in database
#rdd2.coalesce(1).saveAsTextFile("file:///home/hduser/sparkout")
rdd2.count()#Action5

rdd1.unpersist()#requesting GC to clean now after I performed operations on the cached/child RDD ,
# if we still have few more operations performed in this application with different independent RDDs
#Otherwise if dont have any further lines of code, unpersist is not needed, since the when this application is closed the memory will be cleaned automatically

rdd3=sc.textFile("file:///home/hduser/hive/data/txns_big1")
print(rdd3.count())#If GC needs the data in memory, when already it is cached by some other data, GC has a rights to clean it forcefully

#If we don't unpersist, when the session completed, it will automatically cleaned

####################################################################################################################################
print("simple way of understanding cache and unpersist")
from pyspark.sql.session import *
import datetime
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")
rdd1.cache()#2.3 million rows are cached (not preferred)
#Use cache before the first action on the given RDD
#Only use cache if the same rdd or child rdds are going to applied with more actions, otherwise don't cache
print("before action current time",datetime.datetime.now())
print(rdd1.count())#Use the data cached from disk
print("after action1 current time",datetime.datetime.now())
print(rdd1.take(2))#Use the cache (retained) data
print("after action2 current time",datetime.datetime.now())
print(rdd1.top(3))#Use the cache (retained) data
print("after action3 current time",datetime.datetime.now())

rdd2=rdd1.filter(lambda x:'California' in x)##Use the cache (retained) data
rdd2.cache()##3 laksh rows are cached (preferred), After applying transformations, we can preferably cache to avoid applying transformation again and again for every action
print(rdd2.count())##Use the cache (retained) data from rdd1 originally
print("after action4 current time",datetime.datetime.now())
rdd1.unpersist()#Use of caching and unpersist appropriately
print(rdd2.count())#Use the cache (retained) data from rdd2 subsequently
print("after action5 current time",datetime.datetime.now())
print(rdd2.count())#Use the cache (retained) data from rdd2
print("after action6 current time",datetime.datetime.now())

'''
#With cache
after action4 current time 2024-04-26 07:49:42.890952
325874
after action5 current time 2024-04-26 07:49:43.404931
325874
after action6 current time 2024-04-26 07:49:43.938820

#Without cache
325874
after action4 current time 2024-04-26 07:51:45.616096
325874
after action5 current time 2024-04-26 07:51:51.455997
325874
after action6 current time 2024-04-26 07:51:56.883507
'''
####################################################################################################################################


#What is the differece between cache and persist?

#Persistance levels we have (advanced caching)
from pyspark.storagelevel import *
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")
#All the below functions are same, no difference, all are "Memory Serialized 1x Replicated"
#Storage layer (Database) ->serialized -> memory only (1 replica)
#Eg. HDFS (3gb) (balance 1gb retained in the HDFS itself)-> 2gb memory (2gb brought to memory)
rdd1.cache()
rdd1.persist()#persist with NONE
rdd1.persist(StorageLevel.MEMORY_ONLY) #Persist with MEMORY_ONLY if volume of data is less
rdd1.unpersist()

#Storage layer (Database) ->serialized -> memory only (2 replica)
#If the volume of data is low/medium then We use below MEMORY_ONLY_2 replica to retain the complete data in memory which got computed with time consuming functionalities
#For example we did some filter or grouping and sorting operations on the database data (results lesser output volume which can fit in memory itself)
# spending few seconds of time, i dont wanted to spend the same time in case if the one replica is lost due to some memory leakage
rdd2=rdd1.filter(lambda x:"California" in x) #less volume data relatively/comparitively
rdd1.persist(StorageLevel.MEMORY_ONLY_2)#Persist with MEMORY_ONLY with 2 replica if the volume is less and we need fault tolerance to avoid recomputation of the RDD

#Storage layer (Database) -> serialized -> memory (1 replica) until data fit in memory and balance in disk of the respective node
#HDFS (3gb) -> disk(1gb excess data brought to LFS disk) -> 2gb memory (2gb brought to memory)
#When do we use this memory and disk storage level?
#If volume of the data is hught and is not completely fit in the memory allocated and the querying/reading of data from original source takes
#longer time, then we use memory and disk to store applicable data into memory balance to disk, hence we can avoid hitting
# original source for consuming balance data

rdd1.unpersist()
rdd1.persist(StorageLevel.MEMORY_AND_DISK)#Persist with  Disk Memory Serialized 1x Replicated
rdd1.unpersist()
rdd1.persist(StorageLevel.MEMORY_AND_DISK_2)#Persist with  Disk Memory Serialized 2x Replicated

#Storage layer (Database) -> serialized -> memory(cleaned after action) and disk (retained) (1/2/3 replica) of the respective node
#When do we use this DISK_ONLY storage level?
#If volume of the data is medium to large in size, then we can bring into memory and disk at the same time using disk_only option
#Data in the memory will be cleaned and reloaded from the disk for the subsequent iterations
#We will use this option when we need the memory for some other RDD transformation and again we need the old rdd data for some action

rdd1.unpersist()
rdd1.persist(StorageLevel.DISK_ONLY)#If I perform action on rdd1, data from source will be loaded to memory and disk from the source
#for the first time, then memory will be cleaned, but disk will be retained, in the next action on the rdd1 data will be brought from
#disk to the memory and not from original source.
rdd1.unpersist()
rdd1.persist(StorageLevel.DISK_ONLY_2)#disk with 2 replica
rdd1.unpersist()
rdd1.persist(StorageLevel.DISK_ONLY_3)#disk with 3 replica

#Storage layer (Database) -> serialized -> memory(cleaned after action) and disk (retained) (1/2/3 replica) of the respective node
#When do we use this DISK_ONLY storage level?
#We can leverage the additional 10% to 30% of the memory area backed by the disk with a specical serializer used in the background
#This Off heap storage will avoid the Garbage collector overhead (garbage collector will not be used)
#We will be learning about some special serializers for performance optimization like kryo/kyro, tungsten etc.,
rdd1.unpersist()
rdd1.persist(StorageLevel.OFF_HEAP)

#Broadcasting - Important to know for interview and for usage especially in the spark optimized joins
#Broadcasting is the concept of broadcast something that is referred/used/consumed by the consumers frequently/anonymousle/randomly
#Real life Eg. radio, video live streaming
#Interview Question? Have you used broadcasting in spark or Did you tried optimized joins in spark SQL?
# Spark Broadcasting is the special static variable that can broadcasted once for all from driver to worker (executors),
#hence spark rdd partition rows can refer that broadcasted variable locally rather than getting it from driver for every iteration
rdd1=sc.textFile("file:///home/hduser/hive/data/txns").map(lambda x:x.split(","))
driver_offer_pct=5
broadcasted_offer_pct=sc.broadcast(driver_offer_pct)
rdd2=rdd1.map(lambda x:(x[2],float(x[3]),x[4],x[5]))
rdd3=rdd2.map(lambda x:(x[0],x[1]-(x[1]*(driver_offer_pct/100)),x[2],x[3]))
#I have a drawback of getting the driver_offer_pct across the network every time when map function operates on a row of an RDD partition
print(rdd3.take(4))
#Perform some optimization to bring the offer_pct once for all to the respective worker node from the driver and reuse it for every iteration of the mapper
rdd3=rdd2.map(lambda x:(x[0],x[1]-(x[1]*(broadcasted_offer_pct.value/100)),x[2],x[3]))
#This broadcast static variable will be brought into worker memory, from there this variable will be loaded into executor off heap memory
print(rdd3.take(4))
broadcasted_offer_pct.destroy()

#Interview Question? Can we broadcast an RDD? Not directly
#Can we broadcast a DF? Yes directly using broadcast join
cust_rdd1=sc.textFile("file:///home/hduser/hive/data/custs").map(lambda x:x.split(","))
#broadcasted_cust_rdd1=sc.broadcast(cust_rdd1) Not possible
driver_val=cust_rdd1.collect()# first we have to bring the rdd data into driver
broadcasted_cust_rdd1=sc.broadcast(driver_val)#Then we can broadcast the local driver variable as spark broadcasted variable
len(broadcasted_cust_rdd1.value)

#Interview Question? How much data can be collected to the driver at given point of time?
#How much rows a rdd or variable can be to be broadcasted?
#It depends upon the capacity of the driver memory allocation

#Accumulator - Not much important in general usage, but needed for some interview discussions
#Accumulator is special incremental variable used for accumulating the number of tasks performed by the executors
#Accumulator is used to identify the progress completion of tasks running the in the respective exe3cutors...
iz_accum=sc.accumulator(0)#driver variable
print(cust_rdd1.count())
#Difference between map and foreach -
#map is a transformation, map will iterate on every elements and return an rdd
#foreach is an action, foreach will also iterate on every elements and doesn't have any return type
filter_rdd=cust_rdd1.filter(lambda x:int(x[3])>35)
filter_rdd.foreach(lambda x:iz_accum.add(1))#Only action that doesn't returns a result/value
print(iz_accum.value)
cust_rdd1.filter(lambda x:int(x[3])>35).map(lambda x:iz_accum.add(int(x[3]))).count()
print(iz_accum.value)

##We have completed Spark Core - All 5 components, performance optimization
# (basic best practices, partitioning, memory managment, broadcast & accumulator)
# we will see more in detail (very very important) - (we see everything else including what you ask or dont ask)