#Bread & Butter1 program - Python + Pyspark FBP
#Resources to Refer for learning Spark SQL - /home/hduser/install/iz_workouts/handson.pdf, ppt (Spark SQL), spark_sql_EL_BB1.py (priority2), spark_sql_ETL_BB2.py (vvv important bread and butter)
#Documentation Reference:
#https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
#Code Documentation ref?
#/usr/local/spark/python/pyspark/rdd.py
#/usr/local/spark/python/pyspark/sql/session.py
#/usr/local/spark/python/pyspark/sql/readwriter.py
#/usr/local/spark/python/pyspark/sql/functions.py
#/usr/local/spark/python/pyspark/sql/types.py
#/usr/local/spark/python/pyspark/sql/column.py

#How we are going to learn Spark SQL (EL/ETL/ELT):
#HOW to write a typical spark application
#Writing main method, check for for name==main, pass params to the main method, instantiate spark session object .....

#Overview of this module - spark_sql_EL_bb1.py start
#1. how to create dataframes from RDD using named list/reflection (Row object) (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#2. how to create DF from different (builtin) sources by inferring the schema automatically or manually defining the schema (very important)
#3. how to store the transformed data to targets(builtin) Very Important
#spark_sql_EL_bb1.py end

#spark_sql_ETL_bb2.py (vvv important) start
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (main portition  level 2)
#above and beyond (Developed/Worked)
#6. how to Standardize/Modernization the code and how create/consume generic/reusable functions & frameworks (level 3)
# - Testing (Unit, Peer Review, SIT/Integration, Regression, User Acceptance Testing), Masking engine,
# Reusable transformation(munge_data, optimize_performance), (Self Served Data enablement) Data movement automation engine (RPA),
# Quality suite/Data Profiling/Audit engine (Reconcilation) (Audit framework), Data/process Observability
#spark_sql_ETL_2.py end

#7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
#8. performance tuning
#After we get some grip on Cloud platform
#9. Deploying spark applications in Cloud
#10. Creating cloud pipelines using spark SQL programs

#HOW to write a typical spark application?
#Creating SC + SQLContext + HiveContext or session?

#Legacy Way
#when we create spark session object in the name of spark (it comprise of sparkcontext, sqlcontext, hivecontext)
#where we need spark context - to write rdd programs (manually or automatically)
#where we need sql context - to write dsl/sql queries
#where we need hive context - to write hql queries
from pyspark import *
from pyspark.sql import *
sc=SparkContext()#1000 loc (i will run rdd programs directly or what ever spark sql/hql provides)
sqlContext=SQLContext(sc) #100 (converting sql to optimized rdd) -> 1000+100 loc #OOPS - Inheritance concept (I will be inheriting my parent properties rather than earning it)
hiveContext=HiveContext(sc)#50 -> 1000+50

#Modern Way
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.appName("WD32 Bread and Butter App1").config("spark.sql.shuffle.partitions","10")\
    .master("local[2]").enableHiveSupport().getOrCreate()
sc=spark.sparkContext#for spark sql program, sc is indirectly used

####1. creating DF from rdd - starts here######

#1. (not much important) how to create (representation) dataframes from RDD using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and

#How to convert an rdd to dataframe with all possibilities using list option? Not so much important, because we don't create rdd initially to convert to df until it is in evitable

#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter,
rdd1=sc.textFile("file:///home/hduser/sparkdata/nyse.csv")
split_rdd2=rdd1.map(lambda x:x.split("~"))

#2. Iterate on every splitted elements apply the respective, datatype to get the SchemaRDD
schema_rdd3=split_rdd2.map(lambda x:(str(x[0]),str(x[1]),float(x[2])))

#stop working on RDD functions, ASAP represent rdd as dataframe further, to simplify the operation performed on the data
#Below dfs will not have specific column names but datatypes are specified using (schema rdd)
datatype_df1=spark.createDataFrame(schema_rdd3)
datatype_df2=schema_rdd3.toDF()

#In the above dataframe we don't have column names, lets define column names also using 2 methodologies (1. using list of columns, 2. using concept of reflection)
#3. Create column list as per the the structure of data
col_list=["name","stock","price"]

#Below dfs will not have specific column  names and datatypes (string)
spark.createDataFrame(split_rdd2,col_list).printSchema()

# 4. Convert the schemaRDD to DF using toDF or createDataFrame apply the column names by calling the column lists
#Below dfs will have specific column  names and specific datatypes also using the schem_rdd
colname_datatype_df1=spark.createDataFrame(schema_rdd3,col_list)
colname_datatype_df2=schema_rdd3.toDF(col_list)

#or

#How to convert/represent an rdd as dataframe using reflection?
# Not so much important, because we don't create rdd initially to convert to df until it is in evitable

#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter
rdd1=sc.textFile("file:///home/hduser/sparkdata/nyse.csv")
split_rdd2=rdd1.map(lambda x:x.split("~"))

#2. Reflect every splitted elements to the Row class and get a Row object based SchemaRDD created with both columnnames and datatype applied
#import the Row object
from pyspark import Row
row_schema_rdd3=split_rdd2.map(lambda x:Row(exchname=x[0],stockname=x[1],closeprice=float(x[2])))

#3. We can skip step 3 for creating the columname seperately as list, because we had column names already added in the Row
#col_list=[]

# 4. Convert the schemaRDD to DF using toDF or createDataFrame using concept of Reflection using Row Objects
colname_datatype_df1=spark.createDataFrame(row_schema_rdd3)
colname_datatype_df2=row_schema_rdd3.toDF()

#dsl programming
colname_datatype_df1.select("closeprice").where("closeprice >10").show()

#sql programming (familiar)
colname_datatype_df1.createOrReplaceTempView("view1")
spark.sql("select closeprice from view1 where closeprice >10").show()

#Rather writing the code like above (either schemardd+collist or rowrdd to DF) (create rdd then converting to DF),
# preferable write the code (directly create DFs) using the modules as given below
direct_df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep="~",inferSchema=True).toDF("exchname","stockname","closeprice")

direct_df1.select("closeprice").where("closeprice >10").show()

direct_df1.createOrReplaceTempView("view1")
spark.sql("select closeprice from view1 where closeprice >10").show()

#Quick Usecase - Spark wordcount using core and dsl/sql?
#A typical example for forcefully creating rdd then df then temp view query.
#1. create an rdd to convert the data into structured from unstructured
structured_rdd1=sc.textFile("file:///home/hduser/sparkdata/courses.log").flatMap(lambda x:x.split(" ")).map(lambda x:Row(course_name=x))
#2. represent rdd as dataframe
collist=["course_name"]
df1=structured_rdd1.toDF(collist)

df1.createOrReplaceTempView("view2")
spark.sql("select count(1) from view2").show()

df1=structured_rdd1.toDF()#represent rdd as dataframe asap

#3. write dsl queries
df1.distinct().show()#dsl query

#or

#4. represent df as tempview (since sql is simple and more familiar)
df1.createOrReplaceTempView("view1")

#5. write sql queries
spark.sql("select distint course_name from view1").show()
spark.sql("select course_name,count(1) cnt from view1 group by course_name order by 2 desc").show()

#What we learned in this section 1?
#1. How to define spark session or sc, sqlc, hivec seperately using legacy methodology
#2. How to define schema rdd (tuples/Row(tuples)) using list or reflection
#3. How to represent schema rdd in a form of dataframe using toDF or createDataFrame functions
#4. Fundamentally dsl on dataframe and sql on tempview

####1. creating DF from rdd - ends here######


####2. creating DF directly from different sources using different options in different methodologies- starts here (very important)######
#2. how to create DF from different sources (using different options) by inferring the schema automatically
# or manually defining the schema (very important)

#Options to recall
#3 Important options are - delimiter, inferschema, header
#2 more Important options are - schema, mode
#More options are available (we can refer documentation on the need basis to use these addition options)

#methodologies to recall (any one of the methodology)
#(Option, options, inline args, format & load)

#Different sources functions/modules to use
#Builtin - csv, json, orc, parquet, jdbc, table, textfile...

#A. creating DFs directly using different methodologies (Option, options, inline args, format & load) with different options applied (important inferschema, delimiter, header)
###############################################################################################################################################################################
#1. spark session object importance (we can't create DF only by using spark context? spark session (includes sqlContext naturally) or direct sqlcontext in legacy way)
#ensure we have spark or sqlContext available

#2. We can use read module/method of the spark session for applying multiple options
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")

#column names starts with _c0, _c1... (if we don't have header),  with default delimiter ',' with default datatype String

#3. csv module possible way of defining options -> (option/options/inline arguments/format & load) inferschema, header, delimiter/sep
#Three methologies we have to apply options/parameters to provide additional features to the data

#DF will be doing Immediate Evaluation (existance of source, header, delimiter, columns & datatype) & Lazy Execution
#When i define Dataframe the rdd will be doing immediate evaluation rather than lazy evaluation, but execution(processing) will be lazy only....

#What are the methodologies of defining Dataframes (option, options, inline methods) and some important options we are using
#1. Individual option methodology to define one option at a time (with some minimum 3 options that we regularly use)- delimiter, inferschema, header (important options)
#by default delimiter is , and inferschema (asking spark to autodetect datatype by scanning entire dataset) is false
df1=spark.read.option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")

#If I receive header from the source, we have to mention header,true option
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")

#If I don't receive header from the source, but we have to mention header,true option
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","false").csv("file:///home/hduser/sparkdata/nyse.csv").toDF("ex","st","pr")

#If I receive header from the source, but we wanted to change the column name
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv").toDF("ex","st","pr")

#If I have data in comma delimited format and no header is there, then we can mention only one option
df1=spark.read.option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse_csv.csv").toDF("ex","st","pr")

#Conclusion is - use option function if we have minimal options to use

#2. options methodology to define dataframe
df1=spark.read.options(delimiter="~",inferschema="true",header="true").csv("file:///home/hduser/sparkdata/nyse_header.csv").toDF("ex","st","pr")
#Conclusion is - use options function if we have multiple options to use

#3. inline arguments methodology to define dataframes with lot of built in options to use in a guided way
df1=spark.read.csv(path="file:///home/hduser/sparkdata/nyse_header.csv",sep='~',encoding='UTF-8',header=True,inferSchema=True,ignoreTrailingWhiteSpace=True,ignoreLeadingWhiteSpace=True)

#Conclusion is - use inline argument methodology if we want to have multiple different flavors of options in a guided way

#4. format & load or load methodology to define dataframes with different inline options we can use
df1=spark.read.format("csv").load(path="file:///home/hduser/sparkdata/nyse_header.csv",inferSchema='TRUE',sep='~',header='true')
df1=spark.read.load(path="file:///home/hduser/sparkdata/nyse_header.csv",format='csv',inferSchema='TRUE',sep='~',header='true')
#Conclusion is - use format and load methodology is used if we want to source the data from any sources mainly EXTERNAL (internal or external)

#df1=spark.read.load(path="file:///home/hduser/sparkdata/nyse_header.csv",inferSchema='TRUE',sep='~',header='true')
#Important Conclusion is - By default if we don't mention the format, parquet format will be considered as default preferred data format for spark

#Options to recall
#3 Important options are - delimiter, inferschema, header
#2 more Important options are - schema, mode

###How to define schema manually/custom way (very important) starts here###
#Why we have to go with manualcustom way of defining structure/schema for our dataframe?
#1. For customized datatype definition rather than double i want it as float
#2. I don't want spark to take long time for inferring the schema (to overcome we can use sampling option)
#3. If the structure of the data is fixed, usage of manual schema definition is more standard...
#4. We want to define/control both columname and datatype in one place
#StructType - I will hold the list of StructField (like a tablename in hive)
#StructField - I will hold the columname, datatype and nullable true/false (column datatype in hive)
#for eg. create table tbl1(colname datatype) - here tbl1 is like StructType, colname datatype is kept under StructField
#pseudo code - StructType(list([StructField("colname1",StringType(),False),StructField(),StructField()]))
#syntax how to remember StructType([StructField("col",dt,True)])

#/usr/local/spark/python/pyspark/sql/types.py
from pyspark.sql.types import *
nyse_struct=StructType([StructField("exch",StringType(),True),StructField("stock",StringType(),True),StructField("price",FloatType(),True)])
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",schema=nyse_struct)
df1=spark.read.load("file:///home/hduser/sparkdata/nyse_csv.csv",format='csv',schema=nyse_struct)
###How to define schema manually (very important) ends here###

#Tale of usage of infer schema vs custom schema?
#mode option in spark dataframe
#mode (read) (permissive(default), failfast, dropmalformed)
#permissive - permit everything by not evaluate the data with the structure defined hence don't throw any error
#failfast - fail immediately by evaluate the data with the structure defined throw error if any data doesn't fit the structure
#dropmalformed - drop the unwanted data ( if it doesn't fit the structure) by comparing with the structure defined
'''
/home/hduser/sparkdata/nyse_csv.csv
NYSE,CLI,35.3
NYSE,CVH,24.62
NYSE,CVL,30.2a
'''
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",schema=nyse_struct)#default permissive
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",schema=nyse_struct,mode='permissive')#default permissive
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",schema=nyse_struct,mode="dropmalformed")#drop the row that has data fitment issue for a specific column
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",schema=nyse_struct,mode="failfast")#default permissive

#When can we go with inferschema?
#We use mostly defined schema using structtype and structfield for the defined dataset in production
#We use some time the inferschema when the volume of data is small and variable in production or for development/bug fixing purpose we may use inferschema
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_csv.csv",inferSchema="true")
df1.printSchema()#this shows the _c2 column is expected as float, but inferred as string, hence _c2 is the culprit
''' |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
'''

''''
When I do development, i will ask spark to infer the schema of whole data
df1=spark.read.csv("file:///home/hduser/sparkdata/usdata.csv",inferSchema="true",header=True)
>>> df1.printSchema()
root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- company_name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- county: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip: integer (nullable = true)
 |-- age: integer (nullable = true)
 |-- phone1: string (nullable = true)
 |-- phone2: string (nullable = true)
 |-- email: string (nullable = true)
 |-- web: string (nullable = true)

'''


#We learned upto day 4 of Spak SQL - defining dataframe from rdds using reflection, list options,
# creating dataframes using different methodologies and options (important 5 options)
#compared inferschema with customschema

#A. Ending - option(opt1,value1), options(opt1=value,opt2=value), csv(filename,opt1=value,opt2=value), format(csv).option/options.load("file")

#B. Starting - Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table, json) / external later (cloud, api, nosql))
# or programatically by applying DSL/SQL we are going to learn in BB2

#CSV Module
df_csv1=spark.read.csv(path="file:///home/hduser/sparkdata/nyse_header.csv",sep="~",inferSchema=True,header=True,mode="dropmalformed")

#JSON Module - Semistructured data
#When do we go for JSON? If we get data from some applications (IOT, API, Webservises etc.,) or If the data is variable in nature (column order/number of columns are evolving)
df_json1=spark.read.json(path="file:///home/hduser/sparkdata/nyse_json",allowUnquotedFieldNames=True)

#Parquet Module (Spark preferred format)- Serialized data (optimized)
#Columnar data, pushdown optimization, pruning, intelligent data format, featuristic data format (header/type/format is preserved), interiem data, background of deltalake(databricks) is parquet
#Application of Parquet - intermediate data, raw (schema migrated) data, hive table data, optimized data, Archival
df_json1.write.mode("overwrite").parquet("file:///home/hduser/app1data/")
df_parquet1=spark.read.parquet("file:///home/hduser/app1data/")

#Orc Module (Hive preferred but spark prefers orc if parquet is not used)- Serialized data (optimized)
#Columnar data, pushdown optimization, pruning, intelligent data format, featuristic data format (header/type/format is preserved), interiem data, background of deltalake(databricks) is parquet not orc
#Application of Parquet - intermediate data, raw (schema migrated) data, hive table data, optimized data, Archival
df_json1.write.mode("overwrite").orc("file:///home/hduser/orcdata/")
df_orc1=spark.read.orc("file:///home/hduser/orcdata/")
df_orc1.where("exchange='NYSE'").explain(True)

#Avro Module (Not a built in function in Spark, we have to explicitly import avro libraries and use - later)

#Hive Module (We want to perform DSL or HQL on hive tables using spark)
df_hive1=spark.read.table("default.customer")#Entire table is represented as a dataframe
df_hive1.show()#DSL Query

df_hive1=spark.sql("select * from default.customer")#We can write customer query to get dataframe created from a hive table
df_hive1.show()#DSL Query
df_hive1.select("*").where("city>40").show()#DSL Query

df_hive1.createTempView("df_customer")
spark.sql("select * from df_customer where city>40").show() #SQL Query

#Database Source (jdbc) Module (used to read data from database sources present in onprem or cloud)
dburl="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$"
tbl="customer"
prop={"driver":"com.mysql.cj.jdbc.Driver"}
df_db1=spark.read.jdbc(url=dburl,table=tbl,properties=prop)

#How to pull the recently added/modified data from the DB source
dburl="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$"
tbl="(select * from customer where createdt>=current_date)tmp"
prop={"driver":"com.mysql.cj.jdbc.Driver"}
df_db1=spark.read.jdbc(url=dburl,table=tbl,properties=prop)


#Interview Question: Is it possible to transfer data between 2 different spark applications?
# Spark has a big challenge of tranferring data between applications

#B. Ending - Create dataframes using the modules (builtin (csv, orc, parquet, jdbc(DB),table(Hive), json)


#3. how to store the raw/transformed data to targets(builtin)
#A. Write the DF data into different targets in different format using different methodologies with different options
#rather sqoop import we are using spark SQL to achieve - DB to Hive/hdfs/linux - table, csv, json, orc, parquet..
#when sqoop is there why we need spark sql to perform data ingestion
#spark is a data processing tool (but it can do data ingestion with few options customized), sqoop is a data ingestion tool (meant for data ingestion with huge built in options for data ingestion)
# spark can have huge set of sources/targets option
# spark can help you perform unification & federation
# Irfan we are going to one time migrate a dwh into our hadoop datalake - i can go with sqoop or spark? go with sqoop (sqoop can do offline batch data ingestion with lot of features)
# Irfan we are going to ingest data from dwh into our hadoop datalake on a daily basis - i can go with sqoop or spark? go with spark (spark provides lot of on the fly computing features)
# sqoop is decoupled (db1 -> sqoop import -> hdfs -> hive -> transform -> sqoop export -> db2), spark can be coupled (db1 -> memory -> db2)

df_db1=spark.read.jdbc(url="jdbc:mysql://localhost/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.cj.jdbc.Driver"})
df_db1.cache()#read commited
df_db1.count()

#Interview Question: I have executor created with 1 GB capacity, but source DB(singapore) has 2gb data, I want to cache the entire data into spark (US) ?
#Ans: I use memory and disk storage level
from pyspark.storagelevel import StorageLevel
df_db1.persist(StorageLevel.MEMORY_AND_DISK)
df_db1.count()#DATA will be pulled from DB
df_db1.count() #DAta will be pulled from memory and local disk

#Write to CSV
#important options to use - mode (overwrite, append), sep, header
df_db1.cache()
df_db1.write.csv(path="file:///home/hduser/customer",sep="~",mode="overwrite",header=True)

#Interview Question - I want to write the output files into one or more, how to control the number of output files? using repartition/coalesce
df_db1.repartition(2).write.csv(path="file:///home/hduser/customer",sep="~",mode="overwrite",header=True)#Overwrite will help you achieve full refresh of target data

#Write to JSON
#important options to use - mode (overwrite, append)
df_db1=spark.read.jdbc(url="jdbc:mysql://localhost/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.cj.jdbc.Driver"})

df_db1=spark.read.jdbc(url="jdbc:mysql://gcp-inceptez:us-central1:inceptezdb/sampledb?user=irfan&password=Inceptez@123",table="emp",properties={"driver":"com.mysql.cj.jdbc.Driver"})


df_db1.cache()
df_db1.count()
#Data will come from the cache memory
df_db1.write.json("file:///home/hduser/customer_json",mode="overwrite")#overwrite will help you achieve full refresh
df_db1=spark.read.jdbc(url="jdbc:mysql://localhost/custdb?user=root&password=Root123$",table="(select * from customer where createdt>=current_date)tmp",properties={"driver":"com.mysql.cj.jdbc.Driver"})
df_db1.write.json("file:///home/hduser/customer_json",mode="append")#Append will help you achieve incremental data ingestion

#Write to ORC & Parquet
#important options to use - mode (overwrite, append), partitionBy, bucketBy
df_db1=spark.read.jdbc(url="jdbc:mysql://localhost/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.cj.jdbc.Driver"})
df_db1.cache()
df_db1.write.mode("overwrite").orc("file:///home/hduser/customer_orc",compression="lzo")#default is the best (snappy) compression, but we want to override with some other compression
df_db1.write.mode("overwrite").bucketBy(3,"age").orc("file:///home/hduser/customer_orc",partitionBy="createdt")#partitioning, bucketing, compressing, serializing
df_db1.write.mode("overwrite").format("orc").save("file:///home/hduser/customer_orc")


#Balaji - In every interviews, I am getting a question asked? Schema migration with few important options used
#Interview Question: I have executor created with 1 GB capacity, but source DB(singapore) has 2gb data, I want to cache the entire data into spark (US) ?
#I have a spark resources available, which is lesser than the data that I am going to process, how to improve the performance still retaining the same resource capacity?
#To achieve better performance, 1. convert the data into intelligent format (Optimized Row/Column), 2. Partition the data, 3. Cluster(bucket) the data
#By converting to intelligent format of orc/parquet - we are getting PDO, Predicate/projection, pruning, serialization, compression features

df_db1.write.mode("overwrite").orc("file:///home/hduser/customer_orc",partitionBy="createdt")
spark.read.orc("file:///home/hduser/customer_orc").where("createdt='2024-05-13' and city='Chennai'").select("custid","age").explain(True)
df_db1.write.mode("overwrite").parquet("file:///home/hduser/customer_parquet",partitionBy="createdt")

spark.read.parquet("file:///home/hduser/customer_parquet").where("createdt='2024-05-13' and city='Chennai'").select("custid","age").explain(True)


#Write DF data into Hive Table (overwrite/append, partitioning, managed, bucketing, external)

#Can you import data from a DB and write into Hive table (partitioned)?
# Sqoop (if we are doing only this work as a migration) or Spark (if we are going to transform the data and do it frequently as a pipeline run)

df_db1.write.saveAsTable("default.custtabledata",mode="overwrite",partitionBy="createdt")#default parquet table

df_db1.write.bucketBy(3,"custid").saveAsTable("default.custtabledata",mode="overwrite",partitionBy="createdt")#partitioning & bucketing
#Spark Supports bucking of data in a hive table by using not (hash bucketing algorithm) rather it is using (Murmur),
#hence direct hive can't access the buckets/bucketed table created by spark, but spark can access the bucketed table for spark ETL operations performed with buckets more efficiently
#we are going to hive/spark-sql/pyspark(cli) and see whether table is created and loaded with data
#hive>describe formatted custtabledata;
#Num Buckets:        	-1
#Bucket Columns:     	[]
#https://issues.apache.org/jira/browse/SPARK-19256
#spark.read.table("default.custtabledata").join(df_db1,how="inner",on[db."custid"==hive."custid"])#I can use the bucket feature here

#Conclusion: Spark can create bucketing table in hive, but only for spark usage and not for hive usage (The table created by spark by creating bucket cannot be accessed from hive)

#Spark creating external hive table or using Spark Can we use some other methodology to create and load data into hive table
#Yes, using hql writterned in spark.sql() we can achieve
df_db1.write.saveAsTable("default.custtabledata",mode="overwrite",partitionBy="createdt")#default parquet table

#or Another methodology to write the data into the hive table by rewrite the above line into hql query
#1. redefine the df_db1 df as a view
df_db1.createOrReplaceTempView("db_data_view")
spark.sql("select * from db_data_view").show()
#Managed Table load
#2. create a hive table using hql query - spark.sql("create table tablename () partitioned by() row format delimi....")
spark.sql("""create table default.custtabledatahql (custid int,firstname string,lastname string,city string,age int,transactamt int)
partitioned by (createdt date) row format delimited fields terminated by ',' """)
#3. do an insert select from tempview to hive table - spark.sql("insert overwrite hivetable select * from view")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table default.custtabledatahql partition(createdt) select custid,firstname,lastname,city,age,transactamt,createdt from db_data_view")

#Interview Question? Can we write data into external hive table using spark? Yes, using native HQL query, no built in options are provided
#External Table load
#2. create a hive table using hql query - spark.sql("create table tablename () partitioned by() row format delimi....")
spark.sql("""create external table default.custtabledataexthql (custid int,firstname string,lastname string,city string,age int,transactamt int)
partitioned by (createdt date) row format delimited fields terminated by ',' location '/user/hduser/custtabledataexthql'""")
#3. do an insert select from tempview to hive table - spark.sql("insert overwrite hivetable select * from view")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table default.custtabledataexthql partition(createdt) select custid,firstname,lastname,city,age,transactamt,createdt from db_data_view")

#Let's try with external bucket table in hive using spark
df_db1.write.bucketBy(3,"custid").saveAsTable("default.custtabledata",mode="overwrite",partitionBy="createdt")
#Interview Question? Can we create and load bucketed table in hive using spark?
# Yes (if we use spark native DSL functions to create bucket table in hive using murmur algo, but table can be only used by Spark)
# No (Spark can't load hive bucketed table created with HQL using hash bucketing algorithm)

#2. create a hive table using hql query - spark.sql("create table tablename () partitioned by() row format delimi....")
#Table created with hash buckting algorith
spark.sql("""create external table default.custtabledataextbuckhql (custid int,firstname string,lastname string,city string,age int,transactamt int)
partitioned by (createdt date) clustered by (custid) into 3 buckets row format delimited fields terminated by ',' location '/user/hduser/custtabledataextbuckhql'""")
#hive>describe formatted default.custtabledataextbuckhql;
#Num Buckets:        	3
#Bucket Columns:     	[custid]
#3. do an insert select from tempview to hive table - spark.sql("insert overwrite hivetable select * from view")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table default.custtabledataextbuckhql partition(createdt) select custid,firstname,lastname,city,age,transactamt,createdt from db_data_view")
# pyspark.sql.utils.AnalysisException: Output Hive table `default`.`custtabledataextbuckhql` is bucketed but Spark currently
# does NOT populate bucketed output which is compatible with Hive.

#Write DF data into DB Table (overwrite/append)
#Like a Sqoop export the transformed data has to writterned to external DB

from pyspark.sql.functions import *
aggr_df=df_db1.groupBy("city").agg(sum(col("transactamt")).alias("sum_amt"),max(col("transactamt")).alias("max_amt"))
df_db1.createOrReplaceTempView("db_data_view")
aggr_ds=spark.sql("select city,sum(transactamt) as sum_amt,max(transactamt) as max_amt from db_data_view group by city")

aggr_df.write.jdbc(url="jdbc:mysql://localhost/custdb?user=root&password=Root123$",table="customer_exp",mode="overwrite",properties={"driver":"com.mysql.cj.jdbc.Driver"})


#4. Revisiting all the function with additional options... (Not important, but good to know once for all)
#csv,json,orc,parquet,jdbc,hive while writing and reading
#****Possible way of calling the csv and all other functions with different other options refering the document
#All possible methodologies
#Dont spend much time on these options for learning (later in realtime work for implementing, think of using these optionss)
#https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
df1=spark.read.option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df1=spark.read.options(header="true",delimiter=",").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df1=spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv",header="true",sep=",")#get familiarity in this methodology of creating DF
df1=spark.read.load("file:///home/hduser/sparkdata/nyse_header.csv",format='csv',header="true",sep=",")
df1=spark.read.format("csv").load("file:///home/hduser/sparkdata/nyse_header.csv",header="true",sep=",")

#Different Options (Not mandatory) on different functions (csv, json, orc, parquet, jdbc, hive)
df_csv_opt=spark.read.csv(path=["file:///home/hduser/srcdata1/","file:///home/hduser/srcdata2/"],header="true",sep="~",inferSchema=True,mode="permissive",\
                          pathGlobFilter="nyse_header[1-2].csv",recursiveFileLookup=True,encoding='UTF-8')


#"Mohamed Irfan ~"Architect~""
#PURA100,"Pure Soft Detergent - ~"100ml~"", 1.50 , 3.00,2019-12-03 00:00:01,2019-12-01
#PURA100,Pure Soft Detergent - "100ml", 1.50 , 3.00,2019-12-03 00:00:01,2019-12-01
df_csv_opt=spark.read.csv(path=["file:///home/hduser/sparkdata/"],header="true",inferSchema=True,\
                          pathGlobFilter="sales.csv",recursiveFileLookup=True,encoding='UTF-8',quote="\"",escape="~",\
                          ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True,nullValue='na',nanValue='-1',\
                          positiveInf="Infinitive",negativeInf="-Infinitive",dateFormat='yyyy-MM-dd',timestampFormat="yyyy-MM-dd",\
                          maxColumns=10,maxCharsPerColumn=1000,maxMalformedLogPerPartition=10,\
                          samplingRatio=.1,enforceSchema=False,emptyValue="-")

#samplingRatio=inferschema of the 10 percent of overall data
#enforceschema=False - help us validate the defined schema with header of data whether both are matching

#,columnNameOfCorruptRecord="corrupted_column"

#csv Write options
#I have some doubt in the escape option .
# the escape character will be added automatically when we have quoted string or it has to be added manually when we prepare original file.
df_csv_opt.write.csv("file:///home/hduser/sparkcsvout/",mode="overwrite",quote="'",escape="~",quoteAll=True,lineSep='|')

#JSON Read Options
#https://spark.apache.org/docs/3.2.1/sql-data-sources-json.html
df_json_opt=spark.read.json(path=["file:///home/hduser/sparkdata/"],mode='permissive',\
                          pathGlobFilter="ebayjson_corrupt.json",recursiveFileLookup=True,encoding='UTF-8',\
                          dateFormat='yyyy-MM-dd',timestampFormat="yyyy-MM-dd",\
                          samplingRatio=1,primitivesAsString=True,prefersDecimal=False,allowComments=True,\
                           allowUnquotedFieldNames=True,allowSingleQuotes=True,\
                           allowNumericLeadingZero=False,dropFieldIfAllNull=True,allowNonNumericNumbers=True)
#columnNameOfCorruptRecord='corrupted_data',

#Orc/Parquet Options (schema evolution)
#Ingestion Layer
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")

#Writing in Orc/Parquet Formats - Inteligent - Schema preserved, Internally partitioned (strides), indexed(dictionary/range), bucketed(stripes), sorted(column level)
#Raw Layer/Curated Layer
#Benefits: Serialized(conversion of data to machine understandable serialized/bytecode/binary format), Compressed(snappy) & Optimized (predicate/projection PDO)
#Orc Write
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='overwrite')#minimum options
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='overwrite',partitionBy="exch",compression="lzo")#maximum options

#Orc Read
spark.read.orc("hdfs:///user/hduser/nyseorcdata/",pathGlobFilter='part*',recursiveFileLookup=True)

#Parquet (spark is more compatible with parquet)
df1.write.parquet("hdfs:///user/hduser/nysepardata/",mode='overwrite')
df1.write.parquet("hdfs:///user/hduser/nysepardata/",mode='append',partitionBy="exch",compression="gzip")#maximum options

#Schema Evolution (Schema migration csv->orc)
#important option in a case basis we can use: mergeSchema
'''
day1 data
exch~stock~price
NYSE~CLI~35.3
NYSE~CVH~24.62
BSE~AAP~30.2

day2 data
exch~stock~price~inital_price
NYSE~CLI~34.3~36.1
NYSE~CVH~23.62~24.01
BSE~AAP~31.2~30.0

day3 data
list_price~stock~price~inital_price
10~7~34.3~36.1
5~44~23.62~24.01
1~23~31.2~30.0

'''

df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day2.csv")
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='append')#Schema migration in the Raw layer

evolved_df2=spark.read.orc("hdfs:///user/hduser/nyseorcdata/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()#3+1 columns

#Usecase: Try the same above scenario for parquet, with  exch column removed on day3 data, with list_price added and stock column of number type
#provide your findings of difference between orc and parquet - if the datatype is changed orc will not support but parquet support
#Requirement: Source is sending the data in an evolved state, hence we need schema migration, schema evolution to mitigate this...

#Orc schema migration - not working for column datatype change
#day1
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='append')#Schema migration in the Raw layer
#day2
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day2.csv")
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='append')#Schema migration in the Raw layer

evolved_df2=spark.read.orc("hdfs:///user/hduser/nyseorcdata/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()

#day3
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day3.csv")
df1.write.orc("hdfs:///user/hduser/nyseorcdata/",mode='append')#Schema migration in the Raw layer

evolved_df2=spark.read.orc("hdfs:///user/hduser/nyseorcdata/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()

#Parquet schema migration - not working for column datatype change
#day1
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df1.write.mode("overwrite").parquet("hdfs:///user/hduser/nysepardata/",mode='append') #Schema migration in the Raw layer
#day2
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day2.csv")
df1.write.parquet("hdfs:///user/hduser/nysepardata/",mode='append')#Schema migration in the Raw layer

evolved_df2=spark.read.parquet("hdfs:///user/hduser/nysepardata/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()

#day3
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day3.csv")
df1.write.parquet("hdfs:///user/hduser/nysepardata/",mode='append')#Schema migration in the Raw layer

evolved_df2=spark.read.parquet("hdfs:///user/hduser/nysepardata/",mergeSchema=False)
evolved_df2.printSchema()
evolved_df2.show()

#Solution to fix this issue is simple: Don't infer the schema rather just consider all fields as string
#day1
df1=spark.read.option("delimiter","~").option("inferschema","false").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df1.write.mode("overwrite").parquet("hdfs:///user/hduser/nysepardata1/",mode='append') #Schema migration in the Raw layer
#day2
df1=spark.read.option("delimiter","~").option("inferschema","false").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day2.csv")
df1.write.parquet("hdfs:///user/hduser/nysepardata1/",mode='append')#Schema migration in the Raw layer
evolved_df2=spark.read.parquet("hdfs:///user/hduser/nysepardata1/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()
#day3
df1=spark.read.option("delimiter","~").option("inferschema","false").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header_day3.csv")
df1.write.parquet("hdfs:///user/hduser/nysepardata1/",mode='append')#Schema migration in the Raw layer
evolved_df2=spark.read.parquet("hdfs:///user/hduser/nysepardata1/",mergeSchema=True)
evolved_df2.printSchema()
evolved_df2.show()




#JDBC (optimization options batchsize, fetchsize, partitions etc.,)
# Public Cloud AWS RDS (Mysql DB) -> Spark JDBC -> OnPrem (Hadoop Datalake)
# Public Cloud AWS RDS (Mysql DB) -> Spark JDBC -> OnPrem Cloudera Cluster (Hadoop Datalake/Hive Lakehouse)

# Public Cloud GCP Cloud SQL (Mysql DB) -> Spark JDBC -> OnPrem Cloudera/Databricks Cluster (Hadoop Datalake/Hive Lakehouse)
#Articulation is very very important
#Retail data - source is DB source, data is retail/store data, type of data is consumer/business, transaction, sales, products, emp, offices etc.,
#Output - We identify supply and demand, promo, profit/loss, rewards to employees, offers to customers, customer experience we calculate
#Manufacturing data - source is DB source, data is manufacturing data, type of data is consumer/business, transaction, shipping, sales, products, labours, machines (cloud FS source), machine operational (APIs) etc.,
#Business Requirements
#Technology - CloudSQL, Mysql, Spark JDBC, HDFS, Hive
#Terminologies - EL, Schema Migration, DB source to Onprem Datalake (HDFS), DB to Onprem Lakehouse, Data Migration, Data movement,
#Reconcilation
#Performance Optimization - partitioning parameters, skewing,

df_clouddb1=spark.read.jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan&password=Inceptez@123",table="orders")
df_clouddb1.write.orc("hdfs:///user/hduser/clouddbdataorc")#DB to Datalake migration
df_clouddb1.write.saveAsTable("default.clouddbdata")#DB to Lakhouse migration

if (df_clouddb1.count()==spark.read.table("default.clouddbdata").count()):
 print("Recon success")

#Optimization with few options used
boundary_val="(SELECT min(orderNumber),max(orderNumber) FROM ordersproducts.orders)tmp"
df_cloudboundarydb1=spark.read.jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan&password=Inceptez@123",table=boundary_val,\
                            properties={"driver":"com.mysql.cj.jdbc.Driver"})

df_clouddb1=spark.read.jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan&password=Inceptez@123",table="orders",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","partitionColumn":"orderNumber","numPartitions":"3",\
                                        "lowerBound":"10100","upperBound":"10425","fetchSize":"100","pushdownPredicate":"True","pushDownAggregate":"True"})
#https://spark.apache.org/docs/3.2.1/sql-data-sources-jdbc.html
for i in df_clouddb1.rdd.glom().collect():#no skewing of data
 print(len(i))

df_clouddb1=spark.read.jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan&password=Inceptez@123",table="orders",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","partitionColumn":"orderNumber","numPartitions":"3",\
                                        "lowerBound":"10100","upperBound":"10400"})
#100->200 (p1), 201->300 (p2), >300 #light skewing will be there
for i in df_clouddb1.rdd.glom().collect():
 print(len(i))
#,"numPartitions":"3"
#Spark            Sqoop
#partitionColumn=  split-by #high cardinal column preferably number, to avoid skewing of data
#numPartitions  = -m
#lowerBound     = lowerBound (sqoop will run boundary query automatically to calculate upper and lower bound)
#upperBound  = upperBound (sqoop will run boundary query automatically to calculate upper and lower bound)
#fetchsize      =fetchsize
#batchsize      =batchsize
#query_string   =--query

#Write to DB
df1=spark.read.option("delimiter","~").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")

df1.write.mode("overwrite").jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan",table="stocks",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","password":"Inceptez@123","numPartitions":"2",\
                                        "batchsize":"10000","truncate":"true"})#2 db connections, each map task will take 10k records in one iteration

#usecase: Ingesting data from RDBMS to Onprem (lakehouse, datalake) from 3 different dbs custpayments,empoffice, ordersproducts

#spark comprised of sc, sqlContext, hiveContext in future plan to add streamingContext
#Hive Embedded metastore in Pycharm environment
#db notebook -> spark(hive) -> dbfs
#Pycharm IDE -> spark(hive) -> LFS
spark.read.table("customerhive").show()
df1=spark.read.orc("file:///home/hduser/customer_orc")
df1.write.saveAsTable("customerhive")




