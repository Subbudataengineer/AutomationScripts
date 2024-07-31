#Projects evolution:
#hive project -> Cloudera - spark project -> a&b things (frameworks reusable, masking, quality, automation) ->
# GCP(DP)/Azure(DB) cloud migration(lift & shift) -> cloud industrialization/modernization/standardization (GCP - df,dp,gcs,bq,cc/af), (ADF,ADL,ADB,Synapse,workflow)

#spark_sql_ETL_bb2.py start (Bread & Butter2) - FBP-SQL/DSL (FBP-SQL)
#How to use Python to control the SQL
#Py-Spark-SQL -> Python (FBP Use), Spark Framework (leverage), SQL(Completely)
#4. how to apply transformations/business logics/functionality/conversion using DSL(DF) and SQL(view) (main portion level 1)
#hive, spark(DSL/SQL) - cloud native components (SQL)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets
# (main portion  level 2) #above and beyond (Developed/Worked)
#6. how to Standardize/Modernization/Industrializing the code and how create/consume generic/reusable functions & frameworks (level 3)
# - Testing (Unit, Peer Review, SIT/Integration, Regression, User Acceptance Testing), Masking engine,
# Reusable transformation(munge_data, optimize_performance), (Self Served Data enablement) Data movement automation engine (RPA),
# Quality suite/Data Profiling/Audit engine (Reconcilation) (Audit framework), Data/process Observability
#spark_sql_ETL_2.py end

#7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
#8. performance tuning
#After we get some grip on Cloud platform
#9. Deploying spark applications in Cloud
#10. Creating cloud pipelines using spark SQL programs

##### LIFE CYCLE OF ETL and Data Engineering PIPELINEs
# VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW ,
# WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
#Tell me about the common transformations you performed, tell me your daily roles in DE, tell me some business logics you have writtened
#How do you write an entire spark application, levels of DE pipelines or have you created DE pipelines what are the transformations applied,
#how many you have created or are you using existing framework or you created some framework?

#4. how to apply transformations/business logics/functionality/conversion using DSL(DF) and SQL(view) (main portion level 1)
#SDLC Lifecycle of A DE Pipeline
#Source -> sample data -> 1. classification/onboarding (DGovernance/Data Stewards/DSecurity - filter/redact/mask/classify/cluster/tagging/lineage/...)
#2. DEngineers -> Raw Data -> apply data munging process

'''
TRANSFORMATION
Starting point - (Data Governance (security) - Tagging, categorization, classification, masking/filteration)
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy(usable) format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for further Transformation/Enrichment, Egress/Outbound, analytics, model application & Reporting
a. Passive - Data Discovery EDA (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Active - Combining Data + Schema Evolution/Merging (Structuring)
c. Validation, Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format (Dataengineers/consumers)

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration

3. Data Customization & Processing - Application of Tailored Business specific Rules
a. User Defined Functions
b. Building of Frameworks & Reusable Functions

4. Data Curation
a. Curation/Transformation
b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization

5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
a. Lookup/Reference
b. Enrichment
c. Joins
d. Sorting
e. Windowing, Statistical & Analytical processing
f. Set Operation

6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
a. Discovery,
b. Outbound/Egress,
c. Reports/exports
d. Schema migration
'''

#EXTRACTION PART#
print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Passive - Data Discovery (EDA) - Exploratory Data Analysis - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes(columns/datatype) and patterns (format/sequence/alpha/numeric).
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns (statistics)
#Understand the data by applying inferschema structure - what is the natural datatypes
#Apply Custom structure using the inferred schema - copy the above columns and datatype into excel and get structure, GenAI (create a structtype with total 100 columns with all as string)   
#Identify Nulls, De-Duplication, Prioritization, Datatypes, formats etc.,
#If the data is not matching with my structure then apply reject strategy
#Summarize the overall pattern of data
b. Active - Data Structurizing - Combining Data + Schema Evolution/Merging/Melting (Structuring) - 
pathlookupfilter,recursivefilelookup, union, unionbyname,allowmissingfields,convert orc/parquet then mergeSchema
c. Active - Validation, Cleansing, Scrubbing (Preprocessing, Preparation, Validation) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace)
d. Standardization - Column name/type/order/number of columns changes/Replacement & Deletion of columns to make it in a usable format
""")

'''1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) (attributes, patterns/trends) - Exploratory Data Analysis - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes (columns/datatype) and patterns (format/sequence/alpha/numeric).
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns 
#Understand the data by applying inferschema structure - what is the natural datatypes
#Apply Custom structure using the inferred schema - copy the above columns and datatype into excel and get structure, GenAI (create a structtype with total 100 columns with all as string)   
#If the data is not matching with my structure then apply reject algorithm
#Passively Identify Nulls (na.drop, sql(is null)), constraints (structtype nullable=False), 
Duplicates (distinct, dropDuplicates()), Datatypes is not as per the expectation because of the data format hence (rlike or upper()=lower() -> regexp_replace -> cast ) etc.,
Summarize (statistics) the overall trend of data'''

#Define Spark Session Object or sparkContext+sqlContext for writing spark sql program
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.appName("WD32 Bread and Butter App1").config("spark.sql.shuffle.partitions","10")\
    .master("local[2]").enableHiveSupport().getOrCreate()
sc=spark.sparkContext

#a. Data Discovery/understanding/analysis (EDA)
#Understand the data in its own format (read all attributes/columns as string) - total rows, total columns
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified")
print(df_raw.count())
df_raw.show()
df_raw.printSchema()
'''root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)'''


#Understand the data by applying inferschema structure - what is the natural datatypes
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw.show()
df_raw.printSchema()
'''
root
 |-- cid: string (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- prof: string (nullable = true)
'''

#Apply custom schema using infer schema identified columns and datatype, then apply different modes for different data exploration
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
#StructType([StructField("col_name",DataType(),True),StructField("col_name",DataType(),True),StructField("col_name",DataType(),True)......])
customstruct=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw_custom=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="permissive")
df_raw_custom.show(20)
df_raw_custom.printSchema()
df_raw_custom.count()#10005

#Let us identify howmuch percent of malformed data is there in our dataset (data is not as per the structure, number of columns are less than the structure defined)
df_raw_custom_malformed=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="dropmalformed")
df_raw_custom_malformed.show(20)
df_raw_custom_malformed.printSchema()
df_raw_custom_malformed.count()#10005 dropmalformed will not be applied on count()
len(df_raw_custom_malformed.collect())#10002 - alternative solution is collect and calculate length of the list

#Identify only the corrupted data and I need to analyse/log them or send it to our source system to get it corrected
customstruct_corrupted=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True),StructField("corrupted_data",StringType(),True)])
df_raw_custom_malformed=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct_corrupted,mode="permissive",columnNameOfCorruptRecord="corrupted_data")
df_raw_custom_malformed.show(20)
df_raw_custom_malformed.printSchema()
df_raw_custom_malformed.cache()
df_raw_custom_malformed.where("corrupted_data is not null").show()
df_raw_custom_malformed.where("corrupted_data is not null").show(10,False)#culprit data

#Reject Strategy to store this data in some audit table or audit files and send it to the source system or we do analysis/reprocessing of these data
df_raw_custom_malformed.where("corrupted_data is not null").select("corrupted_data").write.csv("file:///home/hduser/cust_corrupted",mode="overwrite")

#Identify Nulls, constraints, Duplicates, Datatypes, formats etc.,
#Null handling on the key column (null in any, all, subset, constraint)
df_raw_custom_permissive=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct_corrupted,mode="permissive",columnNameOfCorruptRecord="corrupted_data")

#DSL builtin functions
dropped_col_df=df_raw_custom_permissive.drop("corrupted_data")#drop on the df to drop a column
#We are using "na" which is a pure null handling function
null_any_cols=dropped_col_df.na.drop(how="any")#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all")#all columns in the given DF has null will be dropped

def drop_any(df,h,ss):
    return df.na.drop(how=h,subset=ss)

null_any_cols=dropped_col_df.na.drop(how="any",subset=["cid","prof"])#any one of the column in the given DF has null will be dropped
null_all_cols=dropped_col_df.na.drop(how="all",subset=["cid","prof"])#all columns in the given DF has null will be dropped
print(null_any_cols.count())
print(null_all_cols.count())

#Aspirant's Equivalent SQL:

#DSL
df_raw_custom_permissive.where("cid is null").count()#(5 count of column missing and unfit data) permissive converted string to null and customer ids are coming as null literally

df_raw_custom_inferschema=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
#DSL
df_raw_custom_inferschema.where("cid is null").count()#(3 count of only column missing data) how many customer ids are coming as null literally or customer id column is missing (not applicable for csv)

#Null Constraints (Rule I want to putforth on the given column which should not have nulls)
#Sanjay's Question?
#Proactive Interview Question? Also we provided Nullable as False. That will also create an issue right, why spark is not failing? This is open limitation in spark, and we have a workaround for it
#Nullable False represents - if we get null in this column fail our program from further processing
#Workaround - Nullable can't be directly applied on DF, But it can be applied on RDD, and the RDD can be converted back to DF.
#https://issues.apache.org/jira/browse/SPARK-10848
customstruct=StructType([StructField("cid",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df_raw_custom=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=customstruct,mode="permissive")
#df_raw_custom.show(20)
df_raw_custom.printSchema()
df_raw_custom.count()#10005
#One scope reduction of spark is the not null constraint of nullable false cannot be applied at the time of DF definition, we need to have a workaround
#
df_raw_custom_constrained=df_raw_custom.rdd.toDF(customstruct)
df_raw_custom_constrained=spark.createDataFrame(df_raw_custom.rdd,customstruct)
df_raw_custom_constrained.printSchema()
#Not null constraint is applied now --- ValueError: field cid: This field is not nullable, but got None

#Duplicates - Identification of duplicate records/column values
#Record level duplicates
de_duplicated_rows_df=df_raw_custom.distinct()#DSL/Dataframe functions
#or
df_raw_custom.createOrReplaceTempView("view1")
de_duplicated_rows_df=spark.sql("select distinct * from view1")#SQL (Familiar language)
#or
spark.sql("select cid,fname,lname,age,prof,count(1) from view1 group by cid,fname,lname,age,prof having count(1)>1").show()

#Colum level duplicates
de_duplicated_columns_df=df_raw_custom.dropDuplicates(subset=["cid"])#retain only first record of the duplicates
de_duplicated_columns_df.show()
de_duplicated_columns_df.count()

#If I want to retain only young aged customer
df_raw_custom.sort("age").where("cid=4000003").dropDuplicates(subset=["cid"]).show()

#If I want to retain only old aged customer
df_raw_custom.sort("age",ascending=False).where("cid=4000003").dropDuplicates(subset=["cid"]).show()

#Interview Question: Difference between distinct and dropduplicates in pyspark DSL
#Answer - distinct can be applied on the entire row (all fields), but dropduplicates is for applying in columns level

#Aspirant's Equivalent SQL:


#Datatypes, formats check on the dataframe
df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_raw.where("rlike(cid,'[a-z|A-z]')=True").show()#DSL Equivalent function
df_raw.createOrReplaceTempView("view1")

#rlike, regexp_replace, upper()<>lower()
spark.sql("""select * from view1 where cast(regexp_replace(cid,"[a-z|A-Z]","0") as int)=0""").show()#SQL
spark.sql("""select * from view1 where age<>regexp_replace(age,"[-,~.#!]",'')""").show()#SQL
#handling wrong format (age=7~7) and wrong data type (cid=ten) using regular expression functions
spark.sql("""select cast(regexp_replace(cid,"[a-z|A-Z]","0") as int) cid,fname,lname,cast(regexp_replace(age,"[-,~.#!]",'') as int)  age,prof from view1 """).show()

#Aspirant's Equivalent SQL:


#Summarize (statistics) the overall trend of data
df_raw.describe().show()
df_raw.summary().show()

#Aspirant's Equivalent SQL: df_raw.describe().show()

#Completed (Passive) a. Data Discovery (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.

print("Active - b. Combining Data + Schema Evolution/Merging (Structuring)")
'''
[hduser@localhost sparkdata]$ cp -R custdata custdata1
[hduser@localhost sparkdata]$ mkdir custdata/dir1
[hduser@localhost sparkdata]$ mkdir custdata/dir2
[hduser@localhost sparkdata]$ cd custdata
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified4
[hduser@localhost custdata]$ cp custsmodified1 dir1/custsmodified5
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified6
[hduser@localhost custdata]$ cp custsmodified1 dir2/custsmodified7
'''

print("b.1. Combining Data - Reading from a path contains multiple pattern of files")
#Thumb rule/Caveat is to have the data in the expected structure with same number and order of columns
df_raw=spark.read.csv(path="file:///home/hduser/sparkdata/custdata/",inferSchema=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
df_raw=spark.read.csv(path=["file:///home/hduser/sparkdata/custdata/","file:///home/hduser/sparkdata/custdata1/"],inferSchema=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.2. Combining Data - Reading from a multiple sub paths contains multiple pattern of files")
df_raw=spark.read.csv(["file:///home/hduser/sparkdata/custdata/","file:///home/hduser/sparkdata/custdata1/"],inferSchema=True,recursiveFileLookup=True,pathGlobFilter="custsmodified[1-9]",header=True)

print("b.3. Schema Merging (Structuring) leads to Schema Evolution - Schema Merging data with different structures (we know the structure of both datasets)")
#When I want to extract data in a proper structure with the right column mapping
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)#We are not sure about the varying structure, hence going with inferSchema
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

#Let's try with union
from pyspark.sql.functions import *
#union works only if we have same structure of data with same number of columns and columns will be taken in positional notation
df_raw1.union(df_raw2).show()#This union will not work since raw1 contains 5 columns and raw2 contains 6 columns

#We need manual intervention to handle this schema merging
df_union1=df_raw1.select("*",lit("null").alias("city")).union(df_raw2)
df_unionall_by_aspirants=df_union1.union(df_raw3.select("custid","fname","lname",lit(0).alias("age"),"prof","city"))
df_unionall_by_aspirants.show()

#special spark DSL function unionByName with allowMissingColumns feature
df_unionall_byspark=df_raw1.unionByName(df_raw2,allowMissingColumns=True).unionByName(df_raw3,allowMissingColumns=True)

#Aspirant's Equivalent SQL: Need the data in this format df_unionall_byspark or df_unionall_by_aspirants


print("b.3. Schema Evolution (Structuring) - source data is evolving with different structure")
df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified1",inferSchema=True,header=True)#We are not sure about the varying structure, hence going with inferSchema
df_raw2=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified2",inferSchema=True,header=True)
df_raw3=spark.read.csv("file:///home/hduser/sparkdata/custdata/custsmodified3",inferSchema=True,header=True)

df_raw3_modified=df_raw3.withColumn("custid",col("custid").cast("string"))#DSL to convert custid into string type
df_raw3_modified.printSchema()
#sql to conver custid into string type
df_raw3.createOrReplaceTempView("view1")
df_raw3_modifiedsql=spark.sql("select cast(custid as string) custid,lname,fname,prof,city from view1")
df_raw3_modifiedsql.printSchema()

#Schema merging can leads to schema Evolution also
df_raw1.write.orc("file:///home/hduser/df_raworc1")
df_raw2.write.orc("file:///home/hduser/df_raworc2")
df_raw3_modified.write.orc("file:///home/hduser/df_raworc3",mode="overwrite")

df_mergeSchema_byspark=spark.read.orc(path=["file:///home/hduser/df_raworc1","file:///home/hduser/df_raworc2","file:///home/hduser/df_raworc3"],mergeSchema=True)
#One challenge in the above scenario is the columns with same name should be of same type
df_mergeSchema_byspark.show(100)#merged and evolved schema dataframe

print("c.1. Data Preparation/Preprocessing - Validation (active)- DeDuplication, Prioritization - distinct, dropDuplicates, windowing (rank/denserank/rownumber), groupby having")

df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True).toDF("cid","fname","lname","age","prof")
df_row_dedup=df_raw.distinct()#row level
df_column_dedup=df_row_dedup.dropDuplicates(["cid"])#columns level (custid occuring first will be retained, rest of the same custid will be dropped)
df_column_priority_dedup_sorted_c_1=df_raw.sort("age",ascending=False).dropDuplicates(["cid"]).sort("cid")#drop the young aged customers and retain old age customer
#where("cid =4000003").
#important and we learn more in detail about this later
#Window-> row_number(over(partitionBy().orderBy()))
from pyspark.sql.window import Window
df_column_priority_dedup=df_row_dedup.where("cid =4000003").\
    select("*",row_number().over(Window.partitionBy("cid").orderBy(col("age").desc()))
    .alias("rownum")).where("rownum=1").\
    drop("rownum")#drop the young aged customers and retain old age customer

df_column_priority_dedup.show()

#Aspirant's Equivalent SQL: try mixing sql and dsl
#df_row_dedup-> tempview->select * from (select row_number(over(partition by cid order by age desc)) as rownum from view) temp where...


'''
+-------+---------+---------+---+--------+
|    cid|    fname|    lname|age|    prof| rownum
+-------+---------+---------+---+--------+
------------------------------------------------
|4000003|vaishnavi|santharam| 30|      IT| 1
|4000003|   Sherri|   Melton| 34|Reporter| 2
|4000003|  mohamed|    irfan| 41|      IT| 3
------------------------------------------------
|4000004|  m|    aaa| 21|      IT| 1
|4000004|  d|    bbb| 22|      IT| 2
------------------------------------------------
+-------+---------+---------+---+--------+
'''

#Very very importand interview question
#how to identify/delete dup data from a table
#how to identify nth max/min sal/sales/age from the data

print("c.2. Active - Data Preparation/Preprocessing/Validation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
#We are going to use na function to achieve both cleansing and scrubbing
#Cleansing - uncleaned vessel will be thrown (na.drop is used for cleansing)
#df_raw=spark.read.csv("file:///home/hduser/sparkdata/custsmodified_na",inferSchema=True,header=True).toDF("cid","lname","fname","age","prof")
df_column_priority_dedup_sorted_c_1.na.drop()#drop is used to remove the rows with null column(s) values (default how=any)

df_cleansed_c_2=df_column_priority_dedup_sorted_c_1.na.drop(how="any")#Default is any (any column in the given row contains null will be removed
df_all_col_null=df_column_priority_dedup_sorted_c_1.na.drop(how="all")#all (all column in the given row contains null will be removed)

df_any_col_null=df_raw.na.drop(how="any",subset=["fname","prof"])#any one column fname and prof has null will be dropped - for learning we are using df_raw
df_all_col_null=df_raw.na.drop(how="all",subset=["fname","prof"])#both fname and prof has null will be dropped - for learning we are using df_raw

df_thresh_col_null=df_raw.na.drop(thresh=2)#Thresh is used to control the threshold of how many columns with not null I am expecting in the dataframe
df_thresh_col_null=df_raw.na.drop(thresh=2,subset=["cid","fname","prof"])#Thresh is used to control the threshold of how many columns with not null I am expecting in the dataframe

#Aspirant's Equivalent SQL:


#Scrubbing (repairing) - repair and reusable vessel (na.fill and na.replace is used for scrubbing or filling gaps)
df_scrubbed_learning=df_raw.na.fill("NA",subset=["fname","lname","prof"])#Fill will help us fill the null values with some values, rather than dropping null, fill it
df_scrubbed_learning=df_raw.na.fill("-1",subset=["cid","age"])

prof_dict={"Therapist":"Physio Therapist","Doctor":"Physician"}#SQL Equivalent case statement
df_scrubbed_learning=df_raw.na.replace(prof_dict,subset=["prof"])#Replace the dictory pattern in the given column of the dataset


df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.fill("NA",subset=["fname","lname","prof"]).na.fill("-1",subset=["cid","age"])

prof_dict={"Therapist":"Physio Therapist","Doctor":"Physician"}#SQL Equivalent case statement
df_dedup_cleansed_scrubbed_c_2=df_cleansed_c_2.na.replace(prof_dict,subset=["prof"])#Replace the dictory pattern in the given column of the dataset

#EDA - Finding any cid is having string value in it using rlike function
df_dedup_cleansed_scrubbed_c_2.where("rlike(cid,'[a-z|A-z]')=True").show()

#Replace the string with some number -1 or age correction respectively on cid, age column
df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("cid",regexp_replace("cid","[a-z|A-Z]","-2"))
df_dedup_cleansed_scrubbed_c_2=df_dedup_cleansed_scrubbed_c_2.withColumn("age",regexp_replace("age","[~|-]",""))
df_dedup_cleansed_scrubbed_c_2.where("rlike(cid,'[a-z|A-z]')=True").show()#no records returned with string data in cid column
df_dedup_cleansed_scrubbed_c_2.where("rlike(age,'[~-$#@]')=True").show()#no records returned with string data in age column


#Aspirant's Equivalent SQL:
#sql("select coalesce(cid,-1) from view1").show()

spark.conf.set("spark.sql.shuffle.partitions","10")

print("d.1. Data/Attributes(name/type) Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement/Renaming)  to make it in a understandable/usable format")
#Apply few column functions to do column - reordering, rename, removal, replacement, add, changing type of the columns
#To make this data more meaningful we have to add the sourcesystem from where we received the data from - withColumn
#To make this data more meaningful we have to rename the columns like cid as custid, prof as profession - withColumnRenamed/alias
#To make this data more meaningful we have to replace the columns like fname with lname from the dataframe - withColumn
#To make this data more meaningful we have to remove the columns like lname from the dataframe - drop
#To make this data more meaningful we have to typecast like custid as int and age as int - cast
#To make this data more meaningful we have to reorder the columns like custid,fname,profession,age - select/withColumn

#How to use important DSL column management functions like -
# withColumn("new_colname",Column(lit/col/functionoutput())) - add/replace/reorder
# withColumnRenamed("existing_column","new_column_name")
# drop
from pyspark.sql.functions import *
#Standardization 1 - Ensure to add source system for all our data
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_c_2.withColumn("sourcesystem",lit("Retail Stores"))

#Standardization 2 - Ensure to rename the columns with the abbrevation extended
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumnRenamed("cid","custid").withColumnRenamed("prof","profession")

#Doing some analysis on the dataframe to check whether we have 3 names in fullname
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("fullname",concat("fname",lit(" "),"lname"))
df_dedup_cleansed_scrubbed_standardized_d_1.where(size(split("fullname"," "))>2).show()

#Remove columns - removing post analysis columns or some source columns that is not needed
#Standardization 3 - Ensure to remove the columns that we found unwanted to propogate further
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.drop("fullname")

#replace profession with upper(profession)
#Standardization 4 - Ensure to have the descriptive columns like profession, category, city, state of upper case
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("profession",upper("profession"))

#Typecasting - Changing the type of the column appropriately - cid int, age int
#Standardization 5 - Ensure to redefine the dataframe with the appropriate datatypes
df_dedup_cleansed_scrubbed_standardized_d_1.printSchema()
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("custid",col("custid").cast("int")).\
withColumn("age",col("age").cast("int"))

#Reordering of the column custid,fname,profession,age
df_dedup_cleansed_scrubbed_standardized_d_1=df_dedup_cleansed_scrubbed_standardized_d_1.select("custid","fname","lname","profession","age","sourcesystem")

#Aspirant's SQL Equivalent

print("********************1. data munging completed****************")


'''2. Starting - Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration
'''

print("*************** Data Enrichment (values)-> Add, Rename, merge(Concat), Split, Casting of Fields, Reformat, "
      "replacement of (values in the columns) - Makes your data rich and detailed *********************")

#Add Columns - for this pipeline, I needed the load date and the timestamp
df_munged_enriched1=df_dedup_cleansed_scrubbed_standardized_d_1.withColumn("loaddt",lit("2024-05-23")).withColumn("loadts",current_timestamp())

#Rename Columns
df_munged_enriched2=df_munged_enriched1.withColumnRenamed("fname","firstname").withColumnRenamed("lname","lastname")

#Swapping Columns
#By renaming and reordering
df_munged_enriched4=df_munged_enriched2.select("custid",col("lastname").alias("firstname"),col("firstname").alias("firstnametmp"),"profession","age","sourcesystem","loaddt","loadts").withColumnRenamed("firstnametmp","lastname")

#By renaming and reordering
df_munged_enriched3=df_munged_enriched2.withColumnRenamed("lastname","firstname1").withColumnRenamed("firstname","lastname").withColumnRenamed("firstname1","firstname")
df_munged_enriched4=df_munged_enriched3.select("custid","firstname","lastname","profession","age","sourcesystem","loaddt","loadts")

#By introducing temp column and dropping it
df_munged_enriched3=df_munged_enriched2.withColumn("lastnametmp",col("lastname")).drop("lastname").withColumnRenamed("firstname","lastname").withColumnRenamed("lastnametmp","firstname")
df_munged_enriched4=df_munged_enriched3.select("custid","firstname","lastname","profession","age","sourcesystem","loaddt","loadts")

#Replace Columns (just for learning)
learn_df=df_munged_enriched2.withColumn("firstname",col("lastname"))#data in fname will be replaced with lname


#b. merge/Concat, split
#merge
df_munged_enriched5=df_munged_enriched4.withColumn("mailid",concat("firstname",lit("."),"lastname",lit("."),"custid",lit("@inceptez.com")))

#Split
df_munged_enriched6=df_munged_enriched5.withColumn("splitcol",split("mailid","@")).withColumn("userid",col("splitcol")[0]).withColumn("domain",col("splitcol")[1])
#try do the above with select

#Remove Columns
df_munged_enriched7=df_munged_enriched6.drop("splitcol")

#c. Type Casting, format & Schema Migration
#Type casting
#We need to change the loaddt into date format
df_munged_enriched8=df_munged_enriched7.select("*",col("loaddt").cast("date").alias("loaddt"))#usage of select with all columns typed is not good here since we require dropping of loaddt1 and reordering
#or
df_munged_enriched8=df_munged_enriched7.withColumn("loaddt",col("loaddt").cast("date"))#best option to choose with huge efforts

#Reformatting & Extraction
#Extraction
df_munged_enriched9=df_munged_enriched8.withColumn("year",year(col("loaddt")))

#Reformatting
df_munged_enriched_final=df_munged_enriched9.withColumn("loaddt",date_format(col("loaddt"),'yyyy/MM/dd')).withColumn("loadts",date_format(col("loadts"),'yyyy/MM/dd hh:mm'))

#Aspirant's SQL Equivalent

print("***************3. Data Customization & Processing (Business logics) -> "
      "Apply User defined functions and utils/FBP/modularization/reusable functions & reusable framework creation *********************")
print("Data Customization can be achived by using UDFs - User Defined Functions converted/registered from a Python Function")
print("User Defined Functions must be used only if it is Inevitable (un avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions.")

#3.a. How to create Python Functions & convert/Register it as a UDF to call these functions in the DSL/SQL Queries respectively
spark.conf.set("spark.sql.shuffle.partitions","5")
#I want to apply customization on the columns of our dataframe/temporary view using some custom functionalities
#Steps We are going to Follow
#Step 1. First evaluate whether the functionality we are going to apply is already available in a form of individual/composable built in functions
#Decide whether to use custom or builtin to customize & enrich the df_munged_enriched_final further

#Step 2. Create a python anonymous/defined (inline/common) functions - If we don't find builtin functions to achive the custom functionality defined by the business then
#Lets assume we don't have builtin function available, hence we are going with custom function
'''
def agecat(age):
    if age<=12:
        return "Childrens"
    else:
        return "Adults"
'''

#IF WE ARE APPLYING UDF ON DATAFRAME USING DSL
#Step 3. Convert the python function to UDF (because python function is not serializable to use in the executor) - If we are going to apply this function in a DSL query
from pyspark.sql.functions import udf
from learn.reusableframework.reusable_functions import agecat
udf_agecat=udf(agecat)#converting python function as udf

#Step 4. Apply the udf on the dataframe columns using DSL
df_munged_enriched_customized=df_munged_enriched_final.withColumn("agecategory",udf_agecat(coalesce("age",lit(0))))

#OR

#IF WE ARE APPLYING UDF ON TEMPORARY VIEW USING SQL
#Step 3. Register the python function as UDF into the Metastore (because python function is not serializable to use in the executor & identifyable in the metastore by Spark SQ:) - If we are going to apply this function in a SQL query
from learn.reusableframework.reusable_functions import agecat
#udf_agecat=udf(agecat)
spark.udf.register("udf_sql_agecat",agecat)#registering python function as udf

#Step 4. Apply the udf on the Tempview columns using SQL
df_munged_enriched_final.createOrReplaceTempView("mun_enr_view")
df_munged_enriched_customized=spark.sql("""select *,udf_sql_agecat(coalesce(age,0)) agecategory 
                                           from mun_enr_view""")

#3.b. Go for UDFs if it is in evitable – Because the usage of UDFs will degrade the performance of your pipeline

#Example of udf using anonymous function & performance optimization
filter_function=lambda x:True if x=='PILOT' else False
spark.udf.register("df_ready_udf_filter_function",filter_function)

df_filtered_pilot=df_munged_enriched_final.where("""df_ready_udf_filter_function(profession)=True""")
#Not optimistic to use because? Catalyst optimized is not used, data requires deserialization & serialization back.
#or
df_filtered_pilot=df_munged_enriched_final.where("""profession='PILOT'""")
#Optimistic to use because? Catalyst optimized is used, built in functions can be applied on the serialized data itself without deserializing.

#3.c. Creation of the Reusable Frameworks with Functions (Later - Modernization/Industrialization)
#We do this in Item 6

print("***************4. Core Curation - Core Data Processing/Transformation (Level1) (Pre Wrangling)  -> "
      "filter, transformation, Grouping, Aggregation/Summarization, Sorting, Analysis/Analytics (EDA) *********************")
#df_munged_enriched_customized - This DF has age with null and profession with blankspaces or nulls, and agecategory with adults & childrens

#transformation (na.fill, coalesce,cast, substr, to_date, concat, trim, isNull, lit, when(cond,value).when(cond2,value2).otherwise(default)), selectExpr(native sql query on the given row/columns))

df_munged_enriched_customized_transformed1=df_munged_enriched_customized.na.fill(0,["age"])#Age is transformed from null to 0

#Nulls are changed to na using coalesce
df_munged_enriched_customized_transformed2=df_munged_enriched_customized_transformed1.withColumn("profession",coalesce("profession",lit('na')))#Coalesce will return the first not null expression

#blank spaces are changed to na using case statement
#syntax of case statement ->
# SQL- (case when profession is null then 'na' when profession ='' then 'na' else profession end as profession)
# DSL- (df.withColumn("profession",when ("profession"=='','na').when ("profession"=='','na').when ("profession"=='','na').otherwise("profession")))
#DSL+SQL - selectExpr(case when profession is null then 'na' when profession='' then 'na' else profession)
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').otherwise(col("profession")))
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').when(coalesce("profession",lit(''))=='','na').otherwise(col("profession")))
df_munged_enriched_customized_transformed3.show()
#or
df_munged_enriched_customized_transformed3=df_munged_enriched_customized_transformed2.withColumn("profession",when (trim("profession")=='','na').when(col("profession").isNull(),'na').otherwise(col("profession"))).show()

#selectExpr function
df_munged_enriched_customized_transformed1.selectExpr("custid","firstname","lastname","case when profession is null then 'na' when profession='' then 'na' else profession end as profession").show()

#We need to handle nulls in age, blankspaces/nulls in profession and we want to introduce one more agecategory called teens
####################################################################################################################################################
#for just learning and understanding the speciality of coalesce/nvl/nvl2/nullif/decode/case...
df_munged_enriched_customized_transformed2_learning=df_munged_enriched_customized_transformed1.where("custid=4000002").withColumn("profession",lit(None))#creating sample data with profession as null
df_munged_enriched_customized_transformed3_learning=df_munged_enriched_customized_transformed1.where("custid=4000003").withColumn("profession",lit(''))#creating sample data with profession as ''
union_df=df_munged_enriched_customized_transformed2_learning.union(df_munged_enriched_customized_transformed3_learning)
#Standardizing & Filtering (all the nulls and blankspaces data has to be filtered)
union_df.where("coalesce(profession,'')=''").show()#coalesce is converting the nulls to '' to standardize the data, then we are filtering the ''
#or
union_df.where("profession is null or profession=''").show()
####################################################################################################################################################

#.withColumn("profession",coalesce("profession",lit('')))
#coalesce("profession",'')

#filter
df_munged_enriched_customized_filter_social=df_munged_enriched_customized_transformed3.where("""profession in ('DOCTOR','LAWYER','ECONOMIST','POLICE OFFICER')""")
df_munged_enriched_customized_filter_adults=df_munged_enriched_customized_transformed3.filter("""agecategory ='Adults' or age between 13 and 19""")

#case statement using selectExpr and then using DSL with().otherwise to modify the agecategory as childrens, teens and adults
#age with null as unknown (use coalesce or case), age <13 - childrens, age>=13 and age<=19 - teens , age>19 - adults df_munged_enriched_customized_transformed3

#transformation (after filter)

#Grouping, Aggregation/Summarization
#One column grouping & aggregation
df_aggr_count_prof_social=df_munged_enriched_customized_filter_social.groupby("profession").agg(count(lit(1)).alias("cnt"))
df_aggr_count_prof_social.show()

#SQL Equivalent
df_munged_enriched_customized_filter_social.createOrReplaceTempView("socialview")
spark.sql("select profession,count(1) from socialview group by profession").show()

#multiple columns grouping & 1 column aggregation
df_aggr_count_prof_age=df_munged_enriched_customized.groupby("profession","age").agg(count(lit(1)).alias("cnt"))

#multiple columns grouping & multiple column aggregation
df_multiple_group_aggr=df_munged_enriched_customized_filter_social.groupby("profession","year","agecategory").agg(count(lit(1)).alias("cnt"),avg("age").alias("avg_age"),mean("age").alias("mean_age"),max("age").alias("max_age"),min("age").alias("min_age"))

#Grouping, Aggregation/Summarization, Sorting
df_multiple_group_aggr_sort_final=df_multiple_group_aggr.orderBy(["cnt","avg_age"])

#Enrichment (Derivation of KPIs (Key Performance Indicators))
df_munged_enriched_customized_transformed_kpi=df_munged_enriched_customized_transformed3.withColumn("agecatind",substring("agecategory",1,1))

#Format Modeling - convert the loaddt from yyyy/MM/dd to yyyy-MM-dd
df_munged_enriched_customized_transformed_kpi=df_munged_enriched_customized_transformed3.withColumn("loaddt",concat(substring("loaddt",1,4),lit('-'),substring("loaddt",6,2),lit('-'),substring("loaddt",9,2)).cast("date"))
#or
#to_date is an important function that will help us convert the date from any format to the standard format of yyyy-MM-dd
#date_format is the reverse of to_date function (help us convert the date from yyyy-MM-dd to any format as per our need)
df_munged_enriched_customized_transformed_kpi_format_final=df_munged_enriched_customized_transformed_kpi.withColumn("loaddt",to_date("loaddt",'yyyy/MM/dd'))

#Aspirant's SQL Query


#Analysis/Analytics (EDA) (On top of Enriched, Customized & level1 Curated/Pre Wrangled data)
#Identifying corelation, most frequent occurance,
#Random Sampling
df_sample1=df_munged_enriched_customized.sample(.1)
df_sample2_1=df_munged_enriched_customized.sample(.2,1)
df_sample2_2=df_munged_enriched_customized.sample(.2,2)

#Frequency Analysis
df_freq1=df_munged_enriched_customized.freqItems(["age"],.1)
df_freq2=df_munged_enriched_customized.freqItems(["age"],.2)

#Corelation Analysis (Know the relation between 2 attributes)
df_munged_enriched_customized.withColumn("age2",col("age")).stat.corr("age","age2")#pearson corelation algorithm
df_munged_enriched_customized.stat.corr("custid","age")#least corelation

#Covariance Analysis
df_munged_enriched_customized.stat.cov("age","age")#Random corelation of data will happen

#Statistical Analysis
df_stats_summary=df_munged_enriched_customized.summary()

#Trend analysis, Market Basket Analysis, Feature selection, identifying relation between the fields, random sampling, statistical analysis, variance of related attributes...


print("*************** 5. Data Wrangling - Complete Data Curation/Processing/Transformation (Level2)  -> "
      "Joins, Lookup, Lookup & Enrichment, Denormalization, Windowing, Analytical, set operations "
      "Summarization (joined/lookup/enriched/denormalized) *********************")

#We are going to introduce new dataset and it is going to undergo all possible stages of our overall data transformation
#Munging -> Enrichment -> Customization -> Curation (Pre Wrangling) -> Wrangling
df_txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","custid","amount","category","product","city","state","spendby")
#The above data doesn't require Customization, but it requires all other stages...
#Munging
#We identified column name is not there, txnno is string type, nulls in the txnno
print(len(df_txns_raw.collect()))#Actual count
#or
print(df_txns_raw.count())
#95904
df_txns_raw.printSchema()
#
df_txns_raw.na.drop("any",subset=["txnno"]).count()#nulls are there in txnno
#95903
df_txns_raw.na.drop("any",subset=["txnno"]).where("upper(txnno)=lower(txnno)").count()#Nulls and string is there in txnno
#95902
df_txns_munged=df_txns_raw.na.drop("any",subset=["txnno"]).where("upper(txnno)=lower(txnno)")
#95902

#Enrichment (Format modeling and enrichment)
df_txns_munged_enriched1=df_txns_munged.withColumn("txnno",col("txnno").cast("int"))

#we need to do type casting of txndt to date format, but not possible directly because date in format of MM-dd-yyyy
#df_txns_munged_enriched.withColumn("txndt",col("txndt").cast("date")).show(2)#this will not work
#to_date function will change the date from unknown format (anything eg. MM-dd-yyyy or MM/dd/yy) to known format of 'yyyy-MM-dd'
df_txns_munged_enriched2=df_txns_munged_enriched1.withColumn("txndt",to_date("txndt",'MM-dd-yyyy'))#converts to MM-dd-yyyy -> yyyy-MM-dd
df_txns_munged_enriched2.show(2)
df_txns_munged_enriched2.printSchema()

#Let's Try to add more derived/enriched fields using some date functions
df_txns_munged_enriched3=df_txns_munged_enriched2.select("*",year("txndt").alias("year"),
                                                         month("txndt").alias("month")
                                                         ,add_months("txndt",1).alias("next_month_sameday"),
                                                         add_months("txndt",-1).alias("prev_month_sameday"),
                                                         last_day("txndt").alias("last_day_ofthe_month"),
                                                         date_add(last_day("txndt"),1).alias("first_day_ofthe_next_month"),
                                                        date_sub("txndt",2).alias("two_days_subtracted_dt"),
                                                         trunc("txndt","mm").alias("first_day_ofthe_month"),
                                                         trunc("txndt","yyyy").alias("first_day_ofthe_year"))


#Curation
#Grouping & Aggregation
df_txns_curated_agg=df_txns_munged_enriched2.groupBy("state").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg.show()

#Wrangling
#Pivoting with Grouping & Aggregation
df_txns_curated_agg=df_txns_munged_enriched2.groupBy("state","spendby").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg.show()
df_txns_curated_agg.show(100)
'''
+--------------------+------------------+-------+-------+
|               state|           sum_amt|max_amt|min_amt|
+--------------------+------------------+-------+-------+
|           Wisconsin|223194.54999999996| 199.97|   5.15|
|                Ohio|359566.70000000007| 199.94|   5.07|
|             Florida| 532213.0099999999| 199.98|   5.13|
|                Utah|168689.39999999985| 199.89|   5.11|
|        Pennsylvania|178550.70000000016| 199.89|   5.01|
|              credit|             28.11|  28.11|  28.11|
|               Idaho| 87323.56999999998| 199.99|   6.06|
'''

#Relate with these 2 dataframes to understand how pivot is working
df_txns_curated_agg_pivoted=df_txns_munged_enriched2.groupBy("state").pivot("spendby").agg(sum("amount").alias("sum_amt"),max("amount").alias("max_amt"),min("amount").alias("min_amt"))
df_txns_curated_agg_pivoted.show()
'''
+--------------------+------------------+------------+------------+------------------+--------------+--------------+
|               state|      cash_sum_amt|cash_max_amt|cash_min_amt|    credit_sum_amt|credit_max_amt|credit_min_amt|
+--------------------+------------------+------------+------------+------------------+--------------+--------------+
|                Utah| 6137.310000000001|       49.77|        5.11|162552.08999999985|        199.89|          5.49|
|        Pennsylvania| 6345.200000000001|       49.55|        5.01|172205.50000000006|        199.89|          5.47|
|             Florida|19669.319999999996|       49.98|        5.19|         512543.69|        199.98|          5.13|
|               Idaho|2889.6399999999994|       49.96|        6.25| 84433.93000000001|        199.99|          6.06|
|           Wisconsin| 7641.409999999998|       49.75|        5.15|215553.13999999996|        199.97|          5.19|
|      North Carolina|           5765.98|        49.6|        5.27| 166431.2299999999|        199.64|          5.15|
|             Indiana| 3755.910000000001|       49.13|        8.38| 84482.05000000002|        199.81|          5.14|
|                Ohio|          13624.91|       49.99|        5.07|         345941.79|        199.94|          5.24|
'''

#Stopping my Transaction data processing here....

#Let's UNDERSTAND & LEARN about different types of joins
#Joins - What is join? Join is the operation of horizontaly connecting the related or un related dataset appling matching
# or un matching conditions or without conditions to get the integrated/expand/denormalized view of the multiple datasets
#Join is used for integrating multiple datasets horizontally
#Important Interview Questions for joins?
#1. What are the types of SQL joins you know/used?
#2. In the below Quick scenario, try to find the respective join produces howmuch result?
#Scenario: I have 2 datasets, datset1 contains 10 rows, dataset2 contains 15 rows, matching rows between these dataset is 8 rows,
#dataset1 has 2 rows not matching with dataset2 and dataset2 has 7 rows not matching with dataset1

df_left=df_munged_enriched_customized.where("custid in (4000001,4000002,4000003)").select("custid","firstname","lastname","age","profession")
df_right=df_munged_enriched_customized.where("custid in (4000003,4000011,4000012,4000013)").select("custid","firstname","lastname","age","profession")

#How to write join syntax in DSL
#Simple Syntax
df_left.join(df_right).show()#cross join/cartesian join/product join (not good to use)
df_left.join(df_right,on="custid",how="inner").show()#Simple syntax with the usage of all parameters for join
#'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'
# In the above simple syntax we have few challenges to resolve, Above simple syntax will work only (if there are common columns with the same name used for join, if there is no other data columns between dataframes with same name)
df_joined=df_left.join(df_right,on="custid",how="inner")
df_joined.select("custid","age").show()#This will not work due to ambiguity issue
#pyspark.sql.utils.AnalysisException: Reference 'age' is ambiguous, could be: mun_enr_view.age, mun_enr_view.age.

#Comprehensive Syntax
df_right=df_right.withColumnRenamed("custid","cid")
df_joined=df_left.join(df_right,on=([col("custid")==col("cid")]),how="inner")#If I don't have common column names for joining
#If I don't have common column names for joining & If we have common columns between 2 dataframes
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")#Comprehensive syntax to use
df_joined.select("l.custid","l.age","r.age").show()

#Wanted to apply multiple conditions
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([(col("l.custid")==col("r.cid")) & (col("l.firstname")==col("r.firstname"))]),how="inner")#Comprehensive syntax to use

#NOW LET'S LEARN THE DIFFERENT TYPES OF JOINS BEHAVIOR
'''df_left.show()
+-------+---------+--------+----+----------+
| custid|firstname|lastname| age|profession|
+-------+---------+--------+----+----------+
|4000001|    Chung|Kristina|  55|     PILOT|
|4000002|     Chen|   Paige|null|     ACTOR|
|4000003|    irfan| mohamed|  41|        IT|
+-------+---------+--------+----+----------+
>>> df_right.show()
+-------+---------+--------+---+----------+
|    cid|firstname|lastname|age|profession|
+-------+---------+--------+---+----------+
|4000003|    irfan| mohamed| 41|        IT|
|4000011| McNamara| Francis| 47| THERAPIST|
|4000012|   Raynor|   Sandy| 26|    WRITER|
|4000013|     Moon|  Marion| 41| CARPENTER|
+-------+---------+--------+---+----------+
'''
#Inner join - Data from dataset1 joined with dataset2 using some join/filter conditions that returns matching values between 2 datasets
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")
df_joined.select("l.custid","r.cid","l.age","r.age").show()#1 row returned

#Outer Join
#Left join - returns ALL values from dataset1 (left) and nulls for unmatched value of dataset2
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="left_outer")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#3 row returned

#Right join - Returns ALL values from dataset2 (right) and nulls for unmatched value of dataset1
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="right")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#4 row returned

#Full Outer - Feturns ALL values from dataset1 (left) & dataset1 (right) with nulls for unmatched value of both datasets
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="outer")
df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()#6 row returned (1 is common row, left 2, right 3)

#Semi Join - Returns MATCHING values from dataset1 (left) alone (Works like a suquery with in condition)
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="semi")
#df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()
df_joined.select("l.custid",col("l.age").alias("l_age")).show()

#or refer the below sql query with in condition

#or (achieving semi join using inner join (but inner join can produce data from right df also))
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")
df_joined.select("l.custid",col("l.age")).show()

#There is no join like Right semi/anti? Yes we have, but not in spark
#If No right semi/anti is there, then how can i achieve it? just swap the dataframes
df_joined=df_right.alias("l").join(df_left.alias("r"),on=([col("r.custid")==col("l.cid")]),how="semi")
df_joined.select("r.cid",col("r.age").alias("r_age")).show()

#Anti Join - Returns UN-MATCHING values from dataset1 (left) alone (Works like a suquery with not in condition)
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="anti")
#df_joined.select("l.custid","r.cid",col("l.age").alias("l_age"),col("r.age").alias("r_age")).show()
df_joined.select("l.custid",col("l.age").alias("l_age")).show()

#or refer the below sql query with not in condition

#or (achieving anti join using left join (but left join can produce data from right df also))
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="left")
df_joined.where("r.cid is null").select("l.custid",col("l.age")).show()

#Self Join - Data from dataset1 joined with dataset1 using some join conditions that returns matching values between 2 datasets
df_left1=df_left.withColumn("custid_ref",col("custid")-1)#Creating hierarchical data for using in self join
df_joined=df_left1.alias("l").join(df_left1.alias("r"),on=([col("l.custid")==col("r.custid_ref")]),how="inner")
df_joined.where("r.cid is null").select("l.custid",col("l.age")).show()
df_joined.selectExpr("concat(r.custid,' is referred by ',l.custid)").show(10,False)

#Cross Join - Data from dataset1 joined with dataset2 WITHOUT using any join conditions that returns  values between 2 datasets in an iteration of (10*15=150 rows will be returned)
df_joined=df_left.alias("l").join(df_right.alias("r"))


#Aspirant's SQL Query
df_left.createOrReplaceTempView("left_view")
df_right.createOrReplaceTempView("right_view")
spark.sql("select l.*,r.* from left_view l join right_view r on l.custid=r.cid and l.firstname=r.firstname").show()

#Semi join (subquery with exists condition)
spark.sql("select l.* from left_view l where exists (select cid from right_view r where r.cid=l.custid)").show() #exists will check the first occurance of the data
#or
spark.sql("select l.* from left_view l where l.custid in (select cid from right_view r)").show()#in will check all values are present or not
#Interview Question: Difference between in and exists condition
#Exists is more efficient because it will return the boolean true if the first occurance is found rather than the values
#Exists is more efficient because it will return the boolean true/false rather than the values

#Interview Question: How do we achieve the above Subquery result in DSL?
# Using Semi join we can achive

#Interview Question: How to Transpose the data from column to row (opposite of pivot)
#Transpose
spark.sql("select current_date()").withColumn("prod",lit("xyz;abc;aaa;bbb")).select(explode(split("prod",';')).alias("prod")).show()

#Pivot
spark.sql("select current_date()").withColumn("prod",lit("xyz;abc;aaa;bbb")).select(explode(split("prod",';')).alias("prod")).select(collect_set("prod")).show()

#Interview Question: How to pass arguments to DSL & SQL to make it more dynamic
col1="custid"
col2="firstname"
df_left.select(col1,col2).show(1)

df_left.createOrReplaceTempView("left_view")
col1="custid"
col2="firstname"
spark.sql(f"select {col1},{col2} from left_view")

#Anti join (subquery with not exists condition)
spark.sql("select l.* from left_view l where not exists (select cid from right_view r where r.cid=l.custid)").show()
#or
spark.sql("select l.* from left_view l where l.custid not in (select cid from right_view r)").show()


#Further we are going to marry the curated cust data (df_munged_enriched_customized_transformed_kpi_format_final)
# with the curated transaction data (df_txns_munged_enriched2)
#Joins, Lookup, Lookup & Enrichment, Denormalization

#APPLICATION OF DIFFERENT TYPES OF JOINS IN OUR REAL LIFE (DBS, DWH, LAKEHOUSES, BIGLAKES, DATALAKES, DELTALAKES)
#-----------------------------------------------------------------------------------------------------------------
#Joins - To Enrich our Data pipeline or to apply different scenarios of our dataset
df_munged_enriched_customized_transformed_kpi_format_final.show(2)
df_txns_munged_enriched3.show(2)

#Rename the above DFs to use it simply
df_cust_dim_wrangle_ready=df_munged_enriched_customized_transformed_kpi_format_final
df_txns_fact_wrangle_ready=df_txns_munged_enriched3

#Lets assume we are in the month of January 2012
#Doing some EDA to understand howmany customers I have
print(df_cust_dim_wrangle_ready.count())#9910 overall customers that I have in my business
df_txns_fact_wrangle_ready.groupBy("year","month").agg(sum("amount")).show()#12 months worth of sales data of 2011 I have

print("a. Lookup (Joins)")
#lookup is an activity of identifying some existing data with the help of new data
#lookup can be achived using joins (semi join, anti join, Left join, inner join)

print("Identify the count of Active Customers or Dormant Customers")
#take latest 1 month worth of txns data and compare with the customer data
#Lets assume we are in the month of January 2012 - Wanted to do some analysis on the last month sales happened
df_txns_2011_12=df_txns_fact_wrangle_ready.where("year=2011 and month=12")

#Active Customers (customers did transactions in last 1 month)
#Using semi join (high priority) why? Because semi will display only the left table data checking right table data only once using exists function in the background...
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="semi").count() #5475 active customers

#Using Left join (mid priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").where("r.custid is not null").\
    select("custid").distinct().count()#5475 active customers

#Using inner join (least priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").\
    select("custid").distinct().count()#5475 active customers

#Dormant Customers (customers didn't do transactions in last 1 month)
#The customers who did not do any transactions last month are dormant customers
#Using semi join (high priority) why? Because semi will display only the left table data checking right table data only once using exists function in the background...
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="anti").count() #4435 dormant/inactive customers

#Using Left join (mid priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").where("r.custid is null").\
    select("custid").distinct().count()#5475 active customers

#Using Left join (low priority)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").where("r.custid is null").\
    select("custid").distinct().count()#5475 dormant customers

#Aspirant's SQL Equivalent

print("b. Lookup & Enrichment (Joins)")
#lookup is an activity of checking whether the given value is matching and enriching is process of getting/using the lookup data ()
#Transaction data of last 1 month I am considering above
#Returns the details of the transactions did by the given customers
#We are doing a lookup based on custid and enriching amount from the transaction data

#We may left join if we need all customer info with or without enriched amount
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="left").select("custid","profession","amount").show()

#We may inner join if we need only customer info with enriched amount is available
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").select("custid","profession","amount").show()

#We may right join if we need show all transactions regardless whether we have customer info is present or not
df_cust_dim_wrangle_ready=df_cust_dim_wrangle_ready.where("custid between 4002613 and 4004613")
#Orphan customers (transactions happened in (callcenter/web) but no customer info captured in our DL later in my customer table)
df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="right").where("custid is null").select("custid","profession","amount").show()

#Aspirant's SQL Equivalent

print("c. Denormalization - (Schema Modeling) - (Creating Wide Data/Fat Data) - (Star Schema model-Normalized) - "
      "Join Scenario to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business for faster access without doing join")

#Flattening/Widing/Fat table/Denormalization
df_cust_txns_denorm_fact_inner=df_cust_dim_wrangle_ready.join(df_txns_fact_wrangle_ready,on=("custid"))#Inner/Natural using custid
#Above join returns only matching cust data with the transaction
df_cust_txns_denorm_fact_left=df_cust_dim_wrangle_ready.join(df_txns_fact_wrangle_ready,on=("custid"),how="left")#Left join using custid
#Above join returns only all cust data with the applicable transaction, else null transactions

#df_cust_txns_denorm_fact_inner.write.saveAsTable("cust_trans_fact")#in stage 6 we will do this..

#We can use full or right join also if we want to know all cust/all trans data regardles whether it matches or not

#Star Schema Model
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

#benefits (flattened/widened/denormalized data)
#Once for all we do costly join and flatten the data and store it in the persistant storage (hive/athena/bq/synapse/redshift/parquet)
#Not doing join again and again when consumer does analysis, rather just query the flattened joined data
#drawback(flattened/widened/denormalized data)
#1. duplicate data
#2. occupies storage space

print("Equivalent SQL for Denormalization (denormalization helps to create a single view for faster query execution without joining)")

#Aspirant's SQL Equivalent

print("d. Windowing Functionalities")
#clauses/functions - row_number(), rank(), dense_rank() - over(), partition(), orderBy()
#Application of Windowing Functions
#Windowing functions are very very important functions, used for multiple applications
#Creating Surrogate key/sequence number/identifier fields/primary key
#Creating change versions (SCD2) - Seeing this in project
#Identifying & Eliminating Duplicates -
#Top N Analysis

df_txns_curated_agg.show()#This is not having any surrogate/sequence number column
print("aa.How to generate seq or surrogate key column on the ENTIRE data kept in a single partition")
df_txns_curated_agg.withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Multiple partitions with scattered sequence number
df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number

df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).select(col("st_spendby_key").alias("seq_no"),"*").drop("st_spendby_key").show(100)
#or
df_txns_curated_agg.coalesce(1).select(monotonically_increasing_id().alias("seq_no"),"*").show(100)


df_raw1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True,header=True).toDF("cid","fname","lname","age","prof")
df_txns_raw=spark.read.csv("file:///home/hduser/sparkdata/txns",inferSchema=True).toDF("txnno","txndt","custid","amount","category","product","city","state","spendby")
df_right=df_raw1
df_left=df_txns_raw
df_joined=df_left.alias("l").join(df_right.alias("r"),on=([col("l.custid")==col("r.cid")]),how="inner")

print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
df_txns_curated_agg.coalesce(1).orderBy("sum_amt").withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number
df_txns_curated_agg.coalesce(1).withColumn("st_spendby_key",monotonically_increasing_id()).show(100)#Single partition with ordered sequence number

#or By using window function

print("bb. How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
from pyspark.sql.window import Window
df_joined_seqno=df_joined.select(row_number().over(Window.orderBy("txndt")).alias("rno"),"*")
df_joined_seqno.show()

print("Least 3 transactions in our overall transactions")
df_joined_least3=df_joined.select(row_number().over(Window.orderBy("amount")).alias("rno"),"*").where("rno<=3")#One person with one unique rank
df_joined_least3.show()
df_joined_least3_drank=df_joined.select(dense_rank().over(Window.orderBy("amount")).alias("drank"),"*").where("drank<=3")#same value should have equal rank and next value jump to next sequence rank
df_joined_least3_drank.show()
df_joined_least3_rank=df_joined.select(rank().over(Window.orderBy("amount")).alias("rank"),"*").where("rank<=18")##same value should have equal rank and next value jump to next count(previous rank)+1 rank
df_joined_least3_rank.show()

print("Top 3 transactions made by the given customer")
df_joined.where("custid in (4000329,4001198)").orderBy("custid").show(100)
df_joined_cust_top3=df_joined.where("custid in (4000329,4001198)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno<=3")
df_joined_cust_top3.show()

print("Top 1 transaction amount made by the given customer 4000000")
df_joined.where("custid in (4000001,4001198)").groupBy("custid").agg(max("amount")).show()
df_joined_cust_top3=df_joined.where("custid in (4000001,4001198)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=1")
df_joined_cust_top3.show()

print("Top 2nd transaction amount made by the given customer 4000001")
df_joined_cust_top3=df_joined.where("custid in (4000001,4001198)").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=2")
df_joined_cust_top3.show()

print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of games atleast once")
df_joined_cust_top3=df_joined.where("category in ('Games')").select(row_number().over(Window.partitionBy("custid").orderBy(desc("amount"))).alias("rno"),"*").where("rno=2")
df_joined_cust_top3.show()

#or
#Equivalent to writing Subquery in DSL (through join only we can achieve)
df_custid_txnno=df_joined.where("category in ('Games')").groupBy("custid").agg(max("txnno").alias("txnno"))
df_all_cust=df_custid_txnno.join(df_joined,on=(["custid","txnno"]),how='inner')#As we have both DFs have common column names custid,txnno we can just mention like this....
#or
df_all_cust=df_custid_txnno.alias("a").join(df_joined.alias("b"),on=([(col("a.custid")==col("b.custid")),(col("a.txnno")==col("b.txnno"))]),how='inner')

#or
#SQL Equivalent
df_joined.createOrReplaceTempView("df_joined_view")
spark.sql("""select * from df_joined_view where concat(custid,txnno) 
             in (select concat(custid,mtxnno) from 
             (select custid,max(txnno) mtxnno from df_joined_view where category in ('Games') group by custid)t)""").count()


#Aspirant's Equivalent SQL:

print("E. Analytical Functionalities")
#Reporting Side regularly used Analytical functions (pivot, rollup & cube)
#Regularly used analytical functions (cov, corr, sample, max/min/sum/count/avg/mean/mode/variance/stddev/freqitems...)

#Lead and Lag functions are HIERARCHICAL ANALYSIS/COMPARISON FUNCTIONS
#Recently using the below analytical functions in my project (lead & lag)
#Eg. We receive customer events data from different sources (learn eventts 8.00/8.40 -> analyse 8.10/8.10-> book 8.20/8.35-> shipped 8.30/8.30-> receive)

#DATA IMPUTATION/FABRICATION
#Channel of Interaction
#1 learn eventts 8.00/8.40 - arrived 8.10
#2 analyse 8.10/8.10 - 8.10 lag(prev) 8.40 - wrong time - swap 8.40 with 8.10 - arrive 8.40 - swap 8.40 with 8.30
#3 book 8.20/8.35 - 8.35 lag(prev) 8.40 - wrong time - swap 8.40 with 8.35 - arrive 8.40 - arrive 8.30 - swap 8.35 with 8.30 - arrive 8.35
#4 shipped 8.30/8.30 - 8.30 lag(prev) 8.40 - wrong time - swap 8.40 with 8.30 - arrive 8.40

print("What is the purchase pattern of the given customer, whether his buying potential is increased or decreased transaction by transaction")
df_distinct_custid4000001=df_joined.distinct().where("custid in (4000001)")
df_distinct_custid4000001.orderBy("txndt").show()
df_joined_prev_txn_amount=df_distinct_custid4000001.select("*",lag("amount",1,0).over(Window.orderBy("txndt")).alias("prev_amt"))
df_joined_prev_next_txn_amount=df_distinct_custid4000001.select("*",lag("amount",1,0).over(Window.orderBy("txndt")).alias("prev_amt"),lead("amount",1,0).over(Window.orderBy("txndt")).alias("next_amt"))

#Purchase pattern of the customer
#Identify the first and last transactions made by the customer
df_joined_prev_next_txn_amount_purchase_pattern_des=df_joined_prev_next_txn_amount.withColumn("first_last_trans",when(col("prev_amt")==0,'first_trans').when(col("next_amt")==0,'last_trans').otherwise("subsequent trans"))

#Identify the purchase capacity of the customer
df_joined_prev_next_txn_amount_purchase_capacity_des=df_joined_prev_next_txn_amount_purchase_pattern_des.\
    withColumn("purchase_capacity_prev_curr",
               when((col("amount")>=col("prev_amt")),'increased from previous').
               when((col("amount")<col("prev_amt")),'decreased from previous')).\
    withColumn("purchase_capacity_next_curr",
               when((col("amount")<=col("next_amt")),'decreased from next').
               when((col("amount")>col("next_amt")),'increased from next'))

# Cube & Rollup functions are (Comprehensive/Settlement/Drill Down or Up/Granularity) Grouping & Aggregation functions
#Cube - Viewing something (data) in all dimensions with all permutation and combination
#Rollup - Aggregate the data at every possible levels of aggregation

df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category","spendby").agg(sum("amount"))
df_rollup=df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").rollup("category","spendby").agg(sum("amount"))

#whatever we provide in cube we can see all the combination of results
df_cube=df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").cube("category","spendby").agg(sum("amount"))

#or to understand cube better, lets do the aggregation at every level manually, not by using cube
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category","spendby").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("category").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").groupBy("spendby").agg(sum("amount")).show()
df_joined.where("custid in (4000001,4005737) and category in ('Outdoor Recreation','Games')").agg(sum("amount")).show()

print("F. Set Operations - (union/unionall/unionbyname)/(subtract/minus/difference)/intersection")
#Thumb rules : Number, order and datatype of the columns must be same, otherwise unionbyname with missingcolumns function you can use.

df1=df_joined.where("custid in (4000001)").select("state","category","spendby")
df2=df_joined.where("custid in (4005737)").select("state","category","spendby")
print(df1.count())
print(df2.count())

print("Complete customers (with duplicates) across the 2 dataframes state,category,spendby")
df3=df1.union(df2)#union and unionall both are same
print(df3.count())

print("Complete customers (without duplicates) across the 2 dataframes state,category,spendby")
df3=df1.union(df2).distinct()

print("Common customer transactions across the 2 dataframes state,category,spendby")
df3=df1.intersect(df2)
df3.show()

print("Find which customer have did more un common transactions (Excess data in df1 than df2 & vice versa)")
df3=df1.subtract(df2)
df3.show()
df3=df2.subtract(df1)
df3.show()

'''6. Data Persistance (LOAD)-> Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
#Consumer Data Product
'''
#a. Discovery layer/Consumption/Publishing/Gold Layer we can enable the data
#Model building, Slicing/Dicing, Drilldown/Drillup, ....
#Star Schema Model
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

#b. Outbound/Egress

df_domant_customer=df_cust_dim_wrangle_ready.alias("l").join(df_txns_2011_12.alias("r"),on="custid",how="inner").where("r.custid is null").\
    select("custid").distinct()

df_domant_customer.write.csv("file:///home/hduser/customer_dormant")#send this data to the consumer downstream system using some tools, ftp mechanism, mail...


#c. Reports/Exports
df_cust_dim_wrangle_ready.write.saveAsTable("cust_dim")
df_txns_fact_wrangle_ready.write.saveAsTable("txnss_fact")

from configparser import *
def writeRDBMSData(df,propfile,db,tbl,mode):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode, properties={"driver": driver})

df_rollup.write.mode("overwrite").jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan",table="trans_rollup",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","password":"Inceptez@123","numPartitions":"2",\
                                        "batchsize":"10000","truncate":"true"})#RDBMS Reporting Server

df_cube.write.mode("overwrite").jdbc(url="jdbc:mysql://34.16.71.60:3306/ordersproducts?user=irfan",table="trans_cube",\
                            properties={"driver":"com.mysql.cj.jdbc.Driver","password":"Inceptez@123","numPartitions":"2",\
                                        "batchsize":"10000","truncate":"true"})#RDBMS Reporting Server

#d. Schema migration
df_munged_enriched_customized.write.json("file:///home/hduser/munged_enriched_customized")

#All We need from here is....
#1. Our Aspirant's are going to create a document about this bb2 program comprise of
#Transformation Terminologies (eg. wrangling, munging),
# Business logics (aggregation, grouping, standardizing),
# Interview scenarios ,
# Function we used (withcolumn, select)

#2. What's next?
#a. We are going to see the SQL equivalent of the the above DSLs to learn both DSL & SQL relatively
#b. We are going to see the modernized/standardized/generic function based BB2 program in a industry standard way..
#c. We package this modernized code and deploy in our cluster (learn about how we can submit spark job with optimized config)