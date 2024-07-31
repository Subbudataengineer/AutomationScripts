#create a skeleton to convert this module as a typical spark application?
#1. def main -> spark session object (import respective libraries), line of code (rdd operation) -> call main() in a controlled way by passing some args if needed
import sys
from pyspark.sql.session import SparkSession
def main(arg):
    spark=SparkSession.builder.getOrCreate()#create spark session object (sparkcontext, sqlcontext, hivecontext)
    lst1=list(range(1,int(arg[1])))
    rdd1=spark.sparkContext.parallelize(lst1)
    print(rdd1.glom().collect())
    print(rdd1.count())

if __name__=="__main__":
  arguments=sys.argv
  if len(arguments)==2:
    main(arguments)
  else:
    print("pass enough arguments usage: script.py 100")
    exit(1)
