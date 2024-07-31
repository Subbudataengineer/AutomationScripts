import sys
from pyspark.sql.session import SparkSession
#Develop a typical spark application using spark core methodology to count the no. of rows in txns or some other file in hdfs
#1. I will define main method - inside the main method, i will write the logics (any logic)
#2. I will call the main method with/without args in a controlled way
def main(arg):
    print("This is just a typical python application")
    spark=SparkSession.builder.getOrCreate()#I am converting the python app to spark app
    print("Once I referred/instantiated the sparksession class and built the spark object, This is a typical spark application")
    rdd1=spark.sparkContext.textFile(arg[1])
    print(rdd1.count())
if __name__=="__main__":
    if len(sys.argv)==2:
        print("The spark application is going run to access data of ",sys.argv[1])
        main(sys.argv)
    else:
        print("Pass the location/file you want to access by spark prog")
        exit(1)

