"""
RDD's are primarily created in 2 ways:

1. Parallelizing an existing application
2. Referencing a dataset in an external storage system(HDFS, S3 and many more..)

"""

from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[*]").appName("PysparkRDD").getOrCreate()
#
# # create RDD from parallelize
# data = [1,2,3,4,5,6,7,8,9,10,11,12]
# rdd = spark.sparkContext.parallelize(data)
# print("Initial partition count rdd :"+str(rdd.getNumPartitions()))
#
# # Create RDD using sparkContext.textFile()
# rdd2 = spark.sparkContext.textFile("file:///c:/data/txns_s")
# print("Initial partition count rdd2 :"+str(rdd2.getNumPartitions()))
#
# # Create RDD using sparkContext.wholeTextFiles()
# rdd3 = spark.sparkContext.wholeTextFiles("file:///c:/data/txnsamplepipe.txt")
# print("Initial partition count rdd3 :"+str(rdd3.getNumPartitions()))
#
# # Create empty RDD with no partition using sparkContext.emptyRDD
# rdd4 = spark.sparkContext.emptyRDD()
# print("Initial partition count rdd4 :"+str(rdd4.getNumPartitions()))

# Creating empty RDD with partition
rdd5 = spark.sparkContext.parallelize([], 10)
print("Initial partition count rdd5 :"+str(rdd5.getNumPartitions()))

# partitioning an RDD using repartition
reparRdd = rdd5.repartition(4)
print("Re-partition count: "+str(reparRdd.getNumPartitions()))

