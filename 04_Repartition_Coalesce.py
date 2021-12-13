from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("Repartition vs Coalesce").master("local[5]").getOrCreate()

df = spark.range(0,20)
print(df.rdd.getNumPartitions())

spark.conf.set("spark.sql.shuffle.partitions","500")

rdd = spark.sparkContext.parallelize((0,20))
print("From local[5] "+str(rdd.getNumPartitions()))

rdd1 = spark.sparkContext.parallelize((0, 25), 6)
print("Parallelize: "+str(rdd1.getNumPartitions()))

# rdd1.saveAsTextFile("file:///c://data//partition3")

# rdd2 = rdd1.repartition(4)
# print("Repartition Size: "+str(rdd2.getNumPartitions()))
# rdd2.saveAsTextFile("file:///c://data/partition4")

rdd3 = rdd1.coalesce(4)
print("Repartition size :"+str(rdd3.getNumPartitions()))
rdd3.saveAsTextFile("file:///c://data/coalesce2")