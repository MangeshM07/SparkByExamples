import pyspark
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("Dataframe partitioning").master("local[5]").getOrCreate()

df = spark.range(0,20).alias("id")
df.show()
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("file:///c://data//df_partition.csv")

# Dataframe repartition
# df2 = df.repartition(6)
# print(df2.rdd.getNumPartitions())

# Dataframe Coalesce
# df3 = df.coalesce(2)
# print(df3.rdd.getNumPartitions())

# Default shuffle partition
"""Calling groupBy(), union(), join() and similar functions on dataframe results in shuffling data
between multiple executors and even machines and finally repartition's data into 200 partitions by default"""
df4 = df.groupBy("id").count()
print(df4.rdd.getNumPartitions())
