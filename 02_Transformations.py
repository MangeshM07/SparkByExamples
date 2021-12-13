# https://sparkbyexamples.com/pyspark-rdd/

from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[*]").appName("Spark Transformations").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("file:///c://data//test.txt")
# print(rdd.take(10))

""""
flatMap – flatMap() transformation flattens the RDD after applying the function and returns a new RDD. 
On the below example, first, it splits each record by space in an RDD and finally flattens it. 
Resulting RDD consists of a single word on each record.
"""
rdd2 = rdd.flatMap(lambda x: x.split(" "))
# rdd2.foreach(print)

"""
map – map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c, 
the output of map transformations would always have the same number of records as input.

In our word count example, we are adding a new column with value 1 for each word, the result of the RDD is PairRDDFunctions 
which contains key-value pairs, word of type String as Key and 1 of type Int as value.
"""
rdd3 = rdd2.map(lambda x: (x, 1))
# rdd3.foreach(print)

"""
reduceByKey – reduceByKey() merges the values for each key with the function specified. 
In our example, it reduces the word string by applying the sum function on value. 
The result of our RDD contains unique words and their count. 
"""
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
# print(rdd4.take(10))

"""
sortByKey – sortByKey() transformation is used to sort RDD elements on key. 
In our example, first, we convert RDD[(String,Int]) to RDD[(Int, String]) using map transformation and apply sortByKey 
which ideally does sort on an integer value. 
And finally, foreach with println statements returns all words in RDD and their count as key-value pair
"""
rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey()
# print(rdd5.take(10))

"""filter – filter() transformation is used to filter the records in an RDD. In our example we are filtering all 
words starts with “a”. 
"""
rdd6 = rdd5.filter(lambda x: 'an' in x[1])
# print(rdd6.collect())

"""
RDD Actions:

count() – Returns the number of records in an RDD
"""
# print("Count :"+str(rdd6.count()))

# first() – Returns the first record
# print(rdd6.first())

# max() – Returns max record
# print(rdd6.max())

# reduce() – Reduces the records to single, we can use this to count or sum.
# totalWordCount = rdd6.reduce(lambda a,b : (a[0]+b[0], a[1]))
# print("DataReduce record : "+str(totalWordCount[1]))

# take() – Returns the record specified as an argument.
# data3 = rdd6.take(3)
# for f in data3:
#     print("data3 key:"+str(f[0]) + ", value: "+f[1])

"""
collect() – Returns all data from RDD as an array. 
Be careful when you use this action when you are working with huge RDD with millions and billions of data as 
you may run out of memory on the driver.
"""
# data = rdd6.collect()
# for f in data:
#     print("data key:"+str(f[0]) + ", value: "+f[1])

# saveAsTextFile
rdd6.saveAsTextFile("file:///c://data//wordCount")