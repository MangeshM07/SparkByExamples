"""What is PySpark Accumulator?
Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to update
and updates from the workers get propagated automatically to the driver program.
But, only the driver program is allowed to access the Accumulator variable using the value property.

    1. sparkContext.accumulator() is used to define accumulator variables.
    2. add() function is used to add/update a value in accumulator
    3. value property on the accumulator variable is used to retrieve the value from the accumulator.

"""
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("Accumulator").master("local[*]").getOrCreate()
sc = spark.sparkContext

##################### Example 1 ##################################
accum = sc.accumulator(0)
rdd = sc.parallelize([1, 2, 3, 4, 5])

rdd.foreach(lambda x: accum.add(x))
print(accum.value)

##################### Example 2 ##################################
accuSum = spark.sparkContext.accumulator(0)


def countSum(x):
    global accuSum
    accuSum += x


rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.foreach(countSum)
print(accuSum.value)

##################### Example 3 ##################################
accumCount = sc.accumulator(0)
rdd2 = sc.parallelize([1, 2, 3, 4, 5])
rdd2.foreach(lambda x: accumCount.add(1))
print(accumCount.value)
