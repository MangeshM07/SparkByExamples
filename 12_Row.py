"""
In PySpark Row class is available by importing pyspark.sql.Row which is represented as a record/row in DataFrame,
one can create a Row object by using named arguments, or create a custom Row like class.
In this article I will explain how to use Row class on RDD, DataFrame and its functions.

* Earlier to Spark 3.0, when used Row class with named arguments, the fields are sorted by name.
* Since 3.0, Rows created from named arguments are not sorted alphabetically instead they will be ordered in the position entered.
* To enable sorting by names, set the environment variable PYSPARK_ROW_FIELD_SORTING_ENABLED to true.
* Row class provides a way to create a struct-type column as well.
"""
# 1. Create a Row Object
from pyspark.sql import Row
row = Row("James", 40)
print(row[0] + "," + str(row[1]))

row1 = Row(name="Alice", age=11)
print(row1.name)
print(row1.age)

# 2. Create custom class from Row
Person = Row("name", "age")
p1 = Person("James", 40)
p2 = Person("Rick", 55)
print(p1.name + ", " + p2.name)

# 3. Using Row class on Pyspark RDD
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ROW").master("local[*]").getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]

rdd = spark.sparkContext.parallelize(data)
# print(rdd.collect())
collData = rdd.collect()
for row in collData:
    print(row.name + ", " + str(row.lang))

# 4. Using Row class on PySpark DataFrame
df = spark.createDataFrame(data)
df.printSchema()
df.show()

columns = ["name", "languageAtSchool", "CurrentState"]
df = spark.createDataFrame(data).toDF(*columns)
df.printSchema()

# 5. Create Nested Struct Using Row Class
from pyspark.sql import Row
data = [Row(name="James", prop=Row(hair="black", eye="Blue")),
        Row(name="Ann", prop=Row(hair="grey", eye="black"))]
df = spark.createDataFrame(data)
df.printSchema()


