"""
PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or SQL expression,
you can also use where() clause instead of the filter() if you are coming from an SQL background,
both these functions operate exactly the same.

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, ArrayType, StructField
from pyspark.sql.functions import col, explode, array_contains
from pyspark import SparkContext

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
]

schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

spark: SparkSession = SparkSession.builder.appName("Filters").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame(data, schema)
df.printSchema()

# Using equals condition
df.filter(df.state == "OH").show(truncate=False)

# not equals condition
df.filter(df.state != "OH").show(truncate=False)

df.filter(~(df.state == "OH")).show(truncate=False)

# using SQL col() function
df.filter(col("state") == "OH").show(truncate=False)

# Using SQL Expression
df.filter("gender == 'M'").show()
# For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

# filter multiple conditions
df.filter((df.state== "OH") & (df.gender == "M")).show(truncate=False)

# Filter based on list values
df.filter(df.state.isin(["OH", "CA", "DE"])).show(truncate=False)


# Using startswith
df.filter(df.state.startswith("N")).show()

# using endswith
df.filter(df.state.endswith("H")).show()

# Contains
df.filter(df.state.contains("H")).show()


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
# This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()

# Filter on an Array column
df.filter(array_contains(df.languages,"Java")).show(truncate=False)

# Filtering on nested struct columns
df.filter(df.name.lastname == "Williams").show(truncate=False)