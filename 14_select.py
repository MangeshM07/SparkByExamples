from pyspark.sql import SparkSession

spark  = SparkSession.builder.appName("Select examples").master("local[*]").getOrCreate()
#
# data = [("James","Smith","USA","CA"),
#     ("Michael","Rose","USA","NY"),
#     ("Robert","Williams","USA","CA"),
#     ("Maria","Jones","USA","FL")
#   ]
# columns = ["firstname","lastname","country","state"]
#
# df = spark.createDataFrame(data, columns)
# df.show()
# df.printSchema()
#
#
# # 1. Select Single & Multiple Columns From PySpark
#
# df.select("firstname", "lastname").show()
# df.select(df.firstname, df.lastname).show()
#
# # using col function
# from pyspark.sql.functions import col
# df.select(col("firstname"), col("lastname")).show()
#
# # select column by regular expressions
# df.select(df.colRegex("`^.*name*`")).show()
#
# # 2. Select All Columns From List
# df.select(*columns).show()
#
# # select all columns
# df.select([col for col in df.columns]).show()
# df.select("*").show()
#
# # 3. Select Columns by Index
# df.select(df.columns[:3]).show(3)
#
# df.select(df.columns[2:4]).show(3)

# 4.select nested struct columns from pyspark
data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
# df2.printSchema()
# df2.show(truncate=False) # shows all columns

df2.select("name").show(truncate=False)
df2.select("name.firstname", "name.lastname").show()

df2.select("name.*").show(truncate=False)

# 5. Complete Example
