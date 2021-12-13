from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark: SparkSession = SparkSession.builder.appName("DFs").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

# create dataframe from rdd

rdd = spark.sparkContext.parallelize(data)

# using toDF()
dfFromRDD1 = rdd.toDF(columns)
# dfFromRDD1.show()
# dfFromRDD1.printSchema()

# using createDataFrame() from SparkSession
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
# dfFromRDD2.show()

# Using createDataFrame() with the Row type
rowData = map(lambda x: Row(*x), data)
dfFromData3 = spark.createDataFrame(rowData, columns)

# Create DataFrame with schema
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
    ])

df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)