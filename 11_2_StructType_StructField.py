from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, MapType, ArrayType, StringType, IntegerType
from pyspark.sql.functions import col, struct, when

spark: SparkSession = SparkSession.builder.appName("StructType and StructFields").master("local[*]").getOrCreate()

structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

structureSchema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True),
])

df1 = spark.createDataFrame(data=structureData, schema=structureSchema)
df1.printSchema()
df1.show(truncate=False)

updatedDF = df1.withColumn("OtherInfo",
                           struct(col("id").alias("identifier"),
                                  col("gender").alias("gender"),
                                  col("salary").alias("salary"),
                                  when(col("salary").cast(IntegerType()) < 2000, "Low")
                                  .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                                  .otherwise("High").alias("Salary_Grade"))).drop("id", "gender", "salary")

updatedDF.show(truncate=False)
updatedDF.printSchema()
