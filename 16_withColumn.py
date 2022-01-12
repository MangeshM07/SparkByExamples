from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)
df.show()
df.printSchema()

# 1.Change DataType using PySpark withColumn()
df.withColumn("salary", col("salary").cast("Integer")).show()

# 2.Update the value of an existing column
df.withColumn("salary", col("salary")*100).show()

# 3.Create a Column from an Existing
df.withColumn("CopiedColumn", col("salary")*-1).show()

# 4.Add a New Column using withColumn()
df.withColumn("Country", lit("India"))\
    .withColumn("AnotherColumn", lit("AnotherValue"))\
    .show()

# 5. Rename Column Name
df.withColumnRenamed("gender", "sex").show()

# 6. Drop column from pyspark dataframe
df.drop("AnotherColumn").show()
