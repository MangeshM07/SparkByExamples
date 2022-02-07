from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction as udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)


def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0].upper() + x[1:len(x)] + " "
    return resStr

convertUDF  = udf(lambda z: convertCase(z), StringType())

df.select(col("sqlno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)