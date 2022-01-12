
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dataCollect = deptDF.collect()
print(dataCollect)

for row in dataCollect:
    print(row["dept_name"]+","+str(row["dept_id"]))

# return first row and first column from dataframe
ff = deptDF.collect()[0][0]
print(ff)

dataCollect = deptDF.select("dept_name").collect()


