import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("union and union all").master("local[*]").getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]

df1 = spark.createDataFrame(data= simpleData, schema= columns)

# df1.show()
# df1.printSchema()

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

# df2.printSchema()
# df2.show(truncate=False)

df3 = df1.union(df2)

df3.show(truncate=False)

df4 = df3.distinct()
df4.show()