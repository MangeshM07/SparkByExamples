from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Column Functions").master("local[*]").getOrCreate()

#######################################################################
#
#       1. Create Column Class Object
#
#######################################################################
# from pyspark.sql.functions import lit
# colObj = lit("DummyValue")
#
# data = [("James", 23), ("Ann", 40)]
# df = spark.createDataFrame(data).toDF("name.fname","age")
# df.printSchema()
#
# # Using dataframe object df
# df.select(df.age).show()
# df.select(df["age"]).show()
#
# # Accessing column names with dot(using backticks)
# df.select(df["`name.fname`"]).show()
#
# # Using SQL col() function
from pyspark.sql.functions import col
# df.select(col("age")).show()
# df.select(col("`name.fname`")).show()
#
# # Create Dataframe with struct using Row class
# from pyspark.sql import Row
# data = [Row(name="James", prop=Row(hair="black", eye="blue")),
#         Row(name="Ann", prop=Row(hair="grey", eye="black"))]
#
# df = spark.createDataFrame(data)
# df.printSchema()
# df.show()
#
# # Access struct column
# df.select(df.prop.hair).show()
# df.select(df["prop.hair"]).show()
# df.select(col("prop.*")).show()

#######################################################################
#
#       2. PySpark Column Operators
#
#######################################################################
# data = [(100,2,1), (200,3,4), (300,4,4)]
# df = spark.createDataFrame(data).toDF("col1", "col2", "col3")
#
# # Arithmetic operations
# df.select(df.col1 + df.col2).show()
# df.select(df.col1 - df.col2).show()
# df.select(df.col1 * df.col2).show()
# df.select(df.col1 / df.col2).show()
# df.select(df.col1 % df.col2).show()
# df.select(df.col1 > df.col2).show()
# df.select(df.col1 < df.col2).show()
# df.select(df.col1 == df.col2).show()

#######################################################################
#
#       3. PySpark Column functions example
#
#######################################################################
from pyspark.sql.functions import expr
# data=[("James","Bond","100",None),
#       ("Ann","Varsa","200",'F'),
#       ("Tom Cruise","XXX","400",''),
#       ("Tom Brand",None,"400",'M')]
# columns=["fname","lname","id","gender"]
# df=spark.createDataFrame(data,columns)

# df.select(df.fname.alias("Firstname"),
#           df.lname.alias("LastName")).show()
#
# df.select(col("fname").alias("Firstname"),
#           col("lname").alias("LastName")).show()

# df.select(expr("fname ||' ' || lname").alias("fullname")).show()
# df.selectExpr(("fname ||' '|| lname").alias("FullName")).show()

# df.sort(df.fname.asc()).show()
# df.sort(df.fname.desc()).show()
#
# # cast
# df.select(df.fname, df.id.cast("int")).printSchema()
#
# # contains
# df.select(df.fname.contains("Tom Cruise")).show()
# df.select(col("fname").contains("Tom Cruise")).show()

# getField and getItem
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, StringType
data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema = StructType([
                StructField("Name", StructType([
                    StructField("FirstName", StringType(), True),
                    StructField("LastName", StringType(), True)
                ])),
                StructField("Languages", ArrayType(StringType()), True),
                StructField("Properties", MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show(truncate=False)

# getField from Maptype
df.select(df.Properties.getField("hair")).show()

# getField from Struct
df.select(df.Name.getField("FirstName")).show()

# getItem() - To get the value by index from MapType or ArrayType and by key for MapType column
# getitem with ArrayType
df.select(df.Languages.getItem(1)).show()

# getitem with MapType
df.select(df.Properties.getItem("hair")).show()