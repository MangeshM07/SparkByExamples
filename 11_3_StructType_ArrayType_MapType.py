"""
SQL StructType also supports ArrayType and MapType to define
the DataFrame columns for array and map collections respectively.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, StringType

spark = SparkSession.builder.appName('ArrayType and MapType').master("local[*]").getOrCreate()

arrayStructureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True),
    ])),
    StructField('hobbies', ArrayType(), True),
    StructField('properties', MapType(StringType(), StringType()), True )
])

