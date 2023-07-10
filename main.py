print('Hello world')
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.getOrCreate()
data=[(1,"sdfsd"),(2,"sdfsdf")]
schema=StructType([StructField("id",IntegerType(),True),
                   StructField("name",StringType(),True)])
df=spark.createDataFrame(data,schema)
df.show()
