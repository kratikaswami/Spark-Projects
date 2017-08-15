from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(lines):
    fields = lines.split(',')
    return Row( ID = int(fields[0]), name =str(fields[1].encode("utf-8")), age = int(fields[2]),numFriends = int(fields[3]))


lines = spark.sparkContext.textFile("/home/kratika/fakefriends.csv");
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenager = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenager.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()
