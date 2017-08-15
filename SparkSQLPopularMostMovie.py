from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("/home/kratika/Desktop/spark/ml-100k/u.item", encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("popularmoviesparksql").getOrCreate()
nameDict = loadMovieNames()
lines = spark.sparkContext.textFile("/home/kratika/Desktop/spark/ml-100k/u.data")
movies = lines.map(lambda x: Row(movieId = int(x.split()[1])))
movieDataset = spark.createDataFrame(movies) 
topMovieIds = movieDataset.groupBy("movieId").count().orderBy("count", ascending = False).cache()
topMovieIds.show()
top10 = topMovieIds.take(10)
print("\n")
for result in top10:
    print("%s: %d" % (nameDict[result[0]], result[1]))

spark.stop()
