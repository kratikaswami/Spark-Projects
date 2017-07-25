from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("path/u.item", encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parsedLines(lines):
    movieId = lines.split()[1]
    return movieId


conf = SparkConf().setMaster("local").setAppName("Most Popular movie in the data set")
sc = SparkContext(conf = conf)


nameDict = sc.broadcast(loadMovieNames())


input = sc.textFile("path/u.data")
rdd = input.map(parsedLines)
ratings = rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)

flipped = ratings.map(lambda xy : (xy[1],xy[0]))
sortedMovies = flipped.sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda countMovie: (nameDict.value[countMovie[0]], countMovie[1]))

results = sortedMoviesWithNames.collect()

for result in results:
   # print(str(result[0])+" "+ str(result[1]))
   print(result)
