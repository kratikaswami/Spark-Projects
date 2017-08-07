import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("/home/kratika/Desktop/spark/ml-100k/movies.dat", encoding = 'ascii',errors = 'ignore') as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2),(rating1,rating2)) 
    
def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sumXX = sumYY = sumXY = 0
    for ratingsX, ratingsY in ratingPairs:
       sumXX +=  ratingsX * ratingsX
       sumYY +=  ratingsY * ratingsY
       sumXY +=  ratingsX * ratingsY
       numPairs += 1

    numerator = sumXY
    denominator = sqrt(sumXX) * sqrt(sumYY)
    
    score = 0
    if denominator:
        score = (numerator/(float(denominator))) 
    
    return (score, numPairs)
    
    
conf = SparkConf()
sc = SparkContext()

print("LOADING MOVIE NAMES.")
nameDict = loadMovieNames()
data = sc.textFile("s3n://kratika36/ratings.data")
ratings = data.map(lambda l : l.split("::")).map(lambda l:(int(l[0]), (int(l[1]), float(l[2]))))
joinedRating = ratings.join(ratings)
uniqueJoinedRatings = joinedRating.filter(filterDuplicates) 
moviePairs= uniqueJoinedRatings.map(makePairs)
moviePairRatings = moviePairs.groupByKey()
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieId = int(sys.argv[1])
    filteredPairs = moviePairSimilarities.filter(lambda pairSim : \
    (pairSim[0][0] == movieId or pairSim[0][0] ==movieId) \
    and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold )

    results = filteredPairs.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("TOP 10 RESULTS FOR MOVIE ID: "+nameDict[movieId])
    for result in results:
        (sim, pair) = result
        similarMovieId = pair[0]
        if similarMovieId == movieId:
            similarMovieId = pair[1]
        print(nameDict[similarMovieId]+ "\tscore: "+str(sim[0])+ "\tstrength: "+str(sim[1]) )

