from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Most Popular Hero")
sc = SparkContext(conf = conf)

def countCoOccurence(line):
    field = line.split()
    return (int(field[0]), len(field) - 1)

def parsedLines(lines):
    fields = lines.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))


names = sc.textFile("path/Marvel-Names.txt")
namesRDD = names.map(parsedLines)

lines = sc.textFile("path/Marvel-Graph.txt")
graphs = lines.map(countCoOccurence)

pairings = graphs.reduceByKey(lambda x, y: x + y)
flipped = pairings.map(lambda x : (x[1],x[0]))

mostPopular = flipped.max()

mostPopularName = namesRDD.lookup(mostPopular[1])[0]
print("***********************************************************************************************")
print(str(mostPopularName)+ "IS THE MOST POPULAR SUPERHERO, WITH "+str(mostPopular[0])+" CO-APPEARANCES")
print("***********************************************************************************************")
