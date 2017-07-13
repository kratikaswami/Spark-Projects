from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("min-temperature")
sc = SparkContext(conf = conf)

def parsedLines(line):
    fields = line.split(',')
    stationId = fields[0] 
    entryType = fields[2]
    temperature = float(fields[3])* 0.1 * (9.0 / 5.0) + 32.0
    return(stationId,entryType,temperature)

lines = sc.textFile("/home/kratika/Downloads/1800.csv")
rdd = lines.map(parsedLines)

minTemp = rdd.filter(lambda x : "TMIN" in x[1])
stationTemp = minTemp.map(lambda x: (x[0], x[2]))
minTemp = stationTemp.reduceByKey(lambda x,y: min(x,y))
results = minTemp.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
