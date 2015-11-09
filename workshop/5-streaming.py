import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso $SPARK_HOME/bin/pyspark 5-streaming.py directorio")
        exit(-1)

    sc = SparkContext(appName = "Contador")
    ssc = StreamingContext(sc, 1)

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda x: (x,1))\
                  .reduceByKey(lambda a, b: a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
