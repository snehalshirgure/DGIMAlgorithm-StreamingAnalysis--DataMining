from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time


# Create a local StreamingContext with two working thread and batch interval of 10 seconds
sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel(logLevel="OFF")
ssc = StreamingContext(sc, 100)
lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (int(word), 1))

# wordCounts = pairs.reduceByKey(lambda x, y: x+y)
# wordCounts.pprint()

def DGIMcount(rdd):
    buckets={}
    for x in range(11):
        buckets[x] = []

    maxsize = 0
    timestamp= 0
    list1 =  rdd.collect()
    actualcount = 0

    def update(t,n):
        if(n==11):
            return 
        llist = buckets[n]

        if(len(llist)==2):
            topelement = buckets[n].pop()
            buckets[n] = []
            update(topelement,n+1)
        
        buckets[n] += [str(t)]
        maxsize = n

    for each in list1:
        
        timestamp+=1

        if (each[0] == 1):
            actualcount+=1
            update(timestamp,0)

    # print buckets
    
    sumcount = 0

    for x in range(10):
        blist = buckets[x]
        nextlist = buckets[x+1]
        
        if(len(nextlist)==0):
            sumcount+= (len(blist)*pow(2,x))/2
            break
        else:
            if(len(blist)!=0):
                sumcount+= len(blist)*pow(2,x)
    
    print("Estimated number of ones in last 1000 bits: " + str(sumcount))
    print("Actual number of ones in last 1000 bits: " + str(actualcount))

pairs.foreachRDD(DGIMcount)


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
