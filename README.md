# DGIMAlgorithm-StreamingAnalysis--DataMining

You are required to implement DGIM algorithm to estimate the number of 1s in the last N bit in a 0/1 streaming.
Before you start running your Spark Stream code, run a simulation of data stream to generate random 0s and 1s on TCP socket.
After running this jar file, a 0/1 streaming will be created on the port 9999 localhost.
In your Spark Stream code, you can use this method to connect to the data stream that you created using the above command line:

val lines = ssc.socketTextStream( "localhost" , 9999)

# Detail

You need to maintain some buskets, each represents a sequence of bits in window.

In this Task, we set the N to 1000, and you need to use DGIM algorithm to estimate the number of 1s in the most recent N bits.
In the streaming, each line is one bit 0 or 1. You also need to maintain the recent 1000 bit to statistic the actually number
of 1s in the recent N bit. In Spark Streaming, set the batch interval of 10 seconds as below:

ssc = StreamingContext( sc , 10)

Every 10 second, while you get batch data in spark streaming, using DGIM algorithm estimate the number of 1s in the last N bit, and print out the number of the estimate and actual number. The percentage error of the estimate result should less than 50%. 
