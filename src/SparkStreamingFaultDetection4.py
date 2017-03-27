"""
 
 Usage: SparkStreamingFaultDetection.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Kafka server
    `$ `
 and then run the example
 bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 /home/maryam/Desktop/Kafka/SparkStreamingFaultDetection.py localhost:9092 test
"""
from __future__ import print_function

import time as t
import sys, os
reload(sys)

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/spark"
# Append pyspark to Python Path
sys.path.append("/usr/local/spark/python")
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.4-src.zip")


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint

from datetime import datetime

counts = 0
totalTime = 0
path= '/home/maryam/Desktop/Kafka/output/new/outputE4.txt'

def feature_extract(sigList,order):
    WIN_LEN = 256
    CHN_NUM =8
    outputdim = order*CHN_NUM
    K=[0] * outputdim
    for i in range(CHN_NUM):
    	s = i*WIN_LEN
    	e = s+WIN_LEN
	signal = sigList[s:e]
	k= feature_extract1D(signal,order)
	s = i * order
	e= s+order
	K[s:e] = k

    return K

def feature_extract1D(sigList, order):
        
    signal = [float(i) for i in sigList]    
        
    L= len(signal)
    k = [0] * order
    f = signal
    b = signal
    for o in range(order):
        num=0
        den=0
        
        f = f[1:]
        L= L -1 
        
        for s in range( L): 
            num = num + f[s] * b[s]
            den = den + f[s] **2 + b[s] **2
        
        k[o] = -2 * num / float(den)
        
        for s in range(L):
            tmp= f[s]
            f[s] = f[s] + k[o] * b[s]
            b[s] = b[s] + k[o] *tmp
    return k


def echo(time, rdd):
    
    global totalTime, startTime
    
    count = rdd.count()
    global counts
    if (count > 0):
        totalTime += t.time() - startTime
        counts += count
        output = "Counts at time %s , %s" % (totalTime, counts )
        print(output)
        
        with open(path,'a') as fileW:
            fileW.write(output + '\n')
            fileW.flush()
        #rdd.saveAsTextFile("/home/maryam/Desktop/Kafka/output.txt")
        print ('***********************')
    else:
        print("No data received")
        

def checkCondition(rdd):
    
    if rdd == 'start':
        print ('********' + str(datetime.now()))
        with open(path,'a') as fileW:
            fileW.write ('start'+ str(datetime.now()) +'\n')
            fileW.flush()
        return 'sign'
        
    if rdd == 'end':
        print ('********' + str(datetime.now()))
        with open(path,'a') as fileW:
            fileW.write ('end'+ str(datetime.now()) +'\n')
            fileW.flush()
        return 'sign'
    else: 
        return rdd
        

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: SparkStreamingFaultDetection.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    
    global totalTime, startTime
    
    conf = (SparkConf()
         .setMaster("local")
         .setAppName("SparkStreamingFaultDetection")
         .set("spark.executor.memory", "16g")
         .set("spark.driver.memory", "16g")
         .set("spark.executor.instances","4")
         #.set("spark.executor.cores", "4")
         )
    
    sc = SparkContext(conf = conf)
    model =  RandomForestModel.load(sc, '/home/maryam/Desktop/Kafka/RandomForest1.model')

    ssc = StreamingContext(sc, 1 )

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    
    testErr=0
    totalData=0
    Order = 10

    #testing Data
    data = kvs.filter(lambda row: checkCondition(row[1]) !='sign')
    
    if (data.count() > 0):
        startTime = t.time()
        testData = data.map(lambda row: LabeledPoint( float(row[1].split(',')[0]), [float(i) for i in row[1].split(',')[1:]] )) 
#    testData.pprint()
#    
#    #print trainData.collect()
#    
#    # train the Random Forest Model
#    model = RandomForest.trainClassifier(trainData, numClasses=2, categoricalFeaturesInfo={},\
#                                     numTrees=3, featureSubsetStrategy="auto",\
#                                     impurity='gini', maxDepth=4, maxBins=32)
#    
#    # save and load model
#    #model.save(sc, 'RandomForest.model')    
    # Predict the test data label
    #result = model.predict(testData.map(lambda x: x.features))
        
        predictions = testData.map(lambda lp:  lp.features).transform(lambda _, rdd: model.predict(rdd))
        
        trueLabel= testData.map(lambda lp:  lp.label)
        
        if (predictions != trueLabel):
            testErr +=1
        
        predictions.foreachRDD(echo)
    #predictions.foreachRDD (lambda rdd: printing(rdd, testErr))
    #predictions.saveAsTextFile("/home/maryam/Desktop/Kafka/output.txt")
    
        print('********')
        
        #trueLabel.pprint()
        predictions.pprint()
#    predictions = model.predict(testData.map(lambda lp: (lp.label, lp.features)))
#    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
#    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
#    str(testErr).pprint()
    #print('Learned classification forest model:')
    #print(model.toDebugString())
    
    ssc.start()
    ssc.awaitTermination()
    
