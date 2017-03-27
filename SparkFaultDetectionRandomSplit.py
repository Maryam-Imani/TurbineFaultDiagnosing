# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 23:42:58 2017

@author: maryam

bin/spark-submit /root/testSpark/SparkFaultDetectionRandomSplit.py

"""
import sys, os
import time

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/spark"
# Append pyspark to Python Path
sys.path.append("/usr/local/spark/python")
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.4-src.zip")
#sys.path.append("/media/maryam/DATAPART1/UTD courses/Big Data/spark-2.0.1-bin-hadoop2.7/python/lib/pyspark.zip")
#sys.path.append("/media/maryam/DATAPART1/UTD courses/Big Data/spark-2.0.1-bin-hadoop2.7/python/pyspark") 

from pyspark import SparkContext
sc = SparkContext()

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint

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
    

#if __name__ == "__main__":
    
#    if len(sys.argv) != 2:
#        print('Number of input is less than required')
#        exit(-1)
#    Signal = float(sys.argv[0])
#    Order = int(sys.argv[1])
#   fileInput= open('/SparkDB.txt', 'r')
#   kf = KFold(n_splits=10)
#   kf.get_n_splits(X)
   
   

Order = 10

#training Data
startTime = time.time()

trainSignalData = sc.textFile('/SparkDBFE2.txt')

(trainingData, testData) = trainSignalData.randomSplit([0.01, 0.99])

#trainData = trainingData.map(lambda row: LabeledPoint( int(row.split(',')[0]) -1, feature_extract(row.split(',')[1:], Order)) )
trainData = trainingData.map(lambda row: LabeledPoint( float(row.split(',')[0]) -1, [float(i) for i in row.split(',')[1:]] )) 
#print trainData.collect()

# train the Random Forest Model

model = RandomForest.trainClassifier(trainData, numClasses=2, categoricalFeaturesInfo={},\
                                 numTrees=3, featureSubsetStrategy="auto",\
                                 impurity='gini', maxDepth=4, maxBins=32)

trainTime= time.time()
print("---train %s seconds ---" % (trainTime - startTime))

# save and load model
#model.save(sc, 'RandomForest1.model')
#model =  RandomForestModel.load(sc, 'RandomForest.model')

#testing Data
testTime= time.time()
#testData = testData.map(lambda row: LabeledPoint( int(row.split(',')[0]) -1, feature_extract((row.split(',')[1:]), Order)) )
testData = testData.map(lambda row: LabeledPoint( float(row.split(',')[0]) -1, [float(i) for i in row.split(',')[1:]] )) 

# Predict the test data label
predictions = model.predict(testData.map(lambda x: x.features))

print("---test %s seconds ---" % (time.time() - testTime))

labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
