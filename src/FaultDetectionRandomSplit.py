#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 19:04:23 2017

@author: maryam
"""

import numpy as np
from sklearn.ensemble import RandomForestClassifier
import time
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_recall_curve


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


Order= 10
kFold = 1
OveralAccuracy = 0

startTime = time.time()

trainSignalData= np.loadtxt('./SparkDBFE2.txt', delimiter=",")

N , M = np.shape(trainSignalData)

print (str(N) +', '+ str(M))
#trainSignalData= np.random.shuffle(trainSignalData)
trainingData = trainSignalData[:int(N*0.99),:]
testData= trainSignalData[int(N*0.99):,:]


#trainDataset= [feature_extract(row[1:], Order)  for row in trainingData]
trainDataset= trainingData[:,1:]
trainLabel= [ int(i[0])-1 for i in trainingData]

rf = RandomForestClassifier(n_estimators=3, max_depth=4)
rf.fit(trainDataset, trainLabel) 

trainTime= time.time()
print("---train %s seconds ---" % (trainTime - startTime))
    
#testDataset= [feature_extract(row[1:], Order)  for row in testData]
testDataset = testData[:,1:]
testLabel= [ int(i[0]) for i in testData]
 
result= rf.predict(testDataset)
#np.save('./Sentence/resultSentAll2', result)

print("---test %s seconds ---" % (time.time() - trainTime))

accuracy = accuracy_score(testLabel, result)
#precision, recall, thresholds = precision_recall_curve(testLabel, result)

OveralAccuracy += accuracy


print('OveralAccuracy: '+ str(OveralAccuracy ))
