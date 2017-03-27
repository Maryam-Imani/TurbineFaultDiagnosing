# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 15:06:02 2016


python producer.py /SparkDBFE.txt <ip> <port>

@author: mary
"""

#simple_producer.py
from kafka import KafkaProducer
from time import sleep
import sys, time

if __name__ == "__main__":
    
    filename= sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    producer = KafkaProducer(bootstrap_servers= ip+':'+port)
    
    with open(filename,'r') as f:
        lines= f.readlines()
    
    producer.send('test', 'start')
    
#    while True:
    for i in range(0, 1):
        startTime = time.time()
        for line in lines: 
            producer.send('test', line)
            #sleep(0.001)
    
    print (time.time()- startTime)
    producer.send('test', 'end')
