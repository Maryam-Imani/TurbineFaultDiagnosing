# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 15:06:02 2016

@author: mary
"""

#simple_producer.py
from kafka import KafkaProducer
from time import sleep
 
#producer = KafkaProducer(
#    bootstrap_servers='localhost:9091,localhost:9092',
#    client_id='simple-producer')
# 
#for i in range(120):
#    msg = "Message {}".format(i)
#    producer.send('simple-test',msg)
#    sleep(1)


producer = KafkaProducer(bootstrap_servers='localhost:9092')

#for i in range(10):
#    producer.send('test', b"Signal vector")
#    sleep(1)

time=20
with open('/home/maryam/Desktop/Kafka/CrossValidation/trainFold1.txt','r') as f:
    for line in f: 
        producer.send('test', line)
        time /= 2.0
#        sleep(time)
        if time <0.001 :
            sleep(0.001)
        else:
            sleep(time)
        

#producer = KafkaProducer(bootstrap_servers=['hostname:9092'])
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10))


# Asynchronous by default
#future = producer.send('test', b'raw_bytes')