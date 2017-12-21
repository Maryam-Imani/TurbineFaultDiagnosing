# TurbineFaultDiagnosing
Code of spark-based turbines' fault diagnosing

To run the producer.py: 

1- Start Kafka in both producer and consumer with the same topic
(http://kafka.apache.org/quickstart)

2- Execute the following command: 
```python
python producer.py /path/to/dataset <IP> <port>
```

To run the FaultDetectionRandomSplit.py, execute the following command: 

```python
python FaultDetectionRandomSplit.py </path/to/dataset> <order>
```

To run the SparkFaultDetectionRandomSplit.py: 

In the Spark directory, run the following command: 

```python
bin/spark-submit /path/to/SparkFaultDetectionRandomSplit.py </path/to/dataset> <order>
```
To run the SparkStreamingFaultDetection.py:

1- Start Kafka

2- Execute the following command: 
```python
bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 /path/to/SparkStreamingFaultDetection.py <IP:port> <topic>
```
