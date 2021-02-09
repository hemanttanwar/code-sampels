# code-sampels

Repository contains various code samples

# Index

## [Service Bus Receive Message and process by data lake client Sample][receive_sample]:
 [ReceiveMessageDataLakeCallAndSettleAsyncSample.java ][receive_sample]
  * This program receive messages from service bus and process them.
  * The output of of running this program is as follows.
```java
Process File  SequenceNumber: 2538, message body: Hello 6, byte array size: 28
Processed the file signalType: onComplete
1. Finished service bus message for sequenceNumber: 2538
Process File  SequenceNumber: 2539, message body: Hello 7, byte array size: 28
Processed the file signalType: onComplete
2. Finished service bus message for sequenceNumber: 2539
Process File  SequenceNumber: 2540, message body: Hello 8, byte array size: 28
Processed the file signalType: onComplete
3. Finished service bus message for sequenceNumber: 2540
Process File  SequenceNumber: 2541, message body: Hello 9, byte array size: 28
Processed the file signalType: onComplete
4. Finished service bus message for sequenceNumber: 2541
Process File  SequenceNumber: 2542, message body: Hello 10, byte array size: 28
Processed the file signalType: onComplete
5. Finished service bus message for sequenceNumber: 2542
Exiting program.

Process finished with exit code 0
```

[receive_sample]: ./src/main/java/com/code/samples/ReceiveMessageDataLakeCallAndSettleAsyncSample.java 
