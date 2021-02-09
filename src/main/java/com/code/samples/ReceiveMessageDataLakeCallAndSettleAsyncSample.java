package com.code.samples;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import reactor.core.Disposable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ReceiveMessageDataLakeCallAndSettleAsyncSample {

    static  DataLakeFileSystemAsyncClient fileSystemAsyncClient;

    public static void main(String[] args) throws InterruptedException {
        AtomicInteger receivedMessageCount = new AtomicInteger();

        // Set these variables for your application.
        String connectionString = System.getenv("AZURE_SERVICEBUS_NAMESPACE_CONNECTION_STRING");
        String queueName = System.getenv("AZURE_SERVICEBUS_SAMPLE_QUEUE_NAME");
        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT_NAME");
        String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");
        String fileSystemName = "test";
        String fileName = "patient.txt";

        // Assuming that message exists in the queue

        ServiceBusReceiverAsyncClient receiver = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
                .disableAutoComplete()
                .buildAsyncClient();

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endPoint = String.format(Locale.ROOT, "https://%s.dfs.core.windows.net", accountName);

        // create DataLakeFileSystemAsyncClient
        fileSystemAsyncClient = new  DataLakeFileSystemClientBuilder().endpoint(endPoint)
                .credential(credential)
                .fileSystemName(fileSystemName)
                .buildAsyncClient();

        // Create  message receiver.
        Disposable subscription = receiver
                .receiveMessages()
                .timeout(Duration.ofSeconds(20))
                .doOnError(throwable -> {
                    System.out.println("Error during receiving " + throwable.getMessage());
                })
                .take(5)
                .flatMapSequential(message -> {
                    receivedMessageCount.incrementAndGet();
                    DataLakeFileAsyncClient dlAsyncClient = fileSystemAsyncClient.getFileAsyncClient(fileName);
                    return dlAsyncClient.read().collectList()
                            .doOnNext((byteBuffers) -> {
                                try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
                                    for (ByteBuffer b : byteBuffers) {
                                        out.write(b.array());
                                    }
                                    System.out.println("Process File  SequenceNumber: " +message.getSequenceNumber()
                                            + ", message body: " +  message.getBody().toString() + ", byte array size: " + out.size());
                                } catch ( IOException e) {
                                    System.out.println("Error " + e.getMessage());
                                    throw new RuntimeException();
                                }
                            })
                            .doFinally(signalType -> {
                                System.out.println("Processed the file signalType: " + signalType);
                            })
                            .then(receiver.complete(message))
                            .thenReturn(message);
                }, 1) // one thread at a time to process the message
                .subscribe(m-> {
                    System.out.println(receivedMessageCount+". Finished service bus message for sequenceNumber: " + m.getSequenceNumber());
            });

        // Subscribe is not a blocking call so we sleep here so the program does not end.
        TimeUnit.SECONDS.sleep(5);
        System.out.println("Exiting program.");

        // Disposing of the subscription will cancel the receive() operation.
        receiver.close();;
        subscription.dispose();
    }
}