package com.azure.eventhub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

public final class App {
    private App() {}
   
    public static void main(String[] args) {

        String namespace = "estudoshubeventos";
        String accessKey = "S6mXr5HHjM5Vqd6cpTjM561KxhQR8pccy+AEhGv+/Ks=";
        String accessKeyName = "RootManageSharedAccessKey";
        String connectionString = getConnectionString(namespace, accessKeyName, accessKey);
        String eventHubName = "estudoshub";

        sendMessage(connectionString, eventHubName);
        receiveMessage(connectionString, eventHubName);
    }

    private static String getConnectionString(String nameSpace, String accessKeyName, String accessKey) {
        return "Endpoint=sb://" + nameSpace + ".servicebus.windows.net/;SharedAccessKeyName="+ accessKeyName + ";SharedAccessKey=" + accessKey;
    }

    private static void receiveMessage(String connectionString, String eventHubName) {
        String consumerGroup = EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME;

        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .consumerGroup(consumerGroup)
                .buildAsyncConsumerClient();

        consumer.receive()
                .subscribe(event -> {
                    System.out.println("Nova mensagem recebida: " + event.getData().getBodyAsString());
                    // Adicionar código de processamento aqui
                }, error -> System.err.println("Erro: " + error),
                        () -> System.out.println("Recepção concluída."));

        try {
            System.out.println("Pressione Enter para encerrar.");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private static void sendMessage(String connectionString, String eventHubName) {
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();

        // partição onde o consumer group se encontra
        CreateBatchOptions options = new CreateBatchOptions().setPartitionId("0");
        EventDataBatch batch = producer.createBatch(options);

        batch.tryAdd(new EventData("Olá Mundo"));
        batch.tryAdd(new EventData("Tudo bem ?"));

        producer.send(batch);

        producer.close();
    }
}
