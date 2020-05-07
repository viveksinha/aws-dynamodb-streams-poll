package foo.bar;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.util.NamedDefaultThreadFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by Wiki on 07/05/20.
 */
public class DynamoDBStreamsPollSample {

    public static void main(String[] args) throws InterruptedException {
        String accessKey = "";
        String secretKey = "";
        String region = "";
        String tableName = "";
        int maxTimeoutMins = 60;

        String destTableName = "destination_" + tableName + "_" + UUID.randomUUID();
        System.out.println(String.format("Writing data to a new destination table %s", destTableName));

        AmazonDynamoDB dynamoDBClient = createClient(accessKey, secretKey, region);
        DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);
        createStreamsTable(dynamoDBClient, destTableName, describeTableResult.getTable());

        BlockingDeque<StreamRecord> blockingDeque = new LinkedBlockingDeque<>(1000);
        DynamoDBStreamsPoller poller = new DynamoDBStreamsPoller(accessKey, secretKey, region, tableName, blockingDeque);

        ExecutorService executorService = Executors.newFixedThreadPool(2, NamedDefaultThreadFactory.of("DynamoDB-Streams-Poller-%d"));
        executorService.submit(poller::process);
        executorService.submit(() -> {
            while (true) {
                StreamRecord streamRecord = blockingDeque.poll();
                if (null != streamRecord) {
                    dynamoDBClient.putItem(destTableName, streamRecord.getNewImage());
                }
            }
        });
        executorService.shutdown();
        try {
            executorService.awaitTermination(maxTimeoutMins, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    public static void createStreamsTable(AmazonDynamoDB dynamoDBClient, String tableName, TableDescription tableDescription) throws InterruptedException {

        List<AttributeDefinition> attributeDefinitions = tableDescription.getAttributeDefinitions();
        List<KeySchemaElement> keySchema = tableDescription.getKeySchema();

        StreamSpecification streamSpecification = new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(StreamViewType.NEW_IMAGE);

        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(keySchema).withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L))
                .withStreamSpecification(streamSpecification);

        dynamoDBClient.createTable(createTableRequest);
        TableUtils.waitUntilActive(dynamoDBClient, tableName);
    }

    private static AmazonDynamoDB createClient(String accessKey, String secretKey, String region) {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(String.format("https://dynamodb.%s.amazonaws.com", region), region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }
}
