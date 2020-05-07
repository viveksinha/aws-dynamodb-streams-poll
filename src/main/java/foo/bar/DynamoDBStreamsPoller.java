package foo.bar;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Wiki on 07/05/20.
 */
public class DynamoDBStreamsPoller {

    private String accessKey;
    private String secretKey;
    private String region;
    private String tableName;
    private BlockingQueue<StreamRecord> streamRecordBlockingQueue;

    public DynamoDBStreamsPoller(String accessKey, String secretKey, String region, String tableName,
                                 BlockingQueue<StreamRecord> streamRecordBlockingQueue) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.tableName = tableName;
        this.streamRecordBlockingQueue = streamRecordBlockingQueue;
    }

    public void process() {
        AmazonDynamoDBStreams dynamoDBStreamsClient = createStreamsClient(accessKey, secretKey, region);
        List<Stream> streams = getStreams(dynamoDBStreamsClient, tableName);

        int streamCount = 0;
        while (streamCount < streams.size()) {
            Stream stream = streams.get(streamCount++);
            String streamArn = stream.getStreamArn();
            processShards(dynamoDBStreamsClient, streamArn);
        }
    }

    private List<Stream> getStreams(AmazonDynamoDBStreams dynamoDBStreamsClient, String tableName) {
        ListStreamsResult listStreamsResult = dynamoDBStreamsClient
                .listStreams(new ListStreamsRequest().withTableName(tableName));
        Set<Stream> allStreams = new HashSet<>(listStreamsResult.getStreams());

        while (null != listStreamsResult.getLastEvaluatedStreamArn()) {
            allStreams.addAll(dynamoDBStreamsClient.listStreams(new ListStreamsRequest().withTableName(tableName)
                    .withExclusiveStartStreamArn(listStreamsResult.getLastEvaluatedStreamArn())).getStreams());
        }
        return new ArrayList<>(allStreams);
    }

    private void processShards(AmazonDynamoDBStreams dynamoDBStreamsClient, String streamArn) {
        StreamDescription streamDescription;
        String lastReadShardId = null;
        do {
            streamDescription = getStreamDescription(dynamoDBStreamsClient, streamArn, lastReadShardId);
            if (null == streamDescription) {
                break;
            }
            lastReadShardId = streamDescription.getLastEvaluatedShardId();
            for (Shard shard : streamDescription.getShards()) {
                processShard(dynamoDBStreamsClient, streamArn, shard);
            }
        } while (lastReadShardId != null);
    }

    public static StreamDescription getStreamDescription(AmazonDynamoDBStreams dynamoDBStreamsClient, String streamArn, String lastShardId) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamArn(streamArn);
        if (null != lastShardId) {
            describeStreamRequest.withExclusiveStartShardId(lastShardId);
        }
        DescribeStreamResult describeStreamResult = dynamoDBStreamsClient.describeStream(describeStreamRequest);
        return describeStreamResult.getStreamDescription();
    }

    private void processShard(AmazonDynamoDBStreams dynamoDBStreamsClient, String streamArn, Shard shard) {
        String shardId = shard.getShardId();
        String currentShardIterator = shardIterator(dynamoDBStreamsClient, shardId, streamArn, shard.getSequenceNumberRange().getStartingSequenceNumber());
        pollShardIterator(dynamoDBStreamsClient, streamArn, shard, currentShardIterator);
    }

    private void pollShardIterator(AmazonDynamoDBStreams dynamoDBStreamsClient, String streamArn, Shard shard, String currentShardIterator) {

        int iterationWithoutRecords = 0;
        while (null != currentShardIterator) {
            GetRecordsResult recordsResult = recordsResult(dynamoDBStreamsClient, streamArn, shard.getShardId(), currentShardIterator, 1000);
            if (null == recordsResult) {
                return;
            }
            List<Record> dynamoRecords = recordsResult.getRecords();
            if (CollectionUtils.isEmpty(dynamoRecords) && iterationWithoutRecords++ > 500) {
                break;
            }
            processRecords(dynamoRecords);
            currentShardIterator = recordsResult.getNextShardIterator();
        }
    }

    private void processRecords(List<Record> dynamoDbRecords) {
        for (Record dynamoDbRecord : dynamoDbRecords) {
            StreamRecord streamRecord = dynamoDbRecord.getDynamodb();
            boolean deleteEvent = OperationType.REMOVE.equals(OperationType.fromValue(dynamoDbRecord.getEventName()));

            if (!deleteEvent) {
                streamRecordBlockingQueue.add(streamRecord);
            }
        }
    }

    public static GetRecordsResult recordsResult(AmazonDynamoDBStreams dynamoDBStreamsClient,
                                                 String streamArn, String shardId, String currentShardIterator, int maxRows) {
        // Use the shard iterator to read the stream records
        GetRecordsResult recordsResult = null;
        try {
            recordsResult = dynamoDBStreamsClient.getRecords(new GetRecordsRequest().withLimit(maxRows).withShardIterator(currentShardIterator));
        } catch (AmazonDynamoDBException dbe) {

            if (dbe.getMessage().contains("Invalid SequenceNumber") || dbe instanceof TrimmedDataAccessException) {
                GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest().withShardId(shardId)
                        .withStreamArn(streamArn).withShardIteratorType(ShardIteratorType.TRIM_HORIZON);

                currentShardIterator = dynamoDBStreamsClient.getShardIterator(shardIteratorRequest).getShardIterator();
                recordsResult = dynamoDBStreamsClient
                        .getRecords(new GetRecordsRequest().withLimit(maxRows).withShardIterator(currentShardIterator));
            }
        }
        return recordsResult;
    }

    public static String shardIterator(AmazonDynamoDBStreams dynamoDBStreamsClient, String shardId, String streamArn, String sequenceNumber) {
        if (null == sequenceNumber) {
            return null;
        }
        GetShardIteratorRequest shardIteratorRequest;
        try {
            shardIteratorRequest = new GetShardIteratorRequest()
                    .withShardId(shardId)
                    .withStreamArn(streamArn)
                    .withSequenceNumber(sequenceNumber)
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);

            GetShardIteratorResult shardIterator = dynamoDBStreamsClient.getShardIterator(shardIteratorRequest);
            if (null == shardIterator) {
                return null;
            }
            return shardIterator.getShardIterator();

        } catch (AmazonDynamoDBException dbe) {
            if (dbe.getMessage().contains("Invalid SequenceNumber") || dbe instanceof TrimmedDataAccessException) {
                shardIteratorRequest = new GetShardIteratorRequest().withShardId(shardId).withStreamArn(streamArn)
                        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
                return dynamoDBStreamsClient.getShardIterator(shardIteratorRequest).getShardIterator();
            }
            return null;
        }
    }

    public static AmazonDynamoDBStreams createStreamsClient(String accessKey, String secretKey, String region) {
        return AmazonDynamoDBStreamsClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(String.format("streams.dynamodb.%s.amazonaws.com", region), region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }
}
