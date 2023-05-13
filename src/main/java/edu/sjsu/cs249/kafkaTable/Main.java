//TODO: DONE 1. work on snapshot ordering and creating snapshots when snapshot period is reached.
//TODO: DONE 2. reading snapshots on coming up. remember to always read the last snapshot in the snapshot topic
//TODO DONE: 3. add callback to subscribe so that consumer is set up with the topic
//TODO DONE: 4. set replica kafka property session timeout ms config to "10000" and then locks or semaphores to release when onPartitionAssigned and acquire above poll while
//TODO: 5. either lock producer or have diff producers for each topic
//TODO: DONE 6. seek after dummy poll might cause not yet assigned exception. do a catch where it polls until it forms the partition
// TODO: 7. check if already present in snapshotOrdering topic before adding replica name
//TODO: 8. replace snapshot offset: currently it is set to current offset. set it to one before and while setting state, set offset+1

// My IP: 172.27.24.15
package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import io.grpc.*;

import java.net.SocketAddress;

import static io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR;


public class Main {
    static {
        // quiet some kafka messages
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }

    @Command(name = "replica", mixinStandardHelpOptions = true, description = "start an ABD register server.")
    static class ServerCli implements Callable<Integer> {

        @Parameters(index = "0", description = "kafka host port")
        static String kafkaServer;

        @Parameters(index = "1", description = "replica name")
        static String replicaName;

        @Parameters(index = "2", description = "host:port listen on.")
        static String serverHostPort;
        static int serverPort;

        @Parameters(index = "3", description = "snapshot period")
        static int snapshotPeriod;

        @Parameters(index = "4", description = "group id")
        static String groupId;


        static private Context.Key<SocketAddress> REMOTE_ADDR = Context.key("REMOTE_ADDR");

//        static boolean isHead = false;
//
//        static boolean isTail = false;
//
//        static List<String> replicas;
//
//        static int lastXid = -1;
//
//        static long lastZxidSeen;
//        static int lastAck = -1;

        //static private ReentrantLock lock = new ReentrantLock();


        static ConcurrentHashMap<String, Integer> clientRequestXidMap = new ConcurrentHashMap<>();

        static ConcurrentHashMap<String, Integer> hashTable = new ConcurrentHashMap<>();

        static ConcurrentHashMap<ClientXid, StreamObserver<IncResponse>> incResponseObserverMap = new ConcurrentHashMap<>();

        static ConcurrentHashMap<ClientXid, StreamObserver<GetResponse>> getResponseObserverMap = new ConcurrentHashMap<>();

        //static ArrayList<ClientXid> servingClientXids = new ArrayList<>();


        static int DEADLINE = 10;

        static int LOCK_WAIT = 10;

        static String serverInfo = "";

        static Server server;

        static String operationsTopicName, snapshotTopicName, snapshotOrderingTopicName;

        static KafkaProducer<String, byte[]> producer;
        static KafkaConsumer<String, byte[]> operationsConsumer,snapshotConsumer
                ,snapshotOrderingConsumer;
        //KafkaProducer<String, byte[]> snapshotProducer;

        static long operationsStartOffset = 0;
        static long snapshotStartOffset = 0;
        static long snapshotOrderingStartOffset = 0;
        static boolean isOperationsTopicSubscribed = true;
        static boolean isSnapshotTopicSubscribed = true;
        static boolean isSnapshotOrderingTopicSubscribed = true;




        @Override
        public Integer call() throws Exception {
            serverPort = Integer.valueOf(serverHostPort.split(":")[1]);
            System.out.printf("listening on %d\n", serverPort);
            server = ServerBuilder.forPort(serverPort).intercept(new ServerInterceptor() {
                        @Override
                        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> sc, Metadata h, ServerCallHandler<ReqT, RespT> next) {
                            var remote = sc.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
                            return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDR, remote), sc, h, next);
                        }
                    }).addService(new KafkaTableService())
                    .addService(new KafkaTableDebugService()).build();
            server.start();
            server.awaitTermination();
            return 0;
        }

        static class KafkaTableService extends KafkaTableGrpc.KafkaTableImplBase {


            void gotoOffset(KafkaConsumer<String, byte[]> consumer, TopicPartition topicPartition, long offset) {
                try {
                    consumer.seek(topicPartition, offset);
                } catch (IllegalStateException e) {
                    consumer.poll(Duration.ofSeconds(1));
                    gotoOffset(consumer, topicPartition, offset);
                }
//                finally{
//                    consumer.seek(topicPartition, offset);
//                }
            }

            private void setupProducer() {
                var properties = new Properties();
                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
                //snapshotProducer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
                //snapshotOrderingProducer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());

            }

            private void setupConsumers() {
                System.out.println("Topic names as follows: " + operationsTopicName + " "
                        + snapshotTopicName + " " + snapshotOrderingTopicName);
                var properties = new Properties();
                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, replicaName + "operations");
                operationsConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
//                operationsConsumer.subscribe(List.of(operationsTopicName));
//                var dummyPoll = operationsConsumer.poll(Duration.ofSeconds(1));
//
//                snapshotConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
//                snapshotConsumer.subscribe(List.of(snapshotTopicName));
//                dummyPoll = snapshotConsumer.poll(Duration.ofSeconds(1));
//
//                snapshotOrderingConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
//                snapshotOrderingConsumer.subscribe(List.of(snapshotOrderingTopicName));
//                dummyPoll = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
                operationsConsumer.subscribe(List.of(operationsTopicName), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        System.out.println("Didn't expect the revoke!");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        System.out.println("Operations Partition assigned");
                        //collection.forEach(t -> operationsConsumer.seek(t, 0));

                        //semOperations.release();
                    }
                });
                //var dummyPoll = operationsConsumer.poll(Duration.ofSeconds(1));
                //if(!dummyPoll.isEmpty()) {
                TopicPartition partition1 = new TopicPartition(operationsTopicName, 0);
                //operationsConsumer.seek(partition1, operationsStartOffset);
                gotoOffset(operationsConsumer, partition1, operationsStartOffset);
                //}
                //System.out.println("operations dummy poll empty?: "+dummyPoll.isEmpty());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, replicaName + "snapshotOrdering");
                snapshotOrderingConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
                snapshotOrderingConsumer.subscribe(List.of(snapshotOrderingTopicName), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        System.out.println("Didn't expect the revoke!");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        System.out.println("SnapshotOrdering Partition assigned");
//                        collection.forEach(t -> snapshotOrderingConsumer.seek(t, 0));
//                        TopicPartition partition1 = new TopicPartition(snapshotOrderingTopicName, 0);
//                        snapshotOrderingConsumer.seek(partition1, 0);
                        //semSnaphotOrdering.release();
                    }
                });
                //dummyPoll = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
                //if(!dummyPoll.isEmpty()) {
                partition1 = new TopicPartition(snapshotOrderingTopicName, 0);
                //snapshotOrderingConsumer.seek(partition1, snapshotOrderingStartOffset);
                gotoOffset(snapshotOrderingConsumer, partition1, snapshotOrderingStartOffset);
//                /}
                //System.out.println("snapshotOrdering dummy poll empty?: "+dummyPoll.isEmpty());

                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, replicaName + "snapshot");
                snapshotConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
                snapshotConsumer.subscribe(List.of(snapshotTopicName), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        System.out.println("Didn't expect the revoke!");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        System.out.println("Snapshot Partition assigned");
//                        collection.forEach(t -> snapshotConsumer.seek(t, 0));
//                        TopicPartition partition1 = new TopicPartition(snapshotTopicName, 0);
//                        snapshotConsumer.seek(partition1, 0);
                        //semSnapshot.release();
                    }
                });
                //dummyPoll = snapshotConsumer.poll(Duration.ofSeconds(1));
                //if(!dummyPoll.isEmpty()) {
                partition1 = new TopicPartition(snapshotTopicName, 0);
                //snapshotConsumer.seek(partition1, snapshotStartOffset);
                gotoOffset(snapshotConsumer, partition1, snapshotStartOffset);
                //}
                //System.out.println("snapshot dummy poll empty?: "+dummyPoll.isEmpty());

            }

            void setupState() throws InvalidProtocolBufferException {
                System.out.println("Setting up replica State...");
                var records = snapshotConsumer.poll(Duration.ofSeconds(1));
                ConsumerRecord<String, byte[]> latestRecord = null;
                for (var record : records) {
                    System.out.println("Record topic: "+record.topic());
                    System.out.println(record.headers());
                    System.out.println(record.timestamp());
                    System.out.println(record.timestampType());
                    System.out.println(record.offset());
                    latestRecord = record;
                }
                if(latestRecord == null){
                    System.out.println("Snapshot topic empty");
                    return;
                }
                Snapshot snapshot = Snapshot.parseFrom(latestRecord.value());
                System.out.println(snapshot);
                operationsStartOffset = snapshot.getOperationsOffset() + 1;
                gotoOffset(operationsConsumer, new TopicPartition(operationsTopicName, 0),
                        operationsStartOffset);
                snapshotOrderingStartOffset = snapshot.getSnapshotOrderingOffset() + 1;
                gotoOffset(snapshotOrderingConsumer, new TopicPartition(snapshotOrderingTopicName, 0),
                        snapshotOrderingStartOffset);
                clientRequestXidMap.putAll(snapshot.getClientCountersMap());
                System.out.println("Client counter map is: "+clientRequestXidMap);
                hashTable.putAll(snapshot.getTableMap());
                System.out.println("Hash table is: "+hashTable);


            }


            public KafkaTableService() throws IOException {
                operationsTopicName = groupId + "operations";
                snapshotTopicName = groupId + "snapshot";
                snapshotOrderingTopicName = groupId + "snapshotOrdering";
                setupProducer();
                setupConsumers();
                try {
                    setupState();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                addSelfToSnapshotOrdering();
                new Thread(() -> {
                    try {
                        consumeOperations();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();

//                new Thread(() -> {
//                    try {
//                        consumeSnapshotOrdering();
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new RuntimeException(e);
//                    }
//                }).start();

//                var records = snapshotConsumer.poll(Duration.ofSeconds(1));
//                Snapshot latestSnapshot = null;
//                for (var record : records) {
//                    try {
//                        latestSnapshot = Snapshot.parseFrom(record.value());
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                hashTable = (ConcurrentHashMap<String, Integer>) latestSnapshot.getTableMap();
//

            }

//            private void checkSnapshotOrderingToAddSelf() throws InvalidProtocolBufferException {
//                SnapshotOrdering orderingMsg = SnapshotOrdering.newBuilder().setReplicaId(replicaName).build();
//                var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
//                Boolean present = false;
//                for (var record : records) {
//                    System.out.println("Record topic: "+record.topic());
//                    System.out.println(record.headers());
//                    System.out.println(record.timestamp());
//                    System.out.println(record.timestampType());
//                    System.out.println(record.offset());
//                    SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
//                    var snapshotOrderingReplicaName = message.getReplicaId();
//                    if(snapshotOrderingReplicaName.equals((replicaName))){
//                        present = true;
//                        break;
//                    }
//                }
//                if(!present){
//                    addSelfToSnapshotOrdering();
//                }
//                else{
//                    System.out.println("I am already present in the snapshotOrdering topic");
//                }
//                gotoOffset(snapshotOrderingConsumer, new TopicPartition(snapshotOrderingTopicName, 0),
//                        snapshotOrderingStartOffset);
//
//
//            }

            private void addSelfToSnapshotOrdering() throws IOException {
                SnapshotOrdering orderingMsg = SnapshotOrdering.newBuilder().setReplicaId(replicaName).build();
                long offsetBeforeCheck = snapshotOrderingConsumer.position(new TopicPartition(snapshotOrderingTopicName, 0));
                boolean present = false;
                var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    System.out.println("Record topic: "+record.topic());
                    System.out.println(record.headers());
                    System.out.println(record.timestamp());
                    System.out.println(record.timestampType());
                    System.out.println(record.offset());
                    SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                    var snapshotOrderingReplicaName = message.getReplicaId();
                    if(snapshotOrderingReplicaName.equals((replicaName))){
                        System.out.println("I am already present in the snapshotOrdering topic.");
                        present = true;
                        break;
                    }
                }
                if(!present) {
                    publish(snapshotOrderingTopicName, orderingMsg.toByteArray());
                    System.out.println("Added myself to snapshot ordering...");
                }
                gotoOffset(snapshotOrderingConsumer, new TopicPartition(snapshotOrderingTopicName, 0),
                        offsetBeforeCheck);
            }

            private void updateHashTable(String key, int incValue) {
                if (!hashTable.containsKey(key)) {
                    hashTable.put(key, 0);
                }
                if (hashTable.get(key) + incValue >= 0) {
                    hashTable.put(key, hashTable.get(key) + incValue);
                }
                // TODO: Ask what happens when initial value for key is less than 0
            }

            private void applyIncRequest(IncRequest incRequest) {
                var clientXid = incRequest.getXid();
//                if (!isNewClientRequest(clientXid)) {
//                    System.out.println("Client IncRequest has old Xid. responding immediately...");
//                    return;
//                }
                updateHashTable(incRequest.getKey(), incRequest.getIncValue());
                updateClientXid(clientXid);
            }

            private void applyGetRequest(GetRequest getRequest) {
                var clientXid = getRequest.getXid();
//                if (!isNewClientRequest(clientXid)) {
//                    System.out.println("Client GetRequest has old Xid. responding immediately...");
//                    return;
//                }
                //updateHashTable(incRequest.getKey(), incRequest.getIncValue());
                var key = getRequest.getKey();
                if (!hashTable.containsKey(key)) {
                    hashTable.put(key, 0);
                }
                updateClientXid(clientXid);
            }


            void publish(String topicName, byte[] msg) throws IOException {
                //var properties = new Properties();
                //properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                //varproducer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
                var record = new ProducerRecord<String, byte[]>(topicName, msg);
                producer.send(record);
            }

            void consumeOperations() throws IOException {
                while (isOperationsTopicSubscribed) {
                    var records = operationsConsumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        System.out.println("Record topic: "+record.topic());
                        System.out.println(record.headers());
                        System.out.println(record.timestamp());
                        System.out.println(record.timestampType());
                        System.out.println(record.offset());
                        PublishedItem message = PublishedItem.parseFrom(record.value());
                        if (message.hasInc()) {
                            //System.out.println("PublishedItem is proper");
                            IncRequest incRequest = message.getInc();
                            if (!isNewClientRequest(incRequest.getXid())) {
                                System.out.println("Client IncRequest has old Xid. Not processing");
                            } else {
                                applyIncRequest(incRequest);
                                //consumeSnapshotOrdering();
                            }
                        } else {
                            GetRequest getRequest = message.getGet();
                            if (!isNewClientRequest(getRequest.getXid())) {
                                System.out.println("Client GetRequest has old Xid. Not processing");
                            } else {
                                applyGetRequest(getRequest);
                            }
                        }
                        if ((record.offset() + 1) % snapshotPeriod == 0) {
                            System.out.println("Snapshot Period reached. Consuming ordering");
                            onSnapshotTriggered();
                        }
                        shouldRespond(message, message.hasInc());
                    }
                }
            }

            private void onSnapshotTriggered() throws IOException {
                String nextReplica = "";
                try {
                    //consumeSnapshotOrdering();
                    nextReplica = getNextReplicaInSnapshotOrdering();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("Replica to take a snapshot is: " + nextReplica);
                if (nextReplica.equals(replicaName)) {
                    takeSnapshot();
                    addSelfToSnapshotOrdering();
                }
            }

            private void takeSnapshot() {
                TopicPartition partition = new TopicPartition(operationsTopicName, 0);
                var operationsOffset = operationsConsumer.position(partition) - 1;
                System.out.println("Snapshot taken at offsets....");
                System.out.println("operationsOffset: " + operationsOffset);
                partition = new TopicPartition(snapshotOrderingTopicName, 0);
                var snapshotOrderingOffset = snapshotOrderingConsumer.position(partition) - 1;
                System.out.println("snapshotOrdering offset: " + snapshotOrderingOffset);
                Snapshot snapshot = Snapshot.newBuilder().setReplicaId(replicaName)
                        .putAllTable(hashTable).setOperationsOffset(operationsOffset)
                        .putAllClientCounters(clientRequestXidMap)
                        .setSnapshotOrderingOffset(snapshotOrderingOffset).build();
                byte[] snapshotInBytes = snapshot.toByteArray();
                try {
                    publish(snapshotTopicName, snapshotInBytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private String getNextReplicaInSnapshotOrdering() throws InvalidProtocolBufferException {
                System.out.println("consuming from ordering...");
//                TopicPartition partition1 = new TopicPartition(snapshotOrderingTopicName, 0);
//                snapshotOrderingConsumer.seek(partition1, 0);
                String nextReplica = "";
                while (nextReplica.equals("")) {
                    var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
                    System.out.println("polling done...");
                    long offset = 0;
                    for (var record : records) {
                        System.out.println("Record topic: "+record.topic());
                        System.out.println(record.headers());
                        System.out.println(record.timestamp());
                        System.out.println(record.timestampType());
                        System.out.println(record.offset());
                        TopicPartition partition = new TopicPartition(snapshotOrderingTopicName, 0);
                        snapshotOrderingConsumer.seek(partition, record.offset() + 1);
                        SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                        nextReplica = message.getReplicaId();
                        break;
                    }
                }
                return nextReplica;
            }

            void consumeSnapshotOrdering() throws InvalidProtocolBufferException {
                System.out.println("consuming snapshot ordering...");
                while (isSnapshotOrderingTopicSubscribed) {
                    //System.out.println("consuming from ordering...");
                    var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
                    //System.out.println("consuming from ordering...");
                    //SnapshotOrdering message = null;
                    for (var record : records) {
                        System.out.println("Record topic: "+record.topic());
                        System.out.println(record.headers());
                        System.out.println(record.timestamp());
                        System.out.println(record.timestampType());
                        System.out.println(record.offset());
                        SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                        System.out.println(message.getReplicaId());
                        //break;
                    }
                }
            }

            private void shouldRespond(PublishedItem message, boolean isIncRequest) {
                if (isIncRequest) {
                    IncRequest incRequest = message.getInc();
                    var clientXid = incRequest.getXid();
                    if (incResponseObserverMap.containsKey(clientXid)) {
                        var responseObserver = incResponseObserverMap.get(clientXid);
                        try {
                            responseObserver.onNext(IncResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        } catch (IllegalStateException e) {
                            System.out.println("catching stream exception...");
                        }
                    }
                } else {
                    GetRequest getRequest = message.getGet();
                    var clientXid = getRequest.getXid();
                    if (getResponseObserverMap.containsKey(clientXid)) {
                        var responseObserver = getResponseObserverMap.get(clientXid);
                        Integer valueRequested = hashTable.get(getRequest.getKey());
                        responseObserver.onNext(GetResponse.newBuilder().setValue(valueRequested).build());
                        responseObserver.onCompleted();
                    }

                }
            }


            private boolean isNewClientRequest(ClientXid clientXid) {
                var clientId = clientXid.getClientid();
                var counter = clientXid.getCounter();
                //clientRequestXidMap.put(clientId, counter);
                return !clientRequestXidMap.containsKey(clientId) || clientRequestXidMap.get(clientId) < counter;
            }

            private void updateClientXid(ClientXid clientXid) {
                var clientId = clientXid.getClientid();
                var counter = clientXid.getCounter();
                clientRequestXidMap.put(clientId, counter);
            }


            @Override
            public void inc(IncRequest request,
                            StreamObserver<IncResponse> responseObserver) {
                System.out.println("Received an inc Request.");
                ClientXid clientXid = request.getXid();
                if (!isNewClientRequest(clientXid)) {
                    System.out.println("Client Request has old Xid. responding immediately...");
                    responseObserver.onNext(IncResponse.newBuilder().build());
                    responseObserver.onCompleted();
                    return;
                }
                ;
                //servingClientXids.add(clientXid);
                incResponseObserverMap.put(clientXid, responseObserver);
                PublishedItem incPublish = PublishedItem.newBuilder().setInc(request).build();
                //var incRequestInBytes = incPublish.toByteArray();
                try {
                    publish(operationsTopicName, incPublish.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void get(GetRequest request,
                            StreamObserver<GetResponse> responseObserver) {
                System.out.println("Received a get Request.");
                ClientXid clientXid = request.getXid();
                if (!isNewClientRequest(clientXid)) {
                    System.out.println("Client Request has old Xid. responding immediately...");
                    Integer valueRequested = hashTable.get(request.getKey());
                    responseObserver.onNext(GetResponse.newBuilder().setValue(valueRequested).build());
                    responseObserver.onCompleted();
                    return;
                }
                ;
                //servingClientXids.add(clientXid);
                getResponseObserverMap.put(clientXid, responseObserver);
                PublishedItem getPublish = PublishedItem.newBuilder().setGet(request).build();
                //var getRequestInBytes = getPublish.toByteArray();
                try {
                    publish(operationsTopicName, getPublish.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            }


        }

        static class KafkaTableDebugService extends KafkaTableDebugGrpc.KafkaTableDebugImplBase {

            @Override
            public void debug(KafkaTableDebugRequest request,
                              StreamObserver<KafkaTableDebugResponse> responseObserver) {

            }

            @Override
            public void exit(ExitRequest request,
                             StreamObserver<ExitResponse> responseObserver) {
                operationsConsumer.unsubscribe();
                snapshotConsumer.unsubscribe();
                snapshotOrderingConsumer.unsubscribe();
                isSnapshotOrderingTopicSubscribed = false;
                isOperationsTopicSubscribed = false;
                isSnapshotTopicSubscribed = false;
                System.out.println("Exiting...");
                server.shutdown();
            }


        }

    }

    @Command(name = "client", mixinStandardHelpOptions = true, description = "start a KafkaTable Client.")
    static class ClientCli implements Callable<Integer> {

        @Parameters(index = "0", description = "client id")
        static String clientId;

//        @Parameters(index = "1", description = "comma separated list of servers to use.")
//        static String serverPorts;

//        static String[] servers;

        @Command
        int get(@Parameters(paramLabel = "key") String key,
                @Parameters(paramLabel = "grpcHost:port") String server) {
            var clientXid = ClientXid.newBuilder().setClientid(clientId).setCounter((int) (System.currentTimeMillis() / 1000)).build();
            System.out.println(clientXid);
            var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
            var rsp = stub.get(GetRequest.newBuilder().setKey(key).setXid(clientXid).build());
            System.out.println(rsp.getValue());
            return 0;
        }

        @Command
        void inc(@Parameters(paramLabel = "key") String key,
                 @Parameters(paramLabel = "amount") int amount,
                 @Parameters(paramLabel = "grpcHost:port") String serverList,
                 @Option(names = "--repeat") boolean repeat,
                 @Option(names = "--concurrent") boolean concurrent) {
            String[] servers = serverList.split(",");
            int count = repeat ? 2 : 1;
            var clientXid = ClientXid.newBuilder().setClientid(clientId).setCounter((int) (System.currentTimeMillis() / 1000)).build();
            System.out.println(clientXid);
            System.out.println("server to connect to: " + Arrays.toString(servers));
            //var server = servers[0];
            for (int i = 0; i < count; i++) {
                var s = Arrays.stream(servers);
                if (concurrent) s = s.parallel();
                var result = s.map(server -> {
                    var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
                    try {
                        stub.inc(IncRequest.newBuilder().setKey(key).setIncValue(amount).setXid(clientXid).build());
                        return server + ": success";
                    } catch (Exception e) {
                        return server + ": " + e.getMessage();
                    }
                }).collect(Collectors.joining(", "));
//                var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
//                //var result = "";
//                try {
//                    stub.inc(IncRequest.newBuilder().setKey(key).setIncValue(amount).setXid(clientXid).build());
//                    System.out.println(server + ": success");
//                } catch (Exception e) {
//                    System.out.println(server + ": " + e.getMessage());
//                }
                System.out.println(result);
            }
        }


        @Override
        public Integer call() throws Exception {
            //servers = serverPorts.split(",");
            //System.out.println("server to connect to: "+ Arrays.toString(servers));
            return 0;
        }

    }

    @Command(name = "test-kafka", mixinStandardHelpOptions = true, description = "start a test client.")
    static class TestCli implements Callable<Integer> {

        @Command
        int publish(@Parameters(paramLabel = "kafkaHost:port") String server,
                    @Parameters(paramLabel = "topic-name") String name) throws IOException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
            var br = new BufferedReader(new InputStreamReader(System.in));
            for (int i = 0; ; i++) {
                var line = br.readLine();
                if (line == null) break;
                var bytes = SimpleMessage.newBuilder()
                        .setMessage(line)
                        .build().toByteArray();
                var record = new ProducerRecord<String, byte[]>(name, bytes);
                producer.send(record);
            }
            return 0;
        }

        @Command
        int consume(@Parameters(paramLabel = "kafkaHost:port") String server,
                    @Parameters(paramLabel = "topic-name") String name,
                    @Parameters(paramLabel = "group-id") String id) throws InvalidProtocolBufferException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
            var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
            System.out.println("Starting at " + new Date());
            var sem = new Semaphore(0);
            consumer.subscribe(List.of(name), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    System.out.println("Didn't expect the revoke!");
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    System.out.println("Partition assigned");
                    collection.stream().forEach(t -> consumer.seek(t, 0));
                    //consumer.seekToEnd(collection);
                    sem.release();
                }
            });
            System.out.println("first poll count: " + consumer.poll(0).count());
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Ready to consume at " + new Date());
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(20));
                System.out.println("Got: " + records.count());
                for (var record : records) {
                    System.out.println("Record topic: "+record.topic());
                    System.out.println(record.headers());
                    System.out.println(record.timestamp());
                    System.out.println(record.timestampType());
                    System.out.println(record.offset());
                    var message = SimpleMessage.parseFrom(record.value());
                    System.out.println(message);
                }
            }
        }

        @Command
        int listTopics(@Parameters(paramLabel = "kafkaHost:port") String server) throws ExecutionException, InterruptedException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            try (var admin = Admin.create(properties)) {
                var rc = admin.listTopics();
                var listings = rc.listings().get();
                for (var l : listings) {
                    System.out.println(l);
                }
            }
            return 0;
        }

        @Command
        int createTopic(@Parameters(paramLabel = "kafkaHost:port") String server,
                        @Parameters(paramLabel = "topic-name") String name) throws InterruptedException, ExecutionException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            try (var admin = Admin.create(properties)) {
                var rc = admin.createTopics(List.of(new NewTopic(name, 1, (short) 1)));
                rc.all().get();
            }
            return 0;
        }

        @Command(description = "delete the operations, snapshotOrder, and snapshot topics for a given prefix")
        int deleteTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                              @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            try (var admin = Admin.create(properties)) {
                List<String> topics = List.of(
                        prefix + "operations",
                        prefix + "snapshot",
                        prefix + "snapshotOrdering"
                );
                admin.deleteTopics(topics);
                System.out.println("deleted topics: " + Arrays.toString(topics.toArray()));
            }
            return 0;
        }

        @Command(description = "create the operations, snapshotOrder, and snapshot topics for a given prefix")
        int createTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                              @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
            var properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            try (var admin = Admin.create(properties)) {
                var rc = admin.createTopics(List.of(
                        new NewTopic(prefix + "operations", 1, (short) 1),
                        new NewTopic(prefix + "snapshot", 1, (short) 1),
                        new NewTopic(prefix + "snapshotOrdering", 1, (short) 1)
                ));
                rc.all().get();
            }
            var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
            var result = producer.send(new ProducerRecord<>(prefix + "snapshot", Snapshot.newBuilder()
                    .setReplicaId("initializer")
                    .setOperationsOffset(-1)
                    .setSnapshotOrderingOffset(-1)
                    .putAllTable(Map.of())
                    .putAllClientCounters(Map.of())
                    .build().toByteArray()));
            result.get();
            return 0;
        }


        @Override
        public Integer call() throws Exception {
            return 0;
        }

    }

    @Command(subcommands = {ServerCli.class, ClientCli.class, TestCli.class})
    static class Cli {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }


//    public static void main(String[] args) {
//        System.exit(new CommandLine(new Main()).execute(args));
//    }
}