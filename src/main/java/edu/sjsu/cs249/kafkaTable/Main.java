//TODO: Update: 05/07: have set up operations inc and get. but need to fix client as error is popping up and then test if inc and get run as expected

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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

            KafkaProducer<String, byte[]> producer;
            KafkaConsumer<String, byte[]> operationsConsumer;
            KafkaConsumer<String, byte[]> snapshotConsumer;
            KafkaConsumer<String, byte[]> snapshotOrderingConsumer;

            private void setupProducer() {
                var properties = new Properties();
                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());

            }

            private void setupConsumers() {
                var properties = new Properties();
                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, replicaName);
                properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

                operationsConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
                operationsConsumer.subscribe(List.of(operationsTopicName));

                snapshotConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
                snapshotConsumer.subscribe(List.of(snapshotTopicName));

                snapshotOrderingConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
                snapshotOrderingConsumer.subscribe(List.of(snapshotOrderingTopicName));

            }


            public KafkaTableService() {
                operationsTopicName = groupId + "operations";
                snapshotTopicName = groupId + "snapshot";
                snapshotOrderingTopicName = groupId + "snapshotOrdering";
                setupProducer();
                setupConsumers();
                new Thread(() -> {
                    try {
                        consumeOperations();
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }).start();

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

            private void updateHashTable(String key, int incValue) {
                if (!hashTable.containsKey(key)){
                    hashTable.put(key, 0);
                }
                if (hashTable.get(key) + incValue >= 0){
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
                if(!hashTable.containsKey(key)){
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

            void consumeOperations() throws InvalidProtocolBufferException {
                while (true) {
                    var records = operationsConsumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        System.out.println(record.headers());
                        System.out.println(record.timestamp());
                        System.out.println(record.timestampType());
                        System.out.println(record.offset());
                        PublishedItem message = PublishedItem.parseFrom(record.value());
                        if(message.hasInc()){
                            System.out.println("PublishedItem is proper");
                            IncRequest incRequest = message.getInc();
                            applyIncRequest(incRequest);
                            shouldRespond(message, true);
                        }
                        else{
                            GetRequest getRequest = message.getGet();
                            applyGetRequest(getRequest);
                            shouldRespond(message, false);
                        }
                    }
                }
            }

            private void shouldRespond(PublishedItem message, boolean isIncRequest) {
                if(isIncRequest){
                    IncRequest incRequest = message.getInc();
                    var clientXid = incRequest.getXid();
                    if(incResponseObserverMap.containsKey(clientXid)){
                        var responseObserver = incResponseObserverMap.get(clientXid);
                        responseObserver.onNext(IncResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }
                }
                else{
                    GetRequest getRequest = message.getGet();
                    var clientXid = getRequest.getXid();
                    if(getResponseObserverMap.containsKey(clientXid)) {
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
                return !clientRequestXidMap.containsKey(clientId) || clientRequestXidMap.get(clientId) > counter;
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
                var incRequestInBytes = incPublish.toByteArray();
                try {
                    publish(operationsTopicName, incRequestInBytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void get(GetRequest request,
                            StreamObserver<GetResponse> responseObserver) {

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
                var getRequestInBytes = getPublish.toByteArray();
                try {
                    publish(operationsTopicName, getRequestInBytes);
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
                 @Parameters(paramLabel = "grpcHost:port") String[] servers,
                @Option(names = "--repeat") boolean repeat,
                @Option(names = "--concurrent") boolean concurrent) {
            //servers = serverPorts.split(",");
            int count = repeat ? 2 : 1;
            var clientXid = ClientXid.newBuilder().setClientid(clientId).setCounter((int) (System.currentTimeMillis() / 1000)).build();
            System.out.println(clientXid);
            System.out.println("server to connect to: "+ Arrays.toString(servers));
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
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
            var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
            consumer.subscribe(List.of(name));
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
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