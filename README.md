# hashamorphosis

The service provides an incrementable distributed hashtable using Kafka.

The service provides following APIs <br>
<code>get(key) - returns the current integer for the key, or zero if the key does not exist. </code> <br><br>
<code>increment(key, value) - increments the key by the given value. this operation will fail if the value would take the key below 0. there can be no negative values. Periodically a replica should snapshot the state of the table into the log to enable fast recovery of replicas or new replicas to come up to speed </code> <br>

The service also takes regular snapshots are provided interval period so that new joining replicas can set their state using the latest snapshot avoiding the need for them to read the Kafka topics from the start. <br>

<dl>
  <dt>Prerequisite:
  <dd> A kafka server cluster </dd> 
  </dt>
</dl>

The codebase consists of 3 services:

<dl>
  <dt>Client Service
    <dd>clients will use gRPC to make inc and get requests to a replica. Clients will have only one request outstanding. Each client will make a request with a monotonically increasing counter.<br>
      <code>Increment cmd: java -jar {application-jar} client {clientId} inc {key} {value} {replica-host:port}</code><br>
      <code>Get cmd: java -jar {application-jar} client {clientId} get {key} {replica-host:port}</code><br>
    </dd>
  </dt>
  <dt>Replica Service
    <dd>A replica receives a client request, publishes the request on Kafka if not duplicate and then updates its state. Finally, it responds back to the client The service also snapshots if it is its turn for snapshotting at the snapshot interval.<br>
      <code>cmd to run a replica service: java -jar {application-jar} replica {kafka-server} {replicaId} {host:port to run replica on} {snapshot period} {topic-prefix} </code> <br>
    </dd>
  </dt>
  <dt>Replica Debug Service
    <dd>Provides APIs for debugging replicas:<br>
      <code>Exit: causes replica to exit.
        cmd: java -jar {application-jar} test-debug-service exit {replica-host:port}</code> <br>
      <code>Debug: causes replica to respond with its snapshot.
        cmd: java -jar {application-jar} test-debug-service debug {replica-host:port}</code> <br>
    </dd>
  </dt>
</dl>


