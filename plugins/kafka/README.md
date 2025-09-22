# ECL Apache Kafka Plugin

This is the ECL plugin to access [Apache Kafka](https://kafka.apache.org), a
publish-subscribe messaging system.  ECL string data can be both published to
and consumed from Apache Kafka brokers.

Client access is via a third-party C++ plugin,
[librdkafka](https://github.com/edenhill/librdkafka).

## Installation and Dependencies

[librdkafka](https://github.com/edenhill/librdkafka) is included as a git
submodule in HPCC-Platform.  It will be built and integrated automatically when
you build the HPCC-Platform project.

The recommended method for obtaining Apache Kafka is via
[download](https://kafka.apache.org/downloads.html).

Note that Apache Kafka has its own set of dependencies, most notably
[zookeeper](https://zookeeper.apache.org).  The Kafka download file does contain
a Zookeeper installation, so for testing purposes you need to download only
Apache Kafka and follow the excellent
[instructions](https://kafka.apache.org/documentation.html#quickstart).  Those
instructions will tell you how to start Zookeeper and Apache Kafka, then test
your installation by creating a topic and interacting with it.

*Note:* Apache Kafka version 0.8.2 or later is recommended.

## Plugin Configuration

*Expand the section that matches your runtime environment (bare metal vs containerized)*

\<details>
\<summary>Configuring in a Bare Metal Environment\</summary>

The Apache Kafka plugin uses sensible default configuration values but these can
be modified via configuration files.

There are two types of configurations:  Global and per-topic.  Some
configuration parameters are applicable only to publishers (producers, in Apache
Kafka's terminology), others only to consumers, and some to both.  Details on
the supported configuration parameters can be found on the [librdkafka
configuration
page](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

A configuration file is a simple text document with a series of key/value
parameters, formatted like:

    key=value
    key=value
    ...
    key=value

A '#' character at the beginning of a line denotes a comment.  Note that this is
the only kind of comment supported in configuration files.

Whenever a new connection is created (either publisher or consumer) the plugin
will scan for configuration files.  All configuration files will reside in the
HPCC configuration directory, which is `/etc/HPCCSystems`.  The global
configuration file should be named `kafka_global.conf`.  Per-topic configuration
files are also supported, and they can be different for a publisher or a
consumer.  For a publisher, the naming convention is
`kafka_publisher_topic_\<TopicName\>.conf` and for a consumer it is
`kafka_consumer_topic_\<TopicName\>.conf`.  In both cases, `\<TopicName\>` is the
name of the topic you are publishing to or consuming from.

Settings that affect the protocol used to connect to the Kafka broker (such as
using SSL) should be placed only in the global configuration file, not in
any per-topic configuration file.

Configuration parameters loaded from a file override those set by the plugin
with one exception:  the `metadata.broker.list` setting, if found in a
configuration file, is ignored.  Apache Kafka brokers are always set in ECL.

The following configuration parameters are set by the plugin for publishers,
overriding their normal default values:

    queue.buffering.max.messages=1000000
    compression.codec=snappy
    message.send.max.retries=3
    retry.backoff.ms=500

The following configuration parameters are set by the plugin for consumers,
overriding their normal default values:

    compression.codec=snappy
    queued.max.messages.kbytes=10000000
    fetch.message.max.bytes=10000000
    auto.offset.reset=smallest
\</details>

\<details>
\<summary>Configuring in a Containerized Environment\</summary>

The Apache Kafka plugin uses sensible default configuration values but these can
be modified via configuration entries in the HPCC Systems Helm chart.

Configuration entries can be placed in the global section of your Helm chart (which
means that the settings are applied everywhere) or they can be placed within
a component (meaning, they apply to only that component). The latter is useful
for settings things differently for differently-configured Thors, for instance.

Note that global and per-component configuration settings are **merged**. Per-
component settings override global settings. Further details regarding the merge
are at the end of this section.

There are two types of configurations:  Global and per-topic.  Some
configuration parameters are applicable only to publishers (producers, in Apache
Kafka's terminology), others only to consumers, and some to both.  Details on
the supported configuration parameters can be found on the [librdkafka
configuration
page](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

A configuration block is a Helm entry with a series of key/value
parameters, formatted like this:

    plugins:
      kafka:
        global:
          - name: key
            value: value
          - name: key
            value: value
          ...
          - name: key
            value: value

This block can be added to the globals section of the Helm chart or
to a specific component. 

Whenever a new connection is created (either publisher or consumer) the plugin
will scan for configuration entries.  The global
configuration block should be named `global`.  Per-topic configuration
blocks are also supported, and they can be different for a publisher or a
consumer.  For a publisher, the naming convention is
`publisher_topic_\<TopicName\>` and for a consumer it is
`consumer_topic_\<TopicName\>`.  In both cases, `\<TopicName\>` is the
name of the topic you are publishing to or consuming from.

Settings that affect the protocol used to connect to the Kafka broker (such as
using SSL) should be placed only in the global configuration block, not in
any per-topic configuration block.

Configuration parameters loaded from a Helm chart override those set by the plugin
with one exception:  the `metadata.broker.list` setting, if found in a
configuration block, is ignored.  Apache Kafka brokers are always set in ECL.

If configuration blocks are found in multiple locations, their keys/values are **merged**.
The order of merging is:

    1. Kafka global settings from the chart's Global/plugins section
    2. Kafka global settings from the per-component section
    3. Kafka consumer/producer settings from the chart's Global/plugins section
    4. Kafka consumer/producer settings from the per-component section

The following configuration parameters are set by the plugin for publishers,
overriding their normal default values:

    - name: queue.buffering.max.messages
      value: 1000000
    - name: compression.codec
      value: snappy
    - name: message.send.max.retries
      value: 3
    - name: retry.backoff.ms
      value: 500

The following configuration parameters are set by the plugin for consumers,
overriding their normal default values:

    - name: compression.codec
      value: snappy
    - name: queued.max.messages.kbytes
      value: 10000000
    - name: fetch.message.max.bytes
      value: 10000000
    - name: auto.offset.reset
      value: smallest
\</details>

## Publishing messages with the plugin

Publishing string messages begins with instantiating an ECL module that defines
the Apache Kafka cluster and the topic into which the messages will be posted. 
The definition of the module is:

    KafkaPublisher(VARSTRING topic, VARSTRING brokers = 'localhost') := MODULE
        ...
    END

The module requires you to designate a topic by name and, optionally, at least
one Apache Kafka broker.  The format of the broker is `BrokerName[:port]` where
`BrokerName` is either an IP address or a DNS name of a broker.  You can
optionally include a port number if the default Apache Kafka broker port is not
used.  Multiple brokers can be listed, separated by commas.  Only one broker in
an Apache Kafka cluster is required; the rest can be discovered once a
connection is made.

Example instantiating a publishing module:

    p := kafka.KafkaPublisher('MyTopic', '10.211.55.13');

The module contains an exported function for publishing a message, defined as:

    BOOLEAN PublishMessage(CONST UTF8 message, CONST UTF8 key = '');

The module function requires a string message and allows you to specify a 'key'
that affects how Apache Kafka stores the message.  Key values act a lot like the
expression argument in ECL's DISTRIBUTE() function:  Messages with the same key
value wind up on the same Apache Kafka partition within the topic.  This can
affect how consumers retrieve the published messages.  More details regarding
partitions and how keys are used can be found Apache Kafka's
[introduction](https://kafka.apache.org/documentation.html#introduction).  If a
key value is not supplied than the messages are distributed among the available
partitions for that topic.

Examples:

    p.PublishMessage('This is a test message');
    p.PublishMessage('A keyed message', 'MyKey');
    p.PublishMessage('Another keyed message', 'MyKey');

Note that keys are not retrieved by the ECL Apache Kafka consumer.  They are
used only to determine how the messages are stored.

You can find out how many partitions are available in a publisher's topic by
calling the following module function:

    partitionCount := p.GetTopicPartitionCount();

`GetTopicPartitionCount()` returns zero if the topic has not been created or
there are has been an error.

## Consuming messages with the plugin

As with publishing, consuming string messages begins with instantiating an ECL
module that defines the Apache Kafka cluster and the topic from which the
messages will be read.  The definition of the module is:

    KafkaConsumer(VARSTRING topic,
                  VARSTRING brokers = 'localhost',
                  VARSTRING consumerGroup = 'hpcc') := MODULE
        ...
    END

The module requires you to designate a topic by name.  Optionally, you may also
cite at least one Apache Kafka broker and a consumer group.  The format and
requirements for a broker are the same as for instantiating a KafkaPublisher
module.  Consumer groups in Apache Kafka allow multiple consumer instances, like
Thor nodes, to form a "logical consumer" and be able to retrieve messages in
parallel and without duplication.  See the "Consumers" subtopic in Apache
Kafka's [introduction](https://kafka.apache.org/documentation.html#introduction)
for more details.

Example:

    c := kafka.KafkaConsumer('MyTopic', '10.211.55.13');

The module contains an exported function for consuming messages, defined as:

    DATASET(KafkaMessage) GetMessages(INTEGER4 maxRecords);

This function returns a new dataset containing messages consumed by the topic
defined in the module.  The layout for that dataset is:

    KafkaMessage := RECORD
        UNSIGNED4   partition;
        INTEGER8    offset;
        UTF8        message;
    END;

Example retrieving up to 10,000 messages:

    myMessages := c.GetMessages(10000);

After you consume some messages it may be beneficial to track the last-read
offset from each Apache Kafka topic partition.  The following module function
does that:

    DATASET(KafkaMessageOffset) LastMessageOffsets(DATASET(KafkaMessage) messages);

Basically, you pass in the just-consumed message dataset to the function and get
back a small dataset containing just the partition numbers and the last-read
message's offset.  The layout of the returned dataset is:

    KafkaMessageOffset := RECORD
        UNSIGNED4   partitionNum;
        INTEGER8    offset;
    END;

Example call:

    myOffsets := c.LastMessageOffsets(myMessages);

If you later find out that you need to "rewind" your consumption -- read old
messages, in other words -- you can use the data within a KafkaMessageOffset
dataset to reset your consumers, making the next `GetMessages()` call pick up
from that point.  Use the following module function to reset the offsets:

    UNSIGNED4 SetMessageOffsets(DATASET(KafkaMessageOffset) offsets);

The function returns the number of partitions reset (which should equal the
number of records you're handing the function).

Example call:

    numPartitionsReset := c.SetMessageOffsets(myOffsets);

You can easily reset all topic partitions to their earliest point with the
following module function:

    UNSIGNED4 ResetMessageOffsets();

This function returns the number of partitions reset.

Example call:

    numPartitionsReset := c.ResetMessageOffsets();

You can find out how many partitions are available in a consumers's topic by
calling the following module function:

    partitionCount := c.GetTopicPartitionCount();

`GetTopicPartitionCount()` returns zero if the topic has not been created or
there are has been an error.

## Complete ECL Examples

The following code will publish 100K messages to a topic named 'MyTestTopic' on
an Apache Kafka broker located at address 10.211.55.13.  If you are running a
single-node HPCC cluster and have installed Kafka on the same node, you can use
'localhost' instead (or omit the parameter, as it defaults to 'localhost').

### Publishing

    IMPORT kafka;

    MyDataLayout := RECORD
        UTF8    message;
    END;

    ds := DATASET
        (
            100000,
            TRANSFORM
                (
                    MyDataLayout,
                    SELF.message := U8'Test message ' + (UTF8)COUNTER
                ),
            DISTRIBUTED
        );

    p := kafka.KafkaPublisher('MyTestTopic', brokers := '10.211.55.13');

    APPLY(ds, ORDERED(p.PublishMessage(message)));

### Consuming

This code will read the messages written by the publishing example, above.  It
will also show the number of partitions in the topic and the offsets of the
last-read messages.

    IMPORT kafka;

    c := kafka.KafkaConsumer('MyTestTopic', brokers := '10.211.55.13');

    ds := c.GetMessages(200000);
    offsets := c.LastMessageOffsets(ds);
    partitionCount := c.GetTopicPartitionCount();

    OUTPUT(ds, NAMED('MessageSample'));
    OUTPUT(COUNT(ds), NAMED('MessageCount'));
    OUTPUT(offsets, NAMED('LastMessageOffsets'));
    OUTPUT(partitionCount, NAMED('PartitionCount'));

### Resetting Offsets

Resetting offsets is useful when you have a topic already published with
messages and you need to reread its messages from the very beginning.

    IMPORT kafka;

    c := kafka.KafkaConsumer('MyTestTopic', brokers := '10.211.55.13');

    c.ResetMessageOffsets();

## Behaviour and Implementation Details

### Partitioning within Apache Kafka Topics

Topic partitioning is covered in Apache Kafka's
[introduction](https://kafka.apache.org/documentation.html#introduction).  There
is a performance relationship between the number of partitions in a topic and
the size of the HPCC cluster when consuming messages.  Ideally, the number of
partitions will exactly equal the number of HPCC nodes consuming messages.  For
Thor, this means the total number of workers rather than the number of nodes, as
that can be different in a multi-worker setup.  For Roxie, the number is always
one.  If there are fewer partitions than nodes (workers) then not all of your
cluster will be utilized when consuming messages; if there are more partitions
than nodes (workers) then some nodes will be performing extra work, consuming
from multiple partitions.  In either mismatch case, you may want to consider
using the ECL DISTRIBUTE() function to redistribute your data before processing.

When messages are published without a 'key' argument to a topic that has more
than one partition, Apache Kafka will distribute those messages among the
partitions.  The distribution is not perfect.  For example, if you publish 20
messages to a topic with two partitions, one partition may wind up with 7
messages and the other with 13 (or some other mix of message counts that total
20).  When testing your code, be aware of this behavior and always request more
messages than you publish.  In the examples above, 100K messages were published
but up to 200K messages were requested.  This ensures that you receive all of
the messages you publish.  This is typically not an issue in a production
environment, as your requested consumption message count is more a function of
how much data you're willing to process in one step than with how many messages
are actually stored in the topic.

Be aware that, by default, Apache Kafka will automatically create a topic that
has never been seen before if someone publishes to it, and that topic will have
only one partition.  Both actions -- whether a topic is automatically created
and how many partitions it will have -- are configurable within Apache Kafka.

### Publisher Connections

This plugin caches the internal publisher objects and their connections. 
Publishing from ECL, technically, only writes the messages to a local cache. 
Those messages are batched and set to Apache Kafka for higher performance in a
background thread.  Because this batching can extend far beyond the time ECL
spends sending the data to the local cache, the objects (and their connections)
need to hang around for some additional time.  The upside is that the cached
objects and connections will be reused for subsequent publish operations,
speeding up the entire process.

### Consumer Connections

Unlike publisher objects, one consumer object is created per thread for each
connection.  A connection is to a specific broker, topic, consumer group, and
partition number combination.  The consumer objects and connections live only as
long as needed.

### Saved Topic Offsets

By default, in a bare-metal environment, consumers save to a file the offset of the
last-read message from a given topic, consumer group, and partition combination.
The offset is saved so that the next time the consumer is fired up for that
particular connection combination, the consumption process can pick up where it left
off.  The file is saved to the HPCC engine's data directory which is typically
`/var/lib/HPCCSystems/mythor/`, `/var/lib/HPCCSystems/myroxie/` or
`/var/lib/HPCCSystems/myeclagent/` depending on the engine you're using (the
exact path may be different if you have named an engine differently in your HPCC
configuration).  The format of the saved offset filename is
`\<TopicName\>-\<PartitionNum\>-\<ConsumerGroup\>.offset`.

Note that saving partition offsets is engine-specific in a bare-metal environment.
One practical consideration of this is that you cannot have one engine (e.g. Thor)
consume from a given topic and then have another engine (e.g. Roxie) consume the
next set of messages from that topic.  Both engines can consume messages without a
problem, but they will not track each other's last-read positions. Note that in a
containerized environment different engines will use each others' offsets provided
that they use the same consumer group.
