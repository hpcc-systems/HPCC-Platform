/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=embedded
//class=3rdparty
//class=3rdpartyservice

IMPORT kafka;
IMPORT Std;

/*******************************************************************************
 * These tests assume a Kafka instance running on the local host with a default
 * Kafka configuration.  Each iteration of the test creates a new Kafka topic
 * named after the WUID of the test.  These should be periodically cleaned
 * off Kafka (or the Kafka instance itself be refreshed).
 ******************************************************************************/

KAFKA_BROKER := '127.0.0.1';
KAFKA_TEST_TOPIC := Std.System.Job.WUID() : INDEPENDENT;
KAFKA_CONSUMER_GROUP := 'regress';

p := kafka.KafkaPublisher(KAFKA_TEST_TOPIC, KAFKA_BROKER);
c := kafka.KafkaConsumer(KAFKA_TEST_TOPIC, KAFKA_BROKER, KAFKA_CONSUMER_GROUP);

SEQUENTIAL
    (
        // Action to prompt Kafka to create a new topic for us; this will
        // will result in a partition count of zero, which is normal
        OUTPUT(c.GetTopicPartitionCount(), NAMED('PingToCreateTopic'));

        // Idle while Kafka prepares topic
        Std.System.Debug.Sleep(1000);

        OUTPUT(c.GetTopicPartitionCount(), NAMED('ConsumerGetTopicPartitionCount'));

        OUTPUT(c.ResetMessageOffsets(), NAMED('ConsumerResetMessageOffsets1'));

        OUTPUT(p.GetTopicPartitionCount(), NAMED('PublisherGetTopicPartitionCount'));

        OUTPUT(p.PublishMessage('Regular message'), NAMED('PublishMessageUnkeyed'));

        OUTPUT(p.PublishMessage('Keyed message', 'mykey'), NAMED('PublishMessageKeyed'));

        // Idle while Kafka publishes
        Std.System.Debug.Sleep(1000);

        OUTPUT(c.GetMessages(10), NAMED('GetMessages1'));

        OUTPUT(c.GetMessages(10), NAMED('GetMessagesEmpty'));

        OUTPUT(c.ResetMessageOffsets(), NAMED('ConsumerResetMessageOffsets2'));

        OUTPUT(c.GetMessages(10), NAMED('GetMessages2'));

        OUTPUT(c.SetMessageOffsets(DATASET([{0,1}], kafka.KafkaMessageOffset)), NAMED('ConsumerSetExplicitMessageOffsets'));

        OUTPUT(c.GetMessages(10), NAMED('GetMessages3'));

        OUTPUT(c.ResetMessageOffsets(), NAMED('ConsumerResetMessageOffsets3'));

        OUTPUT(c.LastMessageOffsets(c.GetMessages(10)), NAMED('ConsumerLastMessageOffsets'));

        // Test UTF-8 support

        OUTPUT(p.PublishMessage(U8'UTF-8: Blah – Unkeyed'), NAMED('PublishMessageUnkeyedUTF8')); // contains en-dash

        OUTPUT(p.PublishMessage(U8'UTF-8: Blah – Keyed', U8'My–Key'), NAMED('PublishMessageKeyedUTF8')); // contains en-dashes

        // Idle while Kafka publishes
        Std.System.Debug.Sleep(1000);

        OUTPUT(c.GetMessages(10), NAMED('GetMessagesUTF8'));
    );
