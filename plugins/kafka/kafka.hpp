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

#ifndef ECL_KAFKA_INCL
#define ECL_KAFKA_INCL

#ifdef _WIN32
#define ECL_KAFKA_CALL _cdecl
#ifdef ECL_KAFKA_EXPORTS
#define ECL_KAFKA_API __declspec(dllexport)
#else
#define ECL_KAFKA_API __declspec(dllimport)
#endif
#else
#define ECL_KAFKA_CALL
#define ECL_KAFKA_API
#endif

#include "platform.h"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"

#include <atomic>
#include <string>
#include <time.h>

#include "librdkafka/rdkafkacpp.h"

#ifdef ECL_KAFKA_EXPORTS
extern "C"
{
    ECL_KAFKA_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
}
#endif

extern "C++"
{
    namespace KafkaPlugin
    {
        class KafkaObj;
        class Poller;
        class Publisher;
        class Consumer;
        class KafkaStreamedDataset;

        /** @class KafkaObj
         *
         *  Parent class for both Publisher and Consumer classes.  Provides
         *  easy way for a Poller object to access either for callbacks, etc.
         */
        class KafkaObj
        {
            public:

                /**
                 * Returns a pointer to the librdkafka object that can be either
                 * a producer or consumer.
                 */
                virtual RdKafka::Handle* handle() = 0;
        };

        //----------------------------------------------------------------------

        /** @class Poller
         *
         *  Background execution of librdkafka's poll() function, which is
         *  required in order to batch I/O.  One Poller will be created
         *  for each Publisher and Consumer object actively used
         */
        class Poller : public Thread
        {
            public:

                /**
                 * Constructor
                 *
                 * @param   _parentPtr      Pointer to Publisher or Consumer object
                 *                          that created this object
                 * @param   _pollTimeout    The number of milliseconds to wait
                 *                          for events within librdkafka
                 */
                Poller(KafkaObj* _parentPtr, __int32 _pollTimeout);

                /**
                 * Starts execution of the thread main event loop
                 */
                virtual void start();

                /**
                 * Stops execution of the thread main event loop.  Note that we
                 * wait until the main event loop has actually stopped before
                 * returning.
                 */
                void stop();

                /**
                 * Entry point to the thread main event loop.  Exiting this
                 * method means that the thread should stop.
                 */
                virtual int run();

            private:

                std::atomic_bool    shouldRun;      //!< If true, we should execute our thread's main event loop
                KafkaObj*           parentPtr;      //!< Pointer to object that started this threaded execution
                __int32             pollTimeout;    //!< The amount of time (in ms) we give to librdkafka's poll() function
        };

        //----------------------------------------------------------------------

        class Publisher : public KafkaObj, public RdKafka::EventCb, public RdKafka::DeliveryReportCb
        {
            public:

                /**
                 * Constructor
                 *
                 * @param   _brokers        One or more Kafka brokers, in the
                 *                          format 'name[:port]' where 'name'
                 *                          is either a host name or IP address;
                 *                          multiple brokers can be delimited
                 *                          with commas
                 * @param   _topic          The name of the topic we will be
                 *                          publishing to
                 * @param   _pollTimeout    The number of milliseconds to wait
                 *                          for events within librdkafka
                 * @param   _traceLevel     Current logging level
                 */
                Publisher(const std::string& _brokers, const std::string& _topic, __int32 _pollTimeout, int _traceLevel);

                virtual ~Publisher();

                /**
                 * @return  A pointer to the librdkafka producer object.
                 */
                virtual RdKafka::Handle* handle();

                /**
                 * @return  Updates the touch time and returns it.
                 */
                time_t updateTimeTouched();

                /**
                 * @return  The time at which this object was created
                 */
                time_t getTimeTouched() const;

                /**
                 * If needed, establish connection to Kafka cluster using the
                 * parameters stored within this object.
                 */
                void ensureSetup();

                /**
                 * Stops the attached poller's main event loop.  This should be
                 * called before deletion.
                 */
                void shutdownPoller();

                /**
                 * @return  Returns the number of messages currently waiting
                 *          in the local outbound queue, ready for transmission
                 *          to the Kafka cluster
                 */
                __int32 messagesWaitingInQueue();

                /**
                 * Send one message
                 *
                 * @param   message     The message to send
                 * @param   key         The key to attach to the message
                 */
                void sendMessage(const std::string& message, const std::string& key);

                /**
                 * Callback function.  librdkafka will call here, outside of a
                 * poll(), when it has interesting things to tell us
                 *
                 * @param   event       Reference to an Event object provided
                 *                      by librdkafka
                 */
                virtual void event_cb(RdKafka::Event& event);

                /**
                 * Callback function.  librdkafka will call here to notify
                 * us of problems with delivering messages to the server
                 *
                 * @param   message     Reference to an Message object provided
                 *                      by librdkafka
                 */
                virtual void dr_cb (RdKafka::Message& message);

            private:

                std::string                     brokers;        //!< One or more Kafka bootstrap brokers; comma-delimited; NameOrIP[:port]
                std::string                     topic;          //!< The name of the topic to publish to
                RdKafka::Producer*              producerPtr;    //!< Pointer to librdkafka producer object
                std::atomic<RdKafka::Topic*>    topicPtr;       //!< Pointer to librdkafka topic object
                CriticalSection                 lock;           //!< Mutex to ensure that only one thread creates the librdkafka object pointers
                Poller*                         pollerPtr;      //!< Pointer to the threaded Poller object that gives time to librdkafka
                __int32                         pollTimeout;    //!< The amount of time (in ms) we give to librdkafka's poll() function
                time_t                          timeCreated;    //!< The time at which this object was created
                int                             traceLevel;     //!< The current logging level
        };

        //----------------------------------------------------------------------

        class Consumer : public KafkaObj, public RdKafka::EventCb
        {
            public:

                /**
                 * Constructor
                 *
                 * @param   _brokers        One or more Kafka brokers, in the
                 *                          format 'name[:port]' where 'name'
                 *                          is either a host name or IP address;
                 *                          multiple brokers can be delimited
                 *                          with commas
                 * @param   _topic          The name of the topic we will be
                 *                          consuming from
                 * @param   _partitionNum   The topic partition number we will be
                 *                          consuming from
                 * @param   _traceLevel     Current logging level
                 */
                Consumer(const std::string& _brokers, const std::string& _topic, const std::string& _consumerGroup, __int32 _partitionNum, int _traceLevel);

                virtual ~Consumer();

                /**
                 * @return  A pointer to the librdkafka consumer object.
                 */
                virtual RdKafka::Handle* handle();

                /**
                 * If needed, establish connection to Kafka cluster using the
                 * parameters stored within this object.
                 */
                void ensureSetup();

                /**
                 * @return  Returns one new message from the inbound Kafka
                 *          topic.  A NON-NULL RESULT MUST EVENTUALLY BE
                 *          DISPOSED OF WITH A CALL TO delete().
                 */
                RdKafka::Message* getOneMessage();

                /**
                 * Retrieves many messages from the inbound Kafka topic and
                 * returns them as a streamed dataset.  Note that this is a
                 * per-brokers/per-topic/per-partition retrieval.
                 *
                 * @param   allocator       The allocator to use with RowBuilder
                 * @param   maxRecords      The maximum number of records
                 *                          to retrieved
                 *
                 * @return  An IRowStream streamed dataset object pointer
                 */
                KafkaStreamedDataset* getMessageDataset(IEngineRowAllocator* allocator, __int64 maxRecords = 1);

                /**
                 * @return  StringBuffer object containing the path to this
                 *          consumer's offset file
                 */
                StringBuffer offsetFilePath() const;

                /**
                 * Commits the given offset to storage so we can pick up
                 * where we left off in a subsequent read.
                 *
                 * @param   offset          The offset to store
                 */
                void commitOffset(__int64 offset) const;

                /**
                 * If the offset file does not exist, create one with a
                 * default offset
                 */
                void initFileOffsetIfNotExist() const;

                /**
                 * Callback function.  librdkafka will call here, outside of a
                 * poll(), when it has interesting things to tell us
                 *
                 * @param   event       Reference to an Event object provided
                 *                      by librdkafka
                 */
                virtual void event_cb(RdKafka::Event& event);

            private:

                std::string                     brokers;        //!< One or more Kafka bootstrap brokers; comma-delimited; NameOrIP[:port]
                std::string                     topic;          //!< The name of the topic to consume from
                std::string                     consumerGroup;  //!< The name of the consumer group for this consumer object
                RdKafka::Consumer*              consumerPtr;    //!< Pointer to librdkafka consumer object
                std::atomic<RdKafka::Topic*>    topicPtr;       //!< Pointer to librdkafka topic object
                CriticalSection                 lock;           //!< Mutex to ensure that only one thread creates the librdkafka object pointers or starts/stops the queue
                __int32                         partitionNum;   //!< The partition within the topic from which we will be pulling messages
                bool                            queueStarted;   //!< If true, we have started the process of reading from the queue
                int                             traceLevel;     //!< The current logging level
        };

        //----------------------------------------------------------------------

        class KafkaStreamedDataset : public RtlCInterface, implements IRowStream
        {
            public:

                /**
                 * Constructor
                 *
                 * @param   _consumerPtr        Pointer to the Consumer object
                 *                              from which we will be retrieving
                 *                              records
                 * @param   _resultAllocator    The memory allocator used to build
                 *                              the result rows; this is provided
                 *                              by the platform during the
                 *                              plugin call
                 * @param   _traceLevel         The current logging level
                 * @param   _maxRecords         The maximum number of records
                 *                              to return; use 0 to return all
                 *                              available records
                 */
                KafkaStreamedDataset(Consumer* _consumerPtr, IEngineRowAllocator* _resultAllocator, int _traceLevel, __int64 _maxRecords = -1);

                virtual ~KafkaStreamedDataset();

                RTLIMPLEMENT_IINTERFACE

                virtual const void* nextRow();

                virtual void stop();

            private:

                Consumer*                       consumerPtr;        //!< Pointer to the Consumer object that we will read from
                Linked<IEngineRowAllocator>     resultAllocator;    //!< Pointer to allocator used when building result rows
                int                             traceLevel;         //!< The current logging level
                bool                            shouldRead;         //!< If true, we should continue trying to read more messages
                __int64                         maxRecords;         //!< The maximum number of messages to read
                __int64                         consumedRecCount;   //!< The number of messages actually read
                __int64                         lastMsgOffset;      //!< The offset of the last message read from the consumer
        };

        //----------------------------------------------------------------------

        /**
         * Queues the message for publishing to a topic on a Kafka cluster.
         *
         * @param   brokers             One or more Kafka brokers, in the
         *                              format 'name[:port]' where 'name'
         *                              is either a host name or IP address;
         *                              multiple brokers can be delimited
         *                              with commas
         * @param   topic               The name of the topic
         * @param   message             The message to send
         * @param   key                 The key to use for the message
         *
         * @return  true if the message was cached successfully
         */
        ECL_KAFKA_API bool ECL_KAFKA_CALL publishMessage(const char* brokers, const char* topic, const char* message, const char* key);

        /**
         * Get the number of partitions currently set up for a topic on a cluster.
         *
         * @param   brokers             One or more Kafka brokers, in the
         *                              format 'name[:port]' where 'name'
         *                              is either a host name or IP address;
         *                              multiple brokers can be delimited
         *                              with commas
         * @param   topic               The name of the topic
         *
         * @return  The number of partitions or zero if either the topic does not
         *          exist or there was an error
         */
        ECL_KAFKA_API __int32 ECL_KAFKA_CALL getTopicPartitionCount(const char* brokers, const char* topic);

        /**
         * Retrieves a set of messages on a topic from a Kafka cluster.
         *
         * @param   ctx                 Platform-provided context point
         * @param   allocator           Platform-provided memory allocator used
         *                              to help build data rows for returning
         * @param   brokers             One or more Kafka brokers, in the
         *                              format 'name[:port]' where 'name'
         *                              is either a host name or IP address;
         *                              multiple brokers can be delimited
         *                              with commas
         * @param   topic               The name of the topic
         * @param   consumerGroup       The name of the consumer group to use; see
         *                              https://kafka.apache.org/documentation.html#introduction
         * @param   partitionNum        The topic partition from which to pull
         *                              messages; this is a zero-based index
         * @param   maxRecords          The maximum number of records return;
         *                              pass zero to return as many messages
         *                              as possible (dangerous)
         *
         * @return  An IRowStream pointer representing the fetched messages
         *          or NULL if no messages could be retrieved
         */
        ECL_KAFKA_API IRowStream* ECL_KAFKA_CALL getMessageDataset(ICodeContext* ctx, IEngineRowAllocator* allocator, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 maxRecords);

        /**
         * Resets the saved offsets for a partition.
         *
         * @param   ctx                 Platform-provided context point
         * @param   brokers             One or more Kafka brokers, in the
         *                              format 'name[:port]' where 'name'
         *                              is either a host name or IP address;
         *                              multiple brokers can be delimited
         *                              with commas
         * @param   topic               The name of the topic
         * @param   consumerGroup       The name of the consumer group to use; see
         *                              https://kafka.apache.org/documentation.html#introduction
         * @param   partitionNum        The topic partition from which to pull
         *                              messages; this is a zero-based index
         * @param   newOffset           The new offset to save
         *
         * @return  The offset that was saved
         */
        ECL_KAFKA_API __int64 ECL_KAFKA_CALL setMessageOffset(ICodeContext* ctx, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 newOffset);
    }
}

#endif
