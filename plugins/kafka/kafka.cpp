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

#include "kafka.hpp"

#include "rtlds_imp.hpp"
#include "jlog.hpp"
#include "jmutex.hpp"
#include "jprop.hpp"
#include "jfile.hpp"
#include "build-config.h"

#include "librdkafka/rdkafka.h"

#include <map>
#include <fstream>
#include <mutex>

//==============================================================================
// Kafka Interface Code
//==============================================================================

namespace KafkaPlugin
{
    //--------------------------------------------------------------------------
    // File Constants
    //--------------------------------------------------------------------------

    // The minimum number of seconds that a cached object can live
    // without activity
    const time_t OBJECT_EXPIRE_TIMEOUT_SECONDS = 60 * 2;

    // The number of milliseconds given to librdkafka to perform explicit
    // background activity
    const __int32 POLL_TIMEOUT = 1000;

    //--------------------------------------------------------------------------
    // Static Variables
    //--------------------------------------------------------------------------

    static std::once_flag pubCacheInitFlag;

    //--------------------------------------------------------------------------
    // Static Methods (internal)
    //--------------------------------------------------------------------------

    /**
     * Look for an optional configuration file and apply any found configuration
     * parameters to a librdkafka configuration object.
     *
     * @param   configFilePath      The path to a configuration file; it is not
     *                              necessary for the file to exist
     * @param   globalConfigPtr     A pointer to the configuration object that
     *                              will receive any found parameters
     * @param   traceLevel          The current log trace level
     */
    static void applyConfig(const char* configFilePath, RdKafka::Conf* globalConfigPtr, int traceLevel)
    {
        if (configFilePath && *configFilePath && globalConfigPtr)
        {
            std::string errStr;
            StringBuffer fullConfigPath;

            fullConfigPath.append(CONFIG_DIR).append(PATHSEPSTR).append(configFilePath);

            Owned<IProperties> properties = createProperties(fullConfigPath.str(), true);
            Owned<IPropertyIterator> props = properties->getIterator();

            ForEach(*props)
            {
                StringBuffer key = props->getPropKey();

                key.trim();

                if (key.length() > 0 && key.charAt(0) != '#')
                {
                    if (strcmp(key.str(), "metadata.broker.list") != 0)
                    {
                        const char* value = properties->queryProp(key);

                        if (value && *value)
                        {
                            if (globalConfigPtr->set(key.str(), value, errStr) != RdKafka::Conf::CONF_OK)
                            {
                                OWARNLOG("Kafka: Failed to set config param from file %s: '%s' = '%s'; error: '%s'", configFilePath, key.str(), value, errStr.c_str());
                            }
                            else if (traceLevel > 4)
                            {
                                DBGLOG("Kafka: Set config param from file %s: '%s' = '%s'", configFilePath, key.str(), value);
                            }
                        }
                    }
                    else
                    {
                        OWARNLOG("Kafka: Setting '%s' ignored in config file %s", key.str(), configFilePath);
                    }
                }
            }
        }
    }

    //--------------------------------------------------------------------------
    // Plugin Classes
    //--------------------------------------------------------------------------

    KafkaStreamedDataset::KafkaStreamedDataset(Consumer* _consumerPtr, IEngineRowAllocator* _resultAllocator, int _traceLevel, __int64 _maxRecords)
        :   consumerPtr(_consumerPtr),
            resultAllocator(_resultAllocator),
            traceLevel(_traceLevel),
            maxRecords(_maxRecords)
    {
        shouldRead = true;
        consumedRecCount = 0;
        lastMsgOffset = 0;
    }

    KafkaStreamedDataset::~KafkaStreamedDataset()
    {
        if (consumedRecCount > 0)
        {
            consumerPtr->commitOffset(lastMsgOffset);
        }

        delete(consumerPtr);
    }

    const void* KafkaStreamedDataset::nextRow()
    {
        const void* result = NULL;
        __int32 maxAttempts = 10;   //!< Maximum number of tries if local queue is full
        __int32 timeoutWait = 100;  //!< Amount of time to wait between retries
        __int32 attemptNum = 0;

        if (maxRecords <= 0 || consumedRecCount < maxRecords)
        {
            RdKafka::Message* messageObjPtr = NULL;
            bool messageConsumed = false;

            while (!messageConsumed && shouldRead && attemptNum < maxAttempts)
            {
                messageObjPtr = consumerPtr->getOneMessage(); // messageObjPtr must be deleted when we are through with it

                if (messageObjPtr)
                {
                    try
                    {
                        switch (messageObjPtr->err())
                        {
                            case RdKafka::ERR_NO_ERROR:
                                {
                                    RtlDynamicRowBuilder rowBuilder(resultAllocator);
                                    unsigned len = sizeof(__int32) + sizeof(__int64) + sizeof(size32_t) + messageObjPtr->len();
                                    byte* row = rowBuilder.ensureCapacity(len, NULL);

                                    // Populating this structure:
                                    //  EXPORT KafkaMessage := RECORD
                                    //      UNSIGNED4   partitionNum;
                                    //      UNSIGNED8   offset;
                                    //      STRING      message;
                                    //  END;

                                    *(__int32*)(row) = messageObjPtr->partition();
                                    *(__int64*)(row + sizeof(__int32)) = messageObjPtr->offset();
                                    *(size32_t*)(row + sizeof(__int32) + sizeof(__int64)) = messageObjPtr->len();
                                    memcpy(row + sizeof(__int32) + sizeof(__int64) + sizeof(size32_t), messageObjPtr->payload(), messageObjPtr->len());

                                    result = rowBuilder.finalizeRowClear(len);

                                    lastMsgOffset = messageObjPtr->offset();
                                    ++consumedRecCount;

                                    // Give opportunity for consumer to pull in any additional messages
                                    consumerPtr->handle()->poll(0);

                                    // Mark as loaded so we don't retry
                                    messageConsumed = true;
                                }
                                break;

                            case RdKafka::ERR__TIMED_OUT:
                                // No new messages arrived and we timed out waiting
                                ++attemptNum;
                                consumerPtr->handle()->poll(timeoutWait);
                                break;

                            case RdKafka::ERR__PARTITION_EOF:
                                // We reached the end of the messages in the partition
                                if (traceLevel > 4)
                                {
                                    DBGLOG("Kafka: EOF reading message from partition %d", messageObjPtr->partition());
                                }
                                shouldRead = false;
                                break;

                            case RdKafka::ERR__UNKNOWN_PARTITION:
                                // Unknown partition; don't throw an error here because
                                // in some configurations (e.g. more Thor slaves than
                                // partitions) not all consumers will have a partition
                                // to read
                                if (traceLevel > 4)
                                {
                                    DBGLOG("Kafka: Unknown partition while trying to read");
                                }
                                shouldRead = false;
                                break;

                            case RdKafka::ERR__UNKNOWN_TOPIC:
                                throw MakeStringException(-1, "Kafka: Error while reading message: '%s'", messageObjPtr->errstr().c_str());
                                break;
                        }
                    }
                    catch (...)
                    {
                        delete(messageObjPtr);
                        throw;
                    }

                    delete(messageObjPtr);
                    messageObjPtr = NULL;
                }
            }
        }

        return result;
    }

    void KafkaStreamedDataset::stop()
    {
        shouldRead = false;
    }

    //--------------------------------------------------------------------------

    Poller::Poller(KafkaObj* _parentPtr, __int32 _pollTimeout)
        :   Thread("Kafka::Poller"),
            parentPtr(_parentPtr),
            pollTimeout(_pollTimeout),
            shouldRun(false)
    {
    }

    void Poller::start()
    {
        if (!isAlive() && parentPtr)
        {
            shouldRun = true;
            Thread::start();
        }
    }

    void Poller::stop()
    {
        if (isAlive())
        {
            shouldRun = false;
            join();
        }
    }

    int Poller::run()
    {
        RdKafka::Handle* handle = parentPtr->handle();

        while (shouldRun)
        {
            handle->poll(pollTimeout);
        }

        return 0;
    }

    //--------------------------------------------------------------------------

    Publisher::Publisher(const std::string& _brokers, const std::string& _topic, __int32 _pollTimeout, int _traceLevel)
        :   brokers(_brokers),
            topic(_topic),
            pollTimeout(_pollTimeout),
            traceLevel(_traceLevel)
    {
        producerPtr = NULL;
        topicPtr = NULL;
        pollerPtr = new Poller(this, _pollTimeout);

        updateTimeTouched();
    }

    Publisher::~Publisher()
    {
        delete(pollerPtr);
        delete(topicPtr.load());
        delete(producerPtr);
    }

    RdKafka::Handle* Publisher::handle()
    {
        return static_cast<RdKafka::Handle*>(producerPtr);
    }

    time_t Publisher::updateTimeTouched()
    {
        timeCreated = time(NULL);

        return timeCreated;
    }

    time_t Publisher::getTimeTouched() const
    {
        return timeCreated;
    }

    void Publisher::shutdownPoller()
    {
        if (pollerPtr)
        {
            // Wait until we send all messages
            while (messagesWaitingInQueue() > 0)
            {
                usleep(pollTimeout);
            }

            // Tell poller to stop
            pollerPtr->stop();
        }
    }

    __int32 Publisher::messagesWaitingInQueue()
    {
        __int32 queueLength = 0;

        if (producerPtr)
        {
            queueLength = producerPtr->outq_len();
        }

        return queueLength;
    }

    void Publisher::ensureSetup()
    {
        if (!topicPtr.load(std::memory_order_acquire))
        {
            CriticalBlock   block(lock);

            if (!topicPtr.load(std::memory_order_relaxed))
            {
                std::string errStr;
                RdKafka::Conf* globalConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

                if (globalConfig)
                {
                    // Set global configuration parameters, used mainly at the producer level
                    globalConfig->set("metadata.broker.list", brokers, errStr);
                    globalConfig->set("queue.buffering.max.messages", "1000000", errStr);
                    globalConfig->set("compression.codec", "snappy", errStr);
                    globalConfig->set("message.send.max.retries", "3", errStr);
                    globalConfig->set("retry.backoff.ms", "500", errStr);

                    // Set any global configurations from file, allowing
                    // overrides of above settings
                    applyConfig("kafka_global.conf", globalConfig, traceLevel);

                    // Set producer callbacks
                    globalConfig->set("event_cb", static_cast<RdKafka::EventCb*>(this), errStr);
                    globalConfig->set("dr_cb", static_cast<RdKafka::DeliveryReportCb*>(this), errStr);

                    // Create the producer
                    producerPtr = RdKafka::Producer::create(globalConfig, errStr);

                    if (producerPtr)
                    {
                        RdKafka::Conf* topicConfPtr = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

                        // Set any topic configurations from file
                        std::string confName = "kafka_publisher_topic_" + topic + ".conf";
                        applyConfig(confName.c_str(), topicConfPtr, traceLevel);

                        // Create the topic
                        topicPtr.store(RdKafka::Topic::create(producerPtr, topic, topicConfPtr, errStr), std::memory_order_release);

                        if (topicPtr)
                        {
                            // Start the attached background poller
                            pollerPtr->start();
                        }
                        else
                        {
                            throw MakeStringException(-1, "Kafka: Unable to create producer topic object for topic '%s'; error: '%s'", topic.c_str(), errStr.c_str());
                        }
                    }
                    else
                    {
                        throw MakeStringException(-1, "Kafka: Unable to create producer object for brokers '%s'; error: '%s'", brokers.c_str(), errStr.c_str());
                    }
                }
                else
                {
                    throw MakeStringException(-1, "Kafka: Unable to create producer global configuration object for brokers '%s'; error: '%s'", brokers.c_str(), errStr.c_str());
                }
            }
        }
    }

    void Publisher::sendMessage(const std::string& message, const std::string& key)
    {
        __int32 maxAttempts = 10;   //!< Maximum number of tries if local queue is full
        __int32 attemptNum = 0;

        // Make sure we have a valid connection to the Kafka cluster
        ensureSetup();

        // Actually send the message
        while (true)
        {
            RdKafka::ErrorCode resp = producerPtr->produce(topicPtr, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(message.c_str()), message.size(), (key.empty() ? NULL : &key), NULL);

            if (resp == RdKafka::ERR_NO_ERROR)
            {
                break;
            }
            else if (resp == RdKafka::ERR__QUEUE_FULL)
            {
                if (attemptNum < maxAttempts)
                {
                    usleep(pollTimeout);
                    ++attemptNum;
                }
                else
                {
                    throw MakeStringException(-1, "Kafka: Unable to send message to topic '%s'; error: '%s'", topic.c_str(), RdKafka::err2str(resp).c_str());
                }
            }
            else
            {
                throw MakeStringException(-1, "Kafka: Unable to send message to topic '%s'; error: '%s'", topic.c_str(), RdKafka::err2str(resp).c_str());
            }
        }
    }

    void Publisher::event_cb(RdKafka::Event& event)
    {
        if (traceLevel > 4)
        {
            switch (event.type())
            {
                case RdKafka::Event::EVENT_ERROR:
                    DBGLOG("Kafka: Error: %s", event.str().c_str());
                    break;

                case RdKafka::Event::EVENT_STATS:
                    DBGLOG("Kafka: Stats: %s", event.str().c_str());
                    break;

                case RdKafka::Event::EVENT_LOG:
                    DBGLOG("Kafka: Log: %s", event.str().c_str());
                    break;
            }
        }
    }

    void Publisher::dr_cb (RdKafka::Message& message)
    {
        if (message.err() != RdKafka::ERR_NO_ERROR)
        {
            StringBuffer    payloadStr;

            if (message.len() == 0)
                payloadStr.append("<no message>");
            else
                payloadStr.append(message.len(), static_cast<const char*>(message.payload()));

            DBGLOG("Kafka: Error publishing message: %d (%s); message: '%s'", message.err(), message.errstr().c_str(), payloadStr.str());
        }
    }

    //--------------------------------------------------------------------------

    Consumer::Consumer(const std::string& _brokers, const std::string& _topic, const std::string& _consumerGroup, __int32 _partitionNum, int _traceLevel)
        :   brokers(_brokers),
            topic(_topic),
            consumerGroup(_consumerGroup),
            partitionNum(_partitionNum),
            traceLevel(_traceLevel)
    {
        consumerPtr = NULL;
        topicPtr = NULL;
    }

    Consumer::~Consumer()
    {
        if (consumerPtr && topicPtr)
        {
            consumerPtr->stop(topicPtr, partitionNum);
        }

        delete(topicPtr.load());
        delete(consumerPtr);
    }

    RdKafka::Handle* Consumer::handle()
    {
        return static_cast<RdKafka::Handle*>(consumerPtr);
    }

    void Consumer::ensureSetup()
    {
        if (!topicPtr.load(std::memory_order_acquire))
        {
            CriticalBlock block(lock);

            if (!topicPtr.load(std::memory_order_relaxed))
            {
                initFileOffsetIfNotExist();

                std::string errStr;
                RdKafka::Conf* globalConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

                if (globalConfig)
                {
                    // Set global configuration parameters, used mainly at the consumer level
                    globalConfig->set("metadata.broker.list", brokers, errStr);
                    globalConfig->set("compression.codec", "snappy", errStr);
                    globalConfig->set("queued.max.messages.kbytes", "10000000", errStr);
                    globalConfig->set("fetch.message.max.bytes", "10000000", errStr);

                    // Set any global configurations from file, allowing
                    // overrides of above settings
                    applyConfig("kafka_global.conf", globalConfig, traceLevel);

                    // Set consumer callbacks
                    globalConfig->set("event_cb", static_cast<RdKafka::EventCb*>(this), errStr);

                    // Create the consumer
                    consumerPtr = RdKafka::Consumer::create(globalConfig, errStr);

                    if (consumerPtr)
                    {
                        RdKafka::Conf* topicConfPtr = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

                        // Set the per-topic configuration parameters
                        topicConfPtr->set("group.id", consumerGroup, errStr);
                        topicConfPtr->set("auto.offset.reset", "smallest", errStr);

                        // Set any topic configurations from file, allowing
                        // overrides of above settings
                        std::string confName = "kafka_consumer_topic_" + topic + ".conf";
                        applyConfig(confName.c_str(), topicConfPtr, traceLevel);

                        // Ensure that some items are set a certain way
                        // by setting them after loading the external conf
                        topicConfPtr->set("auto.commit.enable", "false", errStr);

                        // Create the topic
                        topicPtr.store(RdKafka::Topic::create(consumerPtr, topic, topicConfPtr, errStr), std::memory_order_release);

                        if (!topicPtr)
                        {
                            throw MakeStringException(-1, "Kafka: Unable to create consumer topic object for topic '%s'; error: '%s'", topic.c_str(), errStr.c_str());
                        }
                    }
                    else
                    {
                        throw MakeStringException(-1, "Kafka: Unable to create consumer object for brokers '%s'; error: '%s'", brokers.c_str(), errStr.c_str());
                    }
                }
                else
                {
                    throw MakeStringException(-1, "Kafka: Unable to create consumer global configuration object for brokers '%s'; error: '%s'", brokers.c_str(), errStr.c_str());
                }
            }
        }
    }

    RdKafka::Message* Consumer::getOneMessage()
    {
        return consumerPtr->consume(topicPtr, partitionNum, POLL_TIMEOUT);
    }

    KafkaStreamedDataset* Consumer::getMessageDataset(IEngineRowAllocator* allocator, __int64 maxRecords)
    {
        // Make sure we have a valid connection to the Kafka cluster
        ensureSetup();

        // Start the local read queue
        RdKafka::ErrorCode startErr = consumerPtr->start(topicPtr, partitionNum, RdKafka::Topic::OFFSET_STORED);

        if (startErr == RdKafka::ERR_NO_ERROR)
        {
            if (traceLevel > 4)
            {
                DBGLOG("Kafka: Started Consumer for %s:%d @ %s", topic.c_str(), partitionNum, brokers.c_str());
            }
        }
        else
        {
            throw MakeStringException(-1, "Kafka: Failed to start Consumer read for %s:%d @ %s; error: %d", topic.c_str(), partitionNum, brokers.c_str(), startErr);
        }

        return new KafkaStreamedDataset(this, allocator, traceLevel, maxRecords);
    }

    StringBuffer Consumer::offsetFilePath() const
    {
        StringBuffer offsetPath;

        offsetPath.append(topic.c_str());
        offsetPath.append("-");
        offsetPath.append(partitionNum);
        if (!consumerGroup.empty())
        {
            offsetPath.append("-");
            offsetPath.append(consumerGroup.c_str());
        }
        offsetPath.append(".offset");

        return offsetPath;
    }

    void Consumer::commitOffset(__int64 offset) const
    {
        if (offset >= -1)
        {
            // Not using librdkafka's offset_store because it seems to be broken
            // topicPtr->offset_store(partitionNum, offset);

            // Create/overwrite a file using the same naming convention and
            // file contents that librdkafka uses so it can pick up where
            // we left off; NOTE:  librdkafka does not clean the topic name
            // or consumer group name when constructing this path
            // (which is actually a security concern), so we can't clean, either
            StringBuffer offsetPath = offsetFilePath();

            std::ofstream outFile(offsetPath.str(), std::ofstream::trunc);
            outFile << offset;

            if (traceLevel > 4)
            {
                DBGLOG("Kafka: Saved offset %lld to %s", offset, offsetPath.str());
            }
        }
    }

    void Consumer::initFileOffsetIfNotExist() const
    {
        StringBuffer offsetPath = offsetFilePath();

        if (!checkFileExists(offsetPath.str()))
        {
            commitOffset(-1);

            if (traceLevel > 4)
            {
                DBGLOG("Kafka: Creating initial offset file %s", offsetPath.str());
            }
        }
    }

    void Consumer::event_cb(RdKafka::Event& event)
    {
        if (traceLevel > 4)
        {
            switch (event.type())
            {
                case RdKafka::Event::EVENT_ERROR:
                    DBGLOG("Kafka: Error: %s", event.str().c_str());
                    break;

                case RdKafka::Event::EVENT_STATS:
                    DBGLOG("Kafka: Stats: %s", event.str().c_str());
                    break;

                case RdKafka::Event::EVENT_LOG:
                    DBGLOG("Kafka: Log: %s", event.str().c_str());
                    break;
            }
        }
    }

    //--------------------------------------------------------------------------

    /** @class  PublisherCacheObj
     *
     * Class used to create and cache publisher objects and connections
     */
    static class PublisherCacheObj
    {
        private:

            typedef std::map<std::string, Publisher*> ObjMap;

        public:

            /**
             * Constructor
             *
             * @param   _traceLevel         The current logging level
             */
            PublisherCacheObj(int _traceLevel)
                :   traceLevel(_traceLevel)
            {

            }

            void deleteAll()
            {
                CriticalBlock block(lock);

                for (ObjMap::iterator x = cachedPublishers.begin(); x != cachedPublishers.end(); x++)
                {
                    if (x->second)
                    {
                        // Shutdown the attached poller before deleting
                        x->second->shutdownPoller();

                        // Now delete
                        delete(x->second);
                    }
                }

                cachedPublishers.clear();
            }

            /**
             * Remove previously-created objects that have been inactive
             * for awhile
             */
            void expire()
            {
                if (!cachedPublishers.empty())
                {
                    CriticalBlock block(lock);

                    time_t oldestAllowedTime = time(NULL) - OBJECT_EXPIRE_TIMEOUT_SECONDS;
                    __int32 expireCount = 0;

                    for (ObjMap::iterator x = cachedPublishers.begin(); x != cachedPublishers.end(); /* increment handled explicitly */)
                    {
                        // Expire only if the publisher has been inactive and if
                        // there are no messages in the outbound queue
                        if (x->second && x->second->getTimeTouched() < oldestAllowedTime && x->second->messagesWaitingInQueue() == 0)
                        {
                            // Shutdown the attached poller before deleting
                            x->second->shutdownPoller();

                            // Delete the object
                            delete(x->second);

                            // Erase from map
                            cachedPublishers.erase(x++);

                            ++expireCount;
                        }
                        else
                        {
                            x++;
                        }
                    }

                    if (traceLevel > 4 && expireCount > 0)
                    {
                        DBGLOG("Kafka: Expired %d cached publisher%s", expireCount, (expireCount == 1 ? "" : "s"));
                    }
                }
            }

            /**
             * Gets an established Publisher, based on unique broker/topic
             * pairs, or creates a new one.
             *
             * @param   brokers             One or more Kafka brokers, in the
             *                              format 'name[:port]' where 'name'
             *                              is either a host name or IP address;
             *                              multiple brokers can be delimited
             *                              with commas
             * @param   topic               The name of the topic
             * @param   pollTimeout         The number of milliseconds to give
             *                              to librdkafka when executing
             *                              asynchronous activities
             *
             * @return  A pointer to a Publisher* object.
             */
            Publisher* getPublisher(const std::string& brokers, const std::string& topic, __int32 pollTimeout)
            {
                Publisher* pubObjPtr = NULL;
                StringBuffer suffixStr;
                std::string key;

                // Create the key used to look up previously-created objects
                suffixStr.append(pollTimeout);
                key = brokers + "+" + topic + "+" + suffixStr.str();

                {
                    CriticalBlock block(lock);

                    // Try to find a cached publisher
                    pubObjPtr = cachedPublishers[key];

                    if (pubObjPtr)
                    {
                        pubObjPtr->updateTimeTouched();
                    }
                    else
                    {
                        // Publisher for that set of brokers and topic does not exist; create one
                        pubObjPtr = new Publisher(brokers, topic, pollTimeout, traceLevel);
                        cachedPublishers[key] = pubObjPtr;

                        if (traceLevel > 4)
                        {
                            DBGLOG("Kafka: Created and cached new publisher object: %s @ %s", topic.c_str(), brokers.c_str());
                        }
                    }
                }

                if (!pubObjPtr)
                {
                    throw MakeStringException(-1, "Kafka: Unable to create publisher for brokers '%s' and topic '%s'", brokers.c_str(), topic.c_str());
                }

                return pubObjPtr;
            }

        private:

            ObjMap          cachedPublishers;   //!< std::map of created Publisher object pointers
            CriticalSection lock;               //!< Mutex guarding modifications to cachedPublishers
            int             traceLevel;         //!< The current logging level
    } *publisherCache;

    //--------------------------------------------------------------------------

    /** @class  PublisherCacheExpirerObj
     *          Class used to expire old publisher objects held within publisherCache
     */
    static class PublisherCacheExpirerObj : public Thread
    {
        public:

            PublisherCacheExpirerObj()
                :   Thread("Kafka::PublisherExpirer"),
                    shouldRun(false)
            {

            }

            virtual void start()
            {
                if (!isAlive())
                {
                    shouldRun = true;
                    Thread::start();
                }
            }

            virtual void stop()
            {
                if (isAlive())
                {
                    shouldRun = false;
                    join();
                }
            }

            virtual int run()
            {
                while (shouldRun)
                {
                    if (publisherCache)
                    {
                        publisherCache->expire();
                    }

                    usleep(1000);
                }

                return 0;
            }

        private:

            std::atomic_bool    shouldRun;      //!< If true, we should execute our thread's main event loop
    } *publisherCacheExpirer;

    //--------------------------------------------------------------------------
    // Lazy Initialization
    //--------------------------------------------------------------------------

    /**
     * Make sure the publisher object cache is initialized as well as the
     * associated background thread for expiring idle publishers.  This is
     * called only once.
     *
     * @param   traceLevel      Current logging level
     */
    static void setupPublisherCache(int traceLevel)
    {
        KafkaPlugin::publisherCache = new KafkaPlugin::PublisherCacheObj(traceLevel);

        KafkaPlugin::publisherCacheExpirer = new KafkaPlugin::PublisherCacheExpirerObj;
        KafkaPlugin::publisherCacheExpirer->start();
    }

    //--------------------------------------------------------------------------
    // Advertised Entry Point Functions
    //--------------------------------------------------------------------------

    ECL_KAFKA_API bool ECL_KAFKA_CALL publishMessage(ICodeContext* ctx, const char* brokers, const char* topic, const char* message, const char* key)
    {
        std::call_once(pubCacheInitFlag, setupPublisherCache, ctx->queryContextLogger().queryTraceLevel());

        Publisher* pubObjPtr = publisherCache->getPublisher(brokers, topic, POLL_TIMEOUT);

        pubObjPtr->sendMessage(message, key);

        return true;
    }

    ECL_KAFKA_API __int32 ECL_KAFKA_CALL getTopicPartitionCount(ICodeContext* ctx, const char* brokers, const char* topic)
    {
        // We have to use librdkafka's C API for this right now, as the C++ API
        // does not expose a topic's metadata.  In addition, there is no easy
        // link between the exposed C++ objects and the structs used by the
        // C API, so we are basically creating a brand-new connection from
        // scratch.

        __int32 pCount = 0;
        char errstr[512];
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

        if (rk)
        {
            if (rd_kafka_brokers_add(rk, brokers) != 0)
            {
                rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
                rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic, topic_conf);

                if (rkt)
                {
                    const struct rd_kafka_metadata* metadata = NULL;
                    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &metadata, 5000);

                    if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
                    {
                        pCount = metadata->topics[0].partition_cnt;

                        rd_kafka_metadata_destroy(metadata);
                    }
                    else
                    {
                        if (ctx->queryContextLogger().queryTraceLevel() > 4)
                        {
                            DBGLOG("Kafka: Error retrieving metadata from topic: %s @ %s: '%s'", topic, brokers, rd_kafka_err2str(err));
                        }
                    }

                    rd_kafka_topic_destroy(rkt);
                }
                else
                {
                    if (ctx->queryContextLogger().queryTraceLevel() > 4)
                    {
                        DBGLOG("Kafka: Could not create topic object: %s @ %s", topic, brokers);
                    }
                }
            }
            else
            {
                if (ctx->queryContextLogger().queryTraceLevel() > 4)
                {
                    DBGLOG("Kafka: Could not add brokers: %s @ %s", topic, brokers);
                }
            }

            rd_kafka_destroy(rk);
        }

        if (pCount == 0)
        {
            DBGLOG("Kafka: Unable to retrieve partition count from topic: %s @ %s", topic, brokers);
        }

        return pCount;
    }

    ECL_KAFKA_API IRowStream* ECL_KAFKA_CALL getMessageDataset(ICodeContext* ctx, IEngineRowAllocator* allocator, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 maxRecords)
    {
        Consumer* consumerObjPtr = new Consumer(brokers, topic, consumerGroup, partitionNum, ctx->queryContextLogger().queryTraceLevel());

        return consumerObjPtr->getMessageDataset(allocator, maxRecords);
    }

    ECL_KAFKA_API __int64 ECL_KAFKA_CALL setMessageOffset(ICodeContext* ctx, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 newOffset)
    {
        Consumer consumerObj(brokers, topic, consumerGroup, partitionNum, ctx->queryContextLogger().queryTraceLevel());

        consumerObj.commitOffset(newOffset);

        return newOffset;
    }
}

//==============================================================================
// Plugin Initialization and Teardown
//==============================================================================

#define CURRENT_KAFKA_VERSION "kafka plugin 1.0.0"

static const char* kafkaCompatibleVersions[] = {
    CURRENT_KAFKA_VERSION,
    NULL };

ECL_KAFKA_API bool getECLPluginDefinition(ECLPluginDefinitionBlock* pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx* pbx = static_cast<ECLPluginDefinitionBlockEx*>(pb);
        pbx->compatibleVersions = kafkaCompatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
    {
        return false;
    }

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = CURRENT_KAFKA_VERSION;
    pb->moduleName = "kafka";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for the C++ API in librdkafka++\n";

    return true;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    KafkaPlugin::publisherCache = NULL;
    KafkaPlugin::publisherCacheExpirer = NULL;

    return true;
}

MODULE_EXIT()
{
    // Delete the background thread expiring items from the publisher cache
    // before deleting the publisher cache
    if (KafkaPlugin::publisherCacheExpirer)
    {
        KafkaPlugin::publisherCacheExpirer->stop();
        delete(KafkaPlugin::publisherCacheExpirer);
        KafkaPlugin::publisherCacheExpirer = NULL;
    }

     if (KafkaPlugin::publisherCache)
    {
        KafkaPlugin::publisherCache->deleteAll();
        delete(KafkaPlugin::publisherCache);
        KafkaPlugin::publisherCache = NULL;
    }

    RdKafka::wait_destroyed(3000);
}
