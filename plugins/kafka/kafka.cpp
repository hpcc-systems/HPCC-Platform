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

    // Filename of global Kafka configuration file
#ifdef _CONTAINERIZED
    const char* GLOBAL_CONFIG_NAME = "global";
#else
    const char* GLOBAL_CONFIG_NAME = "kafka_global.conf";
#endif

    // The minimum number of seconds that a cached object can live
    // without activity
    const time_t OBJECT_EXPIRE_TIMEOUT_SECONDS = 60 * 2;

    // The number of milliseconds given to librdkafka to perform explicit
    // background activity
    const __int32 POLL_TIMEOUT = 1000;

    //--------------------------------------------------------------------------
    // Static Methods (internal)
    //--------------------------------------------------------------------------

    static void applyConfigProps(const IPropertyTree * props, const char* configName, RdKafka::Conf* configPtr)
    {
        if (props)
        {
            Owned<IPropertyTreeIterator> iter = props->getElements(configName);
            std::string errStr;

            ForEach(*iter)
            {
                IPropertyTree & entry = iter->query();
                const char* name = entry.queryProp("@name");
                const char* value = entry.queryProp("@value");

                if (name && *name)
                {
                    if (!strisame(name, "metadata.broker.list"))
                    {
                        if (!isEmptyString(value))
                        {
                            if (configPtr->set(name, value, errStr) != RdKafka::Conf::CONF_OK)
                            {
                                OWARNLOG("Kafka: Failed to set config param from entry %s: '%s' = '%s'; error: '%s'", configName, name, value, errStr.c_str());
                            }
                            else if (doTrace(traceKafka))
                            {
                                DBGLOG("Kafka: Set config param from entry %s: '%s' = '%s'", configName, name, value);
                            }
                        }
                    }
                    else
                    {
                        OWARNLOG("Kafka: Setting '%s' ignored in config %s", name, configName);
                    }
                }
            }
        }
    }

    /**
     * Look for an optional configuration entry and apply any found parameters
     * to a librdkafka configuration object.
     *
     * @param   configName          The name of the configuration key within plugins/kafka;
     *                              it is not required for the key to exist
     * @param   configPtr           A pointer to the configuration object that
     *                              will receive any found parameters
     */
    static void applyYAMLConfig(const char* configName, RdKafka::Conf* configPtr)
    {
        if (configName && *configName && configPtr)
        {
            applyConfigProps(getGlobalConfigSP()->queryPropTree("plugins/kafka"), configName, configPtr);
            applyConfigProps(getComponentConfigSP()->queryPropTree("plugins/kafka"), configName, configPtr);
        }
    }

    /**
     * Look for an optional configuration file in a bare metal environment and 
     * apply any found configuration parameters to a librdkafka configuration object.
     *
     * @param   configName          The name of the configuration file; it is not
     *                              necessary for the file to exist
     * @param   configPtr           A pointer to the configuration object that
     *                              will receive any found parameters
     */
    static void applyConfig(const char* configName, RdKafka::Conf* configPtr)
    {
        if (configName && *configName && configPtr)
        {
            std::string errStr;
            StringBuffer fullConfigPath;

            fullConfigPath.append(hpccBuildInfo.configDir).append(PATHSEPSTR).append(configName);
            Owned<IProperties> properties = createProperties(fullConfigPath.str(), true);
            Owned<IPropertyIterator> props = properties->getIterator();

            ForEach(*props)
            {
                StringBuffer key(props->getPropKey());

                key.trim();

                if (key.length() > 0 && key.charAt(0) != '#')
                {
                    if (!strisame(key.str(), "metadata.broker.list"))
                    {
                        const char* value = properties->queryProp(key);

                        if (!isEmptyString(value))
                        {
                            if (configPtr->set(key.str(), value, errStr) != RdKafka::Conf::CONF_OK)
                            {
                                OWARNLOG("Kafka: Failed to set config param from file %s: '%s' = '%s'; error: '%s'", configName, key.str(), value, errStr.c_str());
                            }
                            else if (doTrace(traceKafka))
                            {
                                DBGLOG("Kafka: Set config param from file %s: '%s' = '%s'", configName, key.str(), value);
                            }
                        }
                    }
                    else
                    {
                        OWARNLOG("Kafka: Setting '%s' ignored in config file %s", key.str(), configName);
                    }
                }
            }
        }
    }

    //--------------------------------------------------------------------------
    // Plugin Classes
    //--------------------------------------------------------------------------

    KafkaStreamedDataset::KafkaStreamedDataset(Consumer* _consumerPtr, IEngineRowAllocator* _resultAllocator, __int64 _maxRecords)
        :   consumerPtr(_consumerPtr),
            resultAllocator(_resultAllocator),
            maxRecords(_maxRecords)
    {
        shouldRead = true;
        consumedRecCount = 0;
        lastMsgOffset = 0;
    }

    KafkaStreamedDataset::~KafkaStreamedDataset()
    {
        if (consumerPtr)
        {
            if (consumedRecCount > 0)
            {
                consumerPtr->commitOffset(lastMsgOffset);
            }

            delete(consumerPtr);
        }
    }

    const void* KafkaStreamedDataset::nextRow()
    {
        const void* result = NULL;
        __int32 maxAttempts = 10;   //!< Maximum number of tries if local queue is full
        __int32 timeoutWait = 100;  //!< Amount of time to wait between retries
        __int32 attemptNum = 0;

        if (consumerPtr && (maxRecords <= 0 || consumedRecCount < maxRecords))
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
                                    //      UTF8        message;
                                    //  END;

                                    *(__int32*)(row) = messageObjPtr->partition();
                                    *(__int64*)(row + sizeof(__int32)) = messageObjPtr->offset();
                                    *(size32_t*)(row + sizeof(__int32) + sizeof(__int64)) = rtlUtf8Length(messageObjPtr->len(), messageObjPtr->payload());
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
                                if (doTrace(traceKafka))
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
                                if (doTrace(traceKafka))
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
            Thread::start(false);
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

    Publisher::Publisher(const std::string& _brokers, const std::string& _topic, __int32 _pollTimeout)
        :   brokers(_brokers),
            topic(_topic),
            pollTimeout(_pollTimeout)
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
                    if (isContainerized())
                    {
                        applyYAMLConfig(GLOBAL_CONFIG_NAME, globalConfig);
                    }
                    else
                    {
                        applyConfig(GLOBAL_CONFIG_NAME, globalConfig);
                    }

                    // Set producer callbacks
                    globalConfig->set("event_cb", static_cast<RdKafka::EventCb*>(this), errStr);
                    globalConfig->set("dr_cb", static_cast<RdKafka::DeliveryReportCb*>(this), errStr);

                    // Create the producer
                    producerPtr = RdKafka::Producer::create(globalConfig, errStr);
                    delete globalConfig;

                    if (producerPtr)
                    {
                        RdKafka::Conf* topicConfPtr = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

                        // Set any topic configurations from file
                        std::string confName;
                        if (isContainerized())
                        {
                            confName = "publisher_topic_" + topic;
                            applyYAMLConfig(confName.c_str(), topicConfPtr);
                        }
                        else
                        {
                            confName = "kafka_publisher_topic_" + topic + ".conf";
                            applyConfig(confName.c_str(), topicConfPtr);
                        }

                        // Create the topic
                        topicPtr.store(RdKafka::Topic::create(producerPtr, topic, topicConfPtr, errStr), std::memory_order_release);
                        delete topicConfPtr;

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
        if (doTrace(traceKafka))
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

            OWARNLOG("Kafka: Error publishing message: %d (%s); message: '%s'", message.err(), message.errstr().c_str(), payloadStr.str());
        }
    }

    //--------------------------------------------------------------------------

    Consumer::Consumer(const std::string& _brokers, const std::string& _topic, const std::string& _consumerGroup, __int32 _partitionNum)
        :   brokers(_brokers),
            topic(_topic),
            consumerGroup(_consumerGroup),
            partitionNum(_partitionNum)
    {
        consumerPtr = NULL;
        topicPtr = NULL;

        if (!isContainerized())
        {
            char cpath[_MAX_DIR];

            if (!GetCurrentDirectory(_MAX_DIR, cpath))
                throw MakeStringException(-1, "Unable to determine current directory in order to save Kafka consumer offset file");
            offsetPath.append(cpath);
            addPathSepChar(offsetPath);

            offsetPath.append(topic.c_str());
            offsetPath.append("-");
            offsetPath.append(partitionNum);
            if (!consumerGroup.empty())
            {
                offsetPath.append("-");
                offsetPath.append(consumerGroup.c_str());
            }
            offsetPath.append(".offset");
        }
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
                if (!isContainerized())
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
                    if (isContainerized())
                    {
                        applyYAMLConfig(GLOBAL_CONFIG_NAME, globalConfig);
                    }
                    else
                    {
                        applyConfig(GLOBAL_CONFIG_NAME, globalConfig);
                    }

                    // Set consumer callbacks
                    globalConfig->set("event_cb", static_cast<RdKafka::EventCb*>(this), errStr);

                    // Create the consumer
                    consumerPtr = RdKafka::Consumer::create(globalConfig, errStr);
                    delete globalConfig;

                    if (consumerPtr)
                    {
                        RdKafka::Conf* topicConfPtr = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

                        // Set the per-topic configuration parameters
                        topicConfPtr->set("group.id", consumerGroup, errStr);
                        topicConfPtr->set("auto.offset.reset", "smallest", errStr);

                        // Set any topic configurations from file, allowing
                        // overrides of above settings
                        std::string confName;
                        if (isContainerized())
                        {
                            confName = "consumer_topic_" + topic;
                            applyYAMLConfig(confName.c_str(), topicConfPtr);
                        }
                        else
                        {
                            confName = "kafka_consumer_topic_" + topic + ".conf";
                            applyConfig(confName.c_str(), topicConfPtr);
                        }

                        // Ensure that some items are set a certain way
                        // by setting them after loading the external conf
                        topicConfPtr->set("auto.commit.enable", "false", errStr);
                        topicConfPtr->set("enable.auto.commit", "false", errStr);
                        
                        if (!isContainerized())
                        {
                            topicConfPtr->set("offset.store.method", "file", errStr);
                            topicConfPtr->set("offset.store.path", offsetPath.str(), errStr);
                        }

                        // Create the topic
                        topicPtr.store(RdKafka::Topic::create(consumerPtr, topic, topicConfPtr, errStr), std::memory_order_release);
                        delete topicConfPtr;

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

    void Consumer::prepForMessageFetch()
    {
        // Make sure we have a valid connection to the Kafka cluster
        ensureSetup();

        // Start the local read queue
        RdKafka::ErrorCode startErr = consumerPtr->start(topicPtr, partitionNum, RdKafka::Topic::OFFSET_STORED);

        if (startErr == RdKafka::ERR_NO_ERROR)
        {
            if (doTrace(traceKafka))
            {
                DBGLOG("Kafka: Started Consumer for %s:%d @ %s", topic.c_str(), partitionNum, brokers.c_str());
            }
        }
        else
        {
            throw MakeStringException(-1, "Kafka: Failed to start Consumer read for %s:%d @ %s; error: %d", topic.c_str(), partitionNum, brokers.c_str(), startErr);
        }
    }

    void Consumer::commitOffset(__int64 offset) const
    {
        if (offset >= 0)
        {
            if (isContainerized())
            {
                topicPtr.load()->offset_store(partitionNum, offset);

                if (doTrace(traceKafka))
                {
                    DBGLOG("Kafka: Saved offset %lld", offset);
                }
            }
            else
            {
                // Create/overwrite a file using the same naming convention and
                // file contents that librdkafka uses so it can pick up where
                // we left off; NOTE:  librdkafka does not clean the topic name
                // or consumer group name when constructing this path
                // (which is actually a security concern), so we can't clean, either
                std::ofstream outFile(offsetPath.str(), std::ofstream::trunc);
                outFile << offset;

                if (doTrace(traceKafka))
                {
                    DBGLOG("Kafka: Saved offset %lld to %s", offset, offsetPath.str());
                }
            }
        }
    }

    void Consumer::initFileOffsetIfNotExist() const
    {
        if (!checkFileExists(offsetPath.str()))
        {
            commitOffset(0);

            if (doTrace(traceKafka))
            {
                DBGLOG("Kafka: Creating initial offset file %s", offsetPath.str());
            }
        }
    }

    void Consumer::event_cb(RdKafka::Event& event)
    {
        if (doTrace(traceKafka))
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
    class PublisherCacheObj
    {
        private:

            typedef std::map<std::string, Publisher*> ObjMap;

        public:

            /**
             * Constructor
             *
             */
            PublisherCacheObj()
            {

            }

            /**
             * Destructor
             *
             */
            ~PublisherCacheObj()
            {
                deleteAll();
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
             * for awhile; assumes a lock is held while modifying cachedPublishers
             */
            void expire()
            {
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

                if (doTrace(traceKafka) && expireCount > 0)
                {
                    DBGLOG("Kafka: Expired %d cached publisher%s", expireCount, (expireCount == 1 ? "" : "s"));
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
                        pubObjPtr = new Publisher(brokers, topic, pollTimeout);
                        cachedPublishers[key] = pubObjPtr;

                        if (doTrace(traceKafka))
                        {
                            DBGLOG("Kafka: Created and cached new publisher object: %s @ %s", topic.c_str(), brokers.c_str());
                        }

                        // Expire any old publishers before returning the new one
                        expire();
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
    };

    //--------------------------------------------------------------------------
    // Singleton Initialization
    //--------------------------------------------------------------------------

    static Singleton<PublisherCacheObj> publisherCache;
    static PublisherCacheObj & queryPublisherCache()
    {
        return *publisherCache.query([] () { return new PublisherCacheObj; });
    }

    //--------------------------------------------------------------------------
    // Advertised Entry Point Functions
    //--------------------------------------------------------------------------

    ECL_KAFKA_API bool ECL_KAFKA_CALL publishMessage(ICodeContext* ctx, const char* brokers, const char* topic, const char* message, const char* key)
    {
        Publisher* pubObjPtr = queryPublisherCache().getPublisher(brokers, topic, POLL_TIMEOUT);

        pubObjPtr->sendMessage(message, key);

        return true;
    }

    ECL_KAFKA_API bool ECL_KAFKA_CALL publishMessage(ICodeContext* ctx, const char* brokers, const char* topic, size32_t lenMessage, const char* message, size32_t lenKey, const char* key)
    {
        Publisher*          pubObjPtr = queryPublisherCache().getPublisher(brokers, topic, POLL_TIMEOUT);
        std::string         messageStr(message, rtlUtf8Size(lenMessage, message));
        std::string         keyStr(key, rtlUtf8Size(lenKey, key));

        pubObjPtr->sendMessage(messageStr, keyStr);

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
        RdKafka::Conf* globalConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        if (globalConfig)
        {
            // Load global config to pick up any protocol modifications
            if (isContainerized())
            {
                applyYAMLConfig(GLOBAL_CONFIG_NAME, globalConfig);
            }
            else
            {
                applyConfig(GLOBAL_CONFIG_NAME, globalConfig);
            }

            // rd_kafka_new() takes ownership of the lower-level conf object, which in this case is a
            // pointer currently owned by globalConfig; we need to pass a duplicate
            // the conf pointer to rd_kafka_new() so we don't mangle globalConfig's internals
            rd_kafka_conf_t* conf = rd_kafka_conf_dup(globalConfig->c_ptr_global());
            rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
            delete globalConfig;

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
                            DBGLOG("Kafka: Error retrieving metadata from topic: %s @ %s: '%s'", topic, brokers, rd_kafka_err2str(err));
                        }

                        rd_kafka_topic_destroy(rkt);
                    }
                    else
                    {
                        if (doTrace(traceKafka))
                        {
                            DBGLOG("Kafka: Could not create topic configuration object: %s @ %s", topic, brokers);
                        }
                    }
                }
                else
                {
                    if (doTrace(traceKafka))
                    {
                        DBGLOG("Kafka: Could not add brokers: %s @ %s", topic, brokers);
                    }
                }

                rd_kafka_destroy(rk);
            }
            else
            {
                DBGLOG("Kafka: Could not create consumer configuration object : %s @ %s: '%s'", topic, brokers, errstr);
            }
        }
        else
        {
            if (doTrace(traceKafka))
            {
                DBGLOG("Kafka: Could not create global configuration object: %s @ %s", topic, brokers);
            }
        }

        if (pCount == 0)
        {
            DBGLOG("Kafka: Unable to retrieve partition count from topic: %s @ %s", topic, brokers);
        }

        return pCount;
    }

    ECL_KAFKA_API IRowStream* ECL_KAFKA_CALL getMessageDataset(ICodeContext* ctx, IEngineRowAllocator* allocator, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 maxRecords)
    {
        Consumer* consumerObjPtr = new Consumer(brokers, topic, consumerGroup, partitionNum);

        try
        {
            consumerObjPtr->prepForMessageFetch();
        }
        catch(...)
        {
            delete(consumerObjPtr);
            throw;
        }

        return new KafkaStreamedDataset(consumerObjPtr, allocator, maxRecords);
    }

    ECL_KAFKA_API __int64 ECL_KAFKA_CALL setMessageOffset(ICodeContext* ctx, const char* brokers, const char* topic, const char* consumerGroup, __int32 partitionNum, __int64 newOffset)
    {
        Consumer consumerObj(brokers, topic, consumerGroup, partitionNum);

        consumerObj.commitOffset(newOffset);

        return newOffset;
    }
}

//==============================================================================
// Plugin Initialization and Teardown
//==============================================================================

#define CURRENT_KAFKA_VERSION "kafka plugin 1.1.0"

static const char* kafkaCompatibleVersions[] = {
    "kafka plugin 1.0.0",
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
    pb->description = "ECL plugin library for the C++ API in librdkafka++";

    return true;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    KafkaPlugin::publisherCache.destroy();
    RdKafka::wait_destroyed(3000);
}
