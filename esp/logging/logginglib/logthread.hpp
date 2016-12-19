/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#ifndef _LOGTHREAD_HPP__
#define _LOGTHREAD_HPP__

#include "jthread.hpp"
#include "jqueue.tpp"
#include "loggingagentbase.hpp"
#include "LogFailSafe.hpp"

interface IUpdateLogThread : extends IInterface
{
    virtual int run() = 0;
    virtual void start() = 0;
    virtual void stop() = 0;

    virtual IEspLogAgent* getLogAgent() = 0;

    virtual bool hasService(LOGServiceType service) = 0;
    virtual bool queueLog(IEspUpdateLogRequest* logRequest) = 0;
    virtual bool queueLog(IEspUpdateLogRequestWrap* logRequest) = 0;
    virtual void sendLog() = 0;
};

class CLogThread : public Thread , implements IUpdateLogThread
{
    bool stopping;
    StringAttr agentName;
    int maxLogQueueLength;
    int signalGrowingQueueAt;
    unsigned maxLogRetries;   // Max. # of attempts to send log message

    Owned<IEspLogAgent> logAgent;
    LOGServiceType services[MAXLOGSERVICES];
    QueueOf<IInterface, false> logQueue;
    CriticalSection logQueueCrit;
    Semaphore       m_sem;

    bool failSafeLogging;
    Owned<ILogFailSafe> logFailSafe;
    struct tm         m_startTime;

    unsigned serializeLogRequestContent(IEspUpdateLogRequestWrap* request, StringBuffer& logData);
    IEspUpdateLogRequestWrap* unserializeLogRequestContent(const char* logData);
    bool enqueue(IEspUpdateLogRequestWrap* logRequest);
    void writeJobQueue(IEspUpdateLogRequestWrap* jobToWrite);
    IEspUpdateLogRequestWrap* readJobQueue();

public:
    IMPLEMENT_IINTERFACE;

    CLogThread();
    CLogThread(IPropertyTree* _agentConfig, const char* _service, const char* _agentName, IEspLogAgent* _logAgent = NULL);
    virtual ~CLogThread();

    IEspLogAgent* getLogAgent() {return logAgent;};

    bool hasService(LOGServiceType service)
    {
        unsigned int i = 0;
        while (services[i] != LGSTterm)
        {
            if (services[i] == service)
                return true;
            i++;
        }
        return false;
    }
    void addService(LOGServiceType service)
    {
        unsigned i=0;
        while (services[i] != LGSTterm) i++;
        services[i] = service;
    };

    int run();
    void start();
    void stop();

    bool queueLog(IEspUpdateLogRequest* logRequest);
    bool queueLog(IEspUpdateLogRequestWrap* logRequest);
    void sendLog();

    void checkPendingLogs(bool oneRecordOnly=false);
    void checkRollOver();
};

extern LOGGINGCOMMON_API IUpdateLogThread* createUpdateLogThread(IPropertyTree* _cfg, const char* _service, const char* _agentName, IEspLogAgent* _logAgent);

#endif // _LOGTHREAD_HPP__
