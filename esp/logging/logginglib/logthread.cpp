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

#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jdebug.hpp"
#include "LoggingErrors.hpp"
#include "LogSerializer.hpp"
#include "logthread.hpp"

const char* const PropMaxLogQueueLength = "MaxLogQueueLength";
const char* const PropQueueSizeSignal = "QueueSizeSignal";
const char* const PropMaxTriesRS = "MaxTriesRS";
const char* const PropFailSafe = "FailSafe";
const char* const PropFailSafeLogsDir = "FailSafeLogsDir";

#define     MaxLogQueueLength   500000 //Write a warning into log when queue length is greater than 500000
#define     QueueSizeSignal     10000 //Write a warning into log when queue length is increased by 10000
const int DefaultMaxTriesRS = -1;   // Max. # of attempts to send log message to WsReportService.  Default:  infinite

extern LOGGINGCOMMON_API IUpdateLogThread* createUpdateLogThread(IPropertyTree* _cfg, const char* _service, const char* _agentName, IEspLogAgent* _logAgent)
{
    if (!_cfg)
        return NULL;

    IUpdateLogThread* loggingThread  = new CLogThread(_cfg, _service, _agentName, _logAgent);
    loggingThread->start();

    return loggingThread;
}

CLogThread::CLogThread(IPropertyTree* _cfg , const char* _service, const char* _agentName, IEspLogAgent* _logAgent)
    : stopping(false), agentName(_agentName)
{
    if(!_agentName || !*_agentName)
        throw MakeStringException(-1,"No Logging agent name defined");

    if(!_cfg)
        throw MakeStringException(-1,"No Logging agent Configuration for %s", _agentName);

    if(!_service || !*_service)
        throw MakeStringException(-1,"No service name defined for %s", _agentName);

    if(!_logAgent)
        throw MakeStringException(-1,"No Logging agent interface for %s", _agentName);

    const char* servicesConfig = _cfg->queryProp("@services");
    if (!servicesConfig || !*servicesConfig)
        throw MakeStringException(-1,"No Logging Service defined for %s", _agentName);

    StringArray serviceArray;
    serviceArray.appendListUniq(servicesConfig, ",");

    unsigned i=0;
    ForEachItemIn(s, serviceArray)
    {
        const char* service = serviceArray.item(s);
        if (service && strieq(service, "UpdateLOG"))
            services[i++] = LGSTUpdateLOG;
        else if (service && strieq(service, "GetTransactionSeed"))
            services[i++] = LGSTGetTransactionSeed;
        else if (service && strieq(service, "GetTransactionID"))
            services[i++] = LGSTGetTransactionID;
    }
    services[i] = LGSTterm;

    logAgent.setown(_logAgent);

    maxLogQueueLength = _cfg->getPropInt(PropMaxLogQueueLength, MaxLogQueueLength);
    signalGrowingQueueAt = _cfg->getPropInt(PropQueueSizeSignal, QueueSizeSignal);
    maxLogRetries = _cfg->getPropInt(PropMaxTriesRS, DefaultMaxTriesRS);
    failSafeLogging = _cfg->getPropBool(PropFailSafe);
    if(failSafeLogging)
    {
        const char * logsDir = _cfg->queryProp(PropFailSafeLogsDir);
        if (!logsDir || !*logsDir)
            logsDir = "./FailSafeLogs";

        logFailSafe.setown(createFailSafeLogger(_service, _agentName, logsDir));
    }
}

CLogThread::~CLogThread()
{
    ESPLOG(LogMax, "CLogThread::~CLogThread()");
}

void CLogThread::start()
{
    Thread::start();
}

int CLogThread::run()
{
    Link();
    if(logFailSafe.get())
        checkPendingLogs(false);

    while(!stopping)
    {
        m_sem.wait(UPDATELOGTHREADWAITINGTIME);

        sendLog();
        if(logFailSafe.get())
        {
            checkPendingLogs(true);
            checkRollOver();
        }
    }
    Release();
    return 0;
}

void CLogThread::stop()
{
    try
    {
        CriticalBlock b(logQueueCrit);
        if (!logQueue.ordinality() && logFailSafe.get())
            logFailSafe->RollCurrentLog();
        //If logQueue is not empty, the log files are rolled over so that queued jobs can be read
        //when the CLogThread is restarted.
    }
    catch(...)
    {
        DBGLOG("Exception");
    }
    stopping = true;
    m_sem.signal();
    join();
}

bool CLogThread::queueLog(IEspUpdateLogRequest* logRequest)
{
    if (!logRequest)
        return false;

    Owned<IEspUpdateLogRequestWrap> logRequestWrap = new CUpdateLogRequestWrap(NULL, logRequest->getOption(), logRequest->getLogContent());
    return enqueue(logRequestWrap);
}

bool CLogThread::queueLog(IEspUpdateLogRequestWrap* logRequest)
{
    unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
    logAgent->filterLogContent(logRequest);
    if (getEspLogLevel()>=LogNormal)
        DBGLOG("LThread:filterLog: %dms\n", msTick() -  startTime);
    return enqueue(logRequest);
}

bool CLogThread::enqueue(IEspUpdateLogRequestWrap* logRequest)
{
    if (logFailSafe.get())
    {
        StringBuffer GUID, reqBuf;
        unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
        logFailSafe->GenerateGUID(GUID, NULL);
        logRequest->setGUID(GUID.str());
        if (serializeLogRequestContent(logRequest, reqBuf))
            logFailSafe->Add(GUID, reqBuf.str());
        if (getEspLogLevel()>=LogNormal)
            DBGLOG("LThread:addToFailSafe: %dms\n", msTick() -  startTime);
    }

    writeJobQueue(logRequest);

    m_sem.signal();

    return true;
}

void CLogThread::sendLog()
{
    try
    {
        if(stopping)
            return;

        int recSend = 0;
        while(true)
        {
            IEspUpdateLogRequestWrap* logRequest  = readJobQueue();
            if (!logRequest)
                break;

            const char* GUID= logRequest->getGUID();
            if ((!GUID || !*GUID) && failSafeLogging && logFailSafe.get())
                continue;

            try
            {
                unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
                Owned<IEspUpdateLogResponse> logResponse = createUpdateLogResponse();
                logAgent->updateLog(*logRequest, *logResponse);
                if (!logResponse)
                    throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "no response");
                if (logResponse->getStatusCode())
                {
                    const char* statusMessage = logResponse->getStatusMessage();
                    if(statusMessage && *statusMessage)
                        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "%s", statusMessage);
                    else
                        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Unknown error");
                }
                if (getEspLogLevel()>=LogNormal)
                    DBGLOG("LThread:updateLog: %dms\n", msTick() -  startTime);

                if(failSafeLogging && logFailSafe.get())
                {
                    unsigned startTime1 = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
                    logFailSafe->AddACK(GUID);
                    if (getEspLogLevel()>=LogNormal)
                        DBGLOG("LThread:AddACK: %dms\n", msTick() -  startTime1);
                }
                logRequest->Release();//Make sure that no data (such as GUID) is needed before releasing the logRequest.
            }
            catch(IException* e)
            {
                StringBuffer errorStr, errorMessage;
                errorMessage.appendf("Failed to update log for %s: error code %d, error message %s", GUID, e->errorCode(), e->errorMessage(errorStr).str());
                e->Release();

                bool willRetry = false;
                if (maxLogRetries != 0)
                {
                    unsigned retry = logRequest->incrementRetryCount();
                    if (retry > maxLogRetries)
                        errorMessage.append(" Max logging retries exceeded.");
                    else
                    {
                        willRetry = true;
                        writeJobQueue(logRequest);
                        errorMessage.appendf(" Adding back to logging queue for retrying %d.", retry);
                    }
                }
                if (!willRetry)
                {
                    if(failSafeLogging && logFailSafe.get())
                        logFailSafe->AddACK(GUID);
                    logRequest->Release();
                }
                ERRLOG("%s", errorMessage.str());
            }
        }
    }
    catch(IException* e)
    {
        StringBuffer errorStr, errorMessage;
        errorMessage.append("Exception thrown within update log thread: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
        ERRLOG("%s", errorMessage.str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception thrown within update log thread");
    }

    return;
}

//////////////////////////FailSafe////////////////////////////
void CLogThread::checkRollOver()
{
    try
    {
        bool bRollover = false;

        time_t tNow;
        time(&tNow);
        struct tm ltNow;
        localtime_r(&tNow, &ltNow);
        if ((ltNow.tm_year != m_startTime.tm_year || ltNow.tm_yday != m_startTime.tm_yday))
        {
            bRollover = true;
            localtime_r(&tNow, &m_startTime);  // reset the start time for next rollover check
        }
        if (!bRollover)
            return;

        //Rename .log files to .old files
        logFailSafe->SafeRollover();

        CriticalBlock b(logQueueCrit);

        //Check and add queued requests to tank(.log) files
        unsigned numNewArrivals = logQueue.ordinality();
        if(numNewArrivals <= 0)
            return;

        ESPLOG(LogMax, "writing %d requests in the queue to the rolled over tank file.", numNewArrivals);
        unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
        for(unsigned i = 0; i < numNewArrivals; i++)
        {
            IInterface* pRequest = logQueue.item(i);
            if (!pRequest)
                continue;

            IEspUpdateLogRequestWrap* pEspRequest = dynamic_cast<IEspUpdateLogRequestWrap*>(pRequest);
            if(!pEspRequest)
                continue;

            StringBuffer reqBuf;
            const char* GUID = pEspRequest->getGUID();
            if(GUID && *GUID && serializeLogRequestContent(pEspRequest, reqBuf))
                logFailSafe->Add(GUID, reqBuf.str());
        }
        if (getEspLogLevel()>=LogNormal)
            DBGLOG("LThread:AddFailSafe: %dms\n", msTick() -  startTime);
    }
    catch(IException* Ex)
    {
        StringBuffer str;
        Ex->errorMessage(str);
        ERRLOG("Exception thrown during tank file rollover: %s",str.str());
        Ex->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception thrown during tank file rollover.");
    }
}

unsigned CLogThread::serializeLogRequestContent(IEspUpdateLogRequestWrap* pRequest, StringBuffer& logData)
{
    const char* GUID = pRequest->getGUID();
    const char* option = pRequest->getOption();
    const char* logRequest = pRequest->getUpdateLogRequest();
    if (GUID && *GUID)
        logData.append("<GUID>").append(GUID).append("</GUID>");
    if (option && *option)
        logData.append("<Option>").append(option).append("</Option>");
    if (logRequest && *logRequest)
    {
        StringBuffer buffer;
        JBASE64_Encode(logRequest, strlen(logRequest), buffer);
        logData.append("<LogRequest>").append(buffer.str()).append("</LogRequest>");
    }
    return logData.length();
}

void CLogThread::checkPendingLogs(bool bOneRecOnly)
{
    try
    {
        bool queueLogError = false;
        bool bFirst = true;

        StringBuffer GUID, logData;
        while (logFailSafe->PopPendingLogRecord(GUID, logData))
        {
            if (bFirst && !bOneRecOnly)
            {
                DBGLOG("We have old logs!. Will now try and recover the lost log messages");
                bFirst = false;
            }

            Owned<IEspUpdateLogRequestWrap> logRequest = unserializeLogRequestContent(logData.str());
            if (!logRequest)
                ERRLOG("checkPendingLogs: failed to unserialize: %s", logData.str());
            else if (!enqueue(logRequest))
            {
                ERRLOG("checkPendingLogs: failed to add a log request to queue");
                queueLogError=true;
            }

            if (bOneRecOnly)
                break;
        }
        //if everything went ok then we should be able to rollover the old logs.
        if (!queueLogError && !bOneRecOnly)
            logFailSafe->RolloverAllLogs();
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("CheckPendingLogs: %s:" ,errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception thrown in CheckPendingLogs");
    }
}

IEspUpdateLogRequestWrap* CLogThread::unserializeLogRequestContent(const char* logData)
{
    if (!logData && *logData)
        return NULL;

    Owned<IPropertyTree> pLogTree = createPTreeFromXMLString(logData);
    if (!pLogTree)
        return NULL;

    const char* guid = pLogTree->queryProp("GUID");
    const char* opt = pLogTree->queryProp("Option");
    const char* logRequest = pLogTree->queryProp("LogRequest");
    if (!logRequest || !*logRequest)
        return NULL;

    StringBuffer buffer;
    JBASE64_Decode(logRequest, buffer);

    return new CUpdateLogRequestWrap(guid, opt, buffer.str());
};

void CLogThread::writeJobQueue(IEspUpdateLogRequestWrap* jobToWrite)
{
    if (jobToWrite)
    {
        unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
        CriticalBlock b(logQueueCrit);
        if (getEspLogLevel()>=LogNormal)
            DBGLOG("LThread:waitWQ: %dms\n", msTick() -  startTime);

        int QueueSize = logQueue.ordinality();
        if(QueueSize > maxLogQueueLength)
            ERRLOG("LOGGING QUEUE SIZE %d EXECEEDED MaxLogQueueLength %d, check the logging server.",QueueSize, maxLogQueueLength);

        if(QueueSize!=0 && QueueSize % signalGrowingQueueAt == 0)
            ERRLOG("Logging Queue at %d records. Check the logging server.",QueueSize);

        logQueue.enqueue(LINK(jobToWrite));
    }
}

IEspUpdateLogRequestWrap* CLogThread::readJobQueue()
{
    unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
    CriticalBlock b(logQueueCrit);
    if (getEspLogLevel()>=LogNormal)
        DBGLOG("LThread:waitRQ: %dms\n", msTick() -  startTime);
    return (IEspUpdateLogRequestWrap*)logQueue.dequeue();
}
