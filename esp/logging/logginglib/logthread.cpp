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
#include "compressutil.hpp"

const char* const PropMaxLogQueueLength = "MaxLogQueueLength";
const char* const PropQueueSizeSignal = "QueueSizeSignal";
const char* const PropMaxTriesRS = "MaxTriesRS";
const char* const PropFailSafe = "FailSafe";
const char* const PropDisableFailSafe = "DisableFailSafe";
const char* const PropAckedFiles = "AckedFiles";
const char* const PropDefaultAckedFiles = "AckedFiles";
const char* const PropAckedLogRequests = "AckedLogRequests";
const char* const PropDefaultAckedLogRequests = "AckedLogRequests";
const char* const PropPendingLogBufferSize = "PendingLogBufferSize";
const char* const PropReadRequestWaitingSeconds = "ReadRequestWaitingSeconds";
const char* const sendLogKeyword = "_sending_";
const unsigned sendLogKeywordLen = strlen(sendLogKeyword);
const unsigned dateTimeStringLength = 19; //yyyy_mm_dd_hh_mm_ss

#define     MaxLogQueueLength   500000 //Write a warning into log when queue length is greater than 500000
#define     QueueSizeSignal     10000 //Write a warning into log when queue length is increased by 10000
const int DefaultMaxTriesRS = -1;   // Max. # of attempts to send log message to WsReportService.  Default:  infinite

extern LOGGINGCOMMON_API IUpdateLogThread* createUpdateLogThread(IPropertyTree* _cfg, const char* _service, const char* _agentName,
    const char* _tankFileDir, IEspLogAgent* _logAgent)
{
    if (!_cfg)
        return NULL;

    IUpdateLogThread* loggingThread  = new CLogThread(_cfg, _service, _agentName, _logAgent, _tankFileDir);
    loggingThread->start();

    return loggingThread;
}

CLogThread::CLogThread(IPropertyTree* _cfg , const char* _service, const char* _agentName, IEspLogAgent* _logAgent, const char* _tankFileDir)
    : stopping(false), agentName(_agentName), tankFileDir(_tankFileDir)
{
    if(!_agentName || !*_agentName)
        throw MakeStringException(-1,"No Logging agent name defined");

    if(!_cfg)
        throw MakeStringException(-1,"No Logging agent Configuration for %s", _agentName);

    if(!_service || !*_service)
        throw MakeStringException(-1,"No service name defined for %s", _agentName);

    if(!_logAgent)
        throw MakeStringException(-1,"No Logging agent interface for %s", _agentName);

    logAgent.setown(_logAgent);

    maxLogQueueLength = _cfg->getPropInt(PropMaxLogQueueLength, MaxLogQueueLength);
    signalGrowingQueueAt = _cfg->getPropInt(PropQueueSizeSignal, QueueSizeSignal);
    maxLogRetries = _cfg->getPropInt(PropMaxTriesRS, DefaultMaxTriesRS);
    //For decoupled logging, the fail safe is not needed because the logging agent always
    //picks up the logging requests from tank file.
    ensureFailSafe = _cfg->getPropBool(PropFailSafe) && !_cfg->getPropBool(PropDisableFailSafe, false);
    if(ensureFailSafe)
    {
        logFailSafe.setown(createFailSafeLogger(_cfg, _service, _agentName));
        PROGLOG("FailSafe ensured for %s", agentName.get());
    }

    time_t tNow;
    time(&tNow);
    localtime_r(&tNow, &m_startTime);

    if (tankFileDir.get())
    {
        Owned<CLogRequestReaderSettings> settings = new CLogRequestReaderSettings();
        settings->tankFileDir.set(tankFileDir.get());

        const char* ackedFiles = _cfg->queryProp(PropAckedFiles);
        settings->ackedFileList.set(isEmptyString(ackedFiles) ? PropDefaultAckedFiles : ackedFiles);
        const char* ackedLogRequestFile = _cfg->queryProp(PropAckedLogRequests);
        settings->ackedLogRequestFile.set(isEmptyString(ackedLogRequestFile) ? PropDefaultAckedLogRequests : ackedLogRequestFile);
        int pendingLogBufferSize = _cfg->getPropInt(PropPendingLogBufferSize, DEFAULTPENDINGLOGBUFFERSIZE);
        if (pendingLogBufferSize <= 0)
            throw MakeStringException(-1, "The %s (%d) should be greater than 0.", PropPendingLogBufferSize, pendingLogBufferSize);

        settings->pendingLogBufferSize = pendingLogBufferSize;
        int waitSeconds = _cfg->getPropInt(PropReadRequestWaitingSeconds, DEFAULTREADLOGREQUESTWAITSECOND);
        if (waitSeconds <= 0)
            throw MakeStringException(-1, "The %s (%d) should be greater than 0.", PropReadRequestWaitingSeconds, waitSeconds);

        settings->waitSeconds = waitSeconds;
        PROGLOG("%s %s: %s", agentName.get(), PropAckedFiles, settings->ackedFileList.str());
        PROGLOG("%s %s: %s", agentName.get(), PropDefaultAckedLogRequests, settings->ackedLogRequestFile.str());
        PROGLOG("%s %s: %d. %s: %d", agentName.get(), PropReadRequestWaitingSeconds, settings->waitSeconds, PropPendingLogBufferSize, settings->pendingLogBufferSize);

        checkAndCreateFile(settings->ackedFileList);
        checkAndCreateFile(settings->ackedLogRequestFile);
        logRequestReader.setown(new CLogRequestReader(settings.getClear(), this));
        logRequestReader->setTankFilePattern(_service);
    }
    PROGLOG("%s CLogThread started.", agentName.get());
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
    if (!logRequestReader && logFailSafe.get())
        checkPendingLogs(false);

    while(!stopping)
    {
        m_sem.wait(UPDATELOGTHREADWAITINGTIME);
        sendLog();
        if (!logRequestReader && logFailSafe.get())
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
        if (!logQueue.ordinality() && !logRequestReader && logFailSafe.get() && logFailSafe->canRollCurrentLog())
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
    return enqueue(logRequestWrap, nullptr);
}

bool CLogThread::queueLog(IEspUpdateLogRequestWrap* logRequest)
{
    unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
    Owned<IEspUpdateLogRequestWrap> logRequestFiltered = logAgent->filterLogContent(logRequest);
    ESPLOG(LogNormal, "LThread:filterLog: %dms\n", msTick() -  startTime);
    return enqueue(logRequestFiltered, nullptr);
}

bool CLogThread::enqueue(IEspUpdateLogRequestWrap* logRequest, const char* guid)
{
    if (!logRequestReader && logFailSafe.get())
    {
        StringBuffer GUID, reqBuf;
        unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
        if (isEmptyString(guid))
            logFailSafe->GenerateGUID(GUID, nullptr);
        else
            GUID.set(guid);
        logRequest->setGUID(GUID.str());
        //This part of code can only be reached for non decoupled logging.
        //For non decoupled logging, the manager has filtered out skipped agents.
        //This part of code is called only if the agent is not skipped.
        //So, the scriptValues section is not needed to be in the logFailSafe->Add().
        if (serializeLogRequestContent(logRequest, reqBuf))
            logFailSafe->Add(GUID, nullptr, reqBuf.str(), nullptr);
        ESPLOG(LogNormal, "LThread:addToFailSafe: %dms\n", msTick() -  startTime);
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
            if ((!GUID || !*GUID) && ensureFailSafe && logFailSafe.get())
                continue;

            PROGLOG("Sending %s ...\n", GUID);
            Owned<IEspUpdateLogRequestWrap> logRequestInFile;
            if (!logRequestReader)
                logRequestInFile.setown(checkAndReadLogRequestFromSharedTankFile(logRequest));

            try
            {
                unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
                Owned<IEspUpdateLogResponse> logResponse = createUpdateLogResponse();
                if (logRequestInFile)
                    logAgent->updateLog(*logRequestInFile, *logResponse);
                else
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
                ESPLOG(LogNormal, "LThread:updateLog: %dms\n", msTick() -  startTime);

                if (logRequestReader)
                {
                    unsigned startTime1 = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
                    logRequestReader->addACK(GUID);
                    PROGLOG("%s acked: %dms\n", GUID, msTick() -  startTime1);
                }
                else if(ensureFailSafe && logFailSafe.get())
                {
                    unsigned startTime1 = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
                    logFailSafe->AddACK(GUID);
                    ESPLOG(LogNormal, "LThread:AddACK: %dms\n", msTick() -  startTime1);
                }

                logRequest->Release();//Make sure that no data (such as GUID) is needed before releasing the logRequest.
            }
            catch(IException* e)
            {
                StringBuffer errorStr, errorMessage;
                errorMessage.appendf("Failed to update log for %s: error code %d, error message %s", GUID, e->errorCode(), e->errorMessage(errorStr).str());
                e->Release();

                if (logRequestInFile)
                    logRequest->setNoResend(logRequestInFile->getNoResend());

                bool willRetry = false;
                if (!logRequest->getNoResend() && (maxLogRetries != 0))
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
                    logRequest->Release();
                }
                IERRLOG("%s", errorMessage.str());
            }
        }
    }
    catch(IException* e)
    {
        StringBuffer errorStr, errorMessage;
        errorMessage.append("Exception thrown within update log thread: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
        IERRLOG("%s", errorMessage.str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown exception thrown within update log thread");
    }

    return;
}

//At first, we check whether the logRequest contains the information about original log Request
//in shared tank file created by logging manager. If yes, try to read the original log Request
//based on the information. If the original log Request is found and unserialized, return new
//IEspUpdateLogRequestWrap which contains original log Request.
IEspUpdateLogRequestWrap* CLogThread::checkAndReadLogRequestFromSharedTankFile(IEspUpdateLogRequestWrap* logRequest)
{
    //Read LogRequestInFile info if exists.
    Owned<IPropertyTree> logInFle = createPTreeFromXMLString(logRequest->getUpdateLogRequest());
    if (!logInFle)
        return nullptr;

    const char* GUID = logInFle->queryProp(LOGREQUEST_GUID);
    if (isEmptyString(GUID))
        return nullptr;

    const char* fileName = logInFle->queryProp(LOGCONTENTINFILE_FILENAME);
    if (isEmptyString(fileName))
        return nullptr;

    __int64 pos = logInFle->getPropInt64(LOGCONTENTINFILE_FILEPOS, -1);
    if (pos < 0)
        return nullptr;

    int size = logInFle->getPropInt64(LOGCONTENTINFILE_FILESIZE, -1);
    if (size < 0)
        return nullptr;

    Owned<CLogRequestInFile> reqInFile = new CLogRequestInFile();
    reqInFile->setGUID(GUID);
    reqInFile->setFileName(fileName);
    reqInFile->setPos(pos);
    reqInFile->setSize(size);

    //Read Log Request from the file
    StringBuffer logRequestStr;
    CLogSerializer logSerializer;
    if (!logSerializer.readLogRequest(reqInFile, logRequestStr))
    {
        ERRLOG("Failed to read Log Request from %s", fileName);
        return nullptr;
    }

    try
    {
        Owned<IPropertyTree> logRequestTree = createPTreeFromXMLString(logRequestStr.str());
        if (!logRequestTree)
            return nullptr;

        const char* guid = logRequestTree->queryProp("GUID");
        const char* opt = logRequestTree->queryProp("Option");
        const char* reqBuf = logRequestTree->queryProp("LogRequest");
        if (isEmptyString(reqBuf))
            return nullptr;

        StringBuffer decoded, req;
        JBASE64_Decode(reqBuf, decoded);
        LZWExpand(decoded, decoded.length(), req);

        return new CUpdateLogRequestWrap(guid, opt, req.str());
    }
    catch(IException* e)
    {
        StringBuffer errorStr;
        ERRLOG("Exception when unserializing Log Request Content: %d %s", e->errorCode(), e->errorMessage(errorStr).str());
        e->Release();
    }
    return nullptr;
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
            //This part of code can only be reached for non decoupled logging.
            //For non decoupled logging, the manager has filtered out skipped agents.
            //This part of code is called only if the agent is not skipped.
            //So, the scriptValues section is not needed to be in the logFailSafe->Add().
            if(GUID && *GUID && serializeLogRequestContent(pEspRequest, reqBuf))
                logFailSafe->Add(GUID, nullptr, reqBuf.str(), nullptr);
        }
        ESPLOG(LogNormal, "LThread:AddFailSafe: %dms\n", msTick() -  startTime);
    }
    catch(IException* Ex)
    {
        StringBuffer str;
        Ex->errorMessage(str);
        IERRLOG("Exception thrown during tank file rollover: %s",str.str());
        Ex->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown exception thrown during tank file rollover.");
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
        JBASE64_Encode(logRequest, strlen(logRequest), buffer, true);
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

            Owned<IEspUpdateLogRequestWrap> logRequest = unserializeLogRequestContent(logData.str(), false);
            if (!logRequest)
                IERRLOG("checkPendingLogs: failed to unserialize: %s", logData.str());
            else if (!enqueue(logRequest, GUID))
            {
                OERRLOG("checkPendingLogs: failed to add a log request to queue");
                queueLogError=true;
            }

            if (bOneRecOnly)
                break;
        }
        //if everything went ok then we should be able to rollover the old logs.
        if (!queueLogError && !bOneRecOnly)
            logFailSafe->RollOldLogs();
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        IERRLOG("CheckPendingLogs: %s:" ,errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown exception thrown in CheckPendingLogs");
    }
}

//When the logData is read from a main tank file, it should be decrypted.
//For non-decoupled logging agents, each logging agent may have its own tank file (with the location and
//position of the main tank file). The logData from those agent tank files is not encrypted. So, it should
//not be decrypted.
//BTW: For non-decoupled logging agents, the logData from main tank file is read and decrypted in 
//checkAndReadLogRequestFromSharedTankFile().
IEspUpdateLogRequestWrap* CLogThread::unserializeLogRequestContent(const char* logData, bool decompress)
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

    StringBuffer decoded;
    JBASE64_Decode(logRequest, decoded);
    if (!decompress)
        return new CUpdateLogRequestWrap(guid, opt, decoded.str());

    StringBuffer req;
    LZWExpand(decoded, decoded.length(), req);

    return new CUpdateLogRequestWrap(guid, opt, req.str());
};

void CLogThread::writeJobQueue(IEspUpdateLogRequestWrap* jobToWrite)
{
    if (jobToWrite)
    {
        unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
        CriticalBlock b(logQueueCrit);
        ESPLOG(LogNormal, "LThread:waitWQ: %dms\n", msTick() -  startTime);

        int QueueSize = logQueue.ordinality();
        if(QueueSize > maxLogQueueLength)
            OERRLOG("LOGGING QUEUE SIZE %d EXCEEDED MaxLogQueueLength %d, check the logging server.",QueueSize, maxLogQueueLength);

        if(QueueSize!=0 && QueueSize % signalGrowingQueueAt == 0)
            OERRLOG("Logging Queue at %d records. Check the logging server.",QueueSize);

        logQueue.enqueue(LINK(jobToWrite));
    }
}

IEspUpdateLogRequestWrap* CLogThread::readJobQueue()
{
#define LOG_LEVEL LogNormal
    unsigned startTime = (getEspLogLevel()>=LOG_LEVEL) ? msTick() : 0;
    CriticalBlock b(logQueueCrit);
    unsigned delta = (getEspLogLevel()>=LOG_LEVEL) ? msTick() - startTime : 0;
    if (delta > 1) // <=1ms is not indicative of an unexpected delay
        ESPLOG(LOG_LEVEL, "LThread:waitRQ: %dms", delta);
    return (IEspUpdateLogRequestWrap*)logQueue.dequeue();
#undef LOG_LEVEL
}

void CLogThread::checkAndCreateFile(const char* fileName)
{
    Owned<IFile> file = createIFile(fileName);
    if(file->isFile() != fileBool::notFound)
        return;

    StringBuffer dir;
    splitFilename(fileName, &dir, &dir, nullptr, nullptr);
    recursiveCreateDirectory(dir);

    Owned<IFileIO> io = file->openShared(IFOcreate, IFSHfull);
    PROGLOG("CLogThread::checkAndCreateFile: %s is created.", fileName);
}

CLogRequestReader::~CLogRequestReader()
{
    stopping = true;
    sem.signal();
    threaded.join();
}

void CLogRequestReader::threadmain()
{
    PROGLOG("LogRequest Reader Thread started.");
    readAcked(settings->ackedFileList, ackedLogFileCheckList);
    readAcked(settings->ackedLogRequestFile, ackedLogRequests);

    unsigned waitMillSeconds = 1000*settings->waitSeconds;
    while (!stopping)
    {
        ESPLOG(LogMax, "#### CLogRequestReader: the loop for reading log requests begins.");
        if (!paused)
        {
            try
            {
                ESPLOG(LogMax, "#### CLogRequestReader: waiting for readLogRequest().");
                CriticalBlock b(crit);
                readLogRequest();
                if (newAckedLogFiles.length())
                {
                    updateAckedFileList();
                    updateAckedLogRequestList();
                }

                PROGLOG("CLogRequestReader: finished the loop for reading log requests.");
            }
            catch(IException *e)
            {
                StringBuffer msg;
                IERRLOG("Exception %d:%s in CLogRequestReader::threadmain()", e->errorCode(), e->errorMessage(msg).str());
                e->Release();
            }
            catch(...)
            {
                IERRLOG("Unknown exception in CLogRequestReader::threadmain()");
            }
        }

        sem.wait(waitMillSeconds);
    }
    PROGLOG("LogRequest Reader Thread terminated.");
}

void CLogRequestReader::reportAckedLogFiles(StringArray& ackedLogFiles)
{
    CriticalBlock b(crit);
    for (auto r : ackedLogFileCheckList)
        ackedLogFiles.append(r.c_str());
    ESPLOG(LogMax, "#### The reportAckedLogFiles() done.");
}

//This method is used to setup an acked file list which contains the acked files for all agents.
//The first agent reports all the acked files in that agent using the reportAckedLogFiles().
//Using this method, the list of the acked files is given to the rest agents. If any file inside
//the list has not been acked in the rest agents, the file should be removed from the list.
void CLogRequestReader::removeUnknownAckedLogFiles(StringArray& ackedLogFiles)
{
    CriticalBlock b(crit);
    ForEachItemInRev(i, ackedLogFiles)
    {
        const char* node = ackedLogFiles.item(i);
        if (ackedLogFileCheckList.find(node) == ackedLogFileCheckList.end())
            ackedLogFiles.remove(i);
    }
    ESPLOG(LogMax, "#### The removeUnknownAckedLogFiles() done.");
}

void CLogRequestReader::addNewAckedFileList(const char* list, StringArray& fileNames)
{
    OwnedIFile newList = createIFile(list);
    if (!newList)
        throw makeStringExceptionV(EspLoggingErrors::UpdateLogFailed, "Failed to access %s", list);
    if (newList->exists())
    {
        if (!newList->remove())
            throw makeStringExceptionV(EspLoggingErrors::UpdateLogFailed, "Failed to remove old %s", list);
    }
    OwnedIFileIO newListIO = newList->open(IFOwrite);
    if (!newListIO)
        throw makeStringExceptionV(EspLoggingErrors::UpdateLogFailed, "Failed to open %s", list);

    offset_t pos = 0;
    ForEachItemIn(i, fileNames)
    {
        const char* fileName = fileNames.item(i);
        StringBuffer line(fileName);
        line.append("\r\n");

        unsigned len = line.length();
        newListIO->write(pos, len, line.str());
        pos += len;
        PROGLOG("Add AckedLogFile %s to %s", fileName, list);
    }
}
//The file names in the fileNames should be removed from both ackedLogFileCheckList
//and settings->ackedFileList.
void CLogRequestReader::cleanAckedLogFiles(StringArray& fileNames)
{
    CriticalBlock b(crit);

    //Find which file should not be removed from ackedLogFileCheckList.
    StringArray fileNamesToKeep;
    for (auto r : ackedLogFileCheckList)
    {
        if (!fileNames.contains(r.c_str()))
            fileNamesToKeep.append(r.c_str());
    }

    //Create a temp file with the fileNamesToKeep for replacing the settings->ackedFileList
    VStringBuffer tempFileName("%s.tmp", settings->ackedFileList.str());
    addNewAckedFileList(tempFileName, fileNamesToKeep);

    //Replace the settings->ackedFileList with the temp file
    renameFile(settings->ackedFileList, tempFileName, true);
    PROGLOG("Rename %s to %s", tempFileName.str(), settings->ackedFileList.str());

    //Create new ackedLogFileCheckList based on fileNamesToKeep
    ackedLogFileCheckList.clear();
    ForEachItemIn(j, fileNamesToKeep)
    {
        const char* name = fileNamesToKeep.item(j);
        ackedLogFileCheckList.insert(name);
        PROGLOG("Add %s to new ackedLogFileCheckList", name);
    }
}

void CLogRequestReader::readAcked(const char* fileName, std::set<std::string>& acked)
{
    Owned<IFile> f = createIFile(fileName);
    if (f)
    {
        OwnedIFileIO io = f->openShared(IFOread, IFSHfull);
        if (io)
        {
            StringBuffer line;
            OwnedIFileIOStream ios = createIOStream(io);
            Owned<IStreamLineReader> lineReader = createLineReader(ios, true);
            while(!lineReader->readLine(line.clear()))
            {
                if (line.isEmpty())
                    continue;

                unsigned len = line.length();
                if ((len > 1) && (line.charAt(len - 2) == '\r') && (line.charAt(len - 1) == '\n'))
                    line.setLength(len - 2); //remove \r\n
                else if ((len > 0) && ((line.charAt(len - 1) == '\r') || (line.charAt(len - 1) == '\n')))
                    line.setLength(len - 1); //remove \r or \n
                if (line.length() > 0) //Check just in case
                    acked.insert(line.str());
                PROGLOG("Found Acked %s from %s", line.str(), fileName);
            }
        }
    }
    else
    {
        f.setown(createIFile(fileName));
        Owned<IFileIO> io =  f->open(IFOcreate);
    }
}

void CLogRequestReader::readLogRequest()
{
    ESPLOG(LogMax, "#### Enter readLogRequest()");

    StringAttr tankFileNotFinished;//Today's newest tank file.
    findTankFileNotFinished(tankFileNotFinished);

    offset_t tankFileNotFinishedPos = 0;
    Owned<IDirectoryIterator> it = createDirectoryIterator(settings->tankFileDir.get(), tankFilePattern);
    ForEach (*it)
    {
        const char *fileNameWithPath = it->query().queryFilename();
        const char *fileName = pathTail(fileNameWithPath);

        if (ackedLogFileCheckList.find(fileName) != ackedLogFileCheckList.end())
        {
            ESPLOG(LogMax, "####Skip tank file: %s. It is in the Acked File list.", fileName);
            continue;
        }

        if (readLogRequestsFromTankFile(fileNameWithPath, tankFileNotFinished, tankFileNotFinishedPos))
            addToAckedLogFileList(fileName, fileNameWithPath);
    }

    addPendingLogsToQueue();

    if (tankFileNotFinishedPos)
    {//In the next loop, we may skip the log requests which have been read in this loop.
        lastTankFile = tankFileNotFinished;
        lastTankFilePos = tankFileNotFinishedPos;
    }

    ESPLOG(LogMax, "#### Leave readLogRequest()");
}

void CLogRequestReader::findTankFileNotFinished(StringAttr& tankFileNotFinished)
{
    StringBuffer todayString;
    unsigned year, month, day;
    CDateTime now;
    now.setNow();
    now.getDate(year, month, day, true);
    todayString.appendf("%04d_%02d_%02d_00_00_00", year, month, day);

    StringBuffer lastTimeString;
    Owned<IDirectoryIterator> it = createDirectoryIterator(settings->tankFileDir.get(), tankFilePattern);
    ForEach (*it)
    {
        StringBuffer timeString;
        const char* aFileNameWithPath = it->query().queryFilename();
        const char* aFileName = pathTail(aFileNameWithPath);
        getTankFileTimeString(aFileName, timeString);
        if (timeString.isEmpty())
        {
            IERRLOG("Failed to parse tank file name: %s", aFileName);
            continue;
        }

        if (strcmp(todayString, timeString) > 0) //Not created today
            continue;

        if (strcmp(timeString, lastTimeString) > 0) //a newer file is found.
        {
            lastTimeString.set(timeString);
            tankFileNotFinished.set(aFileNameWithPath);
        }
    }
}

StringBuffer& CLogRequestReader::getTankFileTimeString(const char* fileName, StringBuffer& timeString)
{
    const char* ptr = strstr(fileName, sendLogKeyword);
    if (!ptr)
        return timeString;

    ptr += sendLogKeywordLen;
    if (!ptr)
        return timeString;

    ptr = strchr(ptr, '.');
    if (ptr && (strlen(ptr) > dateTimeStringLength))
        timeString.append(dateTimeStringLength, ++ptr); //yyyy_mm_dd_hh_mm_ss
    return timeString;
}

bool CLogRequestReader::readLogRequestsFromTankFile(const char* fileName, StringAttr& tankFileNotFinished, offset_t& tankFileNotFinishedPos)
{
    ESPLOG(LogMax, "#### Enter readLogRequestsFromTankFile(): %s", fileName);

    Owned<IFile> file = createIFile(fileName);
    if (!file) //This can only happen at start time. So, throw exception.
        throw MakeStringException(-1, "Unable to find logging file %s", fileName);

    Owned<IFileIO> fileIO =  file->open(IFOread);
    if (!fileIO)
        throw MakeStringException(-1, "Unable to open logging file %s", fileName);

    //Sample: 00009902        0421311217.2019_03_29_14_32_11  <cache><GUID>0421311217.2019_03_29_14_32_11</GUID>
    //<option>SingleInsert</option><LogRequest>dUgAAH...AA==</LogRequest></cache>
    offset_t finger = getReadFilePos(fileName);
    unsigned totalMissed = 0;
    while(true)
    {
        MemoryBuffer data;
        CLogSerializer logSerializer;
        if (!logSerializer.readALogLine(fileIO, finger, data))
            break;

        bool skipLogRequest = false;
        StringBuffer GUID, logRequest;
        if (!parseLogRequest(data, GUID, logRequest, skipLogRequest))
        {
            if (skipLogRequest)
                ESPLOG(LogMax, "#### Agent %s skips %s.", logThread->getLogAgent()->getName(), GUID.str());
            else
                IERRLOG("Invalid logging request in %s", fileName);
        }
        else if (ackedLogRequests.find(GUID.str()) == ackedLogRequests.end())
        {//This QUID is not acked.
            totalMissed++;
            if (pendingLogGUIDs.find(GUID.str()) == pendingLogGUIDs.end())
            {//This QUID has not been queued yet.
                PROGLOG("Found new log request %s from tank file %s. Added to pending logs.", GUID.str(), fileName);
                pendingLogGUIDs.insert(GUID.str());
                pendingLogs[GUID.str()] = logRequest.str();
                if (pendingLogs.size() > settings->pendingLogBufferSize)
                    addPendingLogsToQueue();
            }
        }
    }

    bool isTankFileNotFinished = !isEmptyString(tankFileNotFinished) && strieq(fileName, tankFileNotFinished);
    if (isTankFileNotFinished)
        tankFileNotFinishedPos = fileIO->size();

    ESPLOG(LogMax, "#### Leave readLogRequestsFromTankFile(): %s, totalMissed(%d)", fileName, totalMissed);
    return (totalMissed == 0) && !isTankFileNotFinished;
}

offset_t CLogRequestReader::getReadFilePos(const char* fileName)
{
    const char* lastTankFileName = lastTankFile.get();
    if (lastTankFileName && strieq(lastTankFileName, fileName))
        return lastTankFilePos;

    return 0;
}

bool CLogRequestReader::parseLogRequest(MemoryBuffer& rawdata, StringBuffer& GUID, StringBuffer& logLine, bool& skipLogRequest)
{
    //The rawdata should be in the form of 2635473460.05_01_12_16_13_57\t<ScriptValues>...</ScriptValues>\t<cache>...</cache>
    //parse it into GUID and logLine (as <cache>...</cache>)
    //The <ScriptValues> section is optional. If available, it should be used to check whether this agent should skip this
    //log request or not.
    const char* begin = rawdata.toByteArray(); //no string termination character \0
    unsigned len = rawdata.length();
    if (!begin || (len == 0))
        return false;

    const char* ptr = begin;
    const char* end = begin + len;
    while ((ptr < end) && (*ptr != '\t'))
        ptr++;

    if ((ptr == end) || (ptr == begin))
        return false;

    GUID.append(ptr - begin, begin);

    if (++ptr == end)
        return false;

    if (!checkScriptValues(ptr, end, skipLogRequest))
        return false;

    logLine.append(end - ptr, ptr);
    return true;
}

bool CLogRequestReader::checkScriptValues(const char* ptr, const char* end, bool& skipLogRequest)
{
    VStringBuffer scriptValuesTag("<%s>", logRequestScriptValues);
    if (strnicmp(ptr, scriptValuesTag, scriptValuesTag.length()))
        return true;

    ptr += scriptValuesTag.length();
    const char* scriptValuesBegin = ptr;
    while ((ptr < end) && (*ptr != '\t'))
        ptr++;

    if (ptr == end)
        return false; //Invalid logging request

    const char* scriptValuesEnd = ptr - scriptValuesTag.length() - 1; //No include XML CloseTag
    if (scriptValuesEnd < scriptValuesBegin)
        return false;

    StringBuffer scriptValuesStr;
    scriptValuesStr.append(scriptValuesEnd - scriptValuesBegin, scriptValuesBegin);

    Owned<IPropertyTree> scriptValues = createPTreeFromXMLString(scriptValuesStr);
    if (checkSkipThreadQueue(scriptValues, *logThread))
    {
        skipLogRequest = true;
        return false;
    }

    return ++ptr != end;
}

void CLogRequestReader::addPendingLogsToQueue()
{
    ESPLOG(LogMax, "#### Enter addPendingLogsToQueue()");

    //Add the pendingLogs to log queue
    if (pendingLogs.size())
        ESPLOG(LogMin, "Adding %zu Pending Log Request(s) to job queue", pendingLogs.size());
    StringArray queuedPendingLogs;
    for (auto const& x : pendingLogs)
    {
        Owned<IEspUpdateLogRequestWrap> logRequest = logThread->unserializeLogRequestContent(x.second.c_str(), true);
        if (!logRequest)
            IERRLOG("addPendingLogsToQueue: failed to unserialize: %s", x.second.c_str());

        logThread->queueLog(logRequest);
        queuedPendingLogs.append(x.first.c_str());
        PROGLOG("Enqueue: %s", x.first.c_str());
    }

    //Clean the pendingLogs
    ForEachItemIn(i, queuedPendingLogs)
        pendingLogs.erase(queuedPendingLogs.item(i));

    ESPLOG(LogMax, "#### Leave addPendingLogsToQueue()");
}

void CLogRequestReader::addACK(const char* GUID)
{
    ESPLOG(LogMax, "#### Enter addACK(): %s", GUID);

    CriticalBlock b(crit);
    Owned<IFile> f = createIFile(settings->ackedLogRequestFile);
    Owned<IFileIO> io =  f->open(IFOwrite);

    StringBuffer toWrite,size;
    toWrite.appendf("%s\r\n", GUID);
    io->write(io->size(), toWrite.length(), toWrite.str());

    ackedLogRequests.insert(GUID);
    pendingLogGUIDs.erase(GUID);

    ESPLOG(LogMax, "#### addACK(): %s acked", GUID);
}

void CLogRequestReader::addToAckedLogFileList(const char* fileName, const char* fileNameWithPath)
{
    PROGLOG("Found an AckedLogFile %s.", fileNameWithPath);
    newAckedLogFiles.append(fileNameWithPath);
    ackedLogFileCheckList.insert(fileName);
}

//Update newAckedLogFiles to file settings->ackedFileList,
void CLogRequestReader::updateAckedFileList()
{
    ESPLOG(LogMax, "#### Enter updateAckedFileList()");

    OwnedIFile ackedFiles = createIFile(settings->ackedFileList);
    if (!ackedFiles)
        return; //Should never happen

    OwnedIFileIO ackedFilesIO = ackedFiles->open(IFOwrite);
    if (!ackedFilesIO)
        return; //Should never happen

    offset_t pos = ackedFilesIO->size();
    ForEachItemIn(i, newAckedLogFiles)
    {
        const char* fileNameWithPath = newAckedLogFiles.item(i);
        StringBuffer fileName(pathTail(fileNameWithPath));
        PROGLOG("Add AckedLogFile %s to %s", fileName.str(), settings->ackedFileList.str());

        //Remove log request from the ackedLogRequests
        GuidSet logRequestsToRemove;
        CLogSerializer ackedLog(fileNameWithPath);
        ackedLog.loadAckedLogs(logRequestsToRemove);

        for (auto r : logRequestsToRemove)
            ackedLogRequests.erase(r.c_str());

        fileName.append("\r\n");

        unsigned len = strlen(fileName);
        ackedFilesIO->write(pos, len, fileName);
        pos += len;
    }

    newAckedLogFiles.clear();

    ESPLOG(LogMax, "#### Leave updateAckedFileList()");
}

void CLogRequestReader::updateAckedLogRequestList()
{
    ESPLOG(LogMax, "#### Enter updateAckedLogRequestList()");

    OwnedIFile newAckedLogRequestFile = createIFile(settings->ackedLogRequestFile);
    if (newAckedLogRequestFile)
    {
        newAckedLogRequestFile->remove();
        PROGLOG("Clean %s", settings->ackedLogRequestFile.str());
    }

    OwnedIFileIO newAckedLogRequestFileIO = newAckedLogRequestFile->open(IFOwrite);
    if (!newAckedLogRequestFileIO)
        return; //Should never happen

    offset_t pos = 0;
    for (auto r : ackedLogRequests)
    {
        StringBuffer line(r.c_str());
        line.append("\r\n");

        unsigned len = line.length();
        newAckedLogRequestFileIO->write(pos, len, line.str());
        pos += len;
        PROGLOG("Add AckedLogRequest %s to %s", line.str(), settings->ackedLogRequestFile.str());
    }

    ESPLOG(LogMax, "#### Leave updateAckedLogRequestList()");
}

void CLogRequestReader::setTankFilePattern(const char* service)
{
    tankFilePattern.set(service).append("*").append(logFileExt);
}

static bool checkEnabledLogVariant(IPropertyTree *scriptValues, const char *profile, const char *tracename, const char *group, const char *logtype)
{
    bool checkProfile = !isEmptyString(profile);
    bool checkType = !isEmptyString(logtype);

    if (checkProfile && (isEmptyString(group) || !strieq(profile, group)))
    {
        ESPLOG(LogNormal, "'%s' log entry disabled - log profile '%s' disabled", tracename, profile);
        return false;
    }
    else if (checkType)
    {
        VStringBuffer xpath("@disable-log-type-%s", logtype);
        if (scriptValues->getPropBool(xpath, false))
        {
            ESPLOG(LogNormal, "'%s' log entry disabled - log type '%s' disabled", tracename, logtype);
            return false;
        }
     }
    return true;
}

bool checkSkipThreadQueue(IPropertyTree *scriptValues, IUpdateLogThread &logthread)
{
    if (!scriptValues)
        return false;
    Linked<IEspLogAgent> agent = logthread.getLogAgent(); //badly named function get functions should link
    if (!agent)
        return false;

    const char *profile = scriptValues->queryProp("@profile");
    Owned<IEspLogAgentVariantIterator> variants = agent->getVariants();
    if (isEmptyString(profile) && !variants->first())
        return false;

    ForEach(*variants)
    {
        const IEspLogAgentVariant& variant = variants->query();
        if (checkEnabledLogVariant(scriptValues, profile, variant.getName(), variant.getGroup(), variant.getType()))
            return false;
    }
    return true;
}
