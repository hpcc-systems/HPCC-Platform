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

#define DEFAULTREADLOGREQUESTWAITSECOND 15 //How often to read log request from a tank file
#define DEFAULTPENDINGLOGBUFFERSIZE 100    //Max. # of log requests the pending log buffer store before flushing out.

class CLogRequestReaderSettings : public CSimpleInterface
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLogRequestReaderSettings() { };

    StringAttr tankFileDir;
    StringBuffer ackedFileList, ackedLogRequestFile;
    unsigned waitSeconds = DEFAULTREADLOGREQUESTWAITSECOND;
    unsigned pendingLogBufferSize = DEFAULTPENDINGLOGBUFFERSIZE;
};

class CLogThread;

interface ILogRequestReader : extends IThreaded
{
    virtual CLogRequestReaderSettings* getSettings() = 0;
    virtual void setPause(bool pause) = 0;
    virtual void reportAckedLogFiles(StringArray& ackedLogFiles) = 0;
    virtual void removeUnknownAckedLogFiles(StringArray& ackedLogFiles) = 0;
    virtual void cleanAckedLogFiles(StringArray& fileNames) = 0;
    virtual void setTankFilePattern(const char* service) = 0;
};

class CLogRequestReader : public CInterface, implements ILogRequestReader
{
    Owned<CLogRequestReaderSettings> settings;
    StringArray newAckedLogFiles;
    StringAttr lastTankFile;
    StringBuffer tankFilePattern;
    offset_t lastTankFilePos = 0;
    std::set<std::string> ackedLogFileCheckList, ackedLogRequests;
    GuidSet pendingLogGUIDs;
    GuidMap pendingLogs; //used every time when go through tank files to avoid duplicated log requests

    Linked<CLogThread> logThread;
    CThreaded threaded;
    bool stopping = false;
    bool paused = false;
    Semaphore sem;
    CriticalSection crit;

    void readAcked(const char* fileName, std::set<std::string>& acked);
    void readLogRequest();
    void findTankFileNotFinished(StringAttr& tankFileNotFinished);
    StringBuffer& getTankFileTimeString(const char* fileName, StringBuffer& timeString);
    bool readLogRequestsFromTankFile(const char* fileName, StringAttr& tankFileNotFinished, offset_t& tankFileNotFinishedPos);
    offset_t getReadFilePos(const char* fileName);
    bool parseLogRequest(MemoryBuffer& rawdata, StringBuffer& GUID, StringBuffer& data, bool& skipLogRequest);
    bool checkScriptValues(const char* ptr, const char* end, bool& skipLogRequest);
    void addToAckedLogFileList(const char* fileName, const char* fileNameWithPath);
    void addPendingLogsToQueue();
    void updateAckedFileList();
    void updateAckedLogRequestList();
    void addNewAckedFileList(const char* list, StringArray& fileNames);

public:
    CLogRequestReader(CLogRequestReaderSettings* _settings, CLogThread* _logThread)
        : settings(_settings), logThread(_logThread), threaded("LogRequestReader")
    {
        threaded.init(this);
    };

    ~CLogRequestReader();

    virtual void threadmain() override;

    void addACK(const char* GUID);
    virtual CLogRequestReaderSettings* getSettings() override { return settings; };
    virtual void setPause(bool pause) override { paused = pause; };
    virtual void reportAckedLogFiles(StringArray& ackedLogFiles) override;
    virtual void removeUnknownAckedLogFiles(StringArray& ackedLogFiles) override;
    virtual void cleanAckedLogFiles(StringArray& fileNames) override;
    virtual void setTankFilePattern(const char* service) override;
};

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
    virtual ILogRequestReader* getLogRequestReader() = 0;
};

class CLogThread : public Thread , implements IUpdateLogThread
{
    bool stopping;
    StringAttr agentName;
    int maxLogQueueLength;
    int signalGrowingQueueAt;
    unsigned maxLogRetries;   // Max. # of attempts to send log message

    Owned<IEspLogAgent> logAgent;
    QueueOf<IInterface, false> logQueue;
    CriticalSection logQueueCrit;
    Semaphore       m_sem;

    bool ensureFailSafe;
    Owned<ILogFailSafe> logFailSafe;
    struct tm         m_startTime;

    StringAttr tankFileDir;
    Owned<CLogRequestReader> logRequestReader;

    unsigned serializeLogRequestContent(IEspUpdateLogRequestWrap* request, StringBuffer& logData);
    bool enqueue(IEspUpdateLogRequestWrap* logRequest, const char* guid);
    void writeJobQueue(IEspUpdateLogRequestWrap* jobToWrite);
    IEspUpdateLogRequestWrap* readJobQueue();
    IEspUpdateLogRequestWrap* checkAndReadLogRequestFromSharedTankFile(IEspUpdateLogRequestWrap* logRequest);
    void checkAndCreateFile(const char* fileName);

public:
    IMPLEMENT_IINTERFACE;

    CLogThread();
    CLogThread(IPropertyTree* _agentConfig, const char* _service, const char* _agentName, IEspLogAgent* _logAgent = nullptr, const char* _tankFile = nullptr);
    virtual ~CLogThread();

    IEspLogAgent* getLogAgent() {return logAgent;};
    virtual CLogRequestReader* getLogRequestReader() {return logRequestReader;};

    bool hasService(LOGServiceType service)
    {
        return logAgent->hasService(service);
    }

    int run();
    void start();
    void stop();

    bool queueLog(IEspUpdateLogRequest* logRequest);
    bool queueLog(IEspUpdateLogRequestWrap* logRequest);
    void sendLog();

    IEspUpdateLogRequestWrap* unserializeLogRequestContent(const char* logData, bool decompress);
    void checkPendingLogs(bool oneRecordOnly=false);
    void checkRollOver();
};

extern LOGGINGCOMMON_API IUpdateLogThread* createUpdateLogThread(IPropertyTree* _cfg, const char* _service, const char* _agentName, const char* _tankFile, IEspLogAgent* _logAgent);
extern LOGGINGCOMMON_API bool checkSkipThreadQueue(IPropertyTree *scriptValues, IUpdateLogThread &logthread);
#endif // _LOGTHREAD_HPP__
