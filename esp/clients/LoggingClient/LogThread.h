/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#pragma warning (disable : 4786)
// LogThread.h: interface for the CLogThread class.
//
//////////////////////////////////////////////////////////////////////
#ifndef _LOGTHREAD_HPP__
#define _LOGTHREAD_HPP__
#ifdef WIN32
    #ifdef LOGGINGCLIENT_EXPORTS
        #define WSLOGThread_API __declspec(dllexport)
    #else
        #define WSLOGThread_API __declspec(dllimport)
    #endif
#else
    #define WSLOGThread_API
#endif

#include "jthread.hpp"
#include "esploggingservice.hpp"
#include "esp.hpp"
#include "jqueue.tpp"
#include "LogFailSafe.hpp"
#include "esploggingservice_esp.ipp"

interface IClientLogThread : extends IInterface
{
    virtual void start() = 0;
    virtual void finish() = 0;
    virtual void setTreeFlattening(bool bFlattenTree) = 0;
    virtual bool queueLog(const char *user, const char *relm, const char *peer, const char* serviceName, const char* GUID, int RecordsReturned, IPropertyTree& logInfo) = 0;
    virtual bool queueLog(IEspContext & context,const char* serviceName,const char* GUID, int RecordsReturned, IPropertyTree& logInfo) = 0;
    virtual bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, StringBuffer& logInfo) = 0;
    virtual bool queueLog(IEspContext & context,const char* serviceName, const char* request, const char* response) = 0;
    virtual bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IInterface& logInfo) = 0;
    virtual bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,  IArrayOf<IEspLogInfo>& LogArray, StringBuffer& logInfo)=0;
    virtual bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,bool bBlind,bool bEncrypt, IArrayOf<IEspLogInfo>& LogArray, IInterface& logInfo, IConstModelLogInformation* pModelLogInfo=0)=0; 
    virtual bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray) = 0;
    virtual IClientLogInfo& addLogInfoElement(IArrayOf<IEspLogInfo>& LogArray) = 0;
    
    virtual IClientModelLogInformation& getModelLogInformation() = 0;
    virtual IClientModelLogInfo& getModelLogInfo(IClientModelLogInformation* pModelLogInformation) = 0;
    virtual IClientAttributeGroupLogInfo& getAttributeGroupLogInfo(IClientModelLogInformation* pModelLogInformation) = 0;
    virtual IClientAttributeLogInfo& getAttributeLogInfo(IClientAttributeGroupLogInfo* pAttributeGroupLogInfo) = 0;
    virtual IClientScoreLogInfo& getScoreLogInfo(IClientModelLogInfo* pModelLogInfo) = 0;
    virtual IClientReasonCodeLogInfo& getReasonCodeLogInfo(IClientScoreLogInfo* pScoreLogInfo) = 0;

    virtual bool IsModelLogging()=0;
    virtual bool logResponseXml()=0;

    virtual bool GenerateTransactionSeed(StringBuffer& UniqueID,char backendType)=0;
};

extern "C" WSLOGThread_API IClientLogThread * createLogClient(IPropertyTree *cfg, const char *process, const char *service,bool bFlatten = true);
extern "C" WSLOGThread_API IClientLogThread * createLogClient3(IPropertyTree *logcfg, const char *service, bool bFlatten = true);
extern "C" WSLOGThread_API IClientLogThread * createLogClient2(IPropertyTree *cfg, const char *process, const char *service, const char* name, bool bFlatten = true, bool bModelRequest = false);
extern "C" WSLOGThread_API IClientLogThread * createModelLogClient(IPropertyTree *cfg, const char *process, const char *service,bool bFlatten = true);


struct LOG_INFO
{
    const char* serviceName;
    int recordsReturned;
    bool Blind;
    bool Encrypt;
    const char* GUIDSeed;
    StringBuffer GUID;
    StringBuffer RequestStr;
    void init() { memset(this, 0, sizeof(LOG_INFO)); }
    LOG_INFO() { init(); }
    LOG_INFO(const char* _seviceName, int _recordsReturned, bool _blind)
    {
        init();
        serviceName=_seviceName;
        recordsReturned=_recordsReturned;
        Blind=_blind;
    }
    LOG_INFO(const char* _seviceName,const char* _GUID, int _recordsReturned, bool _blind)
    {
        init();
        serviceName=_seviceName;
        recordsReturned=_recordsReturned;
        Blind=_blind;
        GUIDSeed=_GUID;
    }
    LOG_INFO(const char* _GUID) 
    { 
        init(); 
        GUID.appendf("%s",_GUID);
    }
};

class CPooledLogSendingThread : public CInterface, implements IPooledThread
{
private:
    void * m_request;
public:
    IMPLEMENT_IINTERFACE;

    virtual void main()
    {
        CClientWsLogService::espWorkerThread((void *)(IRpcRequestBinding *)(m_request));
    }
    virtual bool stop()
    {
        return true;
    }
    virtual void init(void *param)
    {
        m_request = param;
    }

    virtual bool canReuse() { return false;}
};

class CLogSendingThreadPoolFactory : public CInterface, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;

    virtual IPooledThread *createNew()
    {
        return new CPooledLogSendingThread();
    }
};

class CPooledClientWsLogService : public CClientWsLogService
{
private:
    Owned<CLogSendingThreadPoolFactory> m_pool_factory;
    Owned<IThreadPool> m_thread_pool;
    int m_poolsize;

public: 
    CPooledClientWsLogService(int poolsize) : CClientWsLogService(),
        m_pool_factory(new CLogSendingThreadPoolFactory()),
        m_poolsize(poolsize),
        m_thread_pool(createThreadPool("LoggingThreads", m_pool_factory, NULL, poolsize, 10000))
    {
        if(poolsize <= 0)
            throw MakeStringException(-1, "Thread pool size must be a positive integer");
    }

    virtual void async_UpdateLogService(IClientLOGServiceUpdateRequest *request, IClientWsLogServiceEvents *events,IInterface* state);
};

class WSLOGThread_API CLogThread : public Thread , implements IClientLogThread , IClientWsLogServiceEvents  
{
    CriticalSection crit;
    CriticalSection seed_gen_crit;

    Owned<IClientWsLogService> m_pLoggingService;
    Owned<ILogFailSafe> m_LogFailSafe;
    SafeQueueOf<IClientLOGServiceUpdateRequest, false> m_pServiceLog;

    Semaphore       m_sem;
    Semaphore       m_SenderSem;
    StringBuffer    m_ServiceURL;
    bool m_bRun;
    bool m_bFlattenTree;
    bool m_bFailSafeLogging;
    bool m_bThrottle;
    int m_Logcount;
    int m_LogSend;
    int m_LogTreshhold;
    int m_LogSendDelta;
    int m_MaxLogQueueLength;
    int m_SignalGrowingQueueAt;
    int m_BurstWaitInterval;
    int m_LinearWaitInterval;
    int m_NiceLevel;
    int m_ThreadPoolSize;

    unsigned int id_counter;
    StringBuffer m_InitialTransactionSeedID;
    bool bSeedAvailable;
    bool bMadeSeedRequest;
    bool m_bModelRequest;
    bool m_logResponse;

    struct tm         m_startTime;

private:    
    void addLogInfo(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& logInfo);
    void deserializeLogInfo(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& logInfo);
    void CheckErrorLogs();
    void SendLog();
    bool queueLog(IClientLOGServiceUpdateRequest* pRequest,LOG_INFO& _LogStruct);
    bool queueLog(const char *user, const char *realm, const char *peer, LOG_INFO& _LogStruct ,  IArrayOf<IEspLogInfo>& LogArray);

    StringBuffer& serializeRequest(IEspContext& ctx,IInterface& logInfo, StringBuffer& returnStr);
    IClientLOGServiceUpdateRequest* DeserializeRequest(const char* requestStr, LOG_INFO& _Info);
    void setTreeFlattening(bool bFlattenTree){m_bFlattenTree=bFlattenTree;};
    bool getTreeFlattening(){return m_bFlattenTree;};
    bool FlattenTree(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& tree,StringBuffer& Name);
    bool FlattenArray(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& tree,StringBuffer& Name);
    bool IsArray(IPropertyTree& tree);
    void HandleLoggingServerResponse(IClientLOGServiceUpdateRequest* Request,IClientLOGServiceUpdateResponse *Response);

    virtual int onUpdateLogServiceComplete(IClientLOGServiceUpdateResponse *resp,IInterface* state);
    virtual int onUpdateLogServiceError(IClientLOGServiceUpdateResponse *resp,IInterface* state);

    virtual int onUpdateModelLogServiceComplete(IClientLOGServiceUpdateResponse *resp,IInterface* state);
    virtual int onUpdateModelLogServiceError(IClientLOGServiceUpdateResponse *resp,IInterface* state);

    virtual int onTransactionSeedComplete(IClientTransactionSeedResponse *resp,IInterface* state){return -1;}
    virtual int onTransactionSeedError(IClientTransactionSeedResponse *resp,IInterface* state){return -1;}


    bool queueLog(IEspContext & context,LOG_INFO& _InfoStruct,  IArrayOf<IEspLogInfo>& LogArray, IPropertyTree& logInfo);
    bool queueLog(IEspContext & context,LOG_INFO& _InfoStruct,  IArrayOf<IEspLogInfo>& LogArray, IConstModelLogInformation* pModelLogInfo=0);

    bool FetchTransactionSeed(StringBuffer& TransactionSeedID);
    void UnserializeModelLogInfo(IPropertyTree* pModelTreeInfo,IClientModelLogInformation* pModelLogInformation);
    void checkRollOver();
public:
    IMPLEMENT_IINTERFACE;
    CLogThread();
    CLogThread(IPropertyTree* pServerConfig,  const char* Service, bool bFlatten = true, bool bModelRequest = false);
    virtual ~CLogThread();
    int run();
    void start();
    void finish();
    bool queueLog(const char *user, const char *relm, const char *peer, LOG_INFO& _InfoStruct, StringBuffer& logInfo);
    bool queueLog(const char *user, const char *relm, const char *peer, const char* serviceName, const char* GUID, int RecordsReturned, IPropertyTree& logInfo);


    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, StringBuffer& logInfo);
    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, const char* logInfo);

    bool queueLog(IEspContext & context,const char* serviceName, const char* request, const char* response);

    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IInterface& logInfo);
    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IPropertyTree& logInfo);
    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray, IInterface& logInfo);
    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,bool bBlind, bool bEncrypt , IArrayOf<IEspLogInfo>& LogArray, IInterface& logInfo, IConstModelLogInformation* pModelLogInfo=0);
    

    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,IArrayOf<IEspLogInfo>& LogArray, IPropertyTree& logInfo);

    bool queueLog(IEspContext & context,const char* serviceName,const char* GUID, int RecordsReturned, IPropertyTree& logInfo);



    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray);
    bool queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray, StringBuffer& logInfo);

    IClientLogInfo& addLogInfoElement(IArrayOf<IEspLogInfo>& LogArray);
    virtual bool GenerateTransactionSeed(StringBuffer& UniqueID,char backendType);

    virtual IClientModelLogInformation& getModelLogInformation();
    virtual IClientModelLogInfo& getModelLogInfo(IClientModelLogInformation* pIModelLogInformation);
    virtual IClientAttributeGroupLogInfo& getAttributeGroupLogInfo(IClientModelLogInformation* pIModelLogInformation);
    virtual IClientAttributeLogInfo& getAttributeLogInfo(IClientAttributeGroupLogInfo* pAttributeGroupLogInfo);
    virtual IClientScoreLogInfo& getScoreLogInfo(IClientModelLogInfo* pModelLogInfo);
    virtual IClientReasonCodeLogInfo& getReasonCodeLogInfo(IClientScoreLogInfo* pScoreLogInfo);
    virtual bool IsModelLogging();
    virtual bool logResponseXml() {  return m_logResponse; }

    void CleanQueue();
};

#endif // _LOGTHREAD_HPP__

