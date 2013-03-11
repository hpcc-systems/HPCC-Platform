/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

// LogThread.cpp: implementation of the CLogThread class.
//
//////////////////////////////////////////////////////////////////////

#include "LogThread.h"
#include "jmisc.hpp"
#include "soapbind.hpp"
#include "esploggingservice_esp.ipp"

#include "espcontext.hpp"

#define     MaxLogQueueLength   500000
#define     QueueSizeSignal     10000

static int DefaultThreadPoolSize = 50;
static int LogThreadWaitTime = 90;

#ifndef _WIN32
#include <sys/time.h>
#include <sys/resource.h>
#endif

IClientLogThread * createLogClient(IPropertyTree *cfg, const char *process, const char *service,bool bFlatten)
{
    return createLogClient2(cfg, process, service, "loggingserver", bFlatten);
}

IClientLogThread * createLogClient3(IPropertyTree *logcfg, const char *service, bool bFlatten)
{
    if(!logcfg)
        return NULL;

    DBGLOG("Creating recovery logging client");
    IClientLogThread* pLoggingThread = 0;

    pLoggingThread  = new CLogThread(logcfg,service, bFlatten, false);

    pLoggingThread->start();

    return pLoggingThread;
}

IClientLogThread * createLogClient2(IPropertyTree *cfg, const char *process, const char *service, const char* name, bool bFlatten, bool bModelRequest)
{
    DBGLOG("Creating recovery logging client %s", name);
    IClientLogThread* pLoggingThread = 0;
    StringBuffer xpath,loggingServer;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/%s", process, service, name);
    IPropertyTree* pServerInfo = cfg->queryPropTree(xpath.str());
    if (pServerInfo == 0)
        return  0;

    pLoggingThread  = new CLogThread(pServerInfo,service, bFlatten, bModelRequest);

    pLoggingThread->start();

    return pLoggingThread;
}

IClientLogThread * createModelLogClient(IPropertyTree *cfg, const char *process, const char *service, bool bFlatten )
{
    return createLogClient2( cfg, process, service, "loggingserver", bFlatten, true );
}

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////
CLogThread::CLogThread(IPropertyTree* pServerConfig , const char* Service, bool bFlatten, bool bModelRequest) : m_bRun(true),m_Logcount(0) ,
    m_LogTreshhold(0), m_bModelRequest(bModelRequest), m_logResponse(false)
{
    if(pServerConfig==NULL)
        throw MakeStringException(500,"No Logging Configuration");


    if(pServerConfig->hasProp("url")==false)
        throw MakeStringException(500,"No Logging Server URL");
    m_ServiceURL.appendf("%s",pServerConfig->queryProp("url"));

    m_bFailSafeLogging = false;
    if(pServerConfig->hasProp("failsafe")==true)
    {
        const char* failsafe = pServerConfig->queryProp("failsafe");
        if(failsafe != 0 && strcmp(failsafe,"true") == 0)
            m_bFailSafeLogging = true;
    }

    StringBuffer poolsizebuf;
    if(pServerConfig->hasProp("MaxLoggingThreads"))
        pServerConfig->getProp("MaxLoggingThreads", poolsizebuf);
    if(poolsizebuf.length() > 0)
    {
        m_ThreadPoolSize = atoi(poolsizebuf.str());
    }
    else
    {
        // If thread pool size not even specified, use default size
        m_ThreadPoolSize = DefaultThreadPoolSize;
    }

    pServerConfig->hasProp("MaxLogQueueLength")==true ? m_MaxLogQueueLength = pServerConfig->getPropInt("MaxLogQueueLength") : m_MaxLogQueueLength = MaxLogQueueLength;
    pServerConfig->hasProp("QueueSizeSignal")==true ?   m_SignalGrowingQueueAt = pServerConfig->getPropInt("QueueSizeSignal") : m_SignalGrowingQueueAt = QueueSizeSignal;
    pServerConfig->hasProp("Throttle")==true ?  m_bThrottle = pServerConfig->getPropBool("Throttle") : m_bThrottle = true;

    m_logResponse = pServerConfig->getPropBool("LogResponseXml",false);

    if(pServerConfig->hasProp("BurstWaitInterval"))
        m_BurstWaitInterval = pServerConfig->getPropInt("BurstWaitInterval");
    else
        m_BurstWaitInterval = 0;

    if(pServerConfig->hasProp("LinearWaitInterval"))
        m_LinearWaitInterval = pServerConfig->getPropInt("LinearWaitInterval");
    else
        m_LinearWaitInterval = 0;

    if(pServerConfig->hasProp("NiceLevel"))
        m_NiceLevel = pServerConfig->getPropInt("NiceLevel");
    else
        m_NiceLevel = 0;

    //by default flatten any trees passes into the service
    m_bFlattenTree = bFlatten;
    m_LogSendDelta = 0;
    m_LogSend = 0;


    if(m_ThreadPoolSize > 0)
        m_pLoggingService.setown(new CPooledClientWsLogService(m_ThreadPoolSize));
    else
        m_pLoggingService.setown(createWsLogServiceClient());
    m_pLoggingService->addServiceUrl(m_ServiceURL.str());
    //temporary fix for authentication on logging service....
    const char* loguser = pServerConfig->queryProp("LoggingUser");
    const char* logpasswd = pServerConfig->queryProp("LoggingPassword");
    m_pLoggingService->setUsernameToken((loguser && *loguser)?loguser:"loggingclient", (logpasswd&&*logpasswd)?logpasswd:"loggingpassword","");

    if(m_bFailSafeLogging == true)
    {
        if(pServerConfig->hasProp("LogsDir"))
            m_LogFailSafe.setown(createFailsafelogger(Service, pServerConfig->queryProp("LogsDir")));
        else
            m_LogFailSafe.setown(createFailsafelogger(Service));
    }

    bSeedAvailable = false;
    bMadeSeedRequest = false;
    id_counter=0;

    time_t tNow;
    time(&tNow);
    localtime_r(&tNow, &m_startTime);

}

CLogThread::~CLogThread()
{
    DBGLOG("CLogThread::~CLogThread()");
}

bool CLogThread::FetchTransactionSeed(StringBuffer& TransactionSeedID)
{
    try{
        Owned<IClientTransactionSeedRequest> pSeedReq = m_pLoggingService->createTransactionSeedRequest();
        Owned<IClientTransactionSeedResponse> pSeedResp = m_pLoggingService->TransactionSeed(pSeedReq.get());
        //if we get to here then we have made the request but no seeds are available
        bMadeSeedRequest = true;
        if(pSeedResp->getSeedAvailable()==true)
        {
            TransactionSeedID.appendf("%s",pSeedResp->getSeedId());
            return true;
        }
        else
            return false;
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        ERRLOG("Exception caught generating transaction seed (%d) %s",ex->errorCode(),errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception caught generating transaction seed");
    }
    return false;
}

bool CLogThread::GenerateTransactionSeed(StringBuffer& UniqueID, char backendType)
{
    CriticalBlock b(seed_gen_crit);
    if(bSeedAvailable == false && bMadeSeedRequest==true)
    {
        //we have checked for a seed but none are available
        return false;
    }
    else if (bSeedAvailable == false)
    {
        //we have not checked for a seed or we failed when making the request..
        bSeedAvailable = FetchTransactionSeed(m_InitialTransactionSeedID);
        if(bSeedAvailable==false)
            return false;
        DBGLOG("Fetched Transaction Seed %s\n", m_InitialTransactionSeedID.str());
    }

    UniqueID.appendf("%s%c%u",m_InitialTransactionSeedID.str(),backendType,++id_counter);

    if(UniqueID.length() > 16)
    {
        //Sybase limits transaction_id to 16 bytes. If longer, need to get another seed ID and reset id_counter.
        m_InitialTransactionSeedID.clear();
        bSeedAvailable = FetchTransactionSeed(m_InitialTransactionSeedID);
        if(bSeedAvailable==false)
            return false;
        DBGLOG("TransactionID length exceeded 16 bytes. So re-fetched a Transaction Seed %s.\n", m_InitialTransactionSeedID.str());
        id_counter = 0;
        UniqueID.clear().appendf("%s-%u",m_InitialTransactionSeedID.str(),++id_counter);
    }

    return true;

}

IClientLogInfo& CLogThread::addLogInfoElement(IArrayOf<IEspLogInfo>& LogArray)
{
    IClientLogInfo* logInfo = new CLogInfo("");
    LogArray.append(*(dynamic_cast<IEspLogInfo*>(logInfo)));
    return *logInfo;
}



IClientModelLogInformation& CLogThread::getModelLogInformation()
{
    return *(new CModelLogInformation(""));
}


IClientModelLogInfo& CLogThread::getModelLogInfo(IClientModelLogInformation* pModelLogInformation)
{
    IClientModelLogInfo* pMLogInfo = new CModelLogInfo("");

    if(pModelLogInformation!=0)
    {
        pModelLogInformation->getModels().append(*(dynamic_cast<IEspModelLogInfo*>(pMLogInfo)));
    }


    return *pMLogInfo;
}

IClientAttributeGroupLogInfo& CLogThread::getAttributeGroupLogInfo(IClientModelLogInformation* pModelLogInformation)
{
    IClientAttributeGroupLogInfo* pAttribGrpLogInfo = new CAttributeGroupLogInfo("");

    if(pModelLogInformation!=0)
    {
        pModelLogInformation->getAttributeGroups().append(*(dynamic_cast<IEspAttributeGroupLogInfo*>(pAttribGrpLogInfo)));
    }
    return *pAttribGrpLogInfo;
}


IClientAttributeLogInfo& CLogThread::getAttributeLogInfo(IClientAttributeGroupLogInfo* pAttributeGroupLogInfo)
{
    IClientAttributeLogInfo* pAttribLogInfo = new CAttributeLogInfo("");

    if(pAttributeGroupLogInfo!=0)
    {
        pAttributeGroupLogInfo->getAttributes().append(*(dynamic_cast<IEspAttributeLogInfo*>(pAttribLogInfo)));
    }

    return *pAttribLogInfo;
}



IClientScoreLogInfo& CLogThread::getScoreLogInfo(IClientModelLogInfo* pModelLogInfo)
{
    IClientScoreLogInfo* pScoreLogInfo = new CScoreLogInfo("");


    if(pModelLogInfo!=0)
    {
        pModelLogInfo->getScores().append(*(dynamic_cast<IEspScoreLogInfo*>(pScoreLogInfo)));
    }
    return *pScoreLogInfo;

}

IClientReasonCodeLogInfo& CLogThread::getReasonCodeLogInfo(IClientScoreLogInfo* pScoreLogInfo)
{
    IClientReasonCodeLogInfo* pReasonCodeLogInfo = (IClientReasonCodeLogInfo*) new CReasonCodeLogInfo("");

    if(pScoreLogInfo!=0)
    {
        pScoreLogInfo->getReasonCodes().append(*(dynamic_cast<IEspReasonCodeLogInfo*>(pReasonCodeLogInfo)));
    }
    return *pReasonCodeLogInfo;
}


bool CLogThread::IsModelLogging()
{
    return m_bModelRequest;
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray, IInterface& logInfo)
{
    StringBuffer dataStr;
    serializeRequest(context,logInfo,dataStr);
    Owned<IPropertyTree> pLogTreeInfo = createPTreeFromXMLString(dataStr.str(), ipt_none, ptr_none);
    return queueLog(context,serviceName,RecordsReturned,LogArray, *pLogTreeInfo);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,bool bBlind,bool bEncrypt, IArrayOf<IEspLogInfo>& LogArray, IInterface& logInfo, IConstModelLogInformation* pModelLogInfo)
{

    LOG_INFO _LogStruct(serviceName,RecordsReturned,bBlind);
    _LogStruct.Encrypt = bEncrypt;

    serializeRequest(context,logInfo,_LogStruct.RequestStr);

    Owned<IPropertyTree> pLogTreeInfo = createPTreeFromXMLString(_LogStruct.RequestStr.str(), ipt_none, ptr_none);


    addLogInfo(LogArray,*pLogTreeInfo.get());
    return queueLog(context,_LogStruct, LogArray,pModelLogInfo);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IInterface& logInfo)
{
    StringBuffer dataStr;
    serializeRequest(context,logInfo,dataStr);
    return queueLog(context,serviceName,RecordsReturned,dataStr);
}

StringBuffer& CLogThread::serializeRequest(IEspContext& context,IInterface& logInfo, StringBuffer& returnStr)
{
    IRpcSerializable* rpcreq = dynamic_cast<IRpcSerializable*>(&logInfo);
    if(rpcreq==NULL)
        throw MakeStringException(500,"Issue serializing log information");

    // We want to serialize anything here for logging purpose: e.g., internal user fields: CompanyId
    // rpcreq->serialize(&context,returnStr, "LogData");
    // rpcreq->serialize(NULL,returnStr, "LogData");

    //BUG#26047
    //logInfo function parameter is instance of the incoming request object of the service.
    //instance objects of context and request are dependent upon the protocol binding.
    //Request parameters are relevent for HTTP protocol but are not relevent for protocolX.
    //Since request parameters pointer is not initilized in processing protocolX request it remains NULL
    //and causing this crash.
    IProperties* params = context.queryRequestParameters();
    if(params!=NULL)
    {
        bool notInternal = !params->hasProp("internal");
        if (notInternal)
            params->setProp("internal","1");
        rpcreq->serialize(&context,returnStr, "LogData");
        if (notInternal)
            params->removeProp("internal");
    }else{
        rpcreq->serialize(NULL,returnStr, "LogData");
    }

    return returnStr;
}


bool CLogThread::queueLog(const char *user, const char *realm, const char *peer, const char* serviceName,const char* GUID, int RecordsReturned, IPropertyTree& logInfo)
{
    IArrayOf<IEspLogInfo> LogArray;
    addLogInfo(LogArray, logInfo);

    LOG_INFO _LogInfo(serviceName,GUID, RecordsReturned,false);
    return queueLog(user, realm, peer, _LogInfo, LogArray);
}

bool CLogThread::queueLog(const char *user, const char *realm, const char *peer, LOG_INFO& _LogStruct, IArrayOf<IEspLogInfo>& LogArray)
{
    if(!m_pLoggingService.get())
        return false;

    IClientLOGServiceUpdateRequest* tptrRequest;
    if( m_bModelRequest )
    {
        tptrRequest = dynamic_cast<IClientLOGServiceUpdateRequest*>(m_pLoggingService->createUpdateModelLogServiceRequest());
    } else {
        tptrRequest = m_pLoggingService->createUpdateLogServiceRequest();
    }
    Owned<IClientLOGServiceUpdateRequest> pRequest( tptrRequest );

    if (pRequest == 0)
        return false;


    pRequest->setUserName(user);
    pRequest->setDomainName(realm);
    pRequest->setRecordCount(_LogStruct.recordsReturned);
    pRequest->setServiceName(_LogStruct.serviceName);
    pRequest->setIP(peer);

    //This appends the tree structure into the correct format...
    pRequest->setLogInformation(LogArray);
    return queueLog(pRequest,_LogStruct);

}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IPropertyTree& logInfo)
{
    LOG_INFO _LogInfo(serviceName,RecordsReturned,false);
    IArrayOf<IEspLogInfo> LogArray;
    return queueLog(context,_LogInfo, LogArray, logInfo);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,const char* GUID, int RecordsReturned, IPropertyTree& logInfo)
{
    LOG_INFO _LogInfo(serviceName,GUID, RecordsReturned,false);
    IArrayOf<IEspLogInfo> LogArray;
    return queueLog(context,_LogInfo, LogArray, logInfo);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,IArrayOf<IEspLogInfo>& LogArray, IPropertyTree& logInfo)
{
    LOG_INFO _LogStruct(serviceName,RecordsReturned,0);
    return queueLog(context,_LogStruct,LogArray, logInfo);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, IArrayOf<IEspLogInfo>& LogArray)
{
    LOG_INFO _LogStruct(serviceName,RecordsReturned,false);
    return queueLog(context,_LogStruct,  LogArray);
}

bool CLogThread::queueLog(IEspContext & context,LOG_INFO& _LogStruct, IArrayOf<IEspLogInfo>& LogArray, IPropertyTree& logInfo)
{
    //This appends the tree structure into the correct format...
    addLogInfo(LogArray,logInfo);
    return queueLog(context,_LogStruct, LogArray); ;
}


bool CLogThread::queueLog(IEspContext & context,LOG_INFO& _LogStruct, IArrayOf<IEspLogInfo>& LogArray, IConstModelLogInformation* pModelLogInfo)
{
    if(!m_pLoggingService.get())
        return false;


    //Owned<IClientLOGServiceUpdateRequest> pRequest =  m_pLoggingService->createUpdateLogServiceRequest();
    IClientLOGServiceUpdateRequest* tptrRequest;
    if( m_bModelRequest )
    {
        IClientLOGServiceUpdateModelRequest* pUpdateModelRequest = m_pLoggingService->createUpdateModelLogServiceRequest();

        if(pModelLogInfo!=0)
        {
            pUpdateModelRequest->setModelLogInformation(*pModelLogInfo);
        }
        tptrRequest = dynamic_cast<IClientLOGServiceUpdateRequest*>(pUpdateModelRequest);
    } else {
        tptrRequest = m_pLoggingService->createUpdateLogServiceRequest();
    }
    Owned<IClientLOGServiceUpdateRequest> pRequest( tptrRequest );
    if (pRequest == 0)
        return false;



    StringBuffer UserID,realm,peer;
    pRequest->setUserName(context.getUserID(UserID).str());
    pRequest->setDomainName(context.getRealm(realm).str());
    pRequest->setRecordCount(_LogStruct.recordsReturned);
    pRequest->setServiceName(_LogStruct.serviceName);
    pRequest->setIP(context.getPeer(peer).str());
    bool bBlind = _LogStruct.Blind;
    bool bEncrypt = _LogStruct.Encrypt;

    ISecPropertyList* properties = context.querySecuritySettings();

    if( properties !=NULL)
    {
        if(bBlind==false)
        {
            if(properties->findProperty("blind")!=NULL)
                strncmp(properties->findProperty("blind")->getValue(),"1",1) == 0 ? bBlind=true : bBlind=false;
        }

        if(bEncrypt==false && properties->findProperty("encryptedlogging")!=NULL)
        {
            if(strncmp(properties->findProperty("encryptedlogging")->getValue(),"1",1) == 0)
                bEncrypt=true;
        }
    }
    if(bEncrypt==true)
    {
        //need to do encrpyted logging
        pRequest->setEncryptedLogging(true);
        pRequest->setRawLogInformation(_LogStruct.RequestStr.str());
    }

    pRequest->setBlindLogging(bBlind);
    pRequest->setLogInformation(LogArray);

    return queueLog(pRequest,_LogStruct);
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, StringBuffer& logInfo)
{
    return queueLog(context,serviceName,RecordsReturned,logInfo.str());
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned,IArrayOf<IEspLogInfo>& LogArray, StringBuffer& logInfo)
{
    Owned<IPropertyTree> pLogTreeInfo = createPTreeFromXMLString(logInfo, ipt_none, ptr_none);
    return queueLog(context,serviceName,RecordsReturned,LogArray, *pLogTreeInfo.get());
}

bool CLogThread::queueLog(IEspContext & context,const char* serviceName, const char* request, const char* response)
{
    IProperties* pProperties = context.queryRequestParameters();

    StringBuffer UserID, UserRealm, UserReference, peer;
    if(pProperties != NULL && pProperties->hasProp("userid_"))
        UserID.appendf("%s",pProperties->queryProp("userid_"));
    else
        context.getUserID(UserID);

    if(pProperties != NULL && pProperties->hasProp("fqdn_"))
        UserRealm.appendf("%s",pProperties->queryProp("fqdn_"));
    else
        context.getRealm(UserRealm);

    Owned<IPropertyTree> pLogTreeInfo = createPTreeFromXMLString(request, ipt_none, ptr_none);
    IArrayOf<IEspLogInfo> LogArray;
    addLogInfo(LogArray, *pLogTreeInfo.get());

    if(pProperties != NULL && pProperties->hasProp("referencecode_"))
    {
        //lets manually add the reference number....
        IClientLogInfo& LogInfoTransaction =  addLogInfoElement(LogArray);
        LogInfoTransaction.setName("referencenumber");
        LogInfoTransaction.setValue(pProperties->queryProp("referencecode_"));
    }

    LOG_INFO _LogStruct(serviceName,-1,false);
    return queueLog(UserID.str(), UserRealm.str() , context.getPeer(peer).str(),_LogStruct, LogArray );
}


bool CLogThread::queueLog(IEspContext & context,const char* serviceName,int RecordsReturned, const char* logInfo)
{
    try {
        Owned<IPropertyTree> pLogTreeInfo = createPTreeFromXMLString(logInfo, ipt_none, ptr_none);
        return queueLog(context,serviceName,RecordsReturned,*pLogTreeInfo);
    } catch (IException* e) {
        StringBuffer msg;
        e->errorMessage(msg);
        DBGLOG("Exception caught in CLogThread::queueLog: %s", msg.str());
        return false;
    } catch (...) {
        DBGLOG("Unknown exception caught in CLogThread::queueLog()");
        return false;
    }
}

bool CLogThread::queueLog(IClientLOGServiceUpdateRequest * pRequest,LOG_INFO& _LogStruct)
{
    int QueueSize = m_pServiceLog.ordinality();

    //YMA: this is bad. Transaction records will be lost because of this exception. Better to let the queue keep growing so that transactions will be
    // written to the tank file. Plus, most deployments configure the MaxLogQueueLength to be 10000, which is way too small.
    //if(QueueSize > m_MaxLogQueueLength)
    //  throw MakeStringException(503,"Service Unavailable (Audit)");
    if(QueueSize > m_MaxLogQueueLength)
        ERRLOG("SOS!!! Logging queue size %d execeeded MaxLogQueueLength %d, check the logging server at %s!!!",QueueSize, m_MaxLogQueueLength, m_ServiceURL.str());

    if(QueueSize!=0 && QueueSize % m_SignalGrowingQueueAt == 0)
        ERRLOG("Logging Queue at %d records. Check the logging server at %s.",QueueSize, m_ServiceURL.str());

    if(m_bFailSafeLogging == true && m_LogFailSafe.get())
    {
        //generate a GUID for the query. The cache it within the failsafe object
        StringBuffer _GUID;
        if(_LogStruct.GUID !=NULL && *_LogStruct.GUID!='\0')
        {
            _GUID.appendf("%s",_LogStruct.GUID.str());
        }
        else
        {
            m_LogFailSafe->GenerateGUID(_GUID,_LogStruct.GUIDSeed);
        }
        pRequest->setGUID(_GUID.str());
        m_LogFailSafe->Add(_GUID,*pRequest);
    }

    m_pServiceLog.enqueue(LINK(pRequest));

    if (++m_Logcount >= m_LogTreshhold && m_LogSendDelta == 0)
    {
        m_sem.signal();
        m_Logcount = 0;
    }

    return true;
}

void CLogThread::addLogInfo(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& logInfo)
{

    StringBuffer dataStr,nameStr,valueStr;
    Owned<IPropertyTreeIterator> itr =  logInfo.getElements("*");
    itr->first();
    while(itr->isValid())
    {
        IPropertyTree &node = itr->query();
        const char* name = node.queryName();
        if (getTreeFlattening()==true && node.hasChildren() == true)
        {

            if(IsArray(node)==true)
            {
                FlattenArray(valueArray,node,nameStr);
            }
            else
            {
                FlattenTree(valueArray,node,nameStr);
            }
        //  logElement.setName(node.queryName());
        //  dataStr.clear();
            /*toXML(&node,dataStr);
            //DOM temporary work about for the lack of XML decoding in esp arrays
            StringBuffer encodedData;
            JBASE64_Encode(dataStr.str(), dataStr.length() , encodedData);
            logElement.setData(encodedData.str());
        */

        }
        else if (getTreeFlattening()==false && node.hasChildren() == true)
        {
            IClientLogInfo& logElement = addLogInfoElement(valueArray);
            logElement.setName(node.queryName());
            dataStr.clear();
            toXML(&node,dataStr);
            //DOM temporary work about for the lack of XML decoding in esp arrays
            StringBuffer encodedData;
            JBASE64_Encode(dataStr.str(), dataStr.length() , encodedData);
            logElement.setData(encodedData.str());
        }
        else if (node.queryProp("") != 0 && ( strcmp(node.queryProp(""),"0") != 0 ))
        {
            IClientLogInfo& logElement = addLogInfoElement(valueArray);
            logElement.setName(node.queryName());
            logElement.setValue(node.queryProp(""));
        }
        itr->next();
    }

}

bool CLogThread::IsArray(IPropertyTree& tree)
{
// If the node have more than one children, and all have the same name,
// then it is an array.
    StringBuffer name, temp;
    Owned<IPropertyTreeIterator> itr =  tree.getElements("*");
    int count = 0;
    for (itr->first(); itr->isValid(); itr->next())
    {
        if (count==0)
            itr->query().getName(name);
        else
        {
            itr->query().getName(temp);
            if (stricmp(name,temp)!=0)
                return false;
            temp.clear();
        }
        count++;
    }

    //Loophole in code above if there is only 1 item in the array
    if(count==1)
    {
        if (name!=NULL && stricmp(name,"Item")==0)
            return true;
    }
    return count>1;
}

bool CLogThread::FlattenArray(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& tree,StringBuffer& RootName)
{
    StringBuffer Value,Name;
    if (tree.hasChildren() == true)
    {
        Name.appendf("%s",tree.queryName());
        Owned<IPropertyTreeIterator> itrItem =  tree.getElements("./*");
        itrItem->first();
        while(itrItem->isValid()==true)
        {
            IPropertyTree &node = itrItem->query();
            if(Value.length()!=0)
                Value.append(",");
            Value.appendf("%s",node.queryProp(""));
            itrItem->next();
        }
        IClientLogInfo& logElement = addLogInfoElement(valueArray);
        logElement.setName(Name.str());
        logElement.setValue(Value.str());

    }
    return true;
}

bool CLogThread::FlattenTree(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& tree,StringBuffer& RootName)
{
    StringBuffer Value,Name;
    if (tree.hasChildren() == true)
    {

        Owned<IPropertyTreeIterator> itr =  tree.getElements("*");
        itr->first();
        while(itr->isValid())
        {
            IPropertyTree &node = itr->query();
            if(RootName.length() > 0)
                Name.appendf("%s_",RootName.str());
            Name.appendf("%s",node.queryName());

            if (node.hasChildren() == true)
            {
                if(IsArray(node)==true)
                    FlattenArray(valueArray,node,Name);
                else
                    FlattenTree(valueArray,node,Name);
            }
            else
            {
                const char* _value = tree.queryProp(node.queryName());
                if(tree.hasProp(node.queryName())==true && _value!=0 && _value!='\0')
                {
                    Value.appendf("%s",tree.queryProp(node.queryName()));
                    IClientLogInfo& logElement = addLogInfoElement(valueArray);
                    logElement.setName(Name.str());
                    logElement.setValue(Value.str());
                    //DBGLOG("Add log element: %s, %s", Name.str(), Value.str());
                    Value.clear();
                }
            }
            Name.clear();
            itr->next();
        }

    }
    else
    {
        return false;
    }
    return true;
}

void CLogThread::deserializeLogInfo(IArrayOf<IEspLogInfo>& valueArray,IPropertyTree& logInfo)
{
    Owned<IPropertyTreeIterator> itr =  logInfo.getElements("LogInfo");
    itr->first();
    while(itr->isValid())
    {
        IPropertyTree &node = itr->query();
        IClientLogInfo& logElement = addLogInfoElement(valueArray);
        logElement.setName(node.queryProp("Name"));
        logElement.setValue(node.queryProp("Value"));
        logElement.setData(node.queryProp("Data"));
        itr->next();
    }
}

int CLogThread::run()
{
    Link();
    CheckErrorLogs();

    while(m_bRun)
    {
        m_sem.wait(10000);
        SendLog();
        checkRollOver();
    }
    Release();
    return 0;
}

void CLogThread::checkRollOver()
{
    if(!m_LogFailSafe.get())
        return;

    try
    {
        time_t tNow;
        time(&tNow);
        struct tm ltNow;
        localtime_r(&tNow, &ltNow);
        if(ltNow.tm_year != m_startTime.tm_year || ltNow.tm_yday != m_startTime.tm_yday)
        {
            localtime_r(&tNow, &m_startTime);  // reset the start time for next rollover check
            int numNewArrivals = m_pServiceLog.ordinality();
            {
                MTimeSection mt(NULL, "Tank file rollover");
                m_LogFailSafe->SafeRollover();
            }
            if(numNewArrivals > 0)
            {
                DBGLOG("writing %d requests in the queue to the rolled over tank file.", numNewArrivals);
                for(int i = 0; i < numNewArrivals; i++)
                {
                    IClientLOGServiceUpdateRequest* pRequest = m_pServiceLog.item(i);
                    if (pRequest)
                    {
                        IEspLOGServiceUpdateRequest* pEspRequest = dynamic_cast<IEspLOGServiceUpdateRequest*>(pRequest);
                        if(pEspRequest)
                        {
                            const char* guid = pEspRequest->getGUID();
                            if(guid)
                                m_LogFailSafe->Add(guid,*pRequest);
                        }
                    }
                }
            }
        }
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

void CLogThread::CheckErrorLogs()
{
    if(!m_LogFailSafe.get())
        return;
    try{
        bool bRelogError = false;
        if(m_LogFailSafe->FindOldLogs() == true)
        {

            DBGLOG("We have old logs!!!!!!");
            DBGLOG("Will now try and recover the lost log messages");

            StringArray LostLogMessages;
            m_LogFailSafe->LoadOldLogs(LostLogMessages);
            ForEachItemIn(aidx, LostLogMessages)
            {
                LOG_INFO _Info;
                Owned<IClientLOGServiceUpdateRequest> pRequest = DeserializeRequest(LostLogMessages.item(aidx),_Info);
                if (pRequest==0 || queueLog(pRequest,_Info) == false)
                    bRelogError=true;
            }
        }
        //if everything went ok then we should be able to rollover the old logs.
        if(bRelogError == false)
            m_LogFailSafe->RollOldLogs();
    }
    catch(IException* ex)
    {
        StringBuffer errorStr;
        ex->errorMessage(errorStr);
        errorStr.appendf("%s",errorStr.str());
        ERRLOG("CheckErrorLogs: %s:" ,errorStr.str());
        ex->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception thrown in CheckErrorLogs");
    }
}
IClientLOGServiceUpdateRequest* CLogThread::DeserializeRequest(const char* requestStr,LOG_INFO& _Info)
{
    if(!requestStr)
        return 0;

    //
    //
    IClientLOGServiceUpdateRequest* pRequest =  0;
    if( m_bModelRequest )
    {
        pRequest = dynamic_cast<IClientLOGServiceUpdateRequest*>(m_pLoggingService->createUpdateModelLogServiceRequest());
    } else {
        pRequest = m_pLoggingService->createUpdateLogServiceRequest();
    }
    if (pRequest == 0)
        return 0;

    StringBuffer xPath,GUID,Cache;

    //request should be in the form GUID\tCache

    m_LogFailSafe->SplitLogRecord(requestStr,GUID,Cache);
    _Info.GUID = GUID.str();

    Owned<IPropertyTree> pLogTree = createPTreeFromXMLString(Cache.str(), ipt_none, ptr_none);
    pRequest->setUserName(pLogTree->queryProp("UserName"));
    pRequest->setDomainName(pLogTree->queryProp("DomainName"));
    pRequest->setRecordCount(pLogTree->getPropInt("RecordCount"));
    pRequest->setServiceName(pLogTree->queryProp("ServiceName"));
    pRequest->setIP(pLogTree->queryProp("IP"));
    pRequest->setRawLogInformation(pLogTree->queryProp("RawLogInformation"));
    if(pLogTree->hasProp("EncryptedLogging")==true)
        pRequest->setEncryptedLogging(pLogTree->getPropBool("EncryptedLogging"));

    if(pLogTree->hasProp("BlindLogging")==true)
        pRequest->setBlindLogging(pLogTree->getPropBool("BlindLogging"));
    IArrayOf<IEspLogInfo> logArray;
    IPropertyTree* logInfo  = pLogTree->queryBranch("LogInformation");
    if(logInfo)
        deserializeLogInfo(logArray,*logInfo);
    pRequest->setLogInformation(logArray);

    Owned<IConstModelLogInformation> modelLogInformation;
    if(m_bModelRequest)
    {
        IPropertyTree* pModelLogTree = pLogTree->queryBranch("ModelLogInformation");

        if(pModelLogTree!=0)
        {
            IClientModelLogInformation* pClientModelLogInfo = &getModelLogInformation();

            UnserializeModelLogInfo(pModelLogTree,pClientModelLogInfo);
            modelLogInformation.setown(dynamic_cast<IConstModelLogInformation*>(pClientModelLogInfo));

            IClientLOGServiceUpdateModelRequest* pModelRequest = dynamic_cast<IClientLOGServiceUpdateModelRequest*>(pRequest);
            pModelRequest->setModelLogInformation(dynamic_cast<IConstModelLogInformation&>(*pClientModelLogInfo));
        }

    }

    return pRequest;
}

void CLogThread::UnserializeModelLogInfo(IPropertyTree* pModelTreeInfo,IClientModelLogInformation* pModelLogInformation)
{
    try
    {
        Owned<IPropertyTreeIterator> modelItr = pModelTreeInfo->getElements("Models/Model");

        if (modelItr->first())
        {
            while(modelItr->isValid()==true)
            {
                IPropertyTree& modelSrc = modelItr->query();

                IClientModelLogInfo& model = getModelLogInfo(pModelLogInformation);
                model.setName(modelSrc.queryProp("Name"));

                int scoreSequence = 0;
                Owned<IPropertyTreeIterator> ScoreItr = modelSrc.getElements("Scores/Score");
                if(ScoreItr->first())
                {
                    while(ScoreItr->isValid()==true)
                    {
                        IPropertyTree& ScoreSrc = ScoreItr->query();
                        IClientScoreLogInfo& Score = getScoreLogInfo(&model);

                        Score.setName(ScoreSrc.queryProp("Name"));
                        Score.setLogIdentifier(ScoreSrc.getPropInt("LogIdentifier"));
                        Score.setValue( ScoreSrc.getPropInt("Value") );
                        Score.setSequence(ScoreSrc.getPropInt("Sequence"));

                        int reasonSequence = 0;
                        Owned<IPropertyTreeIterator> ReasonItr = ScoreSrc.getElements("ReasonCodes/ReasonCode");
                        if(ReasonItr->first())
                        {
                            while(ReasonItr->isValid()==true)
                            {
                                IPropertyTree& ReasonSrc = ReasonItr->query();
                                IClientReasonCodeLogInfo& ReasonCode = getReasonCodeLogInfo(&Score);
                                ReasonCode.setValue( ReasonSrc.queryProp("Value") );
                                ReasonCode.setSequence( ReasonSrc.getPropInt("Sequence") );

                                // leave off the description as it is not logged
                                //if(ReasonSrc.hasProp("Description")==true)
                                //  ReasonCode.setDescription( ReasonSrc.queryProp("Description") );

                                ReasonItr->next();
                                reasonSequence++;
                            }
                        }
                        scoreSequence++;
                        ScoreItr->next();
                    } // ScoreIter
                } // ScoreIter
                modelItr->next();
            } // ModelIter
        } // if any models

        Owned<IPropertyTreeIterator> attribGrpItr = pModelTreeInfo->getElements("AttributeGroups/AttributeGroup");
        if (attribGrpItr->first())
        {
            while(attribGrpItr->isValid()==true)
            {
                IPropertyTree& attribGrpSrc = attribGrpItr->query();

                IClientAttributeGroupLogInfo& attribGroup = getAttributeGroupLogInfo(pModelLogInformation);

                attribGroup.setName(attribGrpSrc.queryProp("Name"));

                if(attribGrpSrc.hasProp("index")==true)
                    attribGroup.setLogIdentifier( attribGrpSrc.getPropInt("index") );


                Owned<IPropertyTreeIterator> attribItr = attribGrpSrc.getElements("Attributes/Attribute");
                if(attribItr->first())
                {
                    while(attribItr->isValid()==true)
                    {
                        IPropertyTree& attribSrc = attribItr->query();

                        IClientAttributeLogInfo& attrib = getAttributeLogInfo(&attribGroup);

                        attrib.setName(attribSrc.queryProp("Name"));
                        attrib.setValue(attribSrc.queryProp("Value"));

                        attribItr->next();
                    }
                }
                attribGrpItr->next();
            }
        }
    }
    catch (IException* e)
    {
        StringBuffer msg;
        ERRLOG("Fail to parse Model Log information: %s", e->errorMessage(msg).str());
        e->Release();
    }
    catch (...)
    {
        ERRLOG("Fail to parse Model Log information");
    }
}

void CLogThread::SendLog()
{
    //TODO: returning here might cause SendDelta to be greater than 0, causing tank files not be able to rollover.
    if(m_bRun == false)
        return;

    int recSend = 0;
    IClientLOGServiceUpdateRequest* pRequest  = 0;
    try{
        ForEachQueueItemIn(i,m_pServiceLog)
        {
            m_LogSend++;
            if(m_bThrottle)
            {
                if(m_BurstWaitInterval > 0 && m_LogSend % 10 == 0 && m_LogSendDelta > 5)
                {
                    m_SenderSem.wait(m_BurstWaitInterval);
                }
                else if(m_LinearWaitInterval > 0)
                {
                    m_SenderSem.wait(m_LinearWaitInterval);
                }
            }

            pRequest  = (IClientLOGServiceUpdateRequest*)m_pServiceLog.dequeue();
            if (pRequest != 0)
            {
                IEspLOGServiceUpdateRequest* pEspRequest = dynamic_cast<IEspLOGServiceUpdateRequest*>(pRequest);
                try
                {
                    //need to link once.....
                    //ESPLOG(LogNormal+1,"Sending ACK %s",pEspRequest->getGUID());
                    //DBGLOG("Sending log %s",pEspRequest->getGUID()?pEspRequest->getGUID():"");
                    m_pLoggingService->async_UpdateLogService(pRequest, this, pRequest);
                    pRequest->Release();
                    m_LogSendDelta++;

                }
                catch(IException* Ex)
                {
                    StringBuffer str;
                    Ex->errorMessage(str);
                    ERRLOG("Exception %s thrown within logging client",str.str());
                    m_pServiceLog.enqueue(pRequest);
                    Ex->Release();
                }
                catch(...)
                {
                    ERRLOG("Unknown Error thrown within logging client");
                    m_pServiceLog.enqueue(pRequest);
                }
            }
        }
    }
    catch(IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
    }
    catch(...)
    {
        int i = 0;
    }

    return;
}


void CLogThread::HandleLoggingServerResponse(IClientLOGServiceUpdateRequest* Request,IClientLOGServiceUpdateResponse *Response)
{
    m_LogSendDelta--;
    if(Response==0 || Response->getUpdateLogStatus() == 0 || strlen(Response->getGUID())<10)
    {
        StringBuffer reasonbuf;
        if(Response == 0)
            reasonbuf.append("response is NULL");
        else if(Response->getUpdateLogStatus() == 0)
            reasonbuf.appendf("Log status is 0 for %s", Response->getGUID()?Response->getGUID():"");
        else
            reasonbuf.appendf("GUID(%s) is not correct", Response->getGUID()?Response->getGUID():"");
        DBGLOG("Failed at the server so adding back to the queue. Error: %s", reasonbuf.str());
        //means we failed..... so we will have to try again
        m_pServiceLog.enqueue(LINK(Request));
        return;
    }
    if(m_bFailSafeLogging == true && m_LogFailSafe.get())
    {
        //ESPLOG(LogNormal+1,"Adding ACK %s",Response->getGUID());
        //DBGLOG("Adding ACK %s",Response->getGUID());
        m_LogFailSafe->AddACK(Response->getGUID());
    }
//  m_SenderSem.signal();
    return;
}

int CLogThread::onUpdateLogServiceComplete(IClientLOGServiceUpdateResponse *Response,IInterface* state)
{
    m_LogSend--;
    if(Response==0 && state==0)
    {
        DBGLOG("NULL LogRequest passed into onUpdateLogServiceComplete");
        return 0;
    }

    IClientLOGServiceUpdateRequest* Request  = dynamic_cast<IClientLOGServiceUpdateRequest*>(state);
    if(Request==0)
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateRequest");
    }

    HandleLoggingServerResponse(Request,Response);

    return 0;
}

int CLogThread::onUpdateModelLogServiceComplete(IClientLOGServiceUpdateResponse *Response,IInterface* state)
{
    if(Response==0 && state==0)
    {
        DBGLOG("NULL LogRequest passed into onUpdateModelLogServiceComplete");
        return 0;
    }

    IClientLOGServiceUpdateModelRequest* Request  = dynamic_cast<IClientLOGServiceUpdateModelRequest*>(state);
    if(Request==0)
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateModelRequest");
    }

    // For a UpdateModelRequest, first cast to it's parent class so
    // we can use the same code in HandleLoggingServerResponse
    IClientLOGServiceUpdateRequest* base_request = dynamic_cast<IClientLOGServiceUpdateRequest*>(state);
    if( base_request == 0 )
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateRequest to pass to HandleLoggingServerResponse");
        return 0;
    }

    HandleLoggingServerResponse(base_request,Response);

    return 0;
}

int CLogThread::onUpdateLogServiceError(IClientLOGServiceUpdateResponse *Response,IInterface* state)
{
    DBGLOG("Error Log");
    m_LogSend--;
    IClientLOGServiceUpdateRequest* Request  = dynamic_cast<IClientLOGServiceUpdateRequest*>(state);
    if(Request==0)
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateRequest");
    }
    HandleLoggingServerResponse(Request,Response);
    return 0;
}


int CLogThread::onUpdateModelLogServiceError(IClientLOGServiceUpdateResponse *Response,IInterface* state)
{
    DBGLOG("Error Log");
    m_LogSend--;
    IClientLOGServiceUpdateModelRequest* Request  = dynamic_cast<IClientLOGServiceUpdateModelRequest*>(state);
    if(Request==0)
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateModelRequest");
    }

    // For a UpdateModelRequest, first cast to it's parent class so
    // we can use the same code in HandleLoggingServerResponse
    IClientLOGServiceUpdateRequest* base_request = dynamic_cast<IClientLOGServiceUpdateRequest*>(state);
    if( base_request == 0 )
    {
        DBGLOG("Could not cast state to IClientLOGServiceUpdateRequest to pass to HandleLoggingServerResponse");
        return 0;
    }

    HandleLoggingServerResponse(base_request,Response);

    return 0;
}


void CLogThread::start()
{
    if(m_NiceLevel > 0)
        setNice(m_NiceLevel);

    Thread::start();
}

void CLogThread::finish()
{
    try{
        DBGLOG("Log delta of %d",m_LogSendDelta);
        if (m_pServiceLog.ordinality() == 0 && m_LogFailSafe.get() && m_LogSendDelta == 0)
            m_LogFailSafe->RollCurrentLog();
    }
    catch(...){DBGLOG("Exception");}
    m_bRun = false;
    m_sem.signal();
    join();
}

void CLogThread::CleanQueue()
{

    ForEachQueueItemIn(i,m_pServiceLog)
    {
        IClientLOGServiceUpdateRequest* pRequest  = (IClientLOGServiceUpdateRequest*)m_pServiceLog.dequeue();
        if (pRequest != 0)
            pRequest->Release();
    }


}


void CPooledClientWsLogService::async_UpdateLogService(IClientLOGServiceUpdateRequest *request, IClientWsLogServiceEvents *events,IInterface* state)
{
    if(m_url.length()==0){ throw MakeStringExceptionDirect(-1, "url not set"); }

    CLOGServiceUpdateModelRequest* espModelRequest = dynamic_cast<CLOGServiceUpdateModelRequest*>(request);
    CLOGServiceUpdateRequest* esprequest = dynamic_cast<CLOGServiceUpdateRequest*>(request);
    if(espModelRequest!=0)
    {
        espModelRequest->setMethod("UpdateModelLogService");
        espModelRequest->setReqId(m_reqId++);
        espModelRequest->setEventSink(events);
        espModelRequest->setState(state);
        espModelRequest->setUserId(m_userid.str());
        espModelRequest->setPassword(m_password.str());
        espModelRequest->setRealm(m_realm.str());
        espModelRequest->Link();

        if(state!=NULL)
            state->Link();

        m_thread_pool->start((void *)(IRpcRequestBinding *)(espModelRequest), "Model Logging Thread", LogThreadWaitTime * 1000);
    }else if(esprequest!=0)
    {
        esprequest->setMethod("UpdateLogService");
        esprequest->setReqId(m_reqId++);
        esprequest->setEventSink(events);
        esprequest->setState(state);
        esprequest->setUserId(m_userid.str());
        esprequest->setPassword(m_password.str());
        esprequest->setRealm(m_realm.str());
        esprequest->Link();

        if(state!=NULL)
            state->Link();

        m_thread_pool->start((void *)(IRpcRequestBinding *)(esprequest), "Logging Thread", LogThreadWaitTime * 1000);
    }else{
        throw MakeStringExceptionDirect(-1, "LogServiceUpdateRequest is null.");
    }
}

