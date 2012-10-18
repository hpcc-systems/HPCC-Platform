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

#ifndef _ESPWIZ_WsDeploy_HPP__
#define _ESPWIZ_WsDeploy_HPP__

#include "WsDeploy_esp.ipp"
#include "environment.hpp"
#include "jmutex.hpp"
#include "dasds.hpp"
#include "deployutils.hpp"
#include "buildset.hpp"
#include "jsocket.hpp"
#include "XMLTags.h"
#include "httpclient.hpp"
#include "jqueue.tpp"
#include "build-config.h"

typedef enum EnvAction_
{
    CLOUD_NONE,
    CLOUD_LOCK_ENV,
    CLOUD_UNLOCK_ENV,
    CLOUD_SAVE_ENV,
    CLOUD_ROLLBACK_ENV,
    CLOUD_NOTIFY_INITSYSTEM,
    CLOUD_CHECK_LOCKER
} EnvAction;

#define CLOUD_SOAPCALL_TIMEOUT 10000
#define CONFIG_MONITOR_CHECK_INTERVAL  1000
#define CONFIG_MONITOR_TIMEOUT_PERIOD  6000

class CCloudTask;
class CCloudActionHandler;
class CWsDeployEx;
class CWsDeployExCE;

interface IConfigFileObserver : extends IObserver
{
public:
  virtual const char* getConfigFilePath() = 0;
};

class CWsDeployFileInfo : public CInterface, implements IConfigFileObserver
{
private:
    //==========================================================================================
    // the following class implements notification handler for subscription to dali for environment 
    // updates by other clients.
    //==========================================================================================
    class CSdsSubscription : public CInterface, implements ISDSSubscription
    {
    public:
        CSdsSubscription(CWsDeployFileInfo* pFileInfo)
        { 
            m_pFileInfo.set(pFileInfo);
            sub_id = querySDS().subscribe("/Environment", *this);
        }
        virtual ~CSdsSubscription() { unsubscribe(); }

        void unsubscribe() 
        {
            if (sub_id) {
                if (sub_id) { querySDS().unsubscribe(sub_id); sub_id = 0; }
            }
        }
        IMPLEMENT_IINTERFACE;

        //another client (like configenv) may have updated the environment and we got notified
        //(thanks to our subscription) but don't just reload it yet since this notification is sent on 
        //another thread asynchronously and we may be actively working with the old environment.  Just
        //invoke handleEnvironmentChange() when we are ready to invalidate cache in environment factory.
        //
        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL);

  private:
        SubscriptionId sub_id;
        Owned<CWsDeployFileInfo> m_pFileInfo;
    };

    //==========================================================================================
    // the following class generates JavaScript files required by the service gui, at startup or 
    // whenever the environment is updated.
    //==========================================================================================
    class CGenerateJSFactoryThread : public CInterface, implements IThreaded, implements IInterface
    {
    public:
        CGenerateJSFactoryThread(CWsDeployExCE* pService,IConstEnvironment* pConstEnv)
        { 
            m_pService.set(pService);
            m_pWorkerThread = NULL;
            m_constEnv.set(pConstEnv);
        }
        virtual ~CGenerateJSFactoryThread() 
        { 
            bool joinedOk = m_pWorkerThread->join();

            if(NULL != m_pWorkerThread) {
                delete m_pWorkerThread;
                m_pWorkerThread = NULL;
            }
        }

        IMPLEMENT_IINTERFACE;

        virtual void main() 
        {
            generateHeaders(&m_constEnv->getPTree(), m_constEnv);
        }

        void init()
        {
            m_pWorkerThread = new CThreaded("CGenerateJSFactoryThread");
            IThreaded* pIThreaded = this;
            m_pWorkerThread->init(pIThreaded);
        }

        void refresh(IConstEnvironment* pConstEnv)
        {
            m_constEnv.set(pConstEnv);
        }

    private:
        CThreaded* m_pWorkerThread;
        Linked<CWsDeployExCE> m_pService;
        Linked<IConstEnvironment> m_constEnv;
    };
  public:

    class CConfigChangeNotification : implements INotification
    {
    public:

      CConfigChangeNotification(IObservable *pSource) : m_pSource(pSource)
      {
      };

      virtual ~CConfigChangeNotification()
      {
      };

      virtual NotifyAction getAction(void)
      {
        return NotifyNone;
      }

      virtual IObservable* querySource(void)
      {
        return m_pSource;
      }

    private:
      IObservable* m_pSource;
      CConfigChangeNotification() {};
    };

    class CConfigFileMonitorThread
      : public CInterface, implements IThreaded, implements IObservable
    {
    public:
      CConfigFileMonitorThread(unsigned int uCheckInterval, unsigned int uTimeout)
        : m_pWorkerThread(NULL), m_quitThread(false), m_uCheckInterval(uCheckInterval), m_uTimeout(uTimeout), m_configChangeNotification(this)
      {
      };

      virtual ~CConfigFileMonitorThread()
      {
        m_quitThread = true;
        m_pWorkerThread->join();
        delete m_pWorkerThread;
      };

      IMPLEMENT_IINTERFACE;

      virtual void notify(IDirectoryDifferenceIterator *diffIter)
      {
        if ( diffIter == NULL )
        {
          return;
        }
        else
        {
          CriticalBlock block(m_critsecObserverQueue);

          for (unsigned int idxObservers = 0; idxObservers < m_qObservers.ordinality(); idxObservers++)
          {
            IConfigFileObserver *pConfigFileObserver = m_qObservers.query(idxObservers);

            for (diffIter->first(); diffIter->isValid() == true; diffIter->next())
            {
              bool bDoNotify = true;

              if ( diffIter->getFlags() == IDDIunchanged || diffIter->query().queryFilename() == NULL || pConfigFileObserver == NULL || pConfigFileObserver->getConfigFilePath() == NULL || (strcmp( pConfigFileObserver->getConfigFilePath(), diffIter->query().queryFilename() ) != 0) )
              {
                bDoNotify = false;
              }

              if (bDoNotify == true)
              {
                 m_qObservers.query(idxObservers)->onNotify(m_configChangeNotification);
              }
            }
          }
        }
      }

      virtual void addObserver( IConfigFileObserver &observer )
      {
         CriticalBlock block(m_critsecObserverQueue);

        //allow observers to register only once
        if (m_qObservers.find(&observer) == (unsigned)-1)
        {
          m_qObservers.enqueue(&observer);
        }
      }

      virtual void removeObserver( IConfigFileObserver &observer )
      {
        CriticalBlock block(m_critsecObserverQueue);

        m_qObservers.dequeue(&observer);
      }

      virtual void main()
      {
        Owned<IFile> configFiles = createIFile(CONFIG_SOURCE_DIR);

        while ( m_quitThread == false )
        {
          Owned<IDirectoryDifferenceIterator> diffIter = configFiles->monitorDirectory(NULL, NULL, false, false, m_uCheckInterval, m_uTimeout);

          if (diffIter.get() != NULL)
          {
            notify(diffIter.get());
          }
        }
      };

      void init()
      {
        if ( m_pWorkerThread == NULL)
        {
          m_pWorkerThread = new CThreaded("CConfigFileMonitorThread");
          IThreaded* pIThreaded = this;
          m_pWorkerThread->init(pIThreaded);
        }
      };

      static CConfigFileMonitorThread* getInstance()
      {
        static Owned<CConfigFileMonitorThread> s_configFileMonitorSingleton;
        static CSingletonLock slock;

        if (slock.lock() == true)
        {
          if (s_configFileMonitorSingleton.get() == NULL)
          {
            s_configFileMonitorSingleton.setown(new CWsDeployFileInfo::CConfigFileMonitorThread(CONFIG_MONITOR_CHECK_INTERVAL, CONFIG_MONITOR_TIMEOUT_PERIOD));
            s_configFileMonitorSingleton->init();
          }

          slock.unlock();
        }

        return s_configFileMonitorSingleton.get();
      };

    protected:
      CThreaded* m_pWorkerThread;

      bool m_quitThread;
      unsigned int m_uTimeout;
      unsigned int m_uCheckInterval;
      QueueOf<IConfigFileObserver,false> m_qObservers;
      CriticalSection m_critsecObserverQueue;
      CConfigChangeNotification m_configChangeNotification;

    private:
      CConfigFileMonitorThread() : m_configChangeNotification(NULL) {};
      CConfigFileMonitorThread(const CConfigFileMonitorThread& configFileThread) : m_configChangeNotification(NULL) {};
      CConfigFileMonitorThread& operator=(CConfigFileMonitorThread const&);
    };

    class CClientAliveThread : public CInterface, implements IThreaded, implements IInterface
    {
    public:
        CClientAliveThread(CWsDeployFileInfo* pFileInfo, unsigned brokenConnTimeout)
        { 
            m_pFileInfo = pFileInfo;
            m_pWorkerThread = NULL;
            m_quitThread = false;
            m_brokenConnTimeout = brokenConnTimeout;
        }
        virtual ~CClientAliveThread() 
        { 
            m_quitThread = true;
            m_sem.signal();
            bool joinedOk = m_pWorkerThread->join();
            m_pFileInfo = NULL;

            if(NULL != m_pWorkerThread) {
                delete m_pWorkerThread;
                m_pWorkerThread = NULL;
            }
        }

        IMPLEMENT_IINTERFACE;

        virtual void main() 
        {
            while (!m_quitThread)
            {
                if (!m_sem.wait(m_brokenConnTimeout))
                {
                    if (m_pFileInfo)
                        m_pFileInfo->activeUserNotResponding();

                    break;
                }
            }
        }

        void init()
        {
            m_quitThread = false;
            m_pWorkerThread = new CThreaded("CClientAliveThread");
            IThreaded* pIThreaded = this;
            m_pWorkerThread->init(pIThreaded);
        }

        void signal()
        {
            m_sem.signal();
        }

    private:
        CThreaded* m_pWorkerThread;
        CWsDeployFileInfo* m_pFileInfo;
        Semaphore m_sem;
        bool m_quitThread;
        unsigned m_brokenConnTimeout;
    };

  class CLockerAliveThread : public CInterface, implements IThreaded, implements IInterface
  {
  public:
    CLockerAliveThread(CWsDeployFileInfo* pFileInfo, unsigned brokenConnTimeout, const char* uname, const char* ip)
    { 
      m_pFileInfo = pFileInfo;
      m_pWorkerThread = NULL;
      m_quitThread = false;
      m_brokenConnTimeout = brokenConnTimeout;
      StringBuffer sb;
      sb.appendf("<Computers><Computer netAddress='%s'/></Computers>", ip);
      m_pComputers.setown(createPTreeFromXMLString(sb.str()));
      m_user.clear().append(uname);
    }
    virtual ~CLockerAliveThread() 
    { 
      m_quitThread = true;
      m_sem.signal();
      bool joinedOk = m_pWorkerThread->join();
      m_pFileInfo = NULL;

      if(NULL != m_pWorkerThread) {
        delete m_pWorkerThread;
        m_pWorkerThread = NULL;
      }
    }

    IMPLEMENT_IINTERFACE;

    virtual void main();

    void init()
    {
      m_quitThread = false;
      m_pWorkerThread = new CThreaded("CLockerAliveThread");
      IThreaded* pIThreaded = this;
      m_pWorkerThread->init(pIThreaded);
    }

    void signal()
    {
      m_quitThread = true;
      m_sem.signal();
    }

  private:
    CThreaded* m_pWorkerThread;
    CWsDeployFileInfo* m_pFileInfo;
    bool m_quitThread;
    unsigned m_brokenConnTimeout;
    Owned<IPropertyTree> m_pComputers;
    StringBuffer m_user;
    Semaphore m_sem;
  };

public:
    IMPLEMENT_IINTERFACE;
    CWsDeployFileInfo(CWsDeployExCE* pService, const char* pEnvFile, bool bCloud) : m_configChanged(false), m_pService(pService),m_bCloud(bCloud)
    {
        m_envFile.clear().append(pEnvFile);
    }
    ~CWsDeployFileInfo();
    void initFileInfo(bool createFile, bool bClearEnv = true);
    void setConfigChanged(bool b)
    {
      m_configChanged = b;
    };
    bool getConfigChanged() const
    {
      return m_configChanged;
    };
    bool getSkipNotification() const
    {
      return m_bSkipNotification;
    }
    void setSkipNotification(bool b)
    {
      m_bSkipNotification = b;
    }
    virtual bool onNotify(INotification &notify)
    {
      if (notify.getAction() == NotifyNone && getSkipNotification() == false)
      {
        setConfigChanged(true);
      }
      setSkipNotification(false);
      return true;
    };
    virtual const char* getConfigFilePath()
    {
      if (m_pFile == NULL)
      {
        return NULL;
      } 
       
      return m_pFile->queryFilename();
    };
    static void setFilePath(StringBuffer &filePath, const char* targetName);
    static bool addCopyToPropTree(IPropertyTree* pPropTree, IPropertyTree* pDupTree, const char* tag_name);
    virtual void updateConfigFromFile();
    virtual bool deploy(IEspContext &context, IEspDeployRequest &req, IEspDeployResponse &resp);
    virtual bool graph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp);
    virtual bool navMenuEvent(IEspContext &context, IEspNavMenuEventRequest &req, 
                                            IEspNavMenuEventResponse &resp);
    virtual bool displaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp);
    virtual bool isAlphaNumeric(const char *pstr) const;
    virtual bool saveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp);
    virtual bool getBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp);
    virtual bool getDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp);
    virtual bool startDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp);
    virtual bool getBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp);
    virtual bool importBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp);
    virtual bool getComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp);
    virtual bool handleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp);
    virtual bool handleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp);
    virtual bool handleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp);
    virtual bool handleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp);
    virtual bool handleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp);
    virtual bool handleComponentCopy(IPropertyTree *pComponents, IPropertyTree *pEnvRoot);
    virtual bool handleHardwareCopy(IPropertyTree *pComponents, IPropertyTree *pEnvRoot);
    virtual bool handleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp);
    virtual bool handleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp);
    virtual bool handleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp);
    virtual bool handleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp);
    virtual bool handleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp);
    virtual bool getNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp);
    virtual bool getValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp);
    virtual bool unlockUser(IEspContext &context, IEspUnlockUserRequest &req, IEspUnlockUserResponse &resp);
    virtual bool clientAlive(IEspContext &context, IEspClientAliveRequest &req, IEspClientAliveResponse &resp);
    virtual bool getEnvironment(IEspContext &context, IEspGetEnvironmentRequest &req, IEspGetEnvironmentResponse &resp);
    virtual bool setEnvironment(IEspContext &context, IEspSetEnvironmentRequest &req, IEspSetEnvironmentResponse &resp);
    virtual bool lockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp);
    virtual bool unlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp);
    virtual bool buildEnvironment(IEspContext &context, IEspBuildEnvironmentRequest &req, IEspBuildEnvironmentResponse &resp);
    virtual bool getSubnetIPAddr(IEspContext &context, IEspGetSubnetIPAddrRequest &req, IEspGetSubnetIPAddrResponse &resp);
    virtual bool saveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp);
    virtual bool rollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp);
    virtual bool notifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp);
    virtual bool getSummary(IEspContext &context, IEspGetSummaryRequest &req, IEspGetSummaryResponse &resp);
    virtual bool addReqdComps(IEspContext &context, IEspAddReqdCompsRequest &req, IEspAddReqdCompsResponse &resp);
    
    void environmentUpdated()
    {
        if (m_skipEnvUpdateFromNotification)
            return;

        synchronized block(m_mutex);

        m_pEnvXml.clear();
        m_pGraphXml.clear();
        m_pNavTree.clear();

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();       
        m_constEnvRdOnly.set(factory->openEnvironment());
        m_constEnvRdOnly->clearCache();
    }

    void getNavigationData(IEspContext &context, IPropertyTree* pData);
    IPropertyTree* queryComputersForCloud();
    bool getUserWithLock(StringBuffer& sbUser, StringBuffer& sbIp);
    bool updateEnvironment(const char* xml);
    bool isLocked(StringBuffer& sbUser, StringBuffer& ip);
    void getLastSaved(StringBuffer& sb) { m_lastSaved.getString(sb);}

private:
    void generateGraph(IEspContext &context, IConstWsDeployReqInfo *reqInfo);
    void addDeployableComponentAndInstances( IPropertyTree* pEnvRoot, IPropertyTree* pComp,
                                                          IPropertyTree* pDst,      IPropertyTree* pFolder,
                                                          const char* displayType);
    IPropertyTree* findComponentForFolder(IPropertyTree* pFolder, IPropertyTree* pEnvSoftware);
    const char* GetDisplayProcessName(const char* processName, char* buf) const;
    void addInstance( IPropertyTree* pDst,  const char* comp, const char* displayType, 
                            const char* compName, const char* build, const char* instType, 
                            const char* instName, const char* computer);
    void checkForRefresh(IEspContext &context, IConstWsDeployReqInfo *reqInfo, bool checkWriteAccess);
    IPropertyTree* getEnvTree(IEspContext &context, IConstWsDeployReqInfo *reqInfo);
    void activeUserNotResponding();
    void saveEnvironment(IEspContext* pContext, IConstWsDeployReqInfo *reqInfo, StringBuffer& errMsg, bool saveAs = false);
    void unlockEnvironment(IEspContext* pContext, IConstWsDeployReqInfo *reqInfo, const char* xmlarg, StringBuffer& sbMsg, bool saveEnv = false);
    void setEnvironment(IEspContext &context, IConstWsDeployReqInfo *reqInfo, const char* newEnv, const char* fnName, StringBuffer& sbBackup, bool validate = true, bool updateDali = true);
    bool checkForRequiredComponents(IPropertyTree* pEnvRoot, const char* ip, StringBuffer& reqdCompNames, const char* buildSet, bool autoAdd=false);
  
    Owned<CSdsSubscription>   m_pSubscription;
    Owned<IConstEnvironment>  m_constEnvRdOnly;
    Owned<SCMStringBuffer>    m_pEnvXml;
    Owned<IPropertyTree>      m_pNavTree;
    Owned<SCMStringBuffer>    m_pGraphXml;
    Mutex                     m_mutex;
    Owned<IEnvironment>       m_Environment;
    StringBuffer              m_userWithLock;
    StringBuffer              m_userIp;
    StringBuffer              m_daliServer;
    StringBuffer              m_envFile;
    StringBuffer              m_cloudEnvBkupFileName;
    StringBuffer              m_cloudEnvId;
    short                     m_daliServerPort;
    Owned<CGenerateJSFactoryThread> m_pGenJSFactoryThread;
private:
    bool                      m_skipEnvUpdateFromNotification;
    bool                      m_activeUserNotResp;
    bool                      m_configChanged;
    bool                      m_bSkipNotification;
    bool                      m_bCloud;
    Owned<IFile>              m_pFile;
    Owned<IFileIO>            m_pFileIO;
    MapStringToMyClass<CClientAliveThread> m_keepAliveHTable;
    CDateTime                 m_lastSaved;
    Owned<CLockerAliveThread> m_cloudLockerAliveThread;
    Owned<IPropertyTree>      m_lockedNodesBeforeEnv;
    CWsDeployExCE*              m_pService;
};

class CCloudTaskThread : public CInterface, 
    implements IPooledThread
{
public:
    IMPLEMENT_IINTERFACE;

    CCloudTaskThread()
    {
    }
    virtual ~CCloudTaskThread()
    {
    }

    void init(void *startInfo) 
    {
        m_pTask.set((CCloudTask*)startInfo);
    }
    void main();
    bool canReuse()
    {
        return true;
    }
    bool stop()
    {
        return true;
    }

    virtual bool getAbort() const      { return s_abort;   }
    virtual void setAbort(bool bAbort) { s_abort = bAbort; }

private:
    Owned<CCloudTask> m_pTask;
    static bool s_abort;
};

class CCloudTaskThreadFactory : public CInterface, public IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CCloudTaskThread();
    }
};

CCloudTask* createCloudTask(CCloudActionHandler* pHandler, EnvAction eA, const char* ip);
void expandRange(IPropertyTree* pComputers);
const char* getFnString(EnvAction ea);

class CCloudActionHandler : public CInterface, implements IInterface
{
public:
    CCloudActionHandler(CWsDeployFileInfo* pFileInfo, EnvAction eA, EnvAction cancelEA, 
        const char* user, const char* port, IPropertyTree* pComputers)
    { 
        m_pFileInfo = pFileInfo;
        m_opFailed = false;
        m_eA = eA;
        m_cancelEA = cancelEA;
        m_port.append(port);
        m_user.append(user);
        m_pComputers = pComputers;
    }
    
  virtual ~CCloudActionHandler() 
    { 
        m_pFileInfo = NULL;
    }

  void setSaveActionParams(const char* newEnv, const char* id)
  {
    m_newEnv.clear().append(newEnv);
    m_newEnvId.clear().append(id);
  }

    void setFailed(bool flag){m_opFailed = flag;}

    IMPLEMENT_IINTERFACE;

    bool start(StringBuffer& msg)
    {
        try
        {
            IPropertyTree* pComputers = m_pComputers;

            if (!m_pComputers)
                pComputers = m_pFileInfo->queryComputersForCloud();
            else if (pComputers && pComputers->hasProp("@hasRange"))
                expandRange(pComputers);

            if (!pComputers)
                throw MakeStringException(-1, "No computers found for Cloud Operation %s", getFnString(m_eA));

            if (m_threadPool == NULL)
            {
                IThreadFactory* pThreadFactory = new CCloudTaskThreadFactory();
                m_threadPool.setown(createThreadPool("WsDeploy Cloud Task Thread Pool", pThreadFactory, NULL, pComputers->numChildren()));
                pThreadFactory->Release();
            }
            else
            {
                int nThreads = m_threadPool->runningCount();
                if (nThreads > 0)
                    throw MakeOsException(-1, "Unfinished threads detected!");
            }

            Owned<IPropertyTreeIterator> iter = pComputers->getElements(XML_TAG_COMPUTER);
            StringBuffer localip;
            queryHostIP().getIpText(localip);

            ForEach(*iter)
            {
                IPropertyTree* pComputer = &iter->query();
                const char* netAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                if (!strcmp(netAddr, ".") || 
                    !strcmp(netAddr, "127.0.0.1") || 
                    !strcmp(netAddr, "0.0.0.0") || 
                    !strcmp(netAddr, localip.str()))
                    continue;
                else
                {
                    Owned<CCloudTask> task = createCloudTask(this, m_eA, netAddr);
                    m_threadPool->start(task);//start a thread for this task
                }
            }

            m_threadPool->joinAll();

            if (!m_opFailed && m_eA == CLOUD_SAVE_ENV)
            {
                ForEach(*iter)
                {
                    IPropertyTree* pComputer = &iter->query();
                    const char* netAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                    if (!strcmp(netAddr, ".") || 
                        !strcmp(netAddr, "127.0.0.1") || 
                        !strcmp(netAddr, "0.0.0.0") || 
                        !strcmp(netAddr, localip.str()))
                        continue;
                    else
                    {
                        Owned<CCloudTask> task = createCloudTask(this, CLOUD_NOTIFY_INITSYSTEM, netAddr);
                        m_threadPool->start(task);//start a thread for this task
                    }
                }

                m_threadPool->joinAll();
            }

            if (m_opFailed)
            {
                HashIterator iterHash(m_resultMap);
                ForEach(iterHash)
                {
                    const char* key = (const char*)iterHash.query().getKey();
                    String str((m_resultMap.mapToValue(&iterHash.query()))->str());
                    if (str.startsWith("SOAP Connection error"))
                        msg.appendf("\nIpAddress: %s\nResult:%s\n", key, "SOAP Connection error - Could not connect to the target");
                    else
                        msg.appendf("\nIpAddress: %s\nResult:%s\n", key, str.toCharArray());
                }

                //Perform the appropriate cancel action
                if (m_cancelEA != CLOUD_NONE)
                {
                    ForEach(*iter)
                    {
                        IPropertyTree* pComputer = &iter->query();
                        const char* netAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                        if (!strcmp(netAddr, ".") || 
                            !strcmp(netAddr, "127.0.0.1") || 
                            !strcmp(netAddr, "0.0.0.0") || 
                            !strcmp(netAddr, localip.str()))
                            continue;
                        else
                        {
                            Owned<CCloudTask> task = createCloudTask(this, m_cancelEA, netAddr);
                            m_threadPool->start(task);//start a thread for this task
                        }
                    }

                    m_threadPool->joinAll();
                }

                return false;
            }

            return true;
        }
        catch (IException* e)
        {
            if (m_threadPool)
                m_threadPool->joinAll();

            StringBuffer sErrMsg;
            e->errorMessage(sErrMsg);
            e->Release();
            msg.appendf("Exception throw during cloud operation %s.\nMessage:%s", getFnString(m_eA), sErrMsg.str());
        }
        catch (...)
        {
            if (m_threadPool)
                m_threadPool->joinAll();

            throw MakeErrnoException("Unknown Exception during cloud operation %s", getFnString(m_eA));
        }

        return false;
    }
    
    void setResult(const char* ip, const char* msg)
    {
        synchronized block(m_mutex);
        StringBuffer* pSb = new StringBuffer(msg);
        m_resultMap.setValue(ip, *pSb);
    }

    const char* getPort() {return m_port.str();}
    const char* getUser() {return m_user.str();}
    const char* getNewEnv() {return m_newEnv.str();}
    const char* getNewEnvId() {return m_newEnvId.str();}
    const char* getCurIp(){ if (m_curIp.length() == 0) queryHostIP().getIpText(m_curIp); return m_curIp.str(); }

private:
    CWsDeployFileInfo* m_pFileInfo;
    Mutex m_mutex;
    bool m_opFailed;
    EnvAction m_eA;
    EnvAction m_cancelEA;
    Owned<IThreadPool> m_threadPool;
    MapStrToBuf m_resultMap;
    StringBuffer m_port;
    StringBuffer m_user;
    StringBuffer m_newEnv;
    StringBuffer m_newEnvId;
    StringBuffer m_curIp;
    IPropertyTree* m_pComputers;
};

class CCloudTask : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CCloudTask(CCloudActionHandler* pHandler, EnvAction eA, const char* ip)
    {
        m_caHandler.set(pHandler);
        m_eA = eA;
        m_ip.append(ip);
    }

    bool makeSoapCall()
    {
        try
        {
            Owned<CRpcCall> rpccall;
            rpccall.setown(new CRpcCall);
            StringBuffer sb("http://");
            sb.append(m_ip.str()).append(":").append(m_caHandler->getPort()).append("/WsDeploy");
            rpccall->set_url(sb.str());
            rpccall->set_name(getFnString(m_eA));
            SoapStringParam uName(m_caHandler->getUser());
            uName.marshall(*rpccall.get(), "UserName","", "", "");
            SoapStringParam ipAddr(m_caHandler->getCurIp());
            ipAddr.marshall(*rpccall.get(), "Ip","", "", "");

            if (m_eA == CLOUD_SAVE_ENV)
            {
                SoapStringParam newEnv(m_caHandler->getNewEnv());
                newEnv.marshall(*rpccall.get(), "NewEnv","", "", "");
            }

            if (m_eA == CLOUD_SAVE_ENV || m_eA == CLOUD_ROLLBACK_ENV)
            {
                SoapStringParam newEnvId(m_caHandler->getNewEnvId());
                newEnvId.marshall(*rpccall.get(), "Id","", "", "");
            }

            Owned<IHttpClientContext> httpctx = getHttpClientContext();
            Owned<IHttpClient> httpclient = httpctx->createHttpClient(rpccall->getProxy(), rpccall->get_url());
            httpclient->setUserID("soapclient");
            httpclient->setPassword("");
            httpclient->setTimeOut(CLOUD_SOAPCALL_TIMEOUT);
            Owned<ISoapClient> soapclient;
            httpclient->Link();
            soapclient.setown(new CSoapClient(httpclient));
            soapclient->setUsernameToken("soapclient", "", "");
            StringBuffer soapAction, resultbuf;
            int result = soapclient->postRequest("text/xml","", *rpccall.get(), resultbuf, NULL);
            IPropertyTree* pResult = createPTreeFromXMLString(resultbuf);
            StringBuffer xpath;
            xpath.appendf("soap:Body/%sResponse/Msg", getFnString(m_eA));
            const char* msg = pResult->queryProp(xpath.str());
            xpath.clear().appendf("soap:Body/%sResponse/ReturnCode", getFnString(m_eA));
            int retCode = pResult->getPropInt(xpath.str());

            if (retCode != 1)
            {
                m_caHandler->setFailed(true);
                m_caHandler->setResult(m_ip.str(), msg?msg:"");
            }

            return true;
        }
        catch(IException* e)
        {
            StringBuffer sb;
            e->errorMessage(sb);
            m_caHandler->setFailed(true);
            m_caHandler->setResult(m_ip, sb.str());
        }

        return false;
    }

private:
    Linked<CCloudActionHandler> m_caHandler;
    EnvAction m_eA;
    StringBuffer m_ip;
};


class CConfigHelper
{
public:

  CConfigHelper();
  virtual ~CConfigHelper();

  void init(const IPropertyTree *cfg, const char* esp_name);

  bool isInBuildSet(const char* comp_process_name, const char* comp_name) const;

  const char* getConfigXMLDir() const
  {
    return m_strConfigXMLDir.toCharArray();
  };
  const char* getBuildSetFileName() const
  {
    return m_strBuildSetFileName.toCharArray();
  };
  const char* getEnvConfFile() const
  {
    return m_strEnvConfFile.toCharArray();
  };
  const char* getConfFile() const
  {
    return m_strConfFile.toCharArray();
  };
  const char* getBuildSetFilePath() const
  {
    return m_strBuildSetFilePath.toCharArray();
  };

protected:
  Owned<IPropertyTree> m_pDefBldSet;
  StringBuffer  m_strConfigXMLDir;
  StringBuffer  m_strBuildSetFileName;
  StringBuffer  m_strEnvConfFile;
  StringBuffer  m_strConfFile;
  StringBuffer  m_strBuildSetFilePath;
};

class CWsDeployExCE : public CWsDeploy
{
public:
    IMPLEMENT_IINTERFACE;

    virtual ~CWsDeployExCE();
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    virtual bool onInit(IEspContext &context, IEspEmptyRequest& req, IEspInitResponse& resp);
    virtual bool onDeploy(IEspContext &context, IEspDeployRequest &req, IEspDeployResponse &resp);
    virtual bool onGraph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp);
    virtual bool onNavMenuEvent(IEspContext &context, IEspNavMenuEventRequest &req, 
                                                    IEspNavMenuEventResponse &resp);
    virtual bool onDisplaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp);
    virtual bool onSaveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp);
    virtual bool onGetBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp);
    virtual bool onGetDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp);
    virtual bool onStartDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp);
    virtual bool onGetBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp);
    virtual bool onImportBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp);
    virtual bool onGetComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp);
    virtual bool onHandleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp);
    virtual bool onHandleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp);
    virtual bool onHandleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp);
    virtual bool onHandleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp);
    virtual bool onHandleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp);
    virtual bool onHandleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp);
    virtual bool onHandleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp);
    virtual bool onHandleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp);
    virtual bool onHandleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp);
    virtual bool onHandleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp);
    virtual bool onGetNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp);
    virtual bool onGetValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp);
    virtual bool onUnlockUser(IEspContext &context, IEspUnlockUserRequest &req, IEspUnlockUserResponse &resp);
    virtual bool onClientAlive(IEspContext &context, IEspClientAliveRequest &req, IEspClientAliveResponse &resp);
    virtual bool onGetEnvironment(IEspContext &context, IEspGetEnvironmentRequest &req, IEspGetEnvironmentResponse &resp);
    virtual bool onSetEnvironment(IEspContext &context, IEspSetEnvironmentRequest &req, IEspSetEnvironmentResponse &resp);
    virtual bool onLockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp);
    virtual bool onUnlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp);
    virtual bool onBuildEnvironment(IEspContext &context, IEspBuildEnvironmentRequest &req, IEspBuildEnvironmentResponse &resp);
    virtual bool onGetSubnetIPAddr(IEspContext &context, IEspGetSubnetIPAddrRequest &req, IEspGetSubnetIPAddrResponse &resp);
    virtual bool onSaveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp);
    virtual bool onRollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp);
    virtual bool onNotifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp);
    virtual bool onGetSummary(IEspContext &context, IEspGetSummaryRequest &req, IEspGetSummaryResponse &resp);
    virtual bool onAddReqdComps(IEspContext &context, IEspAddReqdCompsRequest &req, IEspAddReqdCompsResponse &resp);

    void getNavigationData(IEspContext &context, IPropertyTree* pData);
    CWsDeployFileInfo* getFileInfo(const char* fileName, bool addIfNotFound=false, bool createFile = false);
    IPropertyTree* getCfg() { return m_pCfg;}
    const char* getName() { return m_service.str();}
    void getLastStarted(StringBuffer& sb);
    const char* getBackupDir() { return m_backupDir.str(); }
    const char* getProcessName() { return m_process.str(); }
    const char* getSourceDir() { return m_sourceDir.str(); }
    CConfigHelper m_configHelper;

private:
  virtual void getWizOptions(StringBuffer& sb);

protected:
    Mutex                     m_mutexSrv;
    StringBuffer              m_envFile;
    StringBuffer              m_backupDir;
    StringBuffer              m_sourceDir;
    StringBuffer              m_process;
    StringBuffer              m_service;
    typedef MapStringTo<StringBuffer, StringBuffer&> CompHTMLMap;
    CompHTMLMap               m_compHtmlMap;
    bool                      m_bCloud;
    Owned<IPropertyTree>      m_pCfg;
    CDateTime                 m_lastStarted;
    MapStringToMyClass<CWsDeployFileInfo> m_fileInfos;
};

class CWsDeployEx : public CWsDeployExCE
{
public:
    IMPLEMENT_IINTERFACE;

    virtual ~CWsDeployEx(){}
    virtual bool onDeploy(IEspContext &context, IEspDeployRequest &req, IEspDeployResponse &resp);
    virtual bool onGraph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp);
    virtual bool onNavMenuEvent(IEspContext &context, IEspNavMenuEventRequest &req, 
                                                    IEspNavMenuEventResponse &resp);
    virtual bool onDisplaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp);
    virtual bool onSaveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp);
    virtual bool onGetBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp);
    virtual bool onGetDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp);
    virtual bool onStartDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp);
    virtual bool onGetBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp);
    virtual bool onImportBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp);
    virtual bool onGetComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp);
    virtual bool onHandleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp);
    virtual bool onHandleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp);
    virtual bool onHandleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp);
    virtual bool onHandleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp);
    virtual bool onHandleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp);
    virtual bool onHandleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp);
    virtual bool onHandleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp);
    virtual bool onHandleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp);
    virtual bool onHandleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp);
    virtual bool onHandleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp);
    virtual bool onGetNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp);
    virtual bool onGetValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp);
    virtual bool onLockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp);
    virtual bool onUnlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp);
    virtual bool onSaveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp);
    virtual bool onRollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp);
    virtual bool onNotifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp);

private:
  virtual void getWizOptions(StringBuffer& sb);
};
#endif //_ESPWIZ_WsDeploy_HPP__

