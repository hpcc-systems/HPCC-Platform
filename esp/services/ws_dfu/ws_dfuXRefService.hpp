/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _ESPWIZ_WsDfuXRef_HPP__
#define _ESPWIZ_WsDfuXRef_HPP__

#include "ws_dfuXref_esp.ipp"
#include "XRefNodeManager.hpp"
#include "TpWrapper.hpp"
#include "dfuxreflib.hpp"
#include "jqueue.tpp"

class CXRefExBuilderThread : public Thread
{
    Owned<IXRefNode> m_xRefNode;
    bool bRunning;
    Mutex _boolMutex;
    Semaphore m_sem;
    SafeQueueOf<IXRefNode, false> m_pNodeQueue;
    bool m_bRun;
    StringBuffer _CurrentClusterName;
    virtual void SetRunningStatus(bool bStatus)
    {
        CriticalSection(_boolMutex);
        bRunning = bStatus;
    }
public:
   IMPLEMENT_IINTERFACE;
   CXRefExBuilderThread() 
   {
       m_bRun = true;
       bRunning = false; 
   }

   virtual void QueueRequest(IXRefNode* xRefNode,const char* cluster)
   {
       if(xRefNode == 0 || cluster == 0)
           return;

       CriticalSection(_RunningMutex);
       
       xRefNode->setCluster(cluster);
       xRefNode->Link();
       m_pNodeQueue.enqueue(xRefNode);
       m_sem.signal();
   }

   ~CXRefExBuilderThread(){DBGLOG("Destroyed XRef thread");}
    virtual int run()
    {
        Link();
        while(m_bRun)
        {
            m_sem.wait();
            if (m_pNodeQueue.ordinality() != 0)
                RunXRef();
        }
        Release();
        return 0;
    }

    void RunXRef()
    {

        //catch all exceptions so we can signal for the new build to start
        try{
            SetRunningStatus(true);
            Owned<IXRefNode> xRefNode  = (IXRefNode*)m_pNodeQueue.dequeue();
            _CurrentClusterName.clear();
            xRefNode->getCluster(_CurrentClusterName);
            if (xRefNode->useSasha()) // if sasha processing just set submitted
                xRefNode->setStatus("Submitted");
            else {
                Owned<IPropertyTree> tree = runXRefCluster(_CurrentClusterName.str(), xRefNode);
                DBGLOG("finished run");
            }
            SetRunningStatus(false);
        }
        catch(IException* e)
        {
            StringBuffer errorStr;
            e->errorMessage(errorStr);
            IERRLOG("Exception thrown while running XREF: %s",errorStr.str());
            e->Release();
        }
        catch(...)
        {
            IERRLOG("Unknown Exception thrown from XREF");
        }
        //Signal that we are ready to process another job if there is one....
        m_sem.signal();

    }

    virtual bool IsRunning()
    {
        CriticalSection(_boolMutex);
        return bRunning;
    }
    virtual bool IsQueued(const char* Queue)
    {
        if(Queue == 0)
            return false;
        ForEachItemIn(x,m_pNodeQueue)
        {
            IXRefNode* Item = m_pNodeQueue.item(x);
            StringBuffer cachedCluster;
            Item->getCluster(cachedCluster);
            if(strcmp(cachedCluster.str(),Queue) == 0 || strcmp(_CurrentClusterName.str(),Queue) == 0)
                return true;
        }
        return false;
    }
    virtual void Cancel()
    {
        while(m_pNodeQueue.ordinality() >0)
        {
            Owned<IXRefNode> xRefNode  = (IXRefNode*)m_pNodeQueue.dequeue();
        }
        m_sem.signal();
    }
    virtual void Shutdown()
    {
        m_bRun = false;
    }
};

class CWsDFUXRefSoapBindingEx : public CWsDFUXRefSoapBinding
{
public:
    CWsDFUXRefSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsDFUXRefSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
    }
};


class CWsDfuXRefEx : public CWsDFUXRef
{
private:
    StringBuffer  user_;
    StringBuffer  password_;
    Owned<IXRefNodeManager> XRefNodeManager;
    Owned<CXRefExBuilderThread> m_XRefbuilder;

private:
    IXRefFilesNode* getFileNodeInterface(IXRefNode& XRefNode,const char* nodeType);
    void addXRefNode(const char* name, IPropertyTree* pXRefNodeTree);
    void readLostFileQueryResult(IEspContext &context, StringBuffer& buf);
    bool addUniqueXRefNode(const char* processName, BoolHash& uniqueProcesses, IPropertyTree* pXRefNodeTree);
public:
   IMPLEMENT_IINTERFACE;

    CWsDfuXRefEx(){}
    virtual ~CWsDfuXRefEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onDFUXRefList(IEspContext &context, IEspDFUXRefListRequest &req, IEspDFUXRefListResponse &resp);

    bool onDFUXRefBuild(IEspContext &context, IEspDFUXRefBuildRequest &req, IEspDFUXRefBuildResponse &resp);
    bool onDFUXRefLostFiles(IEspContext &context, IEspDFUXRefLostFilesQueryRequest &req, IEspDFUXRefLostFilesQueryResponse &resp);
    bool onDFUXRefFoundFiles(IEspContext &context, IEspDFUXRefFoundFilesQueryRequest &req, IEspDFUXRefFoundFilesQueryResponse &resp);
    bool onDFUXRefOrphanFiles(IEspContext &context, IEspDFUXRefOrphanFilesQueryRequest &req, IEspDFUXRefOrphanFilesQueryResponse &resp);
    bool onDFUXRefMessages(IEspContext &context, IEspDFUXRefMessagesQueryRequest &req, IEspDFUXRefMessagesQueryResponse &resp);
    bool onDFUXRefArrayAction(IEspContext &context, IEspDFUXRefArrayActionRequest &req, IEspDFUXRefArrayActionResponse &resp);
    bool onDFUXRefBuildCancel(IEspContext &context, IEspDFUXRefBuildCancelRequest &req, IEspDFUXRefBuildCancelResponse &resp);
    bool onDFUXRefDirectories(IEspContext &context, IEspDFUXRefDirectoriesQueryRequest &req, IEspDFUXRefDirectoriesQueryResponse &resp);
    bool onDFUXRefCleanDirectories(IEspContext &context, IEspDFUXRefCleanDirectoriesRequest &req, IEspDFUXRefCleanDirectoriesResponse &resp);
    bool onDFUXRefUnusedFiles(IEspContext &context, IEspDFUXRefUnusedFilesRequest &req, IEspDFUXRefUnusedFilesResponse &resp);
};





#endif //_ESPWIZ_WsDfu_HPP__

