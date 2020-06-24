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
#include "ws_dfu_esp.ipp"
#include "ws_dfuHelpers.hpp"

class CXRefExBuilderThread : public Thread
{
    bool stopThread = false;
    bool xRefRunning = false;
    CriticalSection critRunningStatus;
    CriticalSection critQueue;
    Semaphore m_sem;
    SafeQueueOf<IXRefNode, false> nodeQueue;
    StringBuffer currentClusterName;

    void setRunningStatus(bool status)
    {
        CriticalBlock b(critRunningStatus);
        xRefRunning = status;
    }
    void clearCurrentClusterName()
    {
        CriticalBlock b(critQueue);
        currentClusterName.clear();
    }
    IXRefNode* readNodeQueue()
    {
        CriticalBlock b(critQueue);
        Owned<IXRefNode> xRefNode = (IXRefNode*)nodeQueue.dequeue();
        if (!xRefNode)
            return nullptr;

        xRefNode->getCluster(currentClusterName);
        return xRefNode.getClear();
    }
    void writeNodeQueue(IXRefNode* xRefNode)
    {
        if (!xRefNode)
            return;

        CriticalBlock b(critQueue);
        nodeQueue.enqueue(LINK(xRefNode));
    }
public:
    IMPLEMENT_IINTERFACE;

    CXRefExBuilderThread() { };
    ~CXRefExBuilderThread(){DBGLOG("Destroyed XRef thread");};

    virtual void queueRequest(IXRefNode* xRefNode, const char* cluster)
    {
        if (!xRefNode || isEmptyString(cluster))
            return;

        xRefNode->setCluster(cluster);
        writeNodeQueue(xRefNode);
        m_sem.signal();
    }

    virtual int run()
    {
        Link();
        while (!stopThread)
        {
            m_sem.wait();
            runXRef();
        }
        Release();
        return 0;
    }

    void runXRef()
    {
        //catch all exceptions so we can signal for the new build to start
        try
        {
            while (true)
            {
                Owned<IXRefNode> xRefNode  = readNodeQueue();
                if (!xRefNode)
                    break;

                if (xRefNode->useSasha()) // if sasha processing just set submitted
                    xRefNode->setStatus("Submitted");
                else
                {
                    setRunningStatus(true);
                    Owned<IPropertyTree> tree = runXRefCluster(currentClusterName.str(), xRefNode);
                    DBGLOG("finished run XRef for %s", currentClusterName.str());
                    clearCurrentClusterName();
                    setRunningStatus(false);
                }
            }
        }
        catch(IException* e)
        {
            StringBuffer errorStr;
            e->errorMessage(errorStr);
            IERRLOG("Exception thrown while running XREF: %s", errorStr.str());
            e->Release();
        }
        catch(...)
        {
            IERRLOG("Unknown Exception thrown from XREF");
        }
    }

    virtual bool isRunning()
    {
        CriticalBlock b(critRunningStatus);
        return xRefRunning;
    }
    virtual bool isQueued(const char* clusterName)
    {
        if (isEmptyString(clusterName))
            return false;

        CriticalBlock b(critQueue);
        if (!currentClusterName.isEmpty() && streq(currentClusterName, clusterName))
            return true;

        ForEachItemIn(x, nodeQueue)
        {
            IXRefNode* Item = nodeQueue.item(x);
            StringBuffer cachedCluster;
            Item->getCluster(cachedCluster);
            if (streq(cachedCluster, clusterName))
                return true;
        }
        return false;
    }
    virtual void cancel()
    {
        CriticalBlock b(critQueue);
        while (nodeQueue.ordinality() > 0)
        {
            Owned<IXRefNode> xRefNode  = (IXRefNode*)nodeQueue.dequeue();
        }
        m_sem.signal();
    }
    virtual void stop()
    {
        stopThread = true;
        m_sem.signal();
        join();
    }
};

class CWsDFUXRefSoapBindingEx : public CWsDFUXRefSoapBinding
{
public:
    CWsDFUXRefSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsDFUXRefSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
    }
};


class CWsDfuXRefEx : public CWsDFUXRef
{
    Owned<IXRefNodeManager> XRefNodeManager;
    Owned<CXRefExBuilderThread> m_XRefbuilder;

    IXRefFilesNode* getFileNodeInterface(IXRefNode& XRefNode,const char* nodeType);
    void addXRefNode(const char* name, IPropertyTree* pXRefNodeTree);
    void readLostFileQueryResult(IEspContext &context, StringBuffer& buf);
    bool addUniqueXRefNode(const char* processName, BoolHash& uniqueProcesses, IPropertyTree* pXRefNodeTree);
    IXRefNode* getXRefNodeByCluster(const char* cluster);
    IUserDescriptor* getUserDescriptor(IEspContext& context);
    void updateSkew(IPropertyTree &node);
    IDFAttributesIterator* getAllLogicalFilesInCluster(IEspContext &context, const char *cluster, bool &allMatchingFilesReceived);
    void findUnusedFilesWithDetailsInDFS(IEspContext &context, const char *process, const MapStringTo<bool> &usedFileMap, IArrayOf<IEspDFULogicalFile> &unusedFiles);
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

