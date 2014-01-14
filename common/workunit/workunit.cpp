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

#include "jlib.hpp"
#include "workunit.hpp"
#include "jprop.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jptree.hpp"
#include "jtime.ipp"
#include "jencrypt.hpp"
#include "junicode.hpp"
#include "eclrtl.hpp"
#include "deftype.hpp"
#include <time.h>

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dautils.hpp"
#include "dllserver.hpp"
#include "thorhelper.hpp"
#include "workflow.hpp"

#include "nbcd.hpp"
#include "seclib.hpp"

#include "wuerror.hpp"
#include "wujobq.hpp"

#define GLOBAL_WORKUNIT "global"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

static int workUnitTraceLevel = 1;

static StringBuffer &getXPath(StringBuffer &wuRoot, const char *wuid)
{
    // MORE - can fold in the date
    return wuRoot.append("/WorkUnits/").append(wuid);
}

//To be called by eclserver, but esp etc. won't know, so we need to store it.
static StringBuffer & appendLibrarySuffix(StringBuffer & suffix)
{
#ifdef _WIN32
    suffix.append("W");
#else
    suffix.append("L");
#endif
#ifdef __64BIT__
    suffix.append("64");
#else
    suffix.append("32");
 #endif
    return suffix;
}



typedef MapStringTo<bool> UniqueScopes;

static void wuAccessError(const char *username, const char *action, const char *wuscope, const char *wuid, bool excpt, bool log)
{
    StringBuffer err;
    err.append("Workunit Access Denied - action: ").append(action).append(" user:").append(username ? username : "<Unknown>");
    if (wuid)
        err.append(" workunit:").append(wuid);
    if (wuscope)
        err.append(" scope:").append(wuscope);
    //MORE - we would need more information passed in from outside if we want to make the audit message format the same as from higher level ESP calls
    SYSLOG(AUDIT_TYPE_ACCESS_FAILURE, err.str());
    if (log)
        LOG(MCuserError, "%s", err.str());
    if (excpt)
        throw MakeStringException(WUERR_AccessError, "%s", err.str());
}
static bool checkWuScopeSecAccess(const char *wuscope, ISecManager &secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    bool ret=(!secuser) ? true : (secmgr.authorizeEx(RT_WORKUNIT_SCOPE, *secuser, wuscope)>=required);
    if (!ret && (log || excpt))
        wuAccessError(secuser ? secuser->getName() : NULL, action, wuscope, NULL, excpt, log);
    return ret;
}
static bool checkWuScopeListSecAccess(const char *wuscope, ISecResourceList *scopes, int required, const char *action, bool excpt, bool log)
{
    if (!scopes)
        return true;
    bool ret=true;
    if (wuscope)
    {
        Owned<ISecResource> res=scopes->getResource(wuscope);
        if (!res || res->getAccessFlags()<required)
            ret=false;
    }
    else
    {
        for (int seq=0; ret && seq<scopes->count(); seq++)
        {
            ISecResource *res=scopes->queryResource(seq);
            if (res && res->getAccessFlags()<required)
                return false;
        }
    }
    if (!ret && (log || excpt))
        wuAccessError(NULL, action, wuscope, NULL, excpt, log);
    return ret;
}
static bool checkWuSecAccess(IConstWorkUnit &cw, ISecManager &secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    SCMStringBuffer wuscope;
    bool ret=(!secuser) ? true : (secmgr.authorizeEx(RT_WORKUNIT_SCOPE, *secuser, cw.getWuScope(wuscope).str())>=required);
    if (!ret && (log || excpt))
    {
        SCMStringBuffer wuid;
        wuAccessError(secuser ? secuser->getName() : NULL, action, wuscope.str(), cw.getWuid(wuid).str(), excpt, log);
    }
    return ret;
}
static bool checkWuSecAccess(const char *wuid, ISecManager &secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    StringBuffer wuRoot;
    Owned<IRemoteConnection> conn = querySDS().connect(getXPath(wuRoot, wuid).str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        Owned<IPropertyTree> ptree=conn->getRoot();
        return checkWuScopeSecAccess(ptree->queryProp("@scope"), secmgr, secuser, required, action, excpt, log);
    }

    if (log || excpt)
        wuAccessError(secuser ? secuser->getName() : NULL, action, "Unknown", NULL, excpt, log);
    return false;
}

void doDescheduleWorkkunit(char const * wuid)
{
    StringBuffer xpath;
    xpath.append("*/*/*/");
    ncnameEscape(wuid, xpath);
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if(!conn) return;
    Owned<IPropertyTree> root = conn->getRoot();
    bool more;
    do more = root->removeProp(xpath.str()); while(more);
}

#define PROGRESS_FORMAT_V 2



class CConstGraphProgress : public CInterface, implements IConstWUGraphProgress
{
    class CGraphProgress : public CInterface, implements IWUGraphProgress
    {
        CConstGraphProgress &parent;
    public:
        IMPLEMENT_IINTERFACE;
        CGraphProgress(CConstGraphProgress &_parent) : parent(_parent)
        {
            parent.lockWrite();
        }
        ~CGraphProgress()
        {
            parent.unlock();
        }
        virtual IPropertyTree * queryProgressTree() { return parent.queryProgressTree(); }
        virtual WUGraphState queryGraphState() { return parent.queryGraphState(); }
        virtual WUGraphState queryNodeState(WUGraphIDType nodeId) { return parent.queryNodeState(nodeId); }
        virtual IWUGraphProgress * update() { return parent.update(); }
        virtual unsigned queryFormatVersion() { return parent.queryFormatVersion(); }
        virtual IPropertyTree & updateEdge(WUGraphIDType nodeId, const char *edgeId)
        {
            return parent.updateEdge(nodeId, edgeId);
        }
        virtual IPropertyTree & updateNode(WUGraphIDType nodeId, WUNodeIDType id)
        {
            return parent.updateNode(nodeId, id);
        }
        virtual void setGraphState(WUGraphState state)
        {
            parent.setGraphState(state);
        }
        virtual void setNodeState(WUGraphIDType nodeId, WUGraphState state)
        {
            parent.setNodeState(nodeId, state);
        }
    };
    IPropertyTree &updateElement(WUGraphIDType nodeId, const char *elemName, const char *id)
    {
        IPropertyTree *elem = NULL;
        if (!connectedWrite) lockWrite();
        StringBuffer path;
        path.append("node[@id=\"").append(nodeId).append("\"]");
        IPropertyTree *node = progress->queryPropTree(path.str());
        if (!node)
        {
            node = progress->addPropTree("node", createPTree());
            node->setPropInt("@id", (int)nodeId);
            elem = node->addPropTree(elemName, createPTree());
            elem->setProp("@id", id);
        }
        else
        {
            path.clear().append(elemName).append("[@id=\"").append(id).append("\"]");
            elem = node->queryPropTree(path.str());
            if (!elem)
            {
                elem = node->addPropTree(elemName, createPTree());
                elem->setProp("@id", id);
            }
        }
        return *elem;
    }
public:
    IMPLEMENT_IINTERFACE;
    static void deleteWuidProgress(const char *wuid)
    {
        StringBuffer path("/GraphProgress/");
        path.append(wuid);
        Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        if (conn)
            conn->close(true);
    }
    CConstGraphProgress(const char *_wuid, const char *_graphName) : wuid(_wuid), graphName(_graphName)
    {
        rootPath.append("/GraphProgress/").append(wuid).append('/').append(graphName).append('/');
        connected = connectedWrite = false;
        formatVersion = 0;
        progress = NULL;
    }
    void connect()
    {
        conn.clear();
        packProgress(wuid,false);
        conn.setown(querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_READ|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT));

        progress = conn->queryRoot();
        formatVersion = progress->getPropInt("@format");
        connected = true;
    }
    void lockWrite()
    {
        if (connectedWrite) return;
        // JCSMORE - look at using changeMode here.
        if (conn)
            conn.clear();
        else
            packProgress(wuid,false);
        conn.setown(querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT));
        progress = conn->queryRoot();
        if (!progress->hasChildren()) // i.e. blank.
        {
            formatVersion = PROGRESS_FORMAT_V;
            progress->setPropInt("@format", PROGRESS_FORMAT_V);
        }
        else
            formatVersion = progress->getPropInt("@format");
        connected = connectedWrite = true;
    }
    void unlock()
    {
        connected = false;
        connectedWrite = false;
        conn.clear();
    }
    IPropertyTree &updateNode(WUGraphIDType nodeId, WUNodeIDType id)
    {
        StringBuffer s;
        return updateElement(nodeId, "node", s.append(id).str());
    }
    IPropertyTree &updateEdge(WUGraphIDType nodeId, const char *edgeId)
    {
        return updateElement(nodeId, "edge", edgeId);
    }
    static bool getRunningGraph(const char *wuid, IStringVal &graphName, WUGraphIDType &subId)
    {
        StringBuffer path;
        Owned<IRemoteConnection> conn = querySDS().connect(path.append("/GraphProgress/").append(wuid).str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        if (!conn) return false;

        const char *name = conn->queryRoot()->queryProp("Running/@graph");
        if (name)
        {
            graphName.set(name);
            subId = conn->queryRoot()->getPropInt64("Running/@subId");
            return true;
        }
        else
            return false;
    }
    void setGraphState(WUGraphState state)
    {
        progress->setPropInt("@_state", (unsigned)state);
    }
    void setNodeState(WUGraphIDType nodeId, WUGraphState state)
    {
        if (!connectedWrite) lockWrite();
        StringBuffer path;
        path.append("node[@id=\"").append(nodeId).append("\"]");
        IPropertyTree *node = progress->queryPropTree(path.str());
        if (!node)
        {
            node = progress->addPropTree("node", createPTree());
            node->setPropInt("@id", (int)nodeId);
        }
        node->setPropInt("@_state", (unsigned)state);
        
        switch (state)
        {
            case WUGraphRunning:
            {
                StringBuffer path;
                Owned<IRemoteConnection> conn = querySDS().connect(path.append("/GraphProgress/").append(wuid).str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
                IPropertyTree *running = conn->queryRoot()->setPropTree("Running", createPTree());
                running->setProp("@graph", graphName);
                running->setPropInt64("@subId", nodeId);
                break;
            }
            case WUGraphComplete:
            {
                StringBuffer path;
                Owned<IRemoteConnection> conn = querySDS().connect(path.append("/GraphProgress/").append(wuid).str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
                conn->queryRoot()->removeProp("Running"); // only one thing running at any given time and one thing with lockWrite access
                break;
            }
        }
    }
    virtual IPropertyTree * queryProgressTree()
    {
        if (!connected) connect();
        return progress;
    }
    virtual WUGraphState queryGraphState()
    {
        return (WUGraphState)queryProgressTree()->getPropInt("@_state", (unsigned)WUGraphUnknown);
    }
    virtual WUGraphState queryNodeState(WUGraphIDType nodeId)
    {
        StringBuffer path;
        path.append("node[@id=\"").append(nodeId).append("\"]/@_state");
        return (WUGraphState)queryProgressTree()->getPropInt(path.str(), (unsigned)WUGraphUnknown);
    }
    virtual IWUGraphProgress * update()
    {
        return new CGraphProgress(*this);
    }
    virtual unsigned queryFormatVersion()
    {
        if (!connected) connect();
        return formatVersion;
    }

    static bool packProgress(const char *wuid,bool pack)
    {
        StringBuffer path;
        path.append("/GraphProgress/").append(wuid);
        Owned<IRemoteConnection> conn(querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT));
        if (!conn) 
            return false;
        Owned<IPropertyTree> newt;
        MemoryBuffer buf;
        IPropertyTree *root = conn->queryRoot();
        if (root->getPropBin("Packed",buf)) {
            if (pack)
                return true;
            newt.setown(createPTree(buf));
            IPropertyTree *running = root->queryPropTree("Running");
            if (running)
                newt->setPropTree("Running",createPTreeFromIPT(running));
        }
        else {
            if (!pack)
                return true;
            newt.setown(createPTree(wuid));
            IPropertyTree *running = root->queryPropTree("Running");
            if (running) {
                newt->setPropTree("Running",createPTreeFromIPT(running));
                root->removeTree(running);
            }
            root->serialize(buf);
            newt->setPropBin("Packed",buf.length(),buf.bufferBase());
        }
        root->setPropTree(NULL,newt.getClear());
        return true;
    }


private:
    Owned<IRemoteConnection> conn;
    IPropertyTree* progress;
    StringAttr wuid, graphName;
    StringBuffer rootPath;
    bool connected, connectedWrite;
    unsigned formatVersion;
};

class CLocalWUTimeStamp : public CInterface, implements IConstWUTimeStamp
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUTimeStamp(IPropertyTree *p);

    virtual IStringVal & getApplication(IStringVal & str) const;
    virtual IStringVal & getEvent(IStringVal & str) const;
    virtual IStringVal & getDate(IStringVal & dt) const;
};

class CLocalWUAppValue : public CInterface, implements IConstWUAppValue
{
    Owned<IPropertyTree> p;
    StringBuffer prop;
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUAppValue(IPropertyTree *p,unsigned child);

    virtual IStringVal & getApplication(IStringVal & str) const;
    virtual IStringVal & getName(IStringVal & str) const;
    virtual IStringVal & getValue(IStringVal & dt) const;
};


class CLocalWUStatistic : public CInterface, implements IConstWUStatistic
{
    Owned<IPropertyTree> p;
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUStatistic(IPropertyTree *p);

    virtual IStringVal & getFullName(IStringVal & str) const;
    virtual IStringVal & getCreator(IStringVal & str) const;
    virtual IStringVal & getDescription(IStringVal & str) const;
    virtual IStringVal & getName(IStringVal & str) const;
    virtual IStringVal & getScope(IStringVal & str) const;
    virtual StatisticMeasure getKind() const;
    virtual unsigned __int64 getValue() const;
    virtual unsigned __int64 getCount() const;
    virtual unsigned __int64 getMax() const;
};


template <typename T, typename IT> struct CachedTags
{
    CachedTags(): cached(false) {}
    void load(IPropertyTree* p,const char* xpath)
    {
        if (!cached)
        {
            assertex(tags.length() == 0);
            Owned<IPropertyTreeIterator> r = p->getElements(xpath);
            for (r->first(); r->isValid(); r->next())
            {
                IPropertyTree *rp = &r->query();
                rp->Link();
                tags.append(*new T(rp));
            }
            cached = true;
        }
    }

    void append(IPropertyTree * p)
    {
        tags.append(*new T(p));
    }

    operator IArrayOf<IT>&() { return tags; }

    void kill()
    {
        cached = false;
        tags.kill();
    }

    bool cached;
    IArrayOf<IT> tags;
};

template <>  struct CachedTags<CLocalWUAppValue, IConstWUAppValue>
{
    CachedTags(): cached(false) {}
    void load(IPropertyTree* p,const char* xpath)
    {
        if (!cached)
        {
            assertex(tags.length() == 0);
            Owned<IPropertyTreeIterator> r = p->getElements(xpath);
            for (r->first(); r->isValid(); r->next())
            {
                IPropertyTree *rp = &r->query();
                Owned<IPropertyTreeIterator> v = rp->getElements("*");
                unsigned pos = 1;
                for (v->first(); v->isValid(); v->next())
                {
                    rp->Link();
                    tags.append(*new CLocalWUAppValue(rp,pos++));
                }
            }
            cached = true;
        }
    }

    operator IArrayOf<IConstWUAppValue>&() { return tags; }

    void kill()
    {
        cached = false;
        tags.kill();
    }

    bool cached;
    IArrayOf<IConstWUAppValue> tags;
};


class CLocalWorkUnit : public CInterface, implements IConstWorkUnit , implements ISDSSubscription, implements IExtendedWUInterface
{
    friend StringBuffer &exportWorkUnitToXML(const IConstWorkUnit *wu, StringBuffer &str, bool decodeGraphs);
    friend void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool decodeGraphs);

    // NOTE - order is important - we need to construct connection before p and (especially) destruct after p
    Owned<IRemoteConnection> connection;
    Owned<IPropertyTree> p;
    bool dirty;
    bool connectAtRoot;
    mutable bool abortDirty;
    mutable bool abortState;
    mutable CriticalSection crit;
    mutable Owned<IWUQuery> query;
    mutable Owned<IWUWebServicesInfo> webServicesInfo;
    mutable Owned<IWURoxieQueryInfo> roxieQueryInfo;
    mutable Owned<IWorkflowItemIterator> workflowIterator;
    mutable bool workflowIteratorCached;
    mutable bool resultsCached;
    mutable bool temporariesCached;
    mutable bool variablesCached;
    mutable bool exceptionsCached;
    mutable bool pluginsCached;
    mutable bool librariesCached;
    mutable bool activitiesCached;
    mutable bool webServicesInfoCached;
    mutable bool roxieQueryInfoCached;
    mutable IArrayOf<IWUActivity> activities;
    mutable IArrayOf<IWUPlugin> plugins;
    mutable IArrayOf<IWULibrary> libraries;
    mutable IArrayOf<IWUException> exceptions;
    mutable IArrayOf<IWUGraph> graphs;
    mutable IArrayOf<IWUResult> results;
    mutable IArrayOf<IWUResult> temporaries;
    mutable IArrayOf<IWUResult> variables;
    mutable CachedTags<CLocalWUTimeStamp,IConstWUTimeStamp> timestamps;
    mutable CachedTags<CLocalWUAppValue,IConstWUAppValue> appvalues;
    mutable CachedTags<CLocalWUStatistic,IConstWUStatistic> statistics;
    mutable Owned<IUserDescriptor> userDesc;
    Mutex locked;
    Owned<ISecManager> secMgr;
    Owned<ISecUser> secUser;
    mutable Owned<IPropertyTree> cachedGraphs;


public:
    IMPLEMENT_IINTERFACE;

    CLocalWorkUnit(IRemoteConnection *_conn, ISecManager *secmgr, ISecUser *secuser, const char *parentWuid = NULL);
    CLocalWorkUnit(IRemoteConnection *_conn, IPropertyTree* root, ISecManager *secmgr, ISecUser *secuser);
    ~CLocalWorkUnit();
    CLocalWorkUnit(const char *dummyWuid, const char *parentWuid, ISecManager *secmgr, ISecUser *secuser);
    IPropertyTree *getUnpackedTree() const;

    ISecManager *querySecMgr(){return secMgr.get();}
    ISecUser *querySecUser(){return secUser.get();}

    void setSecIfcs(ISecManager *mgr, ISecUser*usr){secMgr.set(mgr); secUser.set(usr);}
    
    virtual bool aborting() const;
    virtual void forceReload();
    virtual WUAction getAction() const;
    virtual IStringVal& getActionEx(IStringVal & str) const;
    virtual IStringVal & getApplicationValue(const char * application, const char * propname, IStringVal & str) const;
    virtual int getApplicationValueInt(const char * application, const char * propname, int defVal) const;
    virtual IConstWUAppValueIterator & getApplicationValues() const;
    virtual bool hasWorkflow() const;
    virtual unsigned queryEventScheduledCount() const;
    virtual IPropertyTree * queryWorkflowTree() const;
    virtual IConstWorkflowItemIterator * getWorkflowItems() const;
    virtual IWorkflowItemArray * getWorkflowClone() const;
    virtual IConstLocalFileUploadIterator * getLocalFileUploads() const;
    virtual bool requiresLocalFileUpload() const;
    virtual bool getIsQueryService() const;
    virtual IStringVal & getClusterName(IStringVal & str) const;
    virtual unsigned getCombineQueries() const;
    virtual WUCompareMode getCompareMode() const;
    virtual IStringVal & getCustomerId(IStringVal & str) const;
    virtual bool hasDebugValue(const char * propname) const;
    virtual IStringVal & getDebugValue(const char * propname, IStringVal & str) const;
    virtual IStringIterator & getDebugValues() const;
    virtual IStringIterator & getDebugValues(const char *prop) const;
    virtual int getDebugValueInt(const char * propname, int defVal) const;
    virtual __int64 getDebugValueInt64(const char * propname, __int64 defVal) const;
    virtual bool getDebugValueBool(const char * propname, bool defVal) const;
    virtual unsigned getExceptionCount() const;
    virtual IConstWUExceptionIterator & getExceptions() const;
    virtual IConstWUResult * getGlobalByName(const char * name) const;
    virtual unsigned getGraphCount() const;
    virtual unsigned getSourceFileCount() const;
    virtual unsigned getResultCount() const;
    virtual unsigned getVariableCount() const;
    virtual unsigned getTimerCount() const;
    virtual unsigned getApplicationValueCount() const;
    virtual IConstWUGraphIterator & getGraphs(WUGraphType type) const;
    virtual IConstWUGraph * getGraph(const char *name) const;
    virtual IConstWUGraphProgress * getGraphProgress(const char * name) const;
    virtual IStringVal & getJobName(IStringVal & str) const;
    virtual IStringVal & getParentWuid(IStringVal & str) const;
    virtual IConstWUPlugin * getPluginByName(const char * name) const;
    virtual IConstWUPluginIterator & getPlugins() const;
    virtual IConstWULibraryIterator & getLibraries() const;
    virtual WUPriorityClass getPriority() const;
    virtual int getPriorityLevel() const;
    virtual int getPriorityValue() const;
    virtual IConstWUQuery * getQuery() const;
    virtual bool getRescheduleFlag() const;
    virtual IConstWUResult * getResultByName(const char * name) const;
    virtual IConstWUResult * getResultBySequence(unsigned seq) const;
    virtual unsigned getResultLimit() const;
    virtual IConstWUResultIterator & getResults() const;
    virtual IConstWUActivityIterator& getActivities() const;
    virtual IConstWUActivity*   getActivity(__int64 id) const;
    virtual IStringVal & getScope(IStringVal & str) const;
    virtual IStringVal & getSecurityToken(IStringVal & str) const;
    virtual WUState getState() const;
    virtual IStringVal & getStateEx(IStringVal & str) const;
    virtual __int64 getAgentSession() const;
    virtual unsigned getAgentPID() const;
    virtual IStringVal & getStateDesc(IStringVal & str) const;
    virtual IConstWUResult * getTemporaryByName(const char * name) const;
    virtual IConstWUResultIterator & getTemporaries() const;
    virtual unsigned getTimerCount(const char * timerName) const;
    virtual unsigned getTimerDuration(const char * timerName) const;
    virtual IStringVal & getTimerDescription(const char * timerName, IStringVal & str) const;
    virtual IStringIterator & getTimers() const;
    virtual IConstWUTimeStampIterator & getTimeStamps() const;
    virtual IConstWUStatisticIterator & getStatistics() const;
    virtual IConstWUStatistic * getStatistic(const char * name) const;
    virtual IConstWUWebServicesInfo * getWebServicesInfo() const;
    virtual IConstWURoxieQueryInfo * getRoxieQueryInfo() const;
    virtual IStringVal & getXmlParams(IStringVal & params) const;
    virtual const IPropertyTree *getXmlParams() const;
    virtual unsigned __int64 getHash() const;
    virtual IStringIterator *getLogs(const char *type, const char *component) const;
    virtual IStringIterator *getProcesses(const char *type) const;
    virtual IPropertyTreeIterator& getProcesses(const char *type, const char *instance) const;

    virtual bool getWuDate(unsigned & year, unsigned & month, unsigned& day);
    virtual IStringVal & getSnapshot(IStringVal & str) const;

    virtual IStringVal & getTimeStamp(const char * name, const char * instance, IStringVal & str) const;
    virtual IStringVal & getUser(IStringVal & str) const;
    virtual IStringVal & getWuScope(IStringVal & str) const;
    virtual IConstWUResult * getVariableByName(const char * name) const;
    virtual IConstWUResultIterator & getVariables() const;
    virtual IStringVal & getWuid(IStringVal & str) const;
    virtual bool getRunningGraph(IStringVal &graphName, WUGraphIDType &subId) const;
    virtual bool isProtected() const;
    virtual bool isPausing() const;
    virtual bool isBilled() const;
    virtual IWorkUnit& lock();
    virtual bool reload();
    virtual void requestAbort();
    virtual void subscribe(WUSubscribeOptions options);
    virtual unsigned calculateHash(unsigned prevHash);
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool all);
    virtual unsigned queryFileUsage(const char *filename) const;
    virtual bool getCloneable() const;
    virtual IUserDescriptor * queryUserDescriptor() const;
    virtual unsigned getCodeVersion() const;
    virtual unsigned getWuidVersion() const;
    virtual void getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const;
    virtual IPropertyTree * getDiskUsageStats();
    virtual IPropertyTreeIterator & getFileIterator() const;
    virtual bool archiveWorkUnit(const char *base,bool del,bool ignoredllerrors,bool deleteOwned);
    virtual void packWorkUnit(bool pack=true);
    virtual IJlibDateTime & getTimeScheduled(IJlibDateTime &val) const;
    virtual IPropertyTreeIterator & getFilesReadIterator() const;
    virtual void protect(bool protectMode);
    virtual IConstWULibrary * getLibraryByName(const char * name) const;
    virtual unsigned getDebugAgentListenerPort() const;
    virtual IStringVal & getDebugAgentListenerIP(IStringVal &ip) const;

    void clearExceptions();
    void commit();
    IWUException *createException();
    void setTimeStamp(const char *name, const char *instance, const char *event);
    void addTimeStamp(const char * name, const char * instance, const char *event);
    void addProcess(const char *type, const char *instance, unsigned pid, const char *log);
    void setAction(WUAction action);
    void setApplicationValue(const char * application, const char * propname, const char * value, bool overwrite);
    void setApplicationValueInt(const char * application, const char * propname, int value, bool overwrite);
    void incEventScheduledCount();
    void setIsQueryService(bool value);
    void setCloneable(bool value);
    void setIsClone(bool value);
    void setClusterName(const char * value);
    void setCodeVersion(unsigned version, const char * buildVersion, const char * eclVersion);
    void setCombineQueries(unsigned combine);
    void setCompareMode(WUCompareMode value);
    void setCustomerId(const char * value);
    void setDebugValue(const char * propname, const char * value, bool overwrite);
    void setDebugValueInt(const char * propname, int value, bool overwrite);
    void setJobName(const char * value);
    void setPriority(WUPriorityClass cls);
    void setPriorityLevel(int level);
    void setRescheduleFlag(bool value);
    void setResultLimit(unsigned value);
    void setState(WUState state);
    void setStateEx(const char * text);
    void setAgentSession(__int64 sessionId);
    void setSecurityToken(const char *value);
    void setStatistic(const char * creator, const char * wuScope, const char * stat, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge);
    void setTimerInfo(const char * name, unsigned ms, unsigned count, unsigned __int64 max);
    void setTracingValue(const char * propname, const char * value);
    void setTracingValueInt(const char * propname, int value);
    void setUser(const char * value);
    void setWuScope(const char * value);
    void setBilled(bool billed);
    void setSnapshot(const char * value);
    void setTimeStamp(const char *application, const char *instance, const char *event, bool add);
    void setDebugAgentListenerPort(unsigned port);
    void setDebugAgentListenerIP(const char * ip);
    void setXmlParams(const char *params);
    void setXmlParams(IPropertyTree *tree);
    void setHash(unsigned __int64 hash);

    IWorkflowItem* addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor);
    IWorkflowItemIterator * updateWorkflowItems();
    void syncRuntimeWorkflow(IWorkflowItemArray * array);
    void resetWorkflow();
    void schedule();
    void deschedule();
    unsigned addLocalFileUpload(LocalFileUploadType type, char const * source, char const * destination, char const * eventTag);
    IWUResult * updateGlobalByName(const char * name);
    IWUGraph * updateGraph(const char * name);
    IWUQuery * updateQuery();
    IWUWebServicesInfo* updateWebServicesInfo(bool create);
    IWURoxieQueryInfo* updateRoxieQueryInfo(const char *wuid, const char *roxieClusterName);
    IWUActivity* updateActivity(__int64 id);
    IWUPlugin * updatePluginByName(const char * name);
    IWULibrary * updateLibraryByName(const char * name);
    IWUResult * updateResultByName(const char * name);
    IWUResult * updateResultBySequence(unsigned seq);
    IWUResult * updateTemporaryByName(const char * name);
    IWUResult * updateVariableByName(const char * name);
    void addFile(const char *fileName, StringArray *clusters, unsigned usageCount, WUFileKind fileKind, const char *graphOwner);
    void noteFileRead(IDistributedFile *file);
    void releaseFile(const char *fileName);
    void clearGraphProgress();
    void resetBeforeGeneration();
    void deleteTempFiles(const char *graph, bool deleteOwned, bool deleteJobOwned);
    void deleteTemporaries();
    void addDiskUsageStats(__int64 avgNodeUsage, unsigned minNode, __int64 minNodeUsage, unsigned maxNode, __int64 maxNodeUsage, __int64 graphId);
    void setTimeScheduled(const IJlibDateTime &val);

// ILocalWorkUnit - used for debugging etc
    void loadXML(const char *xml);
    void serialize(MemoryBuffer &tgt);
    void deserialize(MemoryBuffer &src);

    IWorkUnit &lockRemote(bool commit);
    void unlockRemote(bool closing);
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL);
    void abort();
    void cleanupAndDelete(bool deldll,bool deleteOwned, const StringArray *deleteExclusions=NULL);
    bool switchThorQueue(const char *cluster, IQueueSwitcher *qs);
    void setAllowedClusters(const char *value);
    IStringVal & getAllowedClusters(IStringVal & str) const;
    void remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const;
    void setAllowAutoQueueSwitch(bool val);
    bool getAllowAutoQueueSwitch() const;
    void setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash);

protected:
    IConstWUStatistic * getStatisticByDescription(const char * name) const;

private:
    void init();
    IWUGraph *createGraph();
    IWUResult *createResult();
    void loadGraphs() const;
    void loadResults() const;
    void loadTemporaries() const;
    void loadVariables() const;
    void loadExceptions() const;
    void loadPlugins() const;
    void loadLibraries() const;
    void loadClusters() const;
    void loadActivities() const;
    void unsubscribe();
    void checkAgentRunning(WUState & state);

    // MORE - the two could be a bit more similar...

    class CWorkUnitWatcher : public CInterface, implements ISDSSubscription
    {
        ISDSSubscription *parent; // not linked - it links me
        SubscriptionId change;
        bool sub;
    public:
        IMPLEMENT_IINTERFACE;
        CWorkUnitWatcher(ISDSSubscription *_parent, const char *wuid, bool _sub) : parent(_parent), sub(_sub)
        {
            StringBuffer wuRoot;
            getXPath(wuRoot, wuid);
            change = querySDS().subscribe(wuRoot.str(), *this, sub);
        }
        ~CWorkUnitWatcher()
        {
            assertex(change==0);
        }
        bool watchingChildren()
        {
            return sub;
        }
        void unsubscribe()
        {
            querySDS().unsubscribe(change);
            change = 0;
        }

        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            parent->notify(id, xpath, flags, valueLen, valueData);
        }
    };

    class CWorkUnitAbortWatcher : public CInterface, implements ISDSSubscription
    {
        CLocalWorkUnit *parent; // not linked - it links me
        SubscriptionId abort;
    public:
        IMPLEMENT_IINTERFACE;
        CWorkUnitAbortWatcher(CLocalWorkUnit *_parent, const char *wuid) : parent(_parent)
        {
            StringBuffer wuRoot;
            wuRoot.append("/WorkUnitAborts/").append(wuid);
            abort = querySDS().subscribe(wuRoot.str(), *this);
        }
        ~CWorkUnitAbortWatcher()
        {
            assertex(abort==0);
        }

        void unsubscribe()
        {
            querySDS().unsubscribe(abort);
            abort = 0;
        }

        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            parent->abort();
        }
    };
    Owned<CWorkUnitAbortWatcher> abortWatcher;
    Owned<CWorkUnitWatcher> changeWatcher;

    void ensureGraphsUnpacked ()
    {
        IPropertyTree *t = p->queryPropTree("PackedGraphs");
        MemoryBuffer buf;
        if (t&&t->getPropBin(NULL,buf)) {
            cachedGraphs.clear();
            IPropertyTree *st = createPTree(buf);
            if (st) {
                p->setPropTree("Graphs",st);
                p->removeTree(t);
            }
        }
    }

};

class CLockedWorkUnit : public CInterface, implements ILocalWorkUnit, implements IExtendedWUInterface
{
public:
    Owned<CLocalWorkUnit> c;

    IMPLEMENT_IINTERFACE;
    CLockedWorkUnit(CLocalWorkUnit *_c) : c(_c) {}
    ~CLockedWorkUnit()
    {
        if (workUnitTraceLevel > 1)
        {
            StringAttr x;
            StringAttrAdaptor strval(x);
            getWuid(strval);
            PrintLog("Releasing locked workunit %s", x.get());
        }
        if (c)
            c->unlockRemote(c->IsShared());
    }

    void setSecIfcs(ISecManager *mgr, ISecUser*usr){c->setSecIfcs(mgr, usr);}

    virtual IConstWorkUnit * unlock()
    {
        c->unlockRemote(c->IsShared());
        return c.getClear();
    }
    virtual bool aborting() const
            { return c->aborting(); }
    virtual void forceReload()
            { UNIMPLEMENTED; }
    virtual WUAction getAction() const
            { return c->getAction(); }
    virtual IStringVal& getActionEx(IStringVal & str) const
            { return c->getActionEx(str); }
    virtual IStringVal & getApplicationValue(const char * application, const char * propname, IStringVal & str) const
            { return c->getApplicationValue(application, propname, str); }
    virtual int getApplicationValueInt(const char * application, const char * propname, int defVal) const
            { return c->getApplicationValueInt(application, propname, defVal); }
    virtual IConstWUAppValueIterator & getApplicationValues() const 
            { return c->getApplicationValues(); }
    virtual bool hasWorkflow() const
            { return c->hasWorkflow(); }
    virtual unsigned queryEventScheduledCount() const
            { return c->queryEventScheduledCount(); }
    virtual IPropertyTree * queryWorkflowTree() const
            { return c->queryWorkflowTree(); }
    virtual IConstWorkflowItemIterator * getWorkflowItems() const
            { return c->getWorkflowItems(); }
    virtual IWorkflowItemArray * getWorkflowClone() const
            { return c->getWorkflowClone(); }
    virtual bool requiresLocalFileUpload() const
            { return c->requiresLocalFileUpload(); }
    virtual IConstLocalFileUploadIterator * getLocalFileUploads() const
            { return c->getLocalFileUploads(); }
    virtual bool getIsQueryService() const
            { return c->getIsQueryService(); }
    virtual bool getCloneable() const
            { return c->getCloneable(); }
    virtual IUserDescriptor * queryUserDescriptor() const
            { return c->queryUserDescriptor(); }
    virtual IStringVal & getClusterName(IStringVal & str) const
            { return c->getClusterName(str); }
    virtual unsigned getCodeVersion() const
            { return c->getCodeVersion(); }
    virtual unsigned getWuidVersion() const
            { return c->getWuidVersion(); }
    virtual void getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const
            { c->getBuildVersion(buildVersion, eclVersion); }
    virtual unsigned getCombineQueries() const
            { return c->getCombineQueries(); }
    virtual WUCompareMode getCompareMode() const
            { return c->getCompareMode(); }
    virtual IStringVal & getCustomerId(IStringVal & str) const
            { return c->getCustomerId(str); }
    virtual bool hasDebugValue(const char * propname) const
            { return c->hasDebugValue(propname); }
    virtual IStringVal & getDebugValue(const char * propname, IStringVal & str) const
            { return c->getDebugValue(propname, str); }
    virtual int getDebugValueInt(const char * propname, int defVal) const
            { return c->getDebugValueInt(propname, defVal); }
    virtual __int64 getDebugValueInt64(const char * propname, __int64 defVal) const
            { return c->getDebugValueInt64(propname, defVal); }
    virtual bool getDebugValueBool(const char * propname, bool defVal) const
            { return c->getDebugValueBool(propname, defVal); }
    virtual IStringIterator & getDebugValues() const 
            { return c->getDebugValues(NULL); }
    virtual IStringIterator & getDebugValues(const char *prop) const 
            { return c->getDebugValues(prop); }
    virtual unsigned getExceptionCount() const
            { return c->getExceptionCount(); }
    virtual IConstWUExceptionIterator & getExceptions() const
            { return c->getExceptions(); }
    virtual unsigned getGraphCount() const
            { return c->getGraphCount(); }
    virtual unsigned getSourceFileCount() const
            { return c->getSourceFileCount(); }
    virtual unsigned getResultCount() const
            { return c->getResultCount(); }
    virtual unsigned getVariableCount() const
            { return c->getVariableCount(); }
    virtual unsigned getTimerCount() const
            { return c->getTimerCount(); }
    virtual unsigned getApplicationValueCount() const
            { return c->getApplicationValueCount(); }
    virtual IConstWUGraphIterator & getGraphs(WUGraphType type) const
            { return c->getGraphs(type); }
    virtual IConstWUGraph * getGraph(const char *name) const
            { return c->getGraph(name); }
    virtual IConstWUGraphProgress * getGraphProgress(const char * name) const
            { return c->getGraphProgress(name); }
    virtual IStringVal & getJobName(IStringVal & str) const
            { return c->getJobName(str); }
    virtual IStringVal & getParentWuid(IStringVal & str) const
            { return c->getParentWuid(str); }
    virtual IConstWUPlugin * getPluginByName(const char * name) const
            { return c->getPluginByName(name); }
    virtual IConstWUPluginIterator & getPlugins() const
            { return c->getPlugins(); }
    virtual IConstWULibrary* getLibraryByName(const char *name) const
            { return c->getLibraryByName(name); }
    virtual IConstWULibraryIterator & getLibraries() const
            { return c->getLibraries(); }
    virtual WUPriorityClass getPriority() const
            { return c->getPriority(); }
    virtual int getPriorityLevel() const
            { return c->getPriorityLevel(); }
    virtual int getPriorityValue() const
            { return c->getPriorityValue(); }
    virtual IConstWUQuery * getQuery() const
            { return c->getQuery(); }
    virtual IConstWUWebServicesInfo * getWebServicesInfo() const
            { return c->getWebServicesInfo(); }
    virtual IConstWURoxieQueryInfo* getRoxieQueryInfo() const
        { return c->getRoxieQueryInfo(); }
    virtual bool getRescheduleFlag() const
            { return c->getRescheduleFlag(); }
    virtual IConstWUResult * getResultByName(const char * name) const
            { return c->getResultByName(name); }
    virtual IConstWUResult * getResultBySequence(unsigned seq) const
            { return c->getResultBySequence(seq); }
    virtual unsigned getResultLimit() const
            { return c->getResultLimit(); }
    virtual IConstWUResultIterator & getResults() const
            { return c->getResults(); }
    virtual IConstWUActivityIterator & getActivities() const
            { return c->getActivities(); }
    virtual IConstWUActivity * getActivity(__int64 id) const
            { return c->getActivity(id); }
    virtual IStringVal & getScope(IStringVal & str) const
            { return c->getScope(str); }
    virtual IStringVal & getSecurityToken(IStringVal & str) const
            { return c->getSecurityToken(str); }
    virtual WUState getState() const
            { return c->getState(); }
    virtual IStringVal & getStateEx(IStringVal & str) const
            { return c->getStateEx(str); }
    virtual __int64 getAgentSession() const
            { return c->getAgentSession(); }
    virtual unsigned getAgentPID() const
            { return c->getAgentPID(); }
    virtual IStringVal & getStateDesc(IStringVal & str) const
            { return c->getStateDesc(str); }
    virtual bool getRunningGraph(IStringVal & graphName, WUGraphIDType & subId) const
            { return c->getRunningGraph(graphName, subId); }
    virtual unsigned getTimerCount(const char * timerName) const
            { return c->getTimerCount(timerName); }
    virtual unsigned getTimerDuration(const char * timerName) const
            { return c->getTimerDuration(timerName); }
    virtual IStringVal & getTimerDescription(const char * timerName, IStringVal & str) const
            { return c->getTimerDescription(timerName, str); }
    virtual IStringVal & getTimeStamp(const char * name, const char * instance, IStringVal & str) const
            { return c->getTimeStamp(name, instance, str); }
    virtual IStringIterator & getTimers() const
            { return c->getTimers(); }
    virtual IConstWUTimeStampIterator & getTimeStamps() const
            { return c->getTimeStamps(); }
    virtual IConstWUStatisticIterator & getStatistics() const
            { return c->getStatistics(); }
    virtual IConstWUStatistic * getStatistic(const char * name) const
            { return c->getStatistic(name); }

    virtual bool getWuDate(unsigned & year, unsigned & month, unsigned& day)
            { return c->getWuDate(year,month,day);}

    virtual IStringVal & getSnapshot(IStringVal & str) const
            { return c->getSnapshot(str); } 
    virtual IStringVal & getUser(IStringVal & str) const
            { return c->getUser(str); }
    virtual IStringVal & getWuScope(IStringVal & str) const
            { return c->getWuScope(str); }
    virtual IStringVal & getWuid(IStringVal & str) const
            { return c->getWuid(str); }
    virtual IConstWUResult * getGlobalByName(const char * name) const
            { return c->getGlobalByName(name); }
    virtual IConstWUResult * getTemporaryByName(const char * name) const
            { return c->getTemporaryByName(name); }
    virtual IConstWUResultIterator & getTemporaries() const
            { return c->getTemporaries(); }
    virtual IConstWUResult * getVariableByName(const char * name) const
            { return c->getVariableByName(name); }
    virtual IConstWUResultIterator & getVariables() const
            { return c->getVariables(); }
    virtual bool isProtected() const
            { return c->isProtected(); }
    virtual bool isPausing() const
            { return c->isPausing(); }
    virtual bool isBilled() const
            { return c->isBilled(); }
    virtual IWorkUnit & lock()
            { ((CInterface *)this)->Link(); return (IWorkUnit &) *this; }
    virtual bool reload()
            { UNIMPLEMENTED; }
    virtual void subscribe(WUSubscribeOptions options)
            { c->subscribe(options); }
    virtual void requestAbort()
            { c->requestAbort(); }
    virtual unsigned calculateHash(unsigned prevHash)
            { return c->calculateHash(prevHash); }
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool all)
            { c->copyWorkUnit(cached, all); }
    virtual bool archiveWorkUnit(const char *base,bool del,bool deldll,bool deleteOwned)
            { return c->archiveWorkUnit(base,del,deldll,deleteOwned); }
    virtual void packWorkUnit(bool pack)
            { c->packWorkUnit(pack); }
    virtual unsigned queryFileUsage(const char *filename) const
            { return c->queryFileUsage(filename); }
    virtual IJlibDateTime & getTimeScheduled(IJlibDateTime &val) const
            { return c->getTimeScheduled(val); }
    virtual unsigned getDebugAgentListenerPort() const
            { return c->getDebugAgentListenerPort(); }
    virtual IStringVal & getDebugAgentListenerIP(IStringVal &ip) const
            { return c->getDebugAgentListenerIP(ip); }
    virtual IStringVal & getXmlParams(IStringVal & params) const
            { return c->getXmlParams(params); }
    virtual const IPropertyTree *getXmlParams() const
            { return c->getXmlParams(); }
    virtual unsigned __int64 getHash() const
            { return c->getHash(); }
    virtual IStringIterator *getLogs(const char *type, const char *instance) const
            { return c->getLogs(type, instance); }
    virtual IStringIterator *getProcesses(const char *type) const
            { return c->getProcesses(type); }
    virtual IPropertyTreeIterator& getProcesses(const char *type, const char *instance) const
            { return c->getProcesses(type, instance); }

    virtual void clearExceptions()
            { c->clearExceptions(); }
    virtual void commit()
            { c->commit(); }
    virtual IWUException * createException()
            { return c->createException(); }
    virtual void setTimeStamp(const char * name, const char * instance, const char *event)
            { c->setTimeStamp(name, instance, event); }
    virtual void addTimeStamp(const char * name, const char * instance, const char *event)
            { c->addTimeStamp(name, instance, event); }
    virtual void addProcess(const char *type, const char *instance, unsigned pid, const char *log)
            { c->addProcess(type, instance, pid, log); }
    virtual void protect(bool protectMode)
            { c->protect(protectMode); }
    virtual void setBilled(bool billed)
            { c->setBilled(billed); }
    virtual void setAction(WUAction action)
            { c->setAction(action); }
    virtual void setApplicationValue(const char * application, const char * propname, const char * value, bool overwrite)
            { c->setApplicationValue(application, propname, value, overwrite); }
    virtual void setApplicationValueInt(const char * application, const char * propname, int value, bool overwrite)
            { c->setApplicationValueInt(application, propname, value, overwrite); }
    virtual void incEventScheduledCount()
            { c->incEventScheduledCount(); }
    virtual void setIsQueryService(bool value)
            { c->setIsQueryService(value); }
    virtual void setCloneable(bool value)
            { c->setCloneable(value); }
    virtual void setIsClone(bool value)
            { c->setIsClone(value); }
    virtual void setClusterName(const char * value)
            { c->setClusterName(value); }
    virtual void setCodeVersion(unsigned version, const char * buildVersion, const char * eclVersion)
            { c->setCodeVersion(version, buildVersion, eclVersion); }
    virtual void setCombineQueries(unsigned combine)
            { c->setCombineQueries(combine); }
    virtual void setCompareMode(WUCompareMode value)
            { c->setCompareMode(value); }
    virtual void setCustomerId(const char * value)
            { c->setCustomerId(value); }
    virtual void setDebugValue(const char * propname, const char * value, bool overwrite)
            { c->setDebugValue(propname, value, overwrite); }
    virtual void setDebugValueInt(const char * propname, int value, bool overwrite)
            { c->setDebugValueInt(propname, value, overwrite); }
    virtual void setJobName(const char * value)
            { c->setJobName(value); }
    virtual void setPriority(WUPriorityClass cls)
            { c->setPriority(cls); }
    virtual void setPriorityLevel(int level)
            { c->setPriorityLevel(level); }
    virtual void setRescheduleFlag(bool value)
            { c->setRescheduleFlag(value); }
    virtual void setResultLimit(unsigned value)
            { c->setResultLimit(value); }
    virtual void setSecurityToken(const char *value)
            { c->setSecurityToken(value); }
    virtual void setState(WUState state)
            { c->setState(state); }
    virtual void setStateEx(const char * text)
            { c->setStateEx(text); }
    virtual void setAgentSession(__int64 sessionId)
            { c->setAgentSession(sessionId); }
    virtual void setTimerInfo(const char * name, unsigned ms, unsigned count, unsigned __int64 max)
            { c->setTimerInfo(name, ms, count, max); }
    virtual void setStatistic(const char * creator, const char * wuScope, const char * stat, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge)
            { c->setStatistic(creator, wuScope, stat, description, kind, value, count, maxValue, merge); }

    virtual void setTracingValue(const char * propname, const char * value)
            { c->setTracingValue(propname, value); }
    virtual void setTracingValueInt(const char * propname, int value)
            { c->setTracingValueInt(propname, value); }
    virtual void setUser(const char * value)
            { c->setUser(value); }
    virtual void setWuScope(const char * value)
    {
        if (value && *value)
        {
            ISecManager *secmgr=c->querySecMgr();
            ISecUser *secusr=c->querySecUser();
            if (!secmgr || !secusr)
                throw MakeStringException(WUERR_SecurityNotAvailable, "Trying to change workunit scope without security interfaces available");
            if (checkWuScopeSecAccess(value, *secmgr, secusr, SecAccess_Write, "Change Scope", true, true))
                c->setWuScope(value);
        }
    }
    virtual IWorkflowItem* addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
            { return c->addWorkflowItem(wfid, type, mode, success, failure, recovery, retriesAllowed, contingencyFor); }
    virtual void syncRuntimeWorkflow(IWorkflowItemArray * array)
            { c->syncRuntimeWorkflow(array); }
    virtual IWorkflowItemIterator * updateWorkflowItems()
            { return c->updateWorkflowItems(); }
    virtual void resetWorkflow()
            { c->resetWorkflow(); }
    virtual void schedule()
            { c->schedule(); }
    virtual void deschedule()
            { c->deschedule(); }
    virtual unsigned addLocalFileUpload(LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
            { return c->addLocalFileUpload(type, source, destination, eventTag); }
    virtual IWUResult * updateGlobalByName(const char * name)
            { return c->updateGlobalByName(name); }
    virtual IWUGraph * updateGraph(const char * name) 
            { return c->updateGraph(name); }
    virtual IWUQuery * updateQuery()
            { return c->updateQuery(); }
    virtual IWUWebServicesInfo * updateWebServicesInfo(bool create)
        { return c->updateWebServicesInfo(create); }
    virtual IWURoxieQueryInfo * updateRoxieQueryInfo(const char *wuid, const char *roxieClusterName)
        { return c->updateRoxieQueryInfo(wuid, roxieClusterName); }
    virtual IWUActivity * updateActivity(__int64 id) 
            { return c->updateActivity(id); }
    virtual IWUPlugin * updatePluginByName(const char * name)
            { return c->updatePluginByName(name); }
    virtual IWULibrary * updateLibraryByName(const char * name)
            { return c->updateLibraryByName(name); }
    virtual IWUResult * updateResultByName(const char * name)
            { return c->updateResultByName(name); }
    virtual IWUResult * updateResultBySequence(unsigned seq)
            { return c->updateResultBySequence(seq); }
    virtual IWUResult * updateTemporaryByName(const char * name)
            { return c->updateTemporaryByName(name); }
    virtual IWUResult * updateVariableByName(const char * name)
            { return c->updateVariableByName(name); }
    virtual void addFile(const char *fileName, StringArray *clusters, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
            { c->addFile(fileName, clusters, usageCount, fileKind, graphOwner); }
    virtual void noteFileRead(IDistributedFile *file)
            { c->noteFileRead(file); }
    virtual void releaseFile(const char *fileName)
            { c->releaseFile(fileName); }
    virtual void clearGraphProgress()
            { c->clearGraphProgress(); }
    virtual void resetBeforeGeneration()
            { c->resetBeforeGeneration(); }
    virtual void deleteTempFiles(const char *graph, bool deleteOwned, bool deleteJobOwned)
            { c->deleteTempFiles(graph, deleteOwned, deleteJobOwned); }
    virtual void deleteTemporaries()
            { c->deleteTemporaries(); }
    virtual void addDiskUsageStats(__int64 avgNodeUsage, unsigned minNode, __int64 minNodeUsage, unsigned maxNode, __int64 maxNodeUsage, __int64 graphId)
            { c->addDiskUsageStats(avgNodeUsage, minNode, minNodeUsage, maxNode, maxNodeUsage, graphId); }
    virtual IPropertyTree * getDiskUsageStats()
            { return c->getDiskUsageStats(); }
    virtual IPropertyTreeIterator & getFileIterator() const
            { return c->getFileIterator(); }
    virtual IPropertyTreeIterator & getFilesReadIterator() const
            { return c->getFilesReadIterator(); }

    virtual void setSnapshot(const char * value)
            { c->setSnapshot(value); }
    virtual void setTimeScheduled(const IJlibDateTime &val)
            { c->setTimeScheduled(val); }
    virtual void setDebugAgentListenerPort(unsigned port)
            { c->setDebugAgentListenerPort(port); }
    virtual void setDebugAgentListenerIP(const char * ip)
            { c->setDebugAgentListenerIP(ip); }
    virtual void setXmlParams(const char *params)
            { c->setXmlParams(params); }
    virtual void setXmlParams(IPropertyTree *tree)
            { c->setXmlParams(tree); }
    virtual void setHash(unsigned __int64 hash)
            { c->setHash(hash); }

// ILocalWorkUnit - used for debugging etc
    virtual void loadXML(const char *xml)
            { c->loadXML(xml); }
    virtual void serialize(MemoryBuffer &tgt)
            { c->serialize(tgt); }
    virtual void deserialize(MemoryBuffer &src)
            { c->deserialize(src); }

    virtual bool switchThorQueue(const char *cluster, IQueueSwitcher *qs)
            { return c->switchThorQueue(cluster,qs); }
    virtual void setAllowedClusters(const char *value)
            { c->setAllowedClusters(value); }
    virtual IStringVal& getAllowedClusters(IStringVal &str) const
            { return c->getAllowedClusters(str); }
    virtual void remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const
            { c->remoteCheckAccess(user,writeaccess); }
    virtual void setAllowAutoQueueSwitch(bool val)
            { c->setAllowAutoQueueSwitch(val); }
    virtual bool getAllowAutoQueueSwitch() const
            { return c->getAllowAutoQueueSwitch(); }
    virtual void setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash)
            { c->setLibraryInformation(name, interfaceHash, definitionHash); }

    virtual void setResultInt(const char * name, unsigned sequence, __int64 val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultInt(val);   
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultUInt(const char * name, unsigned sequence, unsigned __int64 val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultUInt(val);  
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultReal(const char *name, unsigned sequence, double val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultReal(val);  
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultVarString(const char * stepname, unsigned sequence, const char *val)
    {
        setResultString(stepname, sequence, strlen(val), val);
    }
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val)
    {
        setResultUnicode(stepname, sequence, rtlUnicodeStrlen(val), val);
    }
    virtual void setResultString(const char * stepname, unsigned sequence, int len, const char *val)
    {
        doSetResultString(type_string, stepname, sequence, len, val);
    }
    virtual void setResultData(const char * stepname, unsigned sequence, int len, const void *val)
    {
        doSetResultString(type_data, stepname, sequence, len, (const char *)val);
    }
    virtual void setResultRaw(const char * name, unsigned sequence, int len, const void *val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultRaw(len, val, ResultFormatRaw); 
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultSet(const char * name, unsigned sequence, bool isAll, size32_t len, const void *val, ISetToXmlTransformer *)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultIsAll(isAll);
            r->setResultRaw(len, val, ResultFormatRaw); 
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultUnicode(const char * name, unsigned sequence, int len, UChar const * val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultUnicode((char const *)val, len);
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultBool(const char *name, unsigned sequence, bool val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultBool(val);
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultDecimal(val, len);
            r->setResultStatus(ResultStatusCalculated);
        }
    }
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            __int64 totalRows = numRows;
            if (extend)
            {
                totalRows += r->getResultRowCount();
                r->addResultRaw(len, val, ResultFormatRaw);
            }
            else
                r->setResultRaw(len, val, ResultFormatRaw);

            r->setResultStatus(ResultStatusCalculated);
            r->setResultRowCount(totalRows);
            r->setResultTotalRowCount(totalRows);
        }
    }

protected:
    IWUResult *updateResult(const char *name, unsigned sequence)
    {
        Owned <IWUResult> result = updateWorkUnitResult(this, name, sequence);
        if (result)
        {
            SCMStringBuffer rname;
            if (!result->getResultName(rname).length())
                result->setResultName(name);
        }
        return result.getClear();
    }

    void doSetResultString(type_t type, const char *name, unsigned sequence, int len, const char *val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultString(val, len);   
            r->setResultStatus(ResultStatusCalculated);
        }
    }

};

class CLocalWUAssociated : public CInterface, implements IConstWUAssociatedFile
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUAssociated(IPropertyTree *p);

    virtual WUFileType getType() const;
    virtual IStringVal & getDescription(IStringVal & ret) const;
    virtual IStringVal & getIp(IStringVal & ret) const;
    virtual IStringVal & getName(IStringVal & ret) const;
    virtual IStringVal & getNameTail(IStringVal & ret) const;
    virtual unsigned getCrc() const;
};

class CLocalWUQuery : public CInterface, implements IWUQuery
{
    Owned<IPropertyTree> p;
    mutable IArrayOf<IConstWUAssociatedFile> associated;
    mutable CriticalSection crit;
    mutable bool associatedCached;

private:
    void addSpecialCaseAssociated(WUFileType type, const char * propname, unsigned crc) const;
    void loadAssociated() const;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUQuery(IPropertyTree *p);

    virtual WUQueryType getQueryType() const;
    virtual IStringVal& getQueryText(IStringVal &str) const;
    virtual IStringVal& getQueryShortText(IStringVal &str) const;
    virtual IStringVal& getQueryName(IStringVal &str) const;
    virtual IStringVal & getQueryMainDefinition(IStringVal & str) const;
    virtual IStringVal& getQueryDllName(IStringVal &str) const;
    virtual unsigned getQueryDllCrc() const;
    virtual IStringVal& getQueryCppName(IStringVal &str) const;
    virtual IStringVal& getQueryResTxtName(IStringVal &str) const;
    virtual IConstWUAssociatedFile * getAssociatedFile(WUFileType type, unsigned index) const;
    virtual IConstWUAssociatedFileIterator& getAssociatedFiles() const;

    virtual void        setQueryType(WUQueryType qt);
    virtual void        setQueryText(const char *pstr);
    virtual void        setQueryName(const char *);
    virtual void        setQueryMainDefinition(const char * str);
    virtual void        addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc);
    virtual void        removeAssociatedFiles();
};

class CLocalWUWebServicesInfo : public CInterface, implements IWUWebServicesInfo
{
    Owned<IPropertyTree> p;
    mutable CriticalSection crit;

private:
    
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUWebServicesInfo(IPropertyTree *p);

    virtual IStringVal& getModuleName(IStringVal &str) const;
    virtual IStringVal& getAttributeName(IStringVal &str) const;
    virtual IStringVal& getDefaultName(IStringVal &str) const;
    virtual IStringVal& getInfo(const char *name, IStringVal &str) const;
    virtual unsigned getWebServicesCRC() const;
 
    virtual void        setModuleName(const char *);
    virtual void        setAttributeName(const char *);
    virtual void        setDefaultName(const char *);
    virtual void        setInfo(const char *name, const char *info);
    virtual void        setWebServicesCRC(unsigned);
};

class CLocalWURoxieQueryInfo : public CInterface, implements IWURoxieQueryInfo
{
    Owned<IPropertyTree> p;
    mutable CriticalSection crit;

private:
    
public:
    IMPLEMENT_IINTERFACE;
    CLocalWURoxieQueryInfo(IPropertyTree *p);

    virtual IStringVal& getQueryInfo(IStringVal &str) const;
    virtual IStringVal& getDefaultPackageInfo(IStringVal &str) const;
    virtual IStringVal& getRoxieClusterName(IStringVal &str) const;
    virtual IStringVal& getWuid(IStringVal &str) const;

    virtual void        setQueryInfo(const char *info); 
    virtual void        setDefaultPackageInfo(const char *, int len);
    virtual void        setRoxieClusterName(const char *str);
    virtual void        setWuid(const char *str);

};

class CLocalWUResult : public CInterface, implements IWUResult
{
    friend class CLocalWorkUnit;

    Owned<IPropertyTree> p;
    void getSchema(TypeInfoArray &types, StringAttrArray &names, IStringVal * ecl=NULL) const;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUResult(IPropertyTree *props);
    ~CLocalWUResult() { try { p.clear(); } catch (IException *E) {E->Release();}}

    virtual WUResultStatus getResultStatus() const;
    virtual IStringVal& getResultName(IStringVal &str) const;
    virtual int         getResultSequence() const;
    virtual bool        isResultScalar() const;
    virtual IStringVal& getResultXml(IStringVal &str) const;
    virtual unsigned    getResultFetchSize() const;
    virtual __int64     getResultTotalRowCount() const;
    virtual __int64     getResultRowCount() const;
    virtual void        getResultDataset(IStringVal & ecl, IStringVal & defs) const;
    virtual IStringVal& getResultLogicalName(IStringVal &ecl) const;
    virtual IStringVal& getResultKeyField(IStringVal& ecl) const;
    virtual unsigned    getResultRequestedRows() const;

    virtual __int64     getResultInt() const;
    virtual bool        getResultBool() const;
    virtual double      getResultReal() const;
    virtual IStringVal& getResultString(IStringVal & str) const;
    virtual IDataVal&   getResultRaw(IDataVal & data, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IDataVal&   getResultUnicode(IDataVal & data) const;
    virtual void        getResultDecimal(void * val, unsigned length, unsigned precision, bool isSigned) const;
    virtual IStringVal& getResultEclSchema(IStringVal & str) const;
    virtual __int64     getResultRawSize(IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IDataVal&   getResultRaw(IDataVal & data, __int64 from, __int64 length, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IStringVal& getResultRecordSizeEntry(IStringVal & str) const;
    virtual IStringVal& getResultTransformerEntry(IStringVal & str) const;
    virtual __int64     getResultRowLimit() const;
    virtual IStringVal& getResultFilename(IStringVal & str) const;
    virtual WUResultFormat getResultFormat() const;
    virtual unsigned    getResultHash() const;
    virtual bool        getResultIsAll() const;

    // interface IWUResult
    virtual void        setResultStatus(WUResultStatus status);
    virtual void        setResultName(const char *name);
    virtual void        setResultSequence(unsigned seq);
    virtual void        setResultSchemaRaw(unsigned len, const void *schema);
    virtual void        setResultScalar(bool isScalar);
    virtual void        setResultRaw(unsigned len, const void *xml, WUResultFormat format);
    virtual void        setResultFetchSize(unsigned rows);      // 0 means file-loaded
    virtual void        setResultTotalRowCount(__int64 rows);   // -1 means unknown
    virtual void        setResultRowCount(__int64 rows);
    virtual void        setResultDataset(const char *ecl, const char *defs);        
    virtual void        setResultLogicalName(const char *logicalName);
    virtual void        setResultKeyField(const char * name);
    virtual void        setResultRequestedRows(unsigned req);
    virtual void        setResultRecordSizeEntry(const char * val);
    virtual void        setResultTransformerEntry(const char * val);
    virtual void        setResultInt(__int64 val);
    virtual void        setResultReal(double val);
    virtual void        setResultBool(bool val);
    virtual void        setResultString(const char * val, unsigned length);
    virtual void        setResultUnicode(const void * val, unsigned length);
    virtual void        setResultData(const void * val, unsigned length);
    virtual void        setResultDecimal(const void * val, unsigned length);
    virtual void        addResultRaw(unsigned len, const void * data, WUResultFormat format);
    virtual void        setResultRowLimit(__int64 value);
    virtual void        setResultFilename(const char * name);
    virtual void        setResultUInt(unsigned __int64 val);
    virtual void        setResultIsAll(bool value);
    virtual void        setResultFormat(WUResultFormat format);
    virtual void        setResultXML(const char *val);
    virtual void        setResultRow(unsigned len, const void * data);
};

class CLocalWUPlugin : public CInterface, implements IWUPlugin
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUPlugin(IPropertyTree *p);

    virtual IStringVal& getPluginName(IStringVal &str) const;
    virtual IStringVal& getPluginVersion(IStringVal &str) const;
    virtual bool        getPluginThor() const;
    virtual bool        getPluginHole() const;

    virtual void        setPluginName(const char *str);
    virtual void        setPluginVersion(const char *str);
    virtual void        setPluginThor(bool on);
    virtual void        setPluginHole(bool on);
};

class CLocalWULibrary : public CInterface, implements IWULibrary
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWULibrary(IPropertyTree *p);

    virtual IStringVal & getName(IStringVal & str) const;
    virtual IConstWULibraryActivityIterator * getActivities() const;

    virtual void setName(const char * str);
    virtual void addActivity(unsigned id);
};

class CLocalWUGraph : public CInterface, implements IWUGraph
{
    const CLocalWorkUnit &owner;
    Owned<IPropertyTree> p;
    mutable Owned<IPropertyTree> graph; // cached copy of graph xgmml
    mutable Linked<IConstWUGraphProgress> progress;
    StringAttr wuid;
    unsigned wuidVersion;

    void mergeProgress(IPropertyTree &tree, IPropertyTree &progressTree, const unsigned &progressV) const;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUGraph(const CLocalWorkUnit &owner, IPropertyTree *p);

    virtual IStringVal & getXGMML(IStringVal & ret, bool mergeProgress) const;
    virtual IStringVal & getDOT(IStringVal & ret) const;
    virtual IStringVal & getName(IStringVal & ret) const;
    virtual IStringVal & getLabel(IStringVal & ret) const;
    virtual IStringVal & getTypeName(IStringVal & ret) const;
    virtual WUGraphType getType() const;
    virtual IPropertyTree * getXGMMLTree(bool mergeProgress) const;
    virtual IPropertyTree * getXGMMLTreeRaw() const;
    virtual bool isValid() const;

    virtual void setName(const char *str);
    virtual void setLabel(const char *str);
    virtual void setType(WUGraphType type);
    virtual void setXGMML(const char *str);
    virtual void setXGMMLTree(IPropertyTree * tree, bool compress=true);
};

class CLocalWUActivity : public CInterface, implements IWUActivity
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUActivity(IPropertyTree *p, __int64 id = 0);

    virtual __int64 getId() const;
    virtual unsigned getKind() const;
    virtual IStringVal & getHelper(IStringVal & ret) const;

    virtual void setKind(unsigned id);
    virtual void setHelper(const char * str);
};

class CLocalWUException : public CInterface, implements IWUException
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUException(IPropertyTree *p);

    virtual IStringVal& getExceptionSource(IStringVal &str) const;
    virtual IStringVal& getExceptionMessage(IStringVal &str) const;
    virtual unsigned    getExceptionCode() const;
    virtual WUExceptionSeverity getSeverity() const;
    virtual IStringVal & getTimeStamp(IStringVal & dt) const;
    virtual IStringVal & getExceptionFileName(IStringVal & str) const;
    virtual unsigned    getExceptionLineNo() const;
    virtual unsigned    getExceptionColumn() const;
    virtual void        setExceptionSource(const char *str);
    virtual void        setExceptionMessage(const char *str);
    virtual void        setExceptionCode(unsigned code);
    virtual void        setSeverity(WUExceptionSeverity level);
    virtual void        setTimeStamp(const char * dt);
    virtual void        setExceptionFileName(const char *str);
    virtual void        setExceptionLineNo(unsigned r);
    virtual void        setExceptionColumn(unsigned c);
};

//==========================================================================================

extern WORKUNIT_API bool isSpecialResultSequence(unsigned sequence)
{
    switch (sequence)
    {
    case ResultSequenceInternal:
    case ResultSequenceOnce:
    case ResultSequencePersist:
    case ResultSequenceStored:
        return true;
    default:
        assertex(sequence <= INT_MAX);
        return false;
    }
}

struct mapEnums { int val; const char *str; };

const char *getEnumText(int value, mapEnums *map) 
{
    const char *defval = map->str;
    while (map->str)
    {
        if (value==map->val)
            return map->str;
        map++;
    }
    assertex(!"Unexpected value in setEnum");
    return defval;
}

void setEnum(IPropertyTree *p, const char *propname, int value, mapEnums *map) 
{
    const char *defval = map->str;
    while (map->str)
    {
        if (value==map->val)
        {
            p->setProp(propname, map->str);
            return;
        }
        map++;
    }
    assertex(!"Unexpected value in setEnum");
    p->setProp(propname, defval);
}

static int getEnum(const char *v, mapEnums *map) 
{
    if (v)
    {
        while (map->str)
        {
            if (stricmp(v, map->str)==0)
                return map->val;
            map++;
        }
        assertex(!"Unexpected value in getEnum");
    }
    return 0;
}

static int getEnum(const IPropertyTree *p, const char *propname, mapEnums *map)
{
    return getEnum(p->queryProp(propname),map);
}

//==========================================================================================

class CConstWUArrayIterator : public CInterface, implements IConstWorkUnitIterator
{
    IArrayOf<IPropertyTree> trees;
    Owned<IConstWorkUnit> cur;
    unsigned curTreeNum;
    Linked<IRemoteConnection> conn;
    Linked<ISecManager> secmgr;
    Linked<ISecUser> secuser;

    void setCurrent()
    {
        cur.setown(new CLocalWorkUnit(LINK(conn), LINK(&trees.item(curTreeNum)), secmgr, secuser));
    }
public:
    IMPLEMENT_IINTERFACE;
    CConstWUArrayIterator(IRemoteConnection *_conn, IArrayOf<IPropertyTree> &_trees, ISecManager *_secmgr=NULL, ISecUser *_secuser=NULL)
        : conn(_conn), secmgr(_secmgr), secuser(_secuser)
    {
        ForEachItemIn(t, _trees)
            trees.append(*LINK(&_trees.item(t)));
        curTreeNum = 0;
    }
    bool first()
    {
        curTreeNum = 0;
        return next();
    }
    bool isValid()
    {
        return (NULL != cur.get());
    }
    bool next()
    {
        if (curTreeNum >= trees.ordinality())
        {
            cur.clear();
            return false;
        }
        setCurrent();
        ++curTreeNum;
        return true;
    }
    IConstWorkUnit & query() { return *cur; }
};
//==========================================================================================

class CStringArrayIterator : public CInterface, implements IStringIterator
{
    StringArray strings;
    unsigned idx;
public:
    IMPLEMENT_IINTERFACE;
    CStringArrayIterator() { idx = 0; };
    void append(const char *str) { strings.append(str); }
    virtual bool first() { idx = 0; return strings.isItem(idx); }
    virtual bool next() { idx ++; return strings.isItem(idx); }
    virtual bool isValid() { return strings.isItem(idx); }
    virtual IStringVal & str(IStringVal &s) { s.set(strings.item(idx)); return s; }
};

class CCachedJobNameIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CCachedJobNameIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryName()+1); return s; }
};

class CEmptyStringIterator : public CInterface, implements IStringIterator
{
public:
    IMPLEMENT_IINTERFACE;
    virtual bool first() { return false; }
    virtual bool next() { return false; }
    virtual bool isValid() { return false; }
    virtual IStringVal & str(IStringVal &s) { s.clear(); return s; }
};

mapEnums workunitSortFields[] =
{
   { WUSFuser, "@submitID" },
   { WUSFcluster, "@clusterName" },
   { WUSFjob, "@jobName" },
   { WUSFstate, "@state" },
   { WUSFpriority, "@priorityClass" },
   { WUSFprotected, "@protected" },
   { WUSFwuid, "@" },
   { WUSFfileread, "FilesRead/File/@name" },
   { WUSFroxiecluster, "RoxieQueryInfo/@roxieClusterName" },
   { WUSFbatchloginid, "Application/Dispatcher/FTPUserID" },
   { WUSFbatchcustomername, "Application/Dispatcher/CustomerName" },
   { WUSFbatchpriority, "Application/Dispatcher/JobPriority" },
   { WUSFbatchinputreccount, "Application/Dispatcher/InputRecords" },
   { WUSFbatchtimeuploaded, "Application/Dispatcher/TimeUploaded" },
   { WUSFbatchtimecompleted, "Application/Dispatcher/TimeCompleted" },
   { WUSFbatchmachine, "Application/Dispatcher/Machine" },
   { WUSFbatchinputfile, "Application/Dispatcher/InputFileName" },
   { WUSFbatchoutputfile, "Application/Dispatcher/OutputFileName" },
   { WUSFtotalthortime, "Timings/Timing[@name='Total thor time']/@duration" },
   { WUSFterm, NULL }
};

mapEnums querySortFields[] =
{
   { WUQSFId, "@id" },
   { WUQSFwuid, "@wuid" },
   { WUQSFname, "@name" },
   { WUQSFdll, "@dll" },
   { WUQSFmemoryLimit, "@memoryLimit" },
   { WUQSFmemoryLimitHi, "@memoryLimit" },
   { WUQSFtimeLimit, "@timeLimit" },
   { WUQSFtimeLimitHi, "@timeLimit" },
   { WUQSFwarnTimeLimit, "@warnTimeLimit" },
   { WUQSFwarnTimeLimitHi, "@warnTimeLimit" },
   { WUQSFpriority, "@priority" },
   { WUQSFpriorityHi, "@priority" },
   { WUQSFQuerySet, "../@id" },
   { WUQSFterm, NULL }
};

class asyncRemoveDllWorkItem: public CInterface, implements IWorkQueueItem // class only used in asyncRemoveDll
{
    StringAttr name;
public:
    IMPLEMENT_IINTERFACE;

    asyncRemoveDllWorkItem(const char * _name) : name(_name)
    {
    }
    void execute()
    {
        PROGLOG("WU removeDll %s", name.get());
        queryDllServer().removeDll(name, true, true); // <name>, removeDlls=true, removeDirectory=true
    }
};      

class asyncRemoveRemoteFileWorkItem: public CInterface, implements IWorkQueueItem // class only used in asyncRemoveFile
{
    RemoteFilename name;
public:
    IMPLEMENT_IINTERFACE;

    asyncRemoveRemoteFileWorkItem(const char * _ip, const char * _name)
    {
        SocketEndpoint ep(_ip);
        name.setPath(ep, _name);
    }
    void execute()
    {
        Owned<IFile> file = createIFile(name);
        PROGLOG("WU removeDll %s",file->queryFilename());
        file->remove();
    }
};

//==========================================================================================

class CConstQuerySetQueryIterator : public CInterface, implements IConstQuerySetQueryIterator
{
    IArrayOf<IPropertyTree> trees;
    unsigned index;
public:
    IMPLEMENT_IINTERFACE;
    CConstQuerySetQueryIterator(IArrayOf<IPropertyTree> &_trees)
    {
        ForEachItemIn(t, _trees)
            trees.append(*LINK(&_trees.item(t)));
        index = 0;
    }
    ~CConstQuerySetQueryIterator()
    {
        trees.kill();
    }
    bool first()
    {
        index = 0;
        return (trees.ordinality()!=0);
    }

    bool next()
    {
        index++;
        return (index<trees.ordinality());
    }

    bool isValid()
    {
        return (index<trees.ordinality());
    }

    IPropertyTree &query()
    {
        return trees.item(index);
    }
};

#define WUID_VERSION 2 // recorded in each wuid created, useful for bkwd compat. checks

class CWorkUnitFactory : public CInterface, implements IWorkUnitFactory, implements IDaliClientShutdown
{
    Owned<IWorkQueueThread> deletedllworkq;
public:
    IMPLEMENT_IINTERFACE;

    CWorkUnitFactory()
    {
        // Assumes dali client configuration has already been done
        sdsManager = &querySDS();
        session = myProcessSession();
        deletedllworkq.setown(createWorkQueueThread());
        addShutdownHook(*this);
    }

    ~CWorkUnitFactory()
    {
        removeShutdownHook(*this);
        // deletepool->joinAll();
    }
    void clientShutdown();
    SessionId startSession()
    {
        // Temporary placeholder until startSession is implemented
#ifdef _WIN32
        return GetTickCount();
#else
        struct timeval tm;
        gettimeofday(&tm,NULL);
        return tm.tv_usec;
#endif
    }
    IWorkUnit* ensureNamedWorkUnit(const char *name)
    {
        if (workUnitTraceLevel > 1)
            PrintLog("ensureNamedWorkUnit created %s", name);
        StringBuffer wuRoot;
        getXPath(wuRoot, name);
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        Owned<CLocalWorkUnit> cw = new CLocalWorkUnit(conn, (ISecManager *)NULL, NULL, (const char *)NULL);
        return &cw->lockRemote(false);
    }

    virtual IWorkUnit* createNamedWorkUnit(const char *wuid,const char *parentWuid, const char *app, const char *user)
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection *conn;
        if (queryDaliServerVersion().compare("2.0") >= 0)
             conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_CREATE_UNIQUE, SDS_LOCK_TIMEOUT);
        else
            conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_CREATE, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        conn->queryRoot()->setPropInt("@wuidVersion", WUID_VERSION);
        Owned<CLocalWorkUnit> cw = new CLocalWorkUnit(conn, (ISecManager*)NULL, NULL, parentWuid);
        IWorkUnit* ret = &cw->lockRemote(false);
        ret->setDebugValue("CREATED_BY", app, true);
        ret->setDebugValue("CREATED_FOR", user, true);
        if (user)
            cw->setWuScope(user);
        return ret;
    }
    virtual IWorkUnit* createWorkUnit(const char *parentWuid, const char *app, const char *user)
    {
        StringBuffer wuid("W");
        char result[32];
        time_t ltime;
        time( &ltime );
        tm *today = localtime( &ltime );   // MORE - this is not threadsafe. But I probably don't care that much!
        strftime(result, sizeof(result), "%Y%m%d-%H%M%S", today);
        wuid.append(result);
        if (queryDaliServerVersion().compare("2.0") < 0)
            wuid.append('-').append(startSession());
        if (workUnitTraceLevel > 1)
            PrintLog("createWorkUnit created %s", wuid.str());
        IWorkUnit* ret = createNamedWorkUnit(wuid.str(),parentWuid, app, user);
        if (workUnitTraceLevel > 1)
        {
            SCMStringBuffer wuidName;
            ret->getWuid(wuidName);
            PrintLog("createWorkUnit created %s", wuidName.str());
        }
        ret->addTimeStamp("workunit", NULL, "Created");
        return ret;
    }
    bool secDeleteWorkUnit(const char * wuid, ISecManager *secmgr, ISecUser *secuser, bool raiseexceptions)
    {
        if (workUnitTraceLevel > 1)
            PrintLog("deleteWorkUnit %s", wuid);
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection *conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_LOCK_SUB, SDS_LOCK_TIMEOUT);
        if (!conn)
        {
            if (workUnitTraceLevel > 0)
                PrintLog("deleteWorkUnit %s not found", wuid);
            return false;
        }
        Owned<CLocalWorkUnit> cw = new CLocalWorkUnit(conn, secmgr, secuser); // takes ownership of conn
        if (secmgr && !checkWuSecAccess(*cw.get(), *secmgr, secuser, SecAccess_Full, "delete", true, true)) {
            if (raiseexceptions) {
                // perhaps raise exception here?
            }
            return false;
        }
        if (raiseexceptions) {
            try
            {
                cw->cleanupAndDelete(true,true);
            }
            catch (IException *E)
            {
                StringBuffer s;
                LOG(MCexception(E, MSGCLS_warning), E, s.append("Exception during deleteWorkUnit: ").append(wuid).str());
                E->Release();
                return false;
            }
        }
        else
            cw->cleanupAndDelete(true,true);
        removeWorkUnitFromAllQueues(wuid); //known active workunits wouldn't make it this far
        return true;
    }

    virtual bool deleteWorkUnitEx(const char * wuid)
    {
        return secDeleteWorkUnit(wuid,NULL,NULL,true);
    }
    virtual bool deleteWorkUnit(const char * wuid)
    {
        return secDeleteWorkUnit(wuid,NULL,NULL,false);
    }
    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner)
    {
        // Not sure what to do about customerID vs user etc
        StringBuffer path("*");
        if (owner && *owner)
            path.append("[@submitID=\"").append(owner).append("\"]");
        return getWorkUnitsByXPath(path.str());
    }
    virtual IConstWorkUnitIterator * getWorkUnitsByState(WUState state);
    virtual IConstWorkUnitIterator * getWorkUnitsByECL(const char* ecl)
    {
        StringBuffer path("*");
        if (ecl && *ecl)
            path.append("[Query/Text=~\"*").append(ecl).append("*\"]");
        return getWorkUnitsByXPath(path.str());
    }
    virtual IConstWorkUnitIterator * getWorkUnitsByCluster(const char* cluster)
    {
        StringBuffer path("*");
        if (cluster && *cluster)
            path.append("[@clusterName=\"").append(cluster).append("\"]");
        return getWorkUnitsByXPath(path.str());
    }

    virtual IConstWorkUnitIterator * getChildWorkUnits(const char *parent)
    {
        StringBuffer path("*[@parent=\"");
        path.append(parent).append("\"]");
        return getWorkUnitsByXPath(path.str());
    }
    virtual IConstWorkUnit* secOpenWorkUnit(const char *wuid, bool lock, ISecManager *secmgr=NULL, ISecUser *secuser=NULL)
    {
        StringBuffer wuidStr(wuid);
        wuidStr.trim();
        if (!wuidStr.length())
            return NULL;

        if (workUnitTraceLevel > 1)
            PrintLog("openWorkUnit %s", wuidStr.str());
        StringBuffer wuRoot;
        getXPath(wuRoot, wuidStr.str());
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, lock ? RTM_LOCK_READ|RTM_LOCK_SUB : 0, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            CLocalWorkUnit *wu = new CLocalWorkUnit(conn, secmgr, secuser);
            if (secmgr && wu)
            {
                if (!checkWuSecAccess(*wu, *secmgr, secuser, SecAccess_Read, "opening", true, true))
                {
                    delete wu;
                    return NULL;
                }
            }
            return wu;
        }
        else
        {
            if (workUnitTraceLevel > 0)
                PrintLog("openWorkUnit %s not found", wuidStr.str());
            return NULL;
        }
    }
    virtual IConstWorkUnit* openWorkUnit(const char *wuid, bool lock)
    {
        return secOpenWorkUnit(wuid, lock);
    }
    virtual IWorkUnit* secUpdateWorkUnit(const char *wuid, ISecManager *secmgr=NULL, ISecUser *secuser=NULL)
    {
        if (workUnitTraceLevel > 1)
            PrintLog("updateWorkUnit %s", wuid);
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_LOCK_SUB, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            Owned<CLocalWorkUnit> cw = new CLocalWorkUnit(conn, secmgr, secuser);
            if (secmgr && cw)
            {
                if (!checkWuSecAccess(*cw.get(), *secmgr, secuser, SecAccess_Write, "updating", true, true))
                    return NULL;
            }
            return &cw->lockRemote(false);
        }
        else
        {
            if (workUnitTraceLevel > 0)
                PrintLog("updateWorkUnit %s not found", wuid);
            return NULL;
        }
    }
    virtual IWorkUnit* updateWorkUnit(const char *wuid)
    {
        return secUpdateWorkUnit(wuid);
    }
    virtual int setTracingLevel(int newLevel)
    {
        if (newLevel)
            PrintLog("Setting workunit trace level to %d", newLevel);
        int level = workUnitTraceLevel;
        workUnitTraceLevel = newLevel;
        return level;
    }
    IConstWorkUnitIterator * getWorkUnitsByXPath(const char *xpath)
    {
        return getWorkUnitsByXPath(xpath,NULL,NULL);
    }

    void descheduleAllWorkUnits()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        if(!conn) return;
        Owned<IPropertyTree> root(conn->queryRoot()->getBranch("."));
        KeptAtomTable entries;
        Owned<IPropertyTreeIterator> iter(root->getElements("*/*/*/*"));
        StringBuffer wuid;
        for(iter->first(); iter->isValid(); iter->next())
        {
            char const * entry = iter->query().queryName();
            if(!entries.find(entry))
            {
                entries.addAtom(entry);
                ncnameUnescape(entry, wuid.clear());
                Owned<IWorkUnit> wu = updateWorkUnit(wuid);
                if(wu && (wu->getState() == WUStateWait))
                    wu->setState(WUStateCompleted);
            }
        }
        bool more;
        do more = root->removeProp("*"); while(more);
    }

    IConstWorkUnitIterator * getWorkUnitsByXPath(const char *xpath, ISecManager *secmgr, ISecUser *secuser)
    {
        Owned<IRemoteConnection> conn = sdsManager->connect("/WorkUnits", session, 0, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            CDaliVersion serverVersionNeeded("3.2");
            Owned<IPropertyTreeIterator> iter(queryDaliServerVersion().compare(serverVersionNeeded) < 0 ? 
                conn->queryRoot()->getElements(xpath) : 
                conn->getElements(xpath));
            return new CConstWUIterator(conn, iter, secmgr, secuser);
        }
        else
            return NULL;
    }

    IConstWorkUnitIterator* getWorkUnitsSorted( WUSortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                WUSortField *filters,   // NULL or list of fields to folteron (terminated by WUSFterm)
                                                const void *filterbuf,  // (appended) string values for filters
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                const char *queryowner, 
                                                __int64 *cachehint,
                                                ISecManager *secmgr, 
                                                ISecUser *secuser,
                                                unsigned *total)
    {
        class CWorkUnitsPager : public CSimpleInterface, implements IElementsPager
        {
            StringAttr xPath;
            StringAttr sortOrder;
            StringAttr nameFilterLo;
            StringAttr nameFilterHi;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWorkUnitsPager(const char* _xPath, const char *_sortOrder, const char* _nameFilterLo, const char* _nameFilterHi)
                : xPath(_xPath), sortOrder(_sortOrder), nameFilterLo(_nameFilterLo), nameFilterHi(_nameFilterHi)
            {
            }
            virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
            {
                Owned<IRemoteConnection> conn = querySDS().connect("WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
                if (!conn)
                    return NULL;
                Owned<IPropertyTreeIterator> iter = conn->getElements(xPath);
                if (!iter)
                    return NULL;
                sortElements(iter, sortOrder.get(), nameFilterLo.get(), nameFilterHi.get(), elements);
                return conn.getClear();
            }
        };
        class CScopeChecker : public CSimpleInterface, implements ISortedElementsTreeFilter
        {
            UniqueScopes done;
            ISecManager *secmgr;
            ISecUser *secuser;
            CriticalSection crit;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CScopeChecker(ISecManager *_secmgr,ISecUser *_secuser)
            {
                secmgr = _secmgr;
                secuser = _secuser;
            }
            bool isOK(IPropertyTree &tree)
            {
                const char *scopename = tree.queryProp("@scope");
                if (!scopename||!*scopename)
                    return true;

                {
                    CriticalBlock block(crit);
                    const bool *b = done.getValue(scopename);
                    if (b)
                        return *b;
                }
                bool ret = checkWuScopeSecAccess(scopename,*secmgr,secuser,SecAccess_Read,"iterating",false,false);
                {
                    // conceivably could have already been checked and added, but ok.
                    CriticalBlock block(crit);
                    done.setValue(scopename,ret);
                }
                return ret;
            }
        };
        Owned<ISortedElementsTreeFilter> sc = new CScopeChecker(secmgr,secuser);
        StringBuffer query;
        StringBuffer so;
        StringAttr namefilter("*");
        StringAttr namefilterlo;
        StringAttr namefilterhi;
        if (filters) {
            const char *fv = (const char *)filterbuf;
            for (unsigned i=0;filters[i]!=WUSFterm;i++) {
                int fmt = filters[i];
                int subfmt = (fmt&0xff);
                if (subfmt==WUSFwuid) 
                    namefilterlo.set(fv);
                else if (subfmt==WUSFwuidhigh) 
                    namefilterhi.set(fv);
                else if (subfmt==WUSFwildwuid)
                    namefilter.set(fv);
                else {
                    query.append('[').append(getEnumText(subfmt,workunitSortFields)).append('=');
                    if (fmt&WUSFnocase)
                        query.append('?');
                    if (fmt&WUSFwild)
                        query.append('~');
                    query.append('"').append(fv).append("\"]");
                }
                fv = fv + strlen(fv)+1;
            }
        }
        query.insert(0, namefilter.get());
        if (sortorder) {
            for (unsigned i=0;sortorder[i]!=WUSFterm;i++) {
                if (so.length())
                    so.append(',');
                int fmt = sortorder[i];
                if (fmt&WUSFreverse) 
                    so.append('-');
                if (fmt&WUSFnocase) 
                    so.append('?');
                if (fmt&WUSFnumeric) 
                    so.append('#');
                so.append(getEnumText(fmt&0xff,workunitSortFields));
            }
        }
        IArrayOf<IPropertyTree> results;
        Owned<IElementsPager> elementsPager = new CWorkUnitsPager(query.str(), so.length()?so.str():NULL, namefilterlo.get(), namefilterhi.get());
        Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,secmgr?sc:NULL,queryowner,cachehint,results,total);
        return new CConstWUArrayIterator(conn, results, secmgr, secuser);
    }

    
    IConstWorkUnitIterator* getWorkUnitsSorted( WUSortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                WUSortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                                const void *filterbuf,  // (appended) string values for filters
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                const char *queryowner, 
                                                __int64 *cachehint,
                                                unsigned *total)
    {
        return getWorkUnitsSorted(sortorder,filters,filterbuf,startoffset,maxnum,queryowner,cachehint, NULL, NULL, total);
    }

    IConstQuerySetQueryIterator* getQuerySetQueriesSorted( WUQuerySortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                WUQuerySortField *filters,   // NULL or list of fields to folteron (terminated by WUSFterm)
                                                const void *filterbuf,  // (appended) string values for filters
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                __int64 *cachehint,
                                                unsigned *total)
    {
        class CQuerySetQueriesPager : public CSimpleInterface, implements IElementsPager
        {
            StringAttr querySet;
            StringAttr xPath;
            StringAttr sortOrder;

            void populateQueryTree(IPropertyTree* queryRegistry, const char* querySetId, IPropertyTree* querySetTree, const char *xPath, IPropertyTree* queryTree)
            {
                Owned<IPropertyTreeIterator> iter = querySetTree->getElements(xPath);
                ForEach(*iter)
                {
                    IPropertyTree &query = iter->query();
                    IPropertyTree *queryWithSetId = queryTree->addPropTree("Query", LINK(&query));
                    queryWithSetId->addProp("@querySetId", querySetId);

                    bool activated = false;
                    const char* queryId = query.queryProp("@id");
                    if (queryId && *queryId)
                    {
                        VStringBuffer xPath("Alias[@id='%s']", queryId);
                        IPropertyTree *alias = queryRegistry->queryPropTree(xPath.str());
                        if (alias)
                            activated = true;
                    }
                    queryWithSetId->addPropBool("@activated", activated);
                }
            }

            IPropertyTree* getAllQuerySetQueries(IRemoteConnection* conn, const char *querySet, const char *xPath)
            {
                Owned<IPropertyTree> queryTree = createPTree("Queries");
                IPropertyTree* root = conn->queryRoot();
                if (querySet && *querySet)
                {
                    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySet, false);
                    VStringBuffer path("QuerySet[@id='%s']/Query%s", querySet, xPath);
                    populateQueryTree(queryRegistry, querySet, root, path.str(), queryTree);
                }
                else
                {
                    Owned<IPropertyTreeIterator> iter = root->getElements("QuerySet");
                    ForEach(*iter)
                    {
                        IPropertyTree &querySetTree = iter->query();
                        const char* id = querySetTree.queryProp("@id");
                        if (id && *id)
                        {
                            Owned<IPropertyTree> queryRegistry = getQueryRegistry(id, false);
                            VStringBuffer path("Query%s", xPath);
                            populateQueryTree(queryRegistry, id, &querySetTree, path.str(), queryTree);
                        }
                    }
                }
                return queryTree.getClear();
            }

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CQuerySetQueriesPager(const char* _querySet, const char* _xPath, const char *_sortOrder)
                : querySet(_querySet), xPath(_xPath), sortOrder(_sortOrder)
            {
            }
            virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
            {
                Owned<IRemoteConnection> conn = querySDS().connect("QuerySets", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
                if (!conn)
                    return NULL;
                Owned<IPropertyTree> elementTree = getAllQuerySetQueries(conn, querySet.get(), xPath.get());
                if (!elementTree)
                    return NULL;
                Owned<IPropertyTreeIterator> iter = elementTree->getElements("*");
                if (!iter)
                    return NULL;
                sortElements(iter, sortOrder.get(), NULL, NULL, elements);
                return conn.getClear();
            }
        };
        StringAttr querySet;
        StringBuffer xPath;
        StringBuffer so;
        if (filters)
        {
            const char *fv = (const char *)filterbuf;
            for (unsigned i=0;filters[i]!=WUQSFterm;i++) {
                int fmt = filters[i];
                int subfmt = (fmt&0xff);
                if (subfmt==WUQSFQuerySet)
                    querySet.set(fv);
                else if ((subfmt==WUQSFmemoryLimit) || (subfmt==WUQSFtimeLimit) || (subfmt==WUQSFwarnTimeLimit) || (subfmt==WUQSFpriority))
                    xPath.append('[').append(getEnumText(subfmt,querySortFields)).append(">=").append(fv).append("]");
                else if ((subfmt==WUQSFmemoryLimitHi) || (subfmt==WUQSFtimeLimitHi) || (subfmt==WUQSFwarnTimeLimitHi) || (subfmt==WUQSFpriorityHi))
                    xPath.append('[').append(getEnumText(subfmt,querySortFields)).append("<=").append(fv).append("]");
                else {
                    xPath.append('[').append(getEnumText(subfmt,querySortFields)).append('=');
                    if (fmt&WUQSFnocase)
                        xPath.append('?');
                    if (fmt&WUQSFnumeric)
                        xPath.append('#');
                    if (fmt&WUQSFwild)
                        xPath.append('~');
                    xPath.append('"').append(fv).append("\"]");
                }
                fv = fv + strlen(fv)+1;
            }
        }
        if (xPath.length() < 1)
            xPath.set("*");
        if (sortorder) {
            for (unsigned i=0;sortorder[i]!=WUQSFterm;i++) {
                if (so.length())
                    so.append(',');
                int fmt = sortorder[i];
                if (fmt&WUQSFreverse)
                    so.append('-');
                if (fmt&WUQSFnocase)
                    so.append('?');
                if (fmt&WUQSFnumeric)
                    so.append('#');
                so.append(getEnumText(fmt&0xff,querySortFields));
            }
        }
        IArrayOf<IPropertyTree> results;
        Owned<IElementsPager> elementsPager = new CQuerySetQueriesPager(querySet.get(), xPath.str(), so.length()?so.str():NULL);
        Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,NULL,"",cachehint,results,total);
        return new CConstQuerySetQueryIterator(results);
    }

    virtual unsigned numWorkUnits()
    {
        Owned<IRemoteConnection> conn = sdsManager->connect("/WorkUnits", session, 0, SDS_LOCK_TIMEOUT);
        if (!conn) 
            return 0;
        IPropertyTree *root = conn->queryRoot();
        return root->numChildren();
    }

    virtual unsigned numWorkUnitsFiltered(WUSortField *filters,
                                        const void *filterbuf,
                                        ISecManager *secmgr, 
                                        ISecUser *secuser)
    {
        unsigned total;
        Owned<IConstWorkUnitIterator> iter =  getWorkUnitsSorted( NULL,filters,filterbuf,0,0x7fffffff,NULL,NULL,secmgr,secuser,&total);
        return total;
    }

    virtual unsigned numWorkUnitsFiltered(WUSortField *filters,const void *filterbuf)
    {
        if (!filters)
            return numWorkUnits();
        return numWorkUnitsFiltered(filters,filterbuf,NULL,NULL);
    }

    void asyncRemoveDll(const char * name)
    {
        deletedllworkq->post(new asyncRemoveDllWorkItem(name));
    }

    void asyncRemoveFile(const char * ip, const char * name)
    {
        deletedllworkq->post(new asyncRemoveRemoteFileWorkItem(ip, name));
    }

    ISDSManager *sdsManager;
    SessionId session;
    ISecManager *secMgr;

private:
    void deleteChildren(IPropertyTree *root, const char *wuid)
    {
        StringBuffer kids("*[@parent=\"");
        kids.append(wuid).append("\"]");
        Owned<IPropertyTreeIterator> it = root->getElements(kids.str());
        ForEach (*it)
        {
            deleteChildren(root, it->query().queryName());
        }
        root->removeProp(wuid);
    }
    class CConstWUIterator : public CInterface, implements IConstWorkUnitIterator
    {
        Owned<IConstWorkUnit> cur;
        Linked<IRemoteConnection> conn;
        Linked<IPropertyTreeIterator> ptreeIter;
        Linked<ISecManager> secmgr;
        Linked<ISecUser> secuser;
        Owned<ISecResourceList> scopes;

        void setCurrent()
        {
            cur.setown(new CLocalWorkUnit(LINK(conn), LINK(&ptreeIter->query()), secmgr, secuser));
        }
        bool getNext() // scan for a workunit with permissions
        {
            if (!scopes)
            {
                setCurrent();
                return true;
            }
            do
            {
                const char *scopeName = ptreeIter->query().queryProp("@scope");
                if (!scopeName || !*scopeName || checkWuScopeListSecAccess(scopeName, scopes, SecAccess_Read, "iterating", false, false))
                {
                    setCurrent();
                    return true;
                }
            }
            while (ptreeIter->next());
            cur.clear();
            return false;
        }
    public:
        IMPLEMENT_IINTERFACE;
        CConstWUIterator(IRemoteConnection *_conn, IPropertyTreeIterator *_ptreeIter, ISecManager *_secmgr=NULL, ISecUser *_secuser=NULL)
            : conn(_conn), ptreeIter(_ptreeIter), secmgr(_secmgr), secuser(_secuser)
        {
            UniqueScopes us;
            if (secmgr /* && secmgr->authTypeRequired(RT_WORKUNIT_SCOPE) tbd */)
            {
                scopes.setown(secmgr->createResourceList("wuscopes"));
                ForEach(*ptreeIter)
                {
                    const char *scopeName = ptreeIter->query().queryProp("@scope");
                    if (scopeName && *scopeName && !us.getValue(scopeName))
                    {
                        scopes->addResource(scopeName);
                        us.setValue(scopeName, true);
                    }
                }
                if (scopes->count())
                    secmgr->authorizeEx(RT_WORKUNIT_SCOPE, *secuser, scopes);
                else
                    scopes.clear();
            }
        }
        bool first()
        {
            if (!ptreeIter->first())
            {
                cur.clear();
                return false;
            }
            return getNext();
        }
        bool isValid()
        {
            return (NULL != cur.get());
        }
        bool next()
        {
            if (!ptreeIter->next())
            {
                cur.clear();
                return false;
            }
            return getNext();
        }
        IConstWorkUnit & query() { return *cur; }
    };
    IRemoteConnection* connect(const char *xpath, unsigned flags)
    {
        return sdsManager->connect(xpath, session, flags, SDS_LOCK_TIMEOUT);
    }

};

static Owned<CWorkUnitFactory> factory;

void CWorkUnitFactory::clientShutdown()
{
    factory.clear();
}

void clientShutdownWorkUnit()
{
    factory.clear();
}

extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory()
{
    if (!factory)
        factory.setown(new CWorkUnitFactory());
    return factory.getLink();
}

class CSecureWorkUnitFactory : public CInterface, implements IWorkUnitFactory
{
public:
    IMPLEMENT_IINTERFACE;

    CSecureWorkUnitFactory(ISecManager &secmgr, ISecUser &secuser)
    {
        if (!factory)
            factory.setown(new CWorkUnitFactory());
        secMgr.set(&secmgr);
        secUser.set(&secuser);
    }
    virtual IWorkUnit* createNamedWorkUnit(const char *wuid,const char *parentWuid, const char *app, const char *user)
    {
        checkWuScopeSecAccess(user, *secMgr.get(), secUser.get(), SecAccess_Write, "Create", true, true);
        IWorkUnit *wu=factory->createNamedWorkUnit(wuid, parentWuid, app, user);
        if (wu)
        {
            CLockedWorkUnit* lw = dynamic_cast<CLockedWorkUnit*>(wu);
            if (lw)
                lw->setSecIfcs(secMgr.get(), secUser.get());
        }
        return wu;
    }
    virtual IWorkUnit* createWorkUnit(const char *parentWuid, const char *app, const char *user)
    {
        checkWuScopeSecAccess(user, *secMgr.get(), secUser.get(), SecAccess_Write, "Create", true, true);
        IWorkUnit *wu=factory->createWorkUnit(parentWuid, app, user);
        if (wu)
        {
            CLockedWorkUnit* lw = dynamic_cast<CLockedWorkUnit*>(wu);
            if (lw)
                lw->setSecIfcs(secMgr.get(), secUser.get());
        }
        return wu;
    }
    virtual bool deleteWorkUnitEx(const char * wuid)
    {
        return factory->secDeleteWorkUnit(wuid, secMgr.get(), secUser.get(), true);
    }
    virtual bool deleteWorkUnit(const char * wuid)
    {
        return factory->secDeleteWorkUnit(wuid, secMgr.get(), secUser.get(), false);
    }
    virtual IConstWorkUnit* openWorkUnit(const char *wuid, bool lock)
    {
        return factory->secOpenWorkUnit(wuid, lock, secMgr.get(), secUser.get());
    }
    virtual IWorkUnit* updateWorkUnit(const char *wuid)
    {
        return factory->secUpdateWorkUnit(wuid, secMgr.get(), secUser.get());
    }

    //make cached workunits a non secure pass through for now.
    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner)
    {
        // Not sure what to do about customerID vs user etc
        StringBuffer path("*");
        if (owner && *owner)
            path.append("[@submitID=\"").append(owner).append("\"]");
        return factory->getWorkUnitsByXPath(path.str(), secMgr.get(), secUser.get());
    }
    virtual IConstWorkUnitIterator * getWorkUnitsByState(WUState state);
    virtual IConstWorkUnitIterator * getWorkUnitsByECL(const char* ecl)
    {   
        return factory->getWorkUnitsByECL(ecl);
    }

    virtual IConstWorkUnitIterator * getWorkUnitsByCluster(const char* cluster)
    {   
        return factory->getWorkUnitsByCluster(cluster);
    }

    virtual IConstWorkUnitIterator * getWorkUnitsByXPath(const char * xpath)
    {
        return factory->getWorkUnitsByXPath(xpath, secMgr.get(), secUser.get());
    }

    virtual void descheduleAllWorkUnits()
    {
        factory->descheduleAllWorkUnits();
    }

    virtual IConstWorkUnitIterator * getChildWorkUnits(const char *parent)
    {
        StringBuffer path("*[@parent=\"");
        path.append(parent).append("\"]");
        return factory->getWorkUnitsByXPath(path.str(), secMgr.get(), secUser.get());
    }
    virtual int setTracingLevel(int newLevel)
    {
        return factory->setTracingLevel(newLevel);
    }

    virtual IConstWorkUnitIterator* getWorkUnitsSorted( WUSortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                        WUSortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                                        const void *filterbuf,  // (appended) string values for filters
                                                        unsigned startoffset,
                                                        unsigned maxnum,
                                                        const char *queryowner, 
                                                        __int64 *cachehint,
                                                        unsigned *total)
    {
        return factory->getWorkUnitsSorted(sortorder,filters,filterbuf,startoffset,maxnum,queryowner,cachehint, secMgr.get(), secUser.get(), total);
    }

    virtual IConstQuerySetQueryIterator* getQuerySetQueriesSorted( WUQuerySortField *sortorder,
                                                WUQuerySortField *filters,
                                                const void *filterbuf,
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                __int64 *cachehint,
                                                unsigned *total)
    {
        return factory->getQuerySetQueriesSorted(sortorder,filters,filterbuf,startoffset,maxnum,cachehint,total);
    }

    virtual unsigned numWorkUnits()
    {
        return factory->numWorkUnits();
    }

    virtual unsigned numWorkUnitsFiltered(WUSortField *filters,
                                        const void *filterbuf)
    {
        return factory->numWorkUnitsFiltered(filters,filterbuf,secMgr.get(), secUser.get());
    }


private:
    Owned<CWorkUnitFactory> base_factory;
    Owned<ISecManager> secMgr;
    Owned<ISecUser> secUser;
};

extern WORKUNIT_API IWorkUnitFactory * getSecWorkUnitFactory(ISecManager &secmgr, ISecUser &secuser)
{
    return new CSecureWorkUnitFactory(secmgr, secuser);
}

extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory(ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return getSecWorkUnitFactory(*secmgr, *secuser);
    else
        return getWorkUnitFactory();
}

//==========================================================================================

class CStringPTreeIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryProp(NULL)); return s; }
};

class CStringPTreeTagIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeTagIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryName()); return s; }
};

class CStringPTreeAttrIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
    StringAttr name;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeAttrIterator(IPropertyTreeIterator *p, const char *_name) : it(p), name(_name) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryProp(name)); return s; }
};
//==========================================================================================

CLocalWorkUnit::CLocalWorkUnit(IRemoteConnection *_conn, ISecManager *secmgr, ISecUser *secuser, const char *parentWuid) : connection(_conn)
{
    connectAtRoot = true;
    init();
    p.setown(connection->getRoot());
    if (parentWuid)
        p->setProp("@parent", parentWuid);
    secMgr.set(secmgr);
    secUser.set(secuser);
}

CLocalWorkUnit::CLocalWorkUnit(IRemoteConnection *_conn, IPropertyTree* root, ISecManager *secmgr, ISecUser *secuser) : connection(_conn)
{
    connectAtRoot = false;
    init();
    p.setown(root);
    secMgr.set(secmgr);
    secUser.set(secuser);
}

void CLocalWorkUnit::init()
{
    p.clear();
    cachedGraphs.clear();
    workflowIterator.clear();
    query.clear();
    graphs.kill();
    results.kill();
    variables.kill();
    plugins.kill();
    libraries.kill();
    activities.kill();
    exceptions.kill();
    temporaries.kill();
    roxieQueryInfo.clear();
    webServicesInfo.clear();
    workflowIteratorCached = false;
    resultsCached = false;
    temporariesCached = false;
    variablesCached = false;
    exceptionsCached = false;
    pluginsCached = false;
    librariesCached = false;
    activitiesCached = false;
    webServicesInfoCached = false;
    roxieQueryInfoCached = false;
    dirty = false;
    abortDirty = true;
    abortState = false;
}

// Dummy workunit support
CLocalWorkUnit::CLocalWorkUnit(const char *_wuid, const char *parentWuid, ISecManager *secmgr, ISecUser *secuser)
{
    connectAtRoot = true;
    init();
    p.setown(createPTree(_wuid));
    p->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
    if (parentWuid)
        p->setProp("@parentWuid", parentWuid);
    secMgr.set(secmgr);
    secUser.set(secuser);
}

CLocalWorkUnit::~CLocalWorkUnit() 
{
    if (workUnitTraceLevel > 1)
    {
        PrintLog("Releasing workunit %s mode %x", p->queryName(), connection ? connection->queryMode() :0);
    }
    try
    {
        unsubscribe();
        query.clear();
        webServicesInfo.clear();
        roxieQueryInfo.clear();
        workflowIterator.clear();

        activities.kill();
        plugins.kill();
        libraries.kill();
        exceptions.kill();
        graphs.kill();
        results.kill();
        temporaries.kill();
        variables.kill();
        timestamps.kill();
        appvalues.kill();
        statistics.kill();

        userDesc.clear();
        secMgr.clear();
        secUser.clear();
        cachedGraphs.clear();
        p.clear();
        connection.clear();
    }
    catch (IException *E) { LOG(MCexception(E, MSGCLS_warning), E, "Exception during ~CLocalWorkUnit"); E->Release(); }
}

void CLocalWorkUnit::cleanupAndDelete(bool deldll, bool deleteOwned, const StringArray *deleteExclusions)
{
    TIME_SECTION("WUDELETE cleanupAndDelete total");
    // Delete any related things in SDS etc that might otherwise be forgotten
    assertex(connectAtRoot); // make sure we don't delete entire workunit tree!
    if (p->getPropBool("@protected", false))
        throw MakeStringException(WUERR_WorkunitProtected, "%s: Workunit is protected",p->queryName());
    switch (getState())
    {
    case WUStateAborted:
    case WUStateCompleted:
    case WUStateFailed:
    case WUStateArchived:
        break;
    case WUStateCompiled:
        if (getAction()==WUActionRun || getAction()==WUActionUnknown)
            throw MakeStringException(WUERR_WorkunitActive, "%s: Workunit is active",p->queryName());
        break;
    case WUStateWait:
        throw MakeStringException(WUERR_WorkunitScheduled, "%s: Workunit is scheduled",p->queryName());
    default:
        throw MakeStringException(WUERR_WorkunitActive, "%s: Workunit is active",p->queryName());
        break;
    }
    if (getIsQueryService())
    {
        Owned<IPropertyTree> registry = getQueryRegistryRoot();
        if (registry)
        {
            VStringBuffer xpath("QuerySet/Query[@wuid='%s']", p->queryName());
            if (registry->hasProp(xpath.str()))
                throw MakeStringException(WUERR_WorkunitPublished, "%s: Workunit is published",p->queryName());
        }
    }
    try
    {
        if (deldll && !p->getPropBool("@isClone", false))
        {
            Owned<IConstWUQuery> q = getQuery();
            if (q)
            {
                Owned<IConstWUAssociatedFileIterator> iter = &q->getAssociatedFiles();
                SCMStringBuffer name;
                ForEach(*iter)
                {
                    IConstWUAssociatedFile & cur = iter->query();
                    cur.getNameTail(name);
                    if (!deleteExclusions || (NotFound == deleteExclusions->find(name.str())))
                    {
                        Owned<IDllEntry> entry = queryDllServer().getEntry(name.str());
                        if (entry.get())
                            factory->asyncRemoveDll(name.str());
                        else
                        {
                            SCMStringBuffer ip, localPath;
                            cur.getName(localPath);
                            cur.getIp(ip);
                            factory->asyncRemoveFile(ip.str(), localPath.str());
                        }
                    }
                }
            }
        }
        StringBuffer apath;
//      PROGLOG("wuid dll files removed");
        {
            apath.append("/WorkUnitAborts/").append(p->queryName());
            Owned<IRemoteConnection> acon = factory->sdsManager->connect(apath.str(), factory->session, RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, SDS_LOCK_TIMEOUT);
            acon.clear();
        }
//      PROGLOG("wuid WorkUnitAborts entry removed");

        deleteTempFiles(NULL, deleteOwned, true); // all, any remaining.
    }
    catch(IException *E)
    {
        StringBuffer s;
        LOG(MCexception(E, MSGCLS_warning), E, s.append("Exception during cleanupAndDelete: ").append(p->queryName()).str());
        E->Release();
    }
    catch (...) 
    { 
        WARNLOG("Unknown exception during cleanupAndDelete: %s", p->queryName()); 
    }
    CConstGraphProgress::deleteWuidProgress(p->queryName());
    connection->close(true);  
    PROGLOG("WUID %s removed",p->queryName());
    connection.clear();
}

void CLocalWorkUnit::setTimeScheduled(const IJlibDateTime &val)
{
    SCMStringBuffer strval;
    val.getGmtString(strval);
    p->setProp("@timescheduled",strval.str());
}

IJlibDateTime & CLocalWorkUnit::getTimeScheduled(IJlibDateTime &val) const
{
    StringBuffer str;
    p->getProp("@timescheduled",str);
    if(str.length())
        val.setGmtString(str.str());
    return val; 
}

bool modifyAndWriteWorkUnitXML(char const * wuid, StringBuffer & buf, StringBuffer & extra, IFileIO * fileio)
{
    // kludge in extra chunks of XML such as GraphProgress and GeneratedDlls
    if(extra.length())
    {
        size32_t l = (size32_t)strlen(wuid);
        size32_t p = buf.length()-l-4; // bit of a kludge
        assertex(memcmp(buf.str()+p+2,wuid,l)==0);
        StringAttr tail(buf.str()+p);
        buf.setLength(p);
        buf.append(extra);
        buf.append(tail);
    }
    return (fileio->write(0,buf.length(),buf.str()) == buf.length());
}

bool CLocalWorkUnit::archiveWorkUnit(const char *base,bool del,bool ignoredllerrors,bool deleteOwned)
{
    CriticalBlock block(crit);
    StringBuffer path(base);
    if (!p)
        return false;
    const char *wuid = p->queryName();
    if (!wuid||!*wuid)
        return false;
    addPathSepChar(path).append(wuid).append(".xml");
    Owned<IFile> file = createIFile(path.str());
    if (!file)
        return false;
    Owned<IFileIO> fileio = file->open(IFOcreate);
    if (!fileio)
        return false;

    StringBuffer buf;
    exportWorkUnitToXML(this, buf, false);

    StringBuffer extraWorkUnitXML;
    StringBuffer xpath("/GraphProgress/");
    xpath.append(wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        Owned<IPropertyTree> tmp = createPTree("GraphProgress");
        mergePTree(tmp,conn->queryRoot());
        toXML(tmp,extraWorkUnitXML,1,XML_Format);
        conn->close();
    }

    Owned<IConstWUQuery> q = getQuery();
    if (!q)
    {
        if (!modifyAndWriteWorkUnitXML(wuid, buf, extraWorkUnitXML, fileio))
           return false;
        if (del)
        {
            if (getState()==WUStateUnknown)
                setState(WUStateArchived);  // to allow delete
            cleanupAndDelete(false,deleteOwned);    // no query, may as well delete 
        }
        return false;   
    }

    StringArray deleteExclusions; // associated files not to delete, added if failure to copy
    Owned<IConstWUAssociatedFileIterator> iter = &q->getAssociatedFiles();
    SCMStringBuffer name;
    Owned<IException> exception;
    Owned<IDllLocation> loc;
    StringBuffer dst, locpath;
    Owned<IPropertyTree> generatedDlls = createPTree("GeneratedDlls");
    ForEach(*iter)
    {
        IConstWUAssociatedFile & cur = iter->query();
        cur.getNameTail(name);
        if (name.length())
        {
            Owned<IDllEntry> entry = queryDllServer().getEntry(name.str());
            if (entry.get())
            {
                Owned<IPropertyTree> generatedDllBranch = createPTree();
                generatedDllBranch->setProp("@name", entry->queryName());
                generatedDllBranch->setProp("@kind", entry->queryKind());
                exception.clear();
                try
                {
                    loc.setown(entry->getBestLocation()); //throws exception if no readable locations
                }
                catch(IException * e)
                {
                    exception.setown(e);
                    loc.setown(entry->getBestLocationCandidate()); //this will be closest of the unreadable locations
                }
                RemoteFilename filename;
                loc->getDllFilename(filename);
                if (!exception)
                {
                    Owned<IFile> srcfile = createIFile(filename);
                    addPathSepChar(dst.clear().append(base));
                    filename.getTail(dst);
                    Owned<IFile> dstfile = createIFile(dst.str());
                    try
                    {
                        if (dstfile->exists())
                        {
                            if (streq(srcfile->queryFilename(), dstfile->queryFilename()))
                                deleteExclusions.append(name.str()); // restored workunit, referencing archive location for query dll
                            // still want to delete if already archived but there are source file copies
                        }
                        else
                            copyFile(dstfile,srcfile);
                        makeAbsolutePath(dstfile->queryFilename(), locpath.clear());
                    }
                    catch(IException * e)
                    {
                        exception.setown(e);
                    }
                }
                if (exception)
                {
                    if (ignoredllerrors)
                    {
                        EXCLOG(exception.get(), "archiveWorkUnit (copying associated file)");
                        //copy failed, so store original (best) location and don't delete the files
                        filename.getRemotePath(locpath.clear());
                        deleteExclusions.append(name.str());
                    }
                    else
                    {
                        throw exception.getLink();
                    }
                }
                generatedDllBranch->setProp("@location", locpath.str());
                generatedDlls->addPropTree("GeneratedDll", generatedDllBranch.getClear());
            }
            else // no generated dll entry
            {
                SCMStringBuffer localPath, ip;
                cur.getName(localPath);
                cur.getIp(ip);
                SocketEndpoint ep(ip.str());
                RemoteFilename rfn;
                rfn.setPath(ep, localPath.str());
                Owned<IFile> srcFile = createIFile(rfn);
                addPathSepChar(dst.clear().append(base));
                rfn.getTail(dst);
                Owned<IFile> dstFile = createIFile(dst.str());
                try
                {
                    copyFile(dstFile, srcFile);
                }
                catch (IException *e)
                {
                    VStringBuffer msg("Failed to archive associated file '%s' to destination '%s'", srcFile->queryFilename(), dstFile->queryFilename());
                    EXCLOG(e, msg.str());
                    e->Release();
                    deleteExclusions.append(name.str());
                }
            }
        }
    }
    iter.clear();
    if (generatedDlls->numChildren())
        toXML(generatedDlls, extraWorkUnitXML, 1, XML_Format);

    if (!modifyAndWriteWorkUnitXML(wuid, buf, extraWorkUnitXML, fileio))
       return false;

    if (del)
    {
        //setState(WUStateArchived);    // this isn't useful as about to delete it!
        q.clear();
        cleanupAndDelete(true, deleteOwned, &deleteExclusions);
    }

    return true;
}

void CLocalWorkUnit::packWorkUnit(bool pack)
{
    // only packs Graph info currently
    CriticalBlock block(crit);
    if (!p)
        return;
    const char *wuid = p->queryName();
    if (!wuid||!*wuid)
        return;
    if (pack) {
        if (!p->hasProp("PackedGraphs")) {
            cachedGraphs.clear();
            IPropertyTree *t = p->queryPropTree("Graphs");
            if (t) {
                MemoryBuffer buf;
                t->serialize(buf);
                p->setPropBin("PackedGraphs",buf.length(),buf.bufferBase());
                p->removeTree(t);
            }
        }
    }
    else {
        ensureGraphsUnpacked();
    }
    CConstGraphProgress::packProgress(wuid,pack);
}

IPropertyTree * pruneBranch(IPropertyTree * from, char const * xpath)
{
    Owned<IPropertyTree> ret;
    IPropertyTree * branch = from->queryPropTree(xpath);
    if(branch) {
        ret.setown(createPTreeFromIPT(branch));
        from->removeTree(branch);
    }
    return ret.getClear();
}

bool restoreWorkUnit(const char *base,const char *wuid)
{
    StringBuffer path(base);
    if (!wuid||!*wuid)
        return false;
    addPathSepChar(path).append(wuid).append(".xml");
    Owned<IFile> file = createIFile(path.str());
    if (!file)
        return false;
    Owned<IFileIO> fileio = file->open(IFOread);
    if (!fileio)
        return false;
    Owned<IPropertyTree> pt = createPTree(*fileio);
    if (!pt)
        return false;
    CDateTime dt;
    dt.setNow();
    StringBuffer dts;
    dt.getString(dts);
    pt->setProp("@restoredDate", dts.str());
    VStringBuffer xpath("/WorkUnits/%s", wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        ERRLOG("restoreWorkUnit could not create to %s", xpath.str());
        return false;
    }
    IPropertyTree *root = conn->queryRoot();
    if (root->hasChildren())
    {
        ERRLOG("restoreWorkUnit WUID %s already exists", wuid);
        return false;
    }
    Owned<IPropertyTree> gprogress = pruneBranch(pt, "GraphProgress[1]");
    Owned<IPropertyTree> generatedDlls = pruneBranch(pt, "GeneratedDlls[1]");
    Owned<IPropertyTree> associatedFiles;
    IPropertyTree *srcAssociated = pt->queryPropTree("Query/Associated");
    if (srcAssociated)
        associatedFiles.setown(createPTreeFromIPT(srcAssociated));
    root->setPropTree(NULL, pt.getClear());
    conn.clear();

    // now kludge back GraphProgress and GeneratedDlls
    if (gprogress)
    {
        VStringBuffer xpath("/GraphProgress/%s", wuid);
        conn.setown(querySDS().connect(xpath, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT));
        if (conn)
        {
            IPropertyTree *groot = conn->queryRoot();
            if (groot->hasChildren())
                WARNLOG("restoreWorkUnit WUID %s graphprogress already exists, replacing",wuid);
            groot->setPropTree(NULL, gprogress.getClear());
        }
    }

    if (generatedDlls)
    {
        Owned<IPropertyTreeIterator> dlls = generatedDlls->getElements("GeneratedDll");
        for(dlls->first(); dlls->isValid(); dlls->next())
        {
            IPropertyTree & dll = dlls->query();
            char const * name = dll.queryProp("@name");
            char const * kind = dll.queryProp("@kind");
            char const * location = dll.queryProp("@location");
            Owned<IDllEntry> got = queryDllServer().getEntry(name);
            if (!got)
                queryDllServer().registerDll(name, kind, location);
        }
    }
    if (associatedFiles)
    {
        Owned<IPropertyTreeIterator> associated = associatedFiles->getElements("*");
        ForEach(*associated)
        {
            IPropertyTree &file = associated->query();
            const char *filename = file.queryProp("@filename");
            SocketEndpoint ep(file.queryProp("@ip"));
            RemoteFilename rfn;
            rfn.setPath(ep, filename);
            OwnedIFile dstFile = createIFile(rfn);
            StringBuffer srcPath(base), name;
            addPathSepChar(srcPath);
            rfn.getTail(name);
            srcPath.append(name);
            if (generatedDlls)
            {
                VStringBuffer gDllPath("GeneratedDll[@name=\"%s\"]", name.str());
                if (generatedDlls->hasProp(gDllPath))
                    continue; // generated dlls handled separately - see above
            }

            OwnedIFile srcFile = createIFile(srcPath);
            if (srcFile->exists())
            {
                try
                {
                    copyFile(dstFile, srcFile);
                }
                catch (IException *e)
                {
                    VStringBuffer msg("Failed to restore associated file '%s' to destination '%s'", srcFile->queryFilename(), dstFile->queryFilename());
                    EXCLOG(e, msg.str());
                    e->Release();
                }
            }
        }
    }
    return true;
}

void CLocalWorkUnit::loadXML(const char *xml)
{
    CriticalBlock block(crit);
    init();
    assertex(xml);
    p.setown(createPTreeFromXMLString(xml));
}

void CLocalWorkUnit::serialize(MemoryBuffer &tgt)
{
    CriticalBlock block(crit);
    StringBuffer x;
    tgt.append(exportWorkUnitToXML(this, x, false).str());
}

void CLocalWorkUnit::deserialize(MemoryBuffer &src)
{
    CriticalBlock block(crit);
    StringAttr value;
    src.read(value);
    loadXML(value);
}

void CLocalWorkUnit::notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
{
    dirty = true;
}

void CLocalWorkUnit::abort()
{
    abortDirty = true;
}

void CLocalWorkUnit::requestAbort()
{
    CriticalBlock block(crit);
    abortWorkUnit(p->queryName());
}

void CLocalWorkUnit::subscribe(WUSubscribeOptions options)
{
    CriticalBlock block(crit);
    bool subscribeAbort = false;
    bool subscribeChange = false;
    bool needChildren = true;
    switch (options)
    {
    case SubscribeOptionAbort:
        subscribeAbort = true;
        break;
    case SubscribeOptionRunningState:
        needChildren = false;
    case SubscribeOptionAnyState:
        subscribeAbort = true;
        subscribeChange = true;
        break;
    case SubscribeOptionProgress:
    case SubscribeOptionAll:
        subscribeChange = true;
        break;
    }
    if (subscribeChange)
    {
        if (changeWatcher && changeWatcher->watchingChildren() != needChildren)
        {
            changeWatcher->unsubscribe();
            changeWatcher.clear();
        }
        if (!changeWatcher)
        {
            changeWatcher.setown(new CWorkUnitWatcher(this, p->queryName(), needChildren));
            dirty = true;
        }
    }
    if (subscribeAbort && !abortWatcher)
    {
        abortWatcher.setown(new CWorkUnitAbortWatcher(this, p->queryName()));
        abortDirty = true;
    }
}

#if 0 // I don't think this is used (I grepped the source), am leaving here just in case I've missed somewhere (PG)
WUState CLocalWorkUnit::waitComplete(int timeout, bool returnOnWaitState)
{
    class WorkUnitWaiter : public CInterface, implements ISDSSubscription, implements IAbortHandler
    {
        Semaphore changed;
        CLocalWorkUnit *parent;
    public:
        IMPLEMENT_IINTERFACE;

        WorkUnitWaiter(CLocalWorkUnit *_parent) : parent(_parent) { aborted = false; };

        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            parent->notify(id, xpath, flags, valueLen, valueData);
            changed.signal();
        }
        bool wait(unsigned timeout)
        {
            return changed.wait(timeout) && !aborted;
        }
        bool onAbort()
        {
            aborted = true;
            changed.signal();
            return false;
        }
        bool aborted;

    } waiter(this);
    Owned<CWorkUnitWatcher> w = new CWorkUnitWatcher(&waiter, p->queryName(), false);
    LocalIAbortHandler abortHandler(waiter);
    forceReload(); // or may miss changes that already happened, between load of wu and now.
    unsigned start = msTick();
    WUState ret;
    loop
    {
        ret = getState();
        switch (ret)
        {
        case WUStateWait:
            if(!returnOnWaitState)
                break;
            //fall thru
        case WUStateCompleted:
        case WUStateFailed:
        case WUStateAborted:
            w->unsubscribe();
            return ret;
        }
        unsigned waited = msTick() - start;
        if (timeout==-1)
        {
            waiter.wait(20000);
            if (waiter.aborted)
            {
                ret = WUStateUnknown;  // MORE - throw an exception?
                break;
            }
        }
        else if (waited > timeout || !waiter.wait(timeout-waited))
        {
            ret = WUStateUnknown;  // MORE - throw an exception?
            break;
        }
        reload();
    }
    w->unsubscribe();
    return ret;
}
#endif

void CLocalWorkUnit::forceReload()
{
    dirty = true;
    reload();
}

bool CLocalWorkUnit::reload()
{
    CriticalBlock block(crit);
    if (dirty)
    {
        if (!connectAtRoot)
        {
            StringBuffer wuRoot;
            getXPath(wuRoot, p->queryName());
            IRemoteConnection *newconn = factory->sdsManager->connect(wuRoot.str(), factory->session, 0, SDS_LOCK_TIMEOUT);
            if (!newconn)
                throw MakeStringException(WUERR_ConnectFailed, "Could not connect to workunit %s (deleted?)",p->queryName());
            connection.setown(newconn);
            connectAtRoot = true;
        }
        else
            connection->reload();
        init();
        p.setown(connection->getRoot());
        return true;
    }
    return false;
}

void CLocalWorkUnit::unsubscribe()
{
    CriticalBlock block(crit);
    if (abortWatcher)
    {
        abortWatcher->unsubscribe();
        abortWatcher.clear();
    }
    if (changeWatcher)
    {
        changeWatcher->unsubscribe();
        changeWatcher.clear();
    }
}

void CLocalWorkUnit::unlockRemote(bool commit)
{
    CriticalBlock block(crit);
    locked.unlock();
    if (commit)  
    {
        try
        {
            assertex(connectAtRoot);
            setTimeStamp("workunit", NULL, "Modified",false);
            try { connection->commit(); }
            catch (IException *e)
            { 
                EXCLOG(e, "Error during workunit commit");
                connection->rollback();
                connection->changeMode(0, SDS_LOCK_TIMEOUT);
                throw;
            }
            connection->changeMode(0, SDS_LOCK_TIMEOUT);
        }
        catch (IException *E)
        {
            StringBuffer s;
            PrintLog("Failed to release write lock on workunit: %s", E->errorMessage(s).str());
            throw;
        }
    }
}

IWorkUnit &CLocalWorkUnit::lockRemote(bool commit)
{
    if (secMgr)
        checkWuSecAccess(*this, *secMgr.get(), secUser.get(), SecAccess_Write, "write lock", true, true);
    locked.lock();
    CriticalBlock block(crit);
    if (commit)
    {
        try
        {
            StringBuffer wuRoot;
            getXPath(wuRoot, p->queryName());
            if (connection&&connectAtRoot) 
                connection->changeMode(RTM_LOCK_WRITE,SDS_LOCK_TIMEOUT);
            else 
                connection.setown(factory->sdsManager->connect(wuRoot.str(), factory->session, RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
            if (!connection)
                throw MakeStringException(WUERR_LockFailed, "Failed to get connection for xpath %s", wuRoot.str());
            connectAtRoot = true;
            init();
            p.setown(connection->getRoot());
        }
        catch (IException *E)
        {
            StringBuffer s;
            PrintLog("Failed to get write lock on workunit: %s", E->errorMessage(s).str());
            locked.unlock();
            throw;
        }
    }
    return *new CLockedWorkUnit(LINK(this));
}

void CLocalWorkUnit::commit()
{
    CriticalBlock block(crit);
    assertex(connectAtRoot);
    if (connection)
        connection->commit();
}

IWorkUnit& CLocalWorkUnit::lock()
{
    return lockRemote(true);
}

IStringVal& CLocalWorkUnit::getWuid(IStringVal &str) const
{
    CriticalBlock block(crit);
    str.set(p->queryName());
    return str;
}

unsigned CLocalWorkUnit::getDebugAgentListenerPort() const
{
    CriticalBlock block(crit);
    return p->getPropInt("@DebugListenerPort", 0);
}

void CLocalWorkUnit::setDebugAgentListenerPort(unsigned port)
{
    CriticalBlock block(crit);
    p->setPropInt("@DebugListenerPort", port);
}

IStringVal& CLocalWorkUnit::getDebugAgentListenerIP(IStringVal &ip) const
{
    CriticalBlock block(crit);
    ip.set(p->queryProp("@DebugListenerIP"));
    return ip;
}

void CLocalWorkUnit::setDebugAgentListenerIP(const char * ip)
{
    CriticalBlock block(crit);
    p->setProp("@DebugListenerIP", ip);
}

IStringVal& CLocalWorkUnit::getSecurityToken(IStringVal &str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@token"));
    return str;
}

void CLocalWorkUnit::setSecurityToken(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@token", value);
}

bool CLocalWorkUnit::getRunningGraph(IStringVal &graphName, WUGraphIDType &subId) const
{
    return CConstGraphProgress::getRunningGraph(p->queryName(), graphName, subId);
}

void CLocalWorkUnit::setJobName(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@jobName", value);
}

IStringVal& CLocalWorkUnit::getJobName(IStringVal &str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@jobName"));
    return str;
}

void CLocalWorkUnit::setClusterName(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@clusterName", value);
}

IStringVal& CLocalWorkUnit::getClusterName(IStringVal &str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@clusterName"));
    return str;
}

void CLocalWorkUnit::setAllowedClusters(const char *value)
{
    setDebugValue("allowedclusters",value, true);
}

IStringVal& CLocalWorkUnit::getAllowedClusters(IStringVal &str) const
{
    CriticalBlock block(crit);
    getDebugValue("allowedclusters",str);
    if (str.length()!=0)
        return str;
    str.set(p->queryProp("@clusterName"));
    return str;
}

void CLocalWorkUnit::setAllowAutoQueueSwitch(bool val)
{ 
    setDebugValueInt("allowautoqueueswitch",val?1:0,true);
}
    
bool CLocalWorkUnit::getAllowAutoQueueSwitch() const
{ 
    CriticalBlock block(crit);
    return getDebugValueBool("allowautoqueueswitch",false);
}


void CLocalWorkUnit::setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash)
{
    StringBuffer suffix;

    if (name && *name)
        setApplicationValue("LibraryModule", "name", name, true);
    setApplicationValueInt("LibraryModule", "interfaceHash", interfaceHash, true);
    setApplicationValueInt("LibraryModule", "definitionHash", definitionHash, true);
    setApplicationValue("LibraryModule", "platform", appendLibrarySuffix(suffix).str(), true);
}

void CLocalWorkUnit::remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const
{
    unsigned auditflags = DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED;
    if (writeaccess)
        auditflags |= DALI_LDAP_WRITE_WANTED;
    int perm = 255;
    const char *scopename = p->queryProp("@scope");
    if (scopename&&*scopename) {
        if (!user)
            user = queryUserDescriptor();
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0) {
            if (perm==-1) 
                perm = 255;
            else 
                perm = 0;
        }
    }
    if (!HASREADPERMISSION(perm)) {
        SCMStringBuffer wuid;
        getWuid(wuid);
        throw MakeStringException(WUERR_WorkunitAccessDenied, "Read access denied for workunit %s",wuid.s.str());
    }
    if (writeaccess&&!HASWRITEPERMISSION(perm)) {
        SCMStringBuffer wuid;
        getWuid(wuid);
        throw MakeStringException(WUERR_WorkunitAccessDenied, "Write access denied for workunit %s",wuid.s.str());
    }
}


IStringVal& CLocalWorkUnit::getParentWuid(IStringVal &str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@parent"));
    return str;
}

void CLocalWorkUnit::setUser(const char * value) 
{ 
    CriticalBlock block(crit);
    p->setProp("@submitID", value); 
}

IStringVal& CLocalWorkUnit::getUser(IStringVal &str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@submitID"));
    return str;
}

void CLocalWorkUnit::setWuScope(const char * value) 
{ 
    CriticalBlock block(crit);
    p->setProp("@scope", value); 
}

IStringVal& CLocalWorkUnit::getWuScope(IStringVal &str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@scope"));
    return str;
}

void CLocalWorkUnit::setCustomerId(const char * value) 
{ 
    CriticalBlock block(crit);
    p->setProp("CustomerID", value); 
}

IStringVal& CLocalWorkUnit::getCustomerId(IStringVal &str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("CustomerID"));
    return str;
}

mapEnums priorityClasses[] = {
   { PriorityClassUnknown, "unknown" },
   { PriorityClassLow, "low" },
   { PriorityClassNormal, "normal" },
   { PriorityClassHigh, "high" },
   { PriorityClassSize, NULL },
};

void CLocalWorkUnit::setPriority(WUPriorityClass cls) 
{
    CriticalBlock block(crit);
    setEnum(p, "@priorityClass", cls, priorityClasses);
}

WUPriorityClass CLocalWorkUnit::getPriority() const 
{
    CriticalBlock block(crit);
    return (WUPriorityClass) getEnum(p, "@priorityClass", priorityClasses);
}

mapEnums states[] = {
   { WUStateUnknown, "unknown" },
   { WUStateCompiled, "compiled" },
   { WUStateRunning, "running" },
   { WUStateCompleted, "completed" },
   { WUStateFailed, "failed" },
   { WUStateArchived, "archived" },
   { WUStateAborting, "aborting" },
   { WUStateAborted, "aborted" },
   { WUStateBlocked, "blocked" },
   { WUStateSubmitted, "submitted" },
   { WUStateScheduled, "scheduled" },
   { WUStateCompiling, "compiling" },
   { WUStateWait, "wait" },
   { WUStateUploadingFiles, "uploading_files" },
   { WUStateDebugPaused, "debugging" },
   { WUStateDebugRunning, "debug_running" },
   { WUStatePaused, "paused" },
   { WUStateSize, NULL }
};

IConstWorkUnitIterator * CWorkUnitFactory::getWorkUnitsByState(WUState state)
{
    StringBuffer path("*");
    path.append("[@state=\"").append(getEnumText(state, states)).append("\"]");
    return getWorkUnitsByXPath(path.str());
}
IConstWorkUnitIterator * CSecureWorkUnitFactory::getWorkUnitsByState(WUState state)
{
    StringBuffer path("*");
    path.append("[@state=\"").append(getEnumText(state, states)).append("\"]");
    return factory->getWorkUnitsByXPath(path.str(), secMgr.get(), secUser.get());
}

void CLocalWorkUnit::setState(WUState value) 
{
    CriticalBlock block(crit);
    if (value==WUStateAborted || value==WUStatePaused || value==WUStateCompleted || value==WUStateFailed || value==WUStateSubmitted || value==WUStateWait)
    {
        if (abortWatcher)
        {
            abortWatcher->unsubscribe();
            abortWatcher.clear();
        }
        StringBuffer apath;
        apath.append("/WorkUnitAborts/").append(p->queryName());
        if(factory)
        {
            Owned<IRemoteConnection> acon = factory->sdsManager->connect(apath.str(), factory->session, RTM_LOCK_WRITE|RTM_LOCK_SUB, SDS_LOCK_TIMEOUT);
            if (acon)
                acon->close(true);
        }
    }
    setEnum(p, "@state", value, states);
    if (getDebugValueBool("monitorWorkunit", false))
    {
        switch(value)
        {
        case WUStateAborted:
            FLLOG(MCoperatorWarning, "Workunit %s aborted", p->queryName());
            break;
        case WUStateCompleted:
            FLLOG(MCoperatorProgress, "Workunit %s completed", p->queryName());
            break;
        case WUStateFailed:
            FLLOG(MCoperatorProgress, "Workunit %s failed", p->queryName());
            break;
        }
    }
    p->removeProp("@stateEx");
}

void CLocalWorkUnit::setStateEx(const char * text)
{
    CriticalBlock block(crit);
    p->setProp("@stateEx", text);
}

void CLocalWorkUnit::setAgentSession(__int64 sessionId)
{
    CriticalBlock block(crit);
    p->setPropInt64("@agentSession", sessionId);
}

bool CLocalWorkUnit::aborting() const 
{
    CriticalBlock block(crit);
    if (abortDirty)
    {
        if (factory)
        {
            StringBuffer apath;
            apath.append("/WorkUnitAborts/").append(p->queryName());
            Owned<IRemoteConnection> acon = factory->sdsManager->connect(apath.str(), factory->session, 0, SDS_LOCK_TIMEOUT);
            if (acon)
                abortState = acon->queryRoot()->getPropInt(NULL)!=0;
            else
                abortState = false;
        }
        abortDirty = false;
    }
    return abortState;
}

bool CLocalWorkUnit::getIsQueryService() const 
{
    CriticalBlock block(crit);
    return p->getPropBool("@isQueryService", false);
}

void CLocalWorkUnit::setIsQueryService(bool value) 
{
    CriticalBlock block(crit);
    p->setPropBool("@isQueryService", value);
}

void CLocalWorkUnit::checkAgentRunning(WUState & state) 
{
    if (queryDaliServerVersion().compare("2.1")<0)
        return;
    switch(state)
    {
    case WUStateRunning:
    case WUStateDebugPaused:
    case WUStateDebugRunning:
    case WUStateBlocked:
    case WUStateAborting:
    case WUStateCompiling:
    case WUStatePaused:
        {
            SessionId agent = getAgentSession();
            if((agent>0) && querySessionManager().sessionStopped(agent, 0))
            {
                forceReload();
                state = (WUState) getEnum(p, "@state", states);
                bool isecl=state==WUStateCompiling;
                if (aborting())
                    state = WUStateAborted;
                else if (state==WUStateRunning || state==WUStatePaused || state==WUStateDebugPaused || state==WUStateDebugRunning || state==WUStateBlocked || state==WUStateCompiling)
                    state = WUStateFailed;
                else
                    return;
                WARNLOG("checkAgentRunning terminated: %"I64F"d state = %d",(__int64)agent,(int)state);
                Owned<IWorkUnit> w = &lock();
                w->setState(state);
                Owned<IWUException> e = w->createException();
                WUAction action = w->getAction();
                switch (action)
                {
                    case WUActionPause:
                    case WUActionPauseNow:
                    case WUActionResume:
                        w->setAction(WUActionUnknown);
                }
                if(isecl)
                {
                    e->setExceptionCode(1001);
                    e->setExceptionMessage("EclServer terminated unexpectedly");
                }
                else
                {
                    e->setExceptionCode(1000);
                    e->setExceptionMessage("Workunit terminated unexpectedly");
                }
            }
        }
    }
}

WUState CLocalWorkUnit::getState() const 
{
    CriticalBlock block(crit);
    WUState state = (WUState) getEnum(p, "@state", states);
    switch (state)
    {
    case WUStateRunning:
    case WUStateDebugPaused:
    case WUStateDebugRunning:
    case WUStateBlocked:
    case WUStateCompiling:
        if (aborting())
            state = WUStateAborting;
        break;
    case WUStateSubmitted:
        if (aborting())
            state = WUStateAborted;
        break;
    }
    const_cast<CLocalWorkUnit *>(this)->checkAgentRunning(state); //need const_cast as will change state if agent has died
    return state;
}

IStringVal& CLocalWorkUnit::getStateEx(IStringVal & str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@stateEx"));
    return str;
}

__int64 CLocalWorkUnit::getAgentSession() const
{
    CriticalBlock block(crit);
    return p->getPropInt64("@agentSession", -1);
}

unsigned CLocalWorkUnit::getAgentPID() const
{
    CriticalBlock block(crit);
    return p->getPropInt("@agentPID", -1);
}

IStringVal& CLocalWorkUnit::getStateDesc(IStringVal &str) const 
{
    // MORE - not sure about this - may prefer a separate interface
    CriticalBlock block(crit);
    try
    {
        str.set(getEnumText(getState(), states));
    }
    catch (...)
    {
        str.set("???");
    }
    return str;
}

mapEnums actions[] = {
   { WUActionUnknown, "unknown" },
   { WUActionCompile, "compile" },
   { WUActionCheck, "check" },
   { WUActionRun, "run" },
   { WUActionExecuteExisting, "execute" },
   { WUActionPause, "pause" },
   { WUActionPauseNow, "pausenow" },
   { WUActionResume, "resume" },
   { WUActionSize, NULL },
};

void CLocalWorkUnit::setAction(WUAction value) 
{
    CriticalBlock block(crit);
    setEnum(p, "Action", value, actions);
}

WUAction CLocalWorkUnit::getAction() const 
{
    CriticalBlock block(crit);
    return (WUAction) getEnum(p, "Action", actions);
}

IStringVal& CLocalWorkUnit::getActionEx(IStringVal & str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("Action"));
    return str;
}

IStringVal& CLocalWorkUnit::getApplicationValue(const char *app, const char *propname, IStringVal &str) const
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    str.set(p->queryProp(prop.str())); 
    return str;
}

int CLocalWorkUnit::getApplicationValueInt(const char *app, const char *propname, int defVal) const
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    return p->getPropInt(prop.str(), defVal); 
}

IConstWUAppValueIterator& CLocalWorkUnit::getApplicationValues() const
{
    CriticalBlock block(crit);
    appvalues.load(p,"Application/*");
    return *new CArrayIteratorOf<IConstWUAppValue,IConstWUAppValueIterator> (appvalues, 0, (IConstWorkUnit *) this);
}


void CLocalWorkUnit::setApplicationValue(const char *app, const char *propname, const char *value, bool overwrite)
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure these lines should be needed....
        StringBuffer sp;
        p->setProp(sp.append("Application").str(), ""); 
        p->setProp(sp.append('/').append(app).str(), ""); 
        p->setProp(prop.str(), value); 
    }
}

void CLocalWorkUnit::setApplicationValueInt(const char *app, const char *propname, int value, bool overwrite)
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure these lines should be needed....
        StringBuffer sp;
        p->setProp(sp.append("Application").str(), ""); 
        p->setProp(sp.append('/').append(app).str(), ""); 
        p->setPropInt(prop.str(), value); 
    }
}

void CLocalWorkUnit::setPriorityLevel(int level) 
{
    CriticalBlock block(crit);
    p->setPropInt("PriorityFlag",  level);
}

int CLocalWorkUnit::getPriorityLevel() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("PriorityFlag"); 
}

int calcPriorityValue(const IPropertyTree * p)
{
    int priority = p->getPropInt("PriorityFlag");
    switch((WUPriorityClass) getEnum(p, "@priorityClass", priorityClasses))
    {
    case PriorityClassLow:
        priority -= 100;
        break;
    case PriorityClassHigh:
        priority += 100;
        break;
    }
    return priority;
}


int CLocalWorkUnit::getPriorityValue() const 
{
    CriticalBlock block(crit);
    return calcPriorityValue(p);
}

void CLocalWorkUnit::setRescheduleFlag(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("RescheduleFlag", (int) value); 
}

bool CLocalWorkUnit::getRescheduleFlag() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("RescheduleFlag") != 0; 
}

class NullIStringIterator : public CInterface, extends IStringIterator
{
public:
    IMPLEMENT_IINTERFACE;
    bool first() { return false; }
    bool next()  { return false; }
    bool isValid()  { return false; }
    IStringVal & str(IStringVal & str) { return str; }

};

ClusterType getClusterType(const char * platform, ClusterType dft)
{
    if (stricmp(platform, "thor") == 0)
        return ThorLCRCluster;
    if (stricmp(platform, "thorlcr") == 0)
        return ThorLCRCluster;
    if (stricmp(platform, "hthor") == 0)
        return HThorCluster;
    if (stricmp(platform, "roxie") == 0)
        return RoxieCluster;
    return dft;
}

const char *clusterTypeString(ClusterType clusterType, bool lcrSensitive)
{
    switch (clusterType)
    {
    case ThorLCRCluster:
        if (lcrSensitive)
            return "thorlcr";
        return "thor";
    case RoxieCluster:
        return "roxie";
    case HThorCluster:
        return "hthor";
    }
    throwUnexpected();
}

IPropertyTree *queryRoxieProcessTree(IPropertyTree *environment, const char *process)
{
    if (!process || !*process)
        return NULL;
    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", process);
    return environment->queryPropTree(xpath.str());
}

void getRoxieProcessServers(IPropertyTree *roxie, SocketEndpointArray &endpoints)
{
    if (!roxie)
        return;
    Owned<IPropertyTreeIterator> servers = roxie->getElements("RoxieServerProcess");
    ForEach(*servers)
    {
        IPropertyTree &server = servers->query();
        const char *netAddress = server.queryProp("@netAddress");
        if (netAddress && *netAddress)
        {
            SocketEndpoint ep(netAddress, server.getPropInt("@port", 9876));
            endpoints.append(ep);
        }
    }
}

void getRoxieProcessServers(const char *process, SocketEndpointArray &servers)
{
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return;
    getRoxieProcessServers(queryRoxieProcessTree(conn->queryRoot(), process), servers);
}

class CEnvironmentClusterInfo: public CInterface, implements IConstWUClusterInfo
{
    StringAttr name;
    StringAttr serverQueue;
    StringAttr agentQueue;
    StringAttr roxieProcess;
    SocketEndpointArray roxieServers;
    StringAttr thorQueue;
    StringArray thorProcesses;
    StringAttr prefix;
    ClusterType platform;
    unsigned clusterWidth;
public:
    IMPLEMENT_IINTERFACE;
    CEnvironmentClusterInfo(const char *_name, const char *_prefix, IPropertyTree *agent, IArrayOf<IPropertyTree> &thors, IPropertyTree *roxie)
        : name(_name), prefix(_prefix)
    {
        StringBuffer queue;
        if (thors.ordinality())
        {
            thorQueue.set(getClusterThorQueueName(queue.clear(), name));
            clusterWidth = 0;
            ForEachItemIn(i,thors) 
            {
                IPropertyTree &thor = thors.item(i);
                thorProcesses.append(thor.queryProp("@name"));
                unsigned nodes = thor.getCount("ThorSlaveProcess");
                if (!nodes)
                    throw MakeStringException(WUERR_MismatchClusterSize,"CEnvironmentClusterInfo: Thor cluster can not have 0 slave processes");
                unsigned ts = nodes * thor.getPropInt("@slavesPerNode", 1);
                if (clusterWidth && (ts!=clusterWidth)) 
                    throw MakeStringException(WUERR_MismatchClusterSize,"CEnvironmentClusterInfo: mismatched thor sizes in cluster");
                clusterWidth = ts;
                bool islcr = !thor.getPropBool("@Legacy");
                if (!islcr)
                    throw MakeStringException(WUERR_MismatchThorType,"CEnvironmentClusterInfo: Legacy Thor no longer supported");
            }
            platform = ThorLCRCluster;
        }
        else if (roxie)
        {
            roxieProcess.set(roxie->queryProp("@name"));
            clusterWidth = roxie->getPropInt("@numChannels", 1);
            platform = RoxieCluster;
            getRoxieProcessServers(roxie, roxieServers);
        }
        else 
        {
            clusterWidth = 1;
            platform = HThorCluster;
        }

        if (agent)
        {
            assertex(!roxie);
            agentQueue.set(getClusterEclAgentQueueName(queue.clear(), name));
        }
        else if (roxie)
            agentQueue.set(getClusterRoxieQueueName(queue.clear(), name));
        // MORE - does this need to be conditional?
        serverQueue.set(getClusterEclCCServerQueueName(queue.clear(), name));
    }
    IStringVal & getName(IStringVal & str) const
    {
        str.set(name.get());
        return str;
    }
    IStringVal & getScope(IStringVal & str) const
    {
        str.set(prefix.get());
        return str;
    }
    IStringVal & getAgentQueue(IStringVal & str) const
    {
        str.set(agentQueue);
        return str;
    }
    virtual IStringVal & getServerQueue(IStringVal & str) const
    {
        str.set(serverQueue);
        return str;
    }
    IStringVal & getThorQueue(IStringVal & str) const
    {
        str.set(thorQueue);
        return str;
    }
    unsigned getSize() const 
    {
        return clusterWidth;
    }
    virtual ClusterType getPlatform() const
    {
        return platform;
    }
    IStringVal & getRoxieProcess(IStringVal & str) const
    {
        str.set(roxieProcess.get());
        return str;
    }
    const StringArray & getThorProcesses() const
    {
        return thorProcesses;
    }

    const SocketEndpointArray & getRoxieServers() const
    {
        return roxieServers;
    }
};

IStringVal &getProcessQueueNames(IStringVal &ret, const char *process, const char *type, const char *suffix)
{
    if (process)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            StringBuffer queueNames;
            StringBuffer xpath;
            xpath.appendf("%s[@process=\"%s\"]", type, process);
            Owned<IPropertyTreeIterator> targets = conn->queryRoot()->getElements("Software/Topology/Cluster");
            ForEach(*targets)
            {
                IPropertyTree &target = targets->query();
                if (target.hasProp(xpath))
                {
                    if (queueNames.length())
                        queueNames.append(',');
                    queueNames.append(target.queryProp("@name")).append(suffix);
                }
            }
            ret.set(queueNames);
        }
    }
    return ret;
}

#define ROXIE_QUEUE_EXT ".roxie"
#define THOR_QUEUE_EXT ".thor"
#define ECLCCSERVER_QUEUE_EXT ".eclserver"
#define ECLSERVER_QUEUE_EXT ECLCCSERVER_QUEUE_EXT
#define ECLSCHEDULER_QUEUE_EXT ".eclscheduler"
#define ECLAGENT_QUEUE_EXT ".agent"

extern WORKUNIT_API IStringVal &getEclCCServerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclCCServerProcess", ECLCCSERVER_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getEclServerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclServerProcess", ECLSERVER_QUEUE_EXT); // shares queue name with EclCCServer
}

extern WORKUNIT_API IStringVal &getEclSchedulerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclSchedulerProcess", ECLSCHEDULER_QUEUE_EXT); // Shares deployment/config with EclCCServer
}

extern WORKUNIT_API IStringVal &getAgentQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclAgentProcess", ECLAGENT_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getRoxieQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "RoxieCluster", ROXIE_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getThorQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "ThorCluster", THOR_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterThorQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(THOR_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterThorGroupName(StringBuffer &ret, const char *cluster)
{
    StringBuffer path;
    Owned<IRemoteConnection> conn = querySDS().connect(path.append("Environment/Software/ThorCluster[@name=\"").append(cluster).append("\"]").str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        getClusterGroupName(*conn->queryRoot(), ret);
    }

    return ret;
}

extern WORKUNIT_API StringBuffer &getClusterRoxieQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ROXIE_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclCCServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLCCSERVER_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLSERVER_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclAgentQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLAGENT_QUEUE_EXT);
}

extern WORKUNIT_API IStringIterator *getTargetClusters(const char *processType, const char *processName)
{
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    Owned<CStringArrayIterator> ret = new CStringArrayIterator;
    if (conn)
    {
        StringBuffer xpath;
        xpath.appendf("%s", processType ? processType : "*");
        if (processName)
            xpath.appendf("[@process=\"%s\"]", processName);
        Owned<IPropertyTreeIterator> targets = conn->queryRoot()->getElements("Software/Topology/Cluster");
        ForEach(*targets)
        {
            IPropertyTree &target = targets->query();
            if (target.hasProp(xpath))
            {
                ret->append(target.queryProp("@name"));
            }
        }
    }
    return ret.getClear();
}

extern WORKUNIT_API bool isProcessCluster(const char *process)
{
    if (!process || !*process)
        return false;
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return false;
    VStringBuffer xpath("Software/*Cluster[@name=\"%s\"]", process);
    return conn->queryRoot()->hasProp(xpath.str());
}

extern WORKUNIT_API bool isProcessCluster(const char *remoteDali, const char *process)
{
    if (!remoteDali || !*remoteDali)
        return isProcessCluster(process);
    if (!process || !*process)
        return false;
    Owned<INode> remote = createINode(remoteDali, 7070);
    if (!remote)
        return false;

    VStringBuffer xpath("Environment/Software/*Cluster[@name=\"%s\"]/@name", process);
    try
    {
        Owned<IPropertyTreeIterator> clusters = querySDS().getElementsRaw(xpath, remote, 1000*60*1);
        return clusters->first();
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        DBGLOG("Exception validating cluster %s/%s: %s", remoteDali, xpath.str(), msg.str());
        E->Release();
    }
    return true;
}

IConstWUClusterInfo* getTargetClusterInfo(IPropertyTree *environment, IPropertyTree *cluster)
{
    const char *clustname = cluster->queryProp("@name");

    // MORE - at the moment configenf specifies eclagent and thor queues by (in effect) placing an 'example' thor or eclagent in the topology 
    // that uses the queue that will be used.
    // We should and I hope will change that, at which point the code below gets simpler

    StringBuffer prefix(cluster->queryProp("@prefix"));
    prefix.toLowerCase();

    StringBuffer xpath;
    StringBuffer querySetName;
    
    IPropertyTree *agent = NULL;
    const char *agentName = cluster->queryProp("EclAgentProcess/@process");
    if (agentName) 
    {
        xpath.clear().appendf("Software/EclAgentProcess[@name=\"%s\"]", agentName);
        agent = environment->queryPropTree(xpath.str());
    }
    Owned<IPropertyTreeIterator> ti = cluster->getElements("ThorCluster");
    IArrayOf<IPropertyTree> thors;
    ForEach(*ti) 
    {
        const char *thorName = ti->query().queryProp("@process");
        if (thorName) 
        {
            xpath.clear().appendf("Software/ThorCluster[@name=\"%s\"]", thorName);
            thors.append(*environment->getPropTree(xpath.str()));
        }
    }
    const char *roxieName = cluster->queryProp("RoxieCluster/@process");
    return new CEnvironmentClusterInfo(clustname, prefix, agent, thors, queryRoxieProcessTree(environment, roxieName));
}

IPropertyTree* getTopologyCluster(Owned<IRemoteConnection> &conn, const char *clustname)
{
    if (!clustname || !*clustname)
        return NULL;
    conn.setown(querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT));
    if (!conn)
        return NULL;
    StringBuffer xpath;
    xpath.appendf("Software/Topology/Cluster[@name=\"%s\"]", clustname);
    return conn->queryRoot()->getPropTree(xpath.str());
}

bool validateTargetClusterName(const char *clustname)
{
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> cluster = getTopologyCluster(conn, clustname);
    return (cluster.get()!=NULL);
}

IConstWUClusterInfo* getTargetClusterInfo(const char *clustname)
{
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> cluster = getTopologyCluster(conn, clustname);
    if (!cluster)
        return NULL;
    return getTargetClusterInfo(conn->queryRoot(), cluster);
}

unsigned getEnvironmentClusterInfo(CConstWUClusterInfoArray &clusters)
{
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;

    return getEnvironmentClusterInfo(conn->queryRoot(), clusters);
}

unsigned getEnvironmentClusterInfo(IPropertyTree* environmentRoot, CConstWUClusterInfoArray &clusters)
{
    if (!environmentRoot)
        return 0;

    Owned<IPropertyTreeIterator> clusterIter = environmentRoot->getElements("Software/Topology/Cluster");
    ForEach(*clusterIter)
    {
        IPropertyTree &node = clusterIter->query();
        Owned<IConstWUClusterInfo> cluster = getTargetClusterInfo(environmentRoot, &node);
        clusters.append(*cluster.getClear());
    }
    return clusters.ordinality();
}

const char *getTargetClusterComponentName(const char *clustname, const char *processType, StringBuffer &name)
{
    if (!clustname)
        return NULL;
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    StringBuffer xpath;

    xpath.appendf("Software/Topology/Cluster[@name=\"%s\"]", clustname);
    Owned<IPropertyTree> cluster = conn->queryRoot()->getPropTree(xpath.str());
    if (!cluster) 
        return NULL;

    StringBuffer xpath1;
    xpath1.appendf("%s/@process", processType);
    name.append(cluster->queryProp(xpath1.str()));
    return name.str();
}

unsigned getEnvironmentThorClusterNames(StringArray &thorNames, StringArray &groupNames, StringArray &targetNames, StringArray &queueNames)
{
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;
    Owned<IPropertyTreeIterator> allTargets = conn->queryRoot()->getElements("Software/Topology/Cluster");
    ForEach(*allTargets)
    {
        IPropertyTree &target = allTargets->query();
        const char *targetName = target.queryProp("@name");
        if (targetName && *targetName)
        {
            Owned<IPropertyTreeIterator> thorClusters = target.getElements("ThorCluster");
            ForEach(*thorClusters)
            {
                const char *thorName = thorClusters->query().queryProp("@process");
                VStringBuffer query("Software/ThorCluster[@name=\"%s\"]",thorName);
                IPropertyTree *thorCluster = conn->queryRoot()->queryPropTree(query.str());
                if (thorCluster)
                {
                    const char *groupName = thorCluster->queryProp("@nodeGroup");
                    if (!groupName||!*groupName)
                        groupName = thorName;
                    thorNames.append(thorName);
                    groupNames.append(groupName);
                    targetNames.append(targetName);
                    StringBuffer queueName(targetName);
                    queueNames.append(queueName.append(THOR_QUEUE_EXT));
                }
            }
        }
    }
    return thorNames.ordinality();
}


unsigned getEnvironmentHThorClusterNames(StringArray &eclAgentNames, StringArray &groupNames, StringArray &targetNames)
{
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;
    Owned<IPropertyTreeIterator> allEclAgents = conn->queryRoot()->getElements("Software/EclAgentProcess");
    ForEach(*allEclAgents)
    {
        IPropertyTree &eclAgent = allEclAgents->query();
        const char *eclAgentName = eclAgent.queryProp("@name");
        if (eclAgentName && *eclAgentName)
        {
            Owned<IPropertyTreeIterator> allTargets = conn->queryRoot()->getElements("Software/Topology/Cluster");
            ForEach(*allTargets)
            {
                IPropertyTree &target = allTargets->query();
                const char *targetName = target.queryProp("@name");
                if (targetName && *targetName)
                {
                    StringBuffer xpath;
                    xpath.appendf("EclAgentProcess[@process=\"%s\"]", eclAgentName);
                    if (target.hasProp(xpath) && !target.hasProp("ThorCluster"))
                    {
                        StringBuffer groupName("hthor__");
                        groupName.append(eclAgentName);

                        groupNames.append(groupName);
                        eclAgentNames.append(eclAgentName);
                        targetNames.append(targetName);
                    }
                }
            }
        }
    }
    return eclAgentNames.ordinality();
}


IStringVal& CLocalWorkUnit::getScope(IStringVal &str) const 
{
    CriticalBlock block(crit);
    if (p->hasProp("Debug/ForceScope"))
    {
        StringBuffer prefix(p->queryProp("Debug/ForceScope"));
        str.set(prefix.toLowerCase().str()); 
    }
    else
    {
        Owned <IConstWUClusterInfo> ci = getTargetClusterInfo(p->queryProp("@clusterName"));
        if (ci)
            ci->getScope(str); 
        else
            str.clear();
    }
    return str;
}

//Queries
void CLocalWorkUnit::setCodeVersion(unsigned codeVersion, const char * buildVersion, const char * eclVersion) 
{
    CriticalBlock block(crit);
    p->setPropInt("@codeVersion", codeVersion);
    p->setProp("@buildVersion", buildVersion);
    p->setProp("@eclVersion", eclVersion);
}

unsigned CLocalWorkUnit::getCodeVersion() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("@codeVersion");
}

unsigned CLocalWorkUnit::getWuidVersion() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("@wuidVersion");
}

void CLocalWorkUnit::getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const 
{
    CriticalBlock block(crit);
    buildVersion.set(p->queryProp("@buildVersion"));
    eclVersion.set(p->queryProp("@eclVersion"));
}

void CLocalWorkUnit::setCloneable(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("@cloneable", value);
}

void CLocalWorkUnit::setIsClone(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("@isClone", value);
}

bool CLocalWorkUnit::getCloneable() const 
{
    CriticalBlock block(crit);
    return p->getPropBool("@cloneable", false);
}

IUserDescriptor *CLocalWorkUnit::queryUserDescriptor() const
{
    CriticalBlock block(crit);
    if (!userDesc)
    {
        SCMStringBuffer token, user, password;
        getSecurityToken(token);
        SCMStringBuffer wuid;
        getWuid(wuid);
        extractToken(token.str(), wuid.str(), user, password);
        userDesc.setown(createUserDescriptor());
        userDesc->set(user.str(), password.str());
    }
    return userDesc;
}

void CLocalWorkUnit::setCombineQueries(unsigned combine) 
{
    CriticalBlock block(crit);
    p->setPropInt("COMBINE_QUERIES", combine);
}

unsigned CLocalWorkUnit::getCombineQueries() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("COMBINE_QUERIES");
}

bool CLocalWorkUnit::isProtected() const
{
    CriticalBlock block(crit);
    return p->getPropBool("@protected", false);
}

bool CLocalWorkUnit::isPausing() const
{
    CriticalBlock block(crit);
    if (WUActionPause == getAction())
    {
        switch (getState())
        {
            case WUStateRunning:
            case WUStateAborting:
                return true;
        }
    }
    return false;
}

void CLocalWorkUnit::protect(bool protectMode)
{
    CriticalBlock block(crit);
    p->setPropBool("@protected", protectMode);
}

bool CLocalWorkUnit::isBilled() const
{
    CriticalBlock block(crit);
    return p->getPropBool("@billed", false);
}

void CLocalWorkUnit::setBilled(bool value)
{
    CriticalBlock block(crit);
    p->setPropBool("@billed", value);
}

void CLocalWorkUnit::setResultLimit(unsigned value)
{
    CriticalBlock block(crit);
    p->setPropInt("resultLimit", value);
}

unsigned CLocalWorkUnit::getResultLimit() const
{
    CriticalBlock block(crit);
    return p->getPropInt("resultLimit");
}

void CLocalWorkUnit::setCompareMode(WUCompareMode value)
{
    CriticalBlock block(crit);
    p->setPropInt("comparemode", (int)value);
}

WUCompareMode CLocalWorkUnit::getCompareMode() const
{
    CriticalBlock block(crit);
    return (WUCompareMode) p->getPropInt("comparemode");
}

IStringVal & CLocalWorkUnit::getSnapshot(IStringVal & str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("SNAPSHOT")); 
    return str;
}

void CLocalWorkUnit::setSnapshot(const char * val)
{
    CriticalBlock block(crit);
    p->setProp("SNAPSHOT", val);
}

static int comparePropTrees(IInterface **ll, IInterface **rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return stricmp(l->queryName(), r->queryName());
};

unsigned CLocalWorkUnit::calculateHash(unsigned crc)
{
    // Any other values in the WU that could affect generated code should be crc'ed here
    IPropertyTree *tree = p->queryBranch("Debug");
    if (tree)
    {
        Owned<IPropertyTreeIterator> sub = tree->getElements("*");
        ICopyArrayOf<IPropertyTree> subs;
        for(sub->first(); sub->isValid(); sub->next())
            subs.append(sub->query());
        subs.sort(comparePropTrees);
        ForEachItemIn(idx, subs)
        {
            const char *name = subs.item(idx).queryName();
            const char *val = subs.item(idx).queryProp(NULL);
            crc = crc32(name, (size32_t)strlen(name), crc);
            if (val)
                crc = crc32(val, (size32_t)strlen(val), crc);
        }
    }
    Owned<IConstWUPluginIterator> plugins = &getPlugins();
    for (plugins->first();plugins->isValid();plugins->next())
    {
        IConstWUPlugin &thisplugin = plugins->query();
        SCMStringBuffer version;
        thisplugin.getPluginVersion(version);
        crc = crc32(version.str(), version.length(), crc);
    }
    return crc;
}

static void updateProp(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    if (!to->hasProp(xpath) && from->hasProp(xpath))
        to->setProp(xpath, from->queryProp(xpath));
}

static void setProp(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    if (from->hasProp(xpath))
        to->setProp(xpath, from->queryProp(xpath));
}

static void copyTree(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    IPropertyTree * match = from->getBranch(xpath); 
    if (match) 
        to->setPropTree(xpath, match);
}

void CLocalWorkUnit::copyWorkUnit(IConstWorkUnit *cached, bool all)
{
    CLocalWorkUnit *from = QUERYINTERFACE(cached, CLocalWorkUnit);
    if (!from)
    {
        CLockedWorkUnit *fl = QUERYINTERFACE(cached, CLockedWorkUnit);
        if (!fl)
            throw MakeStringException(WUERR_InternalUnknownImplementation, "Cached workunit not created using workunit dll");
        from = fl->c;
    }
    // Need to copy the query, the results, and the graphs from the cached query.
    // The cache is made before the query is executed so there is no need to clear them.
    if (!cached->getCloneable())
        throw MakeStringException(WUERR_CannotCloneWorkunit, "Source work unit not marked as clonable");

    const IPropertyTree * fromP = from->p;
    IPropertyTree *pt;

    CriticalBlock block(crit);
    query.clear();
    updateProp(p, fromP, "@jobName");
    copyTree(p, fromP, "Query");
    pt = fromP->getBranch("Application/LibraryModule"); 
    if (pt)
    {
        ensurePTree(p, "Application");
        p->setPropTree("Application/LibraryModule", pt);
    }

    pt = fromP->queryBranch("Debug"); 
    if (pt)
    {
        IPropertyTree *curDebug = p->queryPropTree("Debug");
        if (curDebug)
        {
            Owned<IPropertyTreeIterator> elems = pt->getElements("*");
            ForEach(*elems)
            {
                IPropertyTree *elem = &elems->query();
                if (!curDebug->hasProp(elem->queryName()))
                    curDebug->setPropTree(elem->queryName(),LINK(elem));
            }
        }
        else
            p->setPropTree("Debug", LINK(pt));
    }
    copyTree(p, fromP, "Plugins");
    copyTree(p, fromP, "Libraries");
    copyTree(p, fromP, "Results");
    copyTree(p, fromP, "Graphs");
    copyTree(p, fromP, "Workflow");
    if (all)
    {
        // 'all' mode is used when setting up a dali WU from the embedded wu in a workunit dll

        // Merge timing info from both branches
        pt = fromP->getBranch("Timings");
        if (pt)
        {
            IPropertyTree *tgtTimings = ensurePTree(p, "Timings");
            mergePTree(tgtTimings, pt);
            pt->Release();
        }
        pt = fromP->getBranch("Statistics");
        if (pt)
        {
            IPropertyTree *tgtStatistics = ensurePTree(p, "Statistics");
            mergePTree(tgtStatistics, pt);
            pt->Release();
        }
    }

    updateProp(p, fromP, "@clusterName");
    updateProp(p, fromP, "allowedclusters");
    updateProp(p, fromP, "@submitID");
    updateProp(p, fromP, "CustomerID");
    updateProp(p, fromP, "SNAPSHOT");

    //MORE: This is very adhoc.  All options that should be cloned should really be in a common branch
    if (all)
    {
        setProp(p, fromP, "PriorityFlag");
        setProp(p, fromP, "@priorityClass");
        setProp(p, fromP, "@protected");
        setProp(p, fromP, "@clusterName");
        updateProp(p, fromP, "@scope");
    }

    //Variables may have been set up as parameters to the query - so need to preserve any values that were supplied.
    pt = fromP->getBranch("Variables");
    if (pt)
    {
        IPropertyTree *ptTgtVariables = ensurePTree(p, "Variables");

        Owned<IPropertyTreeIterator> ptiVariable = pt->getElements("Variable");
        for (ptiVariable->first(); ptiVariable->isValid(); ptiVariable->next())
        {
            IPropertyTree *ptSrcVariable = &ptiVariable->query();
            const char *name = ptSrcVariable->queryProp("@name");
            assertex(name);
            StringBuffer xpath;
            xpath.append("Variable[@name='").append(name).append("']");
            IPropertyTree *ptTgtVariable = ptTgtVariables->queryPropTree(xpath.str());
            IPropertyTree *merged = createPTreeFromIPT(ptSrcVariable); // clone entire source info...
            merged->removeProp("Value"); // except value and status
            merged->setProp("@status", "undefined");
            if (!merged->getPropBool("@isScalar"))
                merged->removeProp("totalRowCount");
            merged->removeProp("rowCount");
            // If there are any other fields that get set ONLY by eclagent, strip them out here...

            if (ptTgtVariable)
            {
                // copy status and Value from what is already set in target
                merged->setProp("@status", ptTgtVariable->queryProp("@status"));
                MemoryBuffer value;
                if (ptTgtVariable->getPropBin("Value", value))
                    merged->setPropBin("Value", value.length(), value.toByteArray());
                ptTgtVariable->removeProp(xpath.str());

                // If there are any other fields in a variable that get set by ws_ecl before submitting, copy them across here...
            }
            ptTgtVariables->addPropTree("Variable", merged);
        }
        pt->Release();
    }

    p->setProp("@codeVersion", fromP->queryProp("@codeVersion"));
    p->setPropBool("@cloneable", true);
    p->setPropBool("@isClone", true);
    resetWorkflow();  // the source Workflow section may have had some parts already executed...
    // resetResults(); // probably should be resetting the results as well... rather than waiting for the rerun to overwrite them
}

bool CLocalWorkUnit::hasDebugValue(const char *propname) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    return p->hasProp(prop.append(lower));
}

IStringVal& CLocalWorkUnit::getDebugValue(const char *propname, IStringVal &str) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    str.set(p->queryProp(prop.append(lower).str())); 
    return str;
}

IStringIterator& CLocalWorkUnit::getDebugValues() const
{
    return getDebugValues(NULL);
}

IStringIterator& CLocalWorkUnit::getDebugValues(const char *prop) const
{
    CriticalBlock block(crit);
    StringBuffer path("Debug/");
    if (prop)
    {
        StringBuffer lower;
        lower.append(prop).toLowerCase();
        path.append(lower);
    }
    else
        path.append("*");
    return *new CStringPTreeTagIterator(p->getElements(path.str()));
}

int CLocalWorkUnit::getDebugValueInt(const char *propname, int defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropInt(prop.str(), defVal); 
}

__int64 CLocalWorkUnit::getDebugValueInt64(const char *propname, __int64 defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropInt64(prop.str(), defVal); 
}

bool CLocalWorkUnit::getDebugValueBool(const char * propname, bool defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropBool(prop.str(), defVal); 
}

IStringIterator *CLocalWorkUnit::getLogs(const char *type, const char *instance) const
{
    VStringBuffer xpath("Process/%s/", type);
    if (instance)
        xpath.append(instance);
    else
        xpath.append("*");
    CriticalBlock block(crit);
    if (p->getPropInt("@wuidVersion") < 1) // legacy wuid
    {
        // NB: instance unused
        if (streq("EclAgent", type))
            return new CStringPTreeIterator(p->getElements("Debug/eclagentlog"));
        else if (streq("Thor", type))
            return new CStringPTreeIterator(p->getElements("Debug/thorlog*"));
        VStringBuffer xpath("Debug/%s", type);
        return new CStringPTreeIterator(p->getElements(xpath.str()));
    }
    else
        return new CStringPTreeAttrIterator(p->getElements(xpath.str()), "@log");
}

IPropertyTreeIterator& CLocalWorkUnit::getProcesses(const char *type, const char *instance) const
{
    VStringBuffer xpath("Process/%s/", type);
    if (instance)
        xpath.append(instance);
    else
        xpath.append("*");
    CriticalBlock block(crit);
    return * p->getElements(xpath.str());
}

IStringIterator *CLocalWorkUnit::getProcesses(const char *type) const
{
    VStringBuffer xpath("Process/%s/*", type);
    CriticalBlock block(crit);
    return new CStringPTreeTagIterator(p->getElements(xpath.str()));
}

void CLocalWorkUnit::addProcess(const char *type, const char *instance, unsigned pid, const char *log)
{
    VStringBuffer processType("Process/%s", type);
    VStringBuffer xpath("%s/%s", processType.str(), instance);
    if (log)
        xpath.appendf("[@log=\"%s\"]", log);
    CriticalBlock block(crit);
    if (!p->hasProp(xpath))
    {
        IPropertyTree *node = ensurePTree(p, processType.str());
        node = node->addPropTree(instance, createPTree());
        node->setProp("@log", log);
        node->setPropInt("@pid", pid);
    }
}

void CLocalWorkUnit::setDebugValue(const char *propname, const char *value, bool overwrite)
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure this line should be needed....
        p->setProp("Debug", ""); 
        p->setProp(prop.str(), value); 
    }
}

void CLocalWorkUnit::setDebugValueInt(const char *propname, int value, bool overwrite)
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure this line should be needed....
        p->setProp("Debug", ""); 
        p->setPropInt(prop.str(), value); 
    }
}

void CLocalWorkUnit::setTracingValue(const char *propname, const char *value)
{
    CriticalBlock block(crit);
    // MORE - not sure this line should be needed....
    p->setProp("Tracing", ""); 
    StringBuffer prop("Tracing/");
    p->setProp(prop.append(propname).str(), value); 
}

void CLocalWorkUnit::setTracingValueInt(const char *propname, int value)
{
    CriticalBlock block(crit);
    StringBuffer prop("Tracing/");
    p->setPropInt(prop.append(propname).str(), value); 
}

IConstWUQuery* CLocalWorkUnit::getQuery() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if (!query)
    {
        IPropertyTree *s = p->getPropTree("Query");
        if (s)
            query.setown(new CLocalWUQuery(s)); 
    }
    return query.getLink();
}

IWUQuery* CLocalWorkUnit::updateQuery()
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if (!query)
    {
        IPropertyTree *s = p->queryPropTree("Query");
        if (!s)
            s = p->addPropTree("Query", createPTreeFromXMLString("<Query fetchEntire='1'/>"));
        s->Link();
        query.setown(new CLocalWUQuery(s)); 
    }
    return query.getLink();
}

void CLocalWorkUnit::loadPlugins() const
{
    CriticalBlock block(crit);
    if (!pluginsCached)
    {
        assertex(plugins.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Plugins/Plugin");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            plugins.append(*new CLocalWUPlugin(rp));
        }
        pluginsCached = true;
    }
}

IConstWUPluginIterator& CLocalWorkUnit::getPlugins() const
{
    CriticalBlock block(crit);
    loadPlugins();
    return *new CArrayIteratorOf<IConstWUPlugin,IConstWUPluginIterator> (plugins, 0, (IConstWorkUnit *) this);
}

void CLocalWorkUnit::loadLibraries() const
{
    CriticalBlock block(crit);
    if (!librariesCached)
    {
        assertex(libraries.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Libraries/Library");
        ForEach(*r)
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            libraries.append(*new CLocalWULibrary(rp));
        }
        librariesCached = true;
    }
}

IConstWULibraryIterator& CLocalWorkUnit::getLibraries() const
{
    CriticalBlock block(crit);
    loadLibraries();
    return *new CArrayIteratorOf<IConstWULibrary,IConstWULibraryIterator> (libraries, 0, (IConstWorkUnit *) this);
}

IConstWULibrary * CLocalWorkUnit::getLibraryByName(const char * search) const
{
    CriticalBlock block(crit);
    loadLibraries();
    ForEachItemIn(idx, libraries)
    {
        SCMStringBuffer name;
        IConstWULibrary &cur = libraries.item(idx);
        cur.getName(name);
        if (stricmp(name.str(), search)==0)
            return &OLINK(cur);
    }
    return NULL;
}

unsigned CLocalWorkUnit::getTimerDuration(const char *name) const
{
    Owned<IConstWUStatistic> stat = getStatisticByDescription(name);
    if (stat)
    {
        unsigned __int64 time = stat->getValue();
        return (unsigned)(time / 1000000);
    }

    //Backward compatibility - but only use it if no statistics
    CriticalBlock block(crit);
    if (p->hasProp("Statistics"))
        return 0;

    StringBuffer pname;
    pname.appendf("Timings/Timing[@name=\"%s\"]/@duration", name);
    return p->getPropInt(pname.str(), 0);
}

IStringVal & CLocalWorkUnit::getTimerDescription(const char * name, IStringVal & str) const
{
    Owned<IConstWUStatistic> stat = getStatisticByDescription(name);
    if (stat)
        return stat->getDescription(str);

    //Backward compatibility - but only use it if no statistics
    CriticalBlock block(crit);
    if (p->hasProp("Statistics"))
    {
        str.clear();
        return str;
    }

    str.set(name);
    return str;
}

unsigned CLocalWorkUnit::getTimerCount(const char *name) const
{
    Owned<IConstWUStatistic> stat = getStatisticByDescription(name);
    if (stat)
        return (unsigned)stat->getCount();

    //Backward compatibility - but only use it if no statistics
    CriticalBlock block(crit);
    if (p->hasProp("Statistics"))
        return 0;

    StringBuffer pname;
    pname.appendf("Timings/Timing[@name=\"%s\"]/@count", name);
    return p->getPropInt(pname.str(), 0);
}

IStringIterator& CLocalWorkUnit::getTimers() const
{
    CriticalBlock block(crit);

    if (p->hasProp("Statistics"))
        return *new CStringPTreeAttrIterator(p->getElements("Statistics/Statistic[@unit=\"ns\"]"), "@desc");

    //Backward compatibility - but only use it if no statistics
    return *new CStringPTreeAttrIterator(p->getElements("Timings/Timing"), "@name");
}

StringBuffer &formatGraphTimerLabel(StringBuffer &str, const char *graphName, unsigned subGraphNum, unsigned __int64 subId)
{
    str.append("Graph ").append(graphName);
    if (subGraphNum) str.append(" - ").append(subGraphNum).append(" (").append(subId).append(")");
    else if (subId) str.append(" - id(").append(subId).append(")");
    return str;
}

StringBuffer &formatGraphTimerScope(StringBuffer &str, const char *graphName, unsigned subGraphNum, unsigned __int64 subId)
{
    str.append(graphName);
    if (subId) str.append(":").append(subId);
    return str;
}

bool parseGraphTimerLabel(const char *label, StringAttr &graphName, unsigned & graphNum, unsigned &subGraphNum, unsigned &subId)
{
    // expects format: "Graph <graphname>[ - <subgraphnum> (<subgraphid>)]"
    unsigned len = (size32_t)strlen(label);
    if (len < 6 || (0 != memcmp(label, "Graph ", 6)))
        return false;
    graphNum = 0;
    subGraphNum = 0;
    subId = 0;
    const char *finger = label+6;
    const char *finger2 = strchr(finger, '-');

    if (NULL == finger2) // just graphName
        graphName.set(finger);
    else
    {
        graphName.set(finger, (size32_t)((finger2-1)-finger));
        finger = finger2+2; // skip '-' and space
        finger2 = strchr(finger, ' ');
        if (finger2)
        {
            subGraphNum = atoi_l(finger, (size32_t)(finger2-finger));
            finger = finger2+2; // skip space and '('
            finger2 = strchr(finger, ')');
            if (finger2)
                subId = atoi_l(finger, (size32_t)(finger2-finger));
        }
        else if (((len-(finger-label))>3) && 0 == memcmp(finger, "id(", 3)) // subgraph id only, new format.
        {
            finger += 3;
            finger2 = strchr(finger, ')');
            if (finger2)
                subId = atoi_l(finger, (size32_t)(finger2-finger));
        }
    }

    if (graphName && memicmp(graphName, "graph", 5))
        graphNum = atoi(graphName + 5);

    return true;
}

void CLocalWorkUnit::setTimerInfo(const char *name, unsigned ms, unsigned count, unsigned __int64 max)
{
    CriticalBlock block(crit);
    IPropertyTree *timings = p->queryPropTree("Timings");
    if (!timings)
        timings = p->addPropTree("Timings", createPTree("Timings"));
    StringBuffer xpath;
    xpath.append("Timing[@name=\"").append(name).append("\"]");
    IPropertyTree *timing = timings->queryPropTree(xpath.str());
    if (!timing)
    {
        timing = timings->addPropTree("Timing", createPTree("Timing"));
        timing->setProp("@name", name);
    }
    timing->setPropInt("@count", count);
    timing->setPropInt("@duration", ms);
    if (!max && 1==count) max = milliToNano(ms); // max is in nanoseconds
    if (max)
        timing->setPropInt64("@max", max);
}

void CLocalWorkUnit::setTimeStamp(const char *application, const char *instance, const char *event, bool add)
{
    CriticalBlock block(crit);
    char timeStamp[64];
    time_t tNow;
    time(&tNow);
#ifdef _WIN32
    struct tm *gmtNow;
    gmtNow = gmtime(&tNow);
    strftime(timeStamp, 64, "%Y-%m-%dT%H:%M:%SZ", gmtNow);
#else
    struct tm gmtNow;
    gmtime_r(&tNow, &gmtNow);
    strftime(timeStamp, 64, "%Y-%m-%dT%H:%M:%SZ", &gmtNow);
#endif //_WIN32
    IPropertyTree *ts = p->queryPropTree("TimeStamps");
    if (!ts) {
        ts = p->addPropTree("TimeStamps", createPTree("TimeStamps"));
        add = true;
    }
    IPropertyTree *t=NULL;
    if (!add) {
        StringBuffer path;
        path.appendf("TimeStamp[@application=\"%s\"]",application);
        t = ts->queryBranch(path.str());
    }
    if (!t) {
        t = createPTree("TimeStamp");
        t->setProp("@application", application);
        add = true;
    }
    if (instance)
        t->setProp("@instance", instance);
    t->setProp(event, timeStamp);

    IPropertyTree *et = t->queryPropTree(event);
    if(et)
        et->setPropInt("@ts",(int)tNow);
    if (add)
        ts->addPropTree("TimeStamp", t);
}


mapEnums queryStatMeasure[] =
{
    { SMEASURE_TIME_NS, "ns" },
    { SMEASURE_COUNT, "cnt" },
    { SMEASURE_MEM_KB, "kb" },
    { SMEASURE_MAX, NULL},
};

void CLocalWorkUnit::setStatistic(const char * creator, const char * wuScope, const char * stat, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge)
{
    if (!wuScope) wuScope = "workunit";

    //creator. scope and name must all be present, and must not contain semi colons.
    assertex(creator && wuScope && stat);
    dbgassertex(!strchr(creator, ';') && !strchr(wuScope, ';') && !strchr(stat, ';'));
    if (count == 1 && maxValue < value)
        maxValue = value;

    StringBuffer fullname;
    fullname.append(creator).append(";").append(wuScope).append(";").append(stat);

    StringBuffer xpath;
    xpath.append("Statistic[@name=\"").append(fullname).append("\"]");

    CriticalBlock block(crit);
    IPropertyTree * stats = p->queryPropTree("Statistics");
    if (!stats)
        stats = p->addPropTree("Statistics", createPTree("Statistics"));
    IPropertyTree * statTree = stats->queryPropTree(xpath.str());
    if (!statTree)
    {
        //MORE: When getTimings is removed the default description could be dynamically calculated
        StringBuffer descriptionText;
        if (!description || !*description)
        {
            bool isDefaultName = streq(stat, "time");
            bool isDefaultScope = streq(wuScope, "workunit");

            descriptionText.append(creator);
            if (isDefaultName || !isDefaultScope)
                descriptionText.append(": ").append(wuScope);
            if (!isDefaultName)
                descriptionText.append(": ").append(stat);
            description = descriptionText;
        }

        statTree = stats->addPropTree("Statistic", createPTree("Statistic"));
        statTree->setProp("@name", fullname.str());

        if (description)
            statTree->setProp("@desc", description);
        setEnum(statTree, "@unit", kind, queryStatMeasure);

        statTree->setPropInt64("@value", value);
        statTree->setPropInt64("@count", count);
        if (maxValue)
            statTree->setPropInt64("@max", maxValue);

        if (statistics.cached)
            statistics.append(LINK(statTree));
    }
    else
    {
        if (merge)
        {
            unsigned __int64 oldValue = statTree->getPropInt64("@value", 0);
            unsigned __int64 oldCount = statTree->getPropInt64("@count", 0);
            unsigned __int64 oldMax = statTree->getPropInt64("@max", 0);

            statTree->setPropInt64("@value", value + oldValue);
            statTree->setPropInt64("@count", count + oldCount);
            if (maxValue > oldMax)
                statTree->setPropInt64("@max", maxValue);
        }
        else
        {
            statTree->setPropInt64("@value", value);
            statTree->setPropInt64("@count", count);
            if (maxValue)
                statTree->setPropInt64("@max", maxValue);
        }
    }
}

void CLocalWorkUnit::setTimeStamp(const char *application, const char *instance, const char *event)
{
    setTimeStamp(application,instance,event,false);
}

void CLocalWorkUnit::addTimeStamp(const char *application, const char *instance, const char *event)
{
    setTimeStamp(application,instance,event,true);
}

IStringVal &CLocalWorkUnit::getTimeStamp(const char *name, const char *application, IStringVal &str) const
{
    CriticalBlock block(crit);

    str.clear();

    StringBuffer pname("TimeStamps/TimeStamp");
    if (application)
        pname.appendf("[@application=\"%s\"]", application);
    pname.appendf("/%s", name);

    Owned<IPropertyTreeIterator> stamps = p->getElements(pname.str());
    if (stamps && stamps->first())
        str.set(stamps->query().queryProp(NULL));

    return str;
}

IConstWUTimeStampIterator& CLocalWorkUnit::getTimeStamps() const
{
    CriticalBlock block(crit);
    timestamps.load(p,"TimeStamps/*");
    return *new CArrayIteratorOf<IConstWUTimeStamp,IConstWUTimeStampIterator> (timestamps, 0, (IConstWorkUnit *) this);
}

IConstWUStatisticIterator& CLocalWorkUnit::getStatistics() const
{
    CriticalBlock block(crit);
    statistics.load(p,"Statistics/*");
    return *new CArrayIteratorOf<IConstWUStatistic,IConstWUStatisticIterator> (statistics, 0, (IConstWorkUnit *) this);
}

IConstWUStatistic * CLocalWorkUnit::getStatisticByDescription(const char * desc) const
{
    StringBuffer xpath;
    xpath.appendf("Statistics/Statistic[@desc=\"%s\"]", desc);
    CriticalBlock block(crit);
    IPropertyTree * match = p->queryPropTree(xpath);
    if (!match)
        return NULL;
    return new CLocalWUStatistic(LINK(match));
}

IConstWUStatistic * CLocalWorkUnit::getStatistic(const char * name) const
{
    StringBuffer xpath;
    xpath.appendf("Statistics/Statistic[@name=\"%s\"]", name);
    CriticalBlock block(crit);
    IPropertyTree * match = p->queryPropTree(xpath);
    if (!match)
        return NULL;
    return new CLocalWUStatistic(LINK(match));
}

bool CLocalWorkUnit::getWuDate(unsigned & year, unsigned & month, unsigned& day)
{
    CriticalBlock block(crit);
    SCMStringBuffer wuidstr;
    const char *wuid = getWuid(wuidstr).str();

    if (sscanf(wuid, "W%4u%2u%2u", &year, &month, &day)==3)
    {
    }
    
    return false;
}

IWUPlugin* CLocalWorkUnit::updatePluginByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUPlugin *existing = getPluginByName(qname);
    if (existing)
        return (IWUPlugin *) existing;
    if (!plugins.length())
        p->addPropTree("Plugins", createPTree("Plugins"));
    IPropertyTree *pl = p->queryPropTree("Plugins");
    IPropertyTree *s = pl->addPropTree("Plugin", createPTree("Plugin"));
    s->Link();
    IWUPlugin* q = new CLocalWUPlugin(s); 
    q->Link();
    plugins.append(*q);
    q->setPluginName(qname);
    return q;
}

IConstWUPlugin* CLocalWorkUnit::getPluginByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadPlugins();
    ForEachItemIn(idx, plugins)
    {
        SCMStringBuffer name;
        IConstWUPlugin &cur = plugins.item(idx);
        cur.getPluginName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IWULibrary* CLocalWorkUnit::updateLibraryByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWULibrary *existing = getLibraryByName(qname);
    if (existing)
        return (IWULibrary *) existing;
    if (!libraries.length())
        p->addPropTree("Libraries", createPTree("Libraries"));
    IPropertyTree *pl = p->queryPropTree("Libraries");
    IPropertyTree *s = pl->addPropTree("Library", createPTree("Library"));
    s->Link();
    IWULibrary* q = new CLocalWULibrary(s); 
    q->Link();
    libraries.append(*q);
    q->setName(qname);
    return q;
}

void CLocalWorkUnit::loadExceptions() const
{
    CriticalBlock block(crit);
    if (!exceptionsCached)
    {
        
        assertex(exceptions.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Exceptions/Exception");


        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            exceptions.append(*new CLocalWUException(rp));
        }
        exceptionsCached = true;
    }
}

IConstWUExceptionIterator& CLocalWorkUnit::getExceptions() const
{
    CriticalBlock block(crit);
    loadExceptions();
    return *new CArrayIteratorOf<IConstWUException,IConstWUExceptionIterator> (exceptions, 0, (IConstWorkUnit *) this);
}

unsigned CLocalWorkUnit::getExceptionCount() const
{
    CriticalBlock block(crit);
    loadExceptions();
    return exceptions.length();
}

void CLocalWorkUnit::clearExceptions()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    exceptions.kill();
    exceptionsCached = true;
    p->removeProp("Exceptions");
}


IWUException* CLocalWorkUnit::createException()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadExceptions();

    if (!exceptions.length())
        p->addPropTree("Exceptions", createPTree("Exceptions"));
    IPropertyTree *r = p->queryPropTree("Exceptions");
    IPropertyTree *s = r->addPropTree("Exception", createPTree("Exception"));
    IWUException* q = new CLocalWUException(LINK(s)); 
    exceptions.append(*LINK(q));

    Owned<IJlibDateTime> now = createDateTimeNow();
    SCMStringBuffer temp;
    now->getString(temp);
    q->setTimeStamp(temp.str());
    return q;
}


IConstWUWebServicesInfo* CLocalWorkUnit::getWebServicesInfo() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if (!webServicesInfoCached)
    {
        assertex(!webServicesInfo);
        IPropertyTree *s = p->getPropTree("WebServicesInfo");
        if (s)
            webServicesInfo.setown(new CLocalWUWebServicesInfo(s)); 
        webServicesInfoCached = true;
    }
    return webServicesInfo.getLink();
}

IWUWebServicesInfo* CLocalWorkUnit::updateWebServicesInfo(bool create)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if (!webServicesInfoCached)
    {
        IPropertyTree *s = p->queryPropTree("WebServicesInfo");
        if (!s)
        {
            if (create)
                s = p->addPropTree("WebServicesInfo", createPTreeFromXMLString("<WebServicesInfo />"));
            else
                return NULL;
        }
        s->Link();
        webServicesInfo.setown(new CLocalWUWebServicesInfo(s)); 
        webServicesInfoCached = true;
    }
    return webServicesInfo.getLink();
}

IConstWURoxieQueryInfo* CLocalWorkUnit::getRoxieQueryInfo() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if (!roxieQueryInfoCached)
    {
        assertex(!roxieQueryInfo);
        IPropertyTree *s = p->getPropTree("RoxieQueryInfo");
        if (s)
            roxieQueryInfo.setown(new CLocalWURoxieQueryInfo(s)); 
        roxieQueryInfoCached = true;
    }
    return roxieQueryInfo.getLink();
}

IWURoxieQueryInfo* CLocalWorkUnit::updateRoxieQueryInfo(const char *wuid, const char *roxieClusterName)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if (!roxieQueryInfoCached)
    {
        IPropertyTree *s = p->queryPropTree("RoxieQueryInfo");
        if (!s)
            s = p->addPropTree("RoxieQueryInfo", createPTreeFromXMLString("<RoxieQueryInfo />"));
        if (wuid && *wuid)
            s->addProp("@wuid", wuid);

        if (roxieClusterName && *roxieClusterName)
            s->addProp("@roxieClusterName", roxieClusterName);

        s->Link();
        roxieQueryInfo.setown(new CLocalWURoxieQueryInfo(s)); 
        roxieQueryInfoCached = true;
    }
    return roxieQueryInfo.getLink();
}

static int compareResults(IInterface **ll, IInterface **rr)
{
    CLocalWUResult *l = (CLocalWUResult *) *ll;
    CLocalWUResult *r = (CLocalWUResult *) *rr;
    return l->getResultSequence() - r->getResultSequence();
}

void CLocalWorkUnit::loadResults() const
{
    CriticalBlock block(crit);
    if (!resultsCached)
    {
        assertex(results.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Results/Result");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();

            rp->Link();
            results.append(*new CLocalWUResult(rp));
        }
        results.sort(compareResults);
        resultsCached = true;
    }
}

void CLocalWorkUnit::loadVariables() const
{
    CriticalBlock block(crit);
    if (!variablesCached)
    {
        assertex(variables.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Variables/Variable");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            variables.append(*new CLocalWUResult(rp));
        }
        variablesCached = true;
    }
}

void CLocalWorkUnit::loadTemporaries() const
{
    CriticalBlock block(crit);
    if (!temporariesCached)
    {
        assertex(temporaries.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Temporaries/Variable");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            temporaries.append(*new CLocalWUResult(rp));
        }
        temporariesCached = true;
    }
}

void CLocalWorkUnit::deleteTemporaries()
{
    CriticalBlock block(crit);
    if (temporariesCached)
    {
        temporaries.kill();
        temporariesCached = false;
    }
    p->removeProp("Temporaries");
}

IWUResult* CLocalWorkUnit::createResult()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadResults();
    if (!results.length())
        p->addPropTree("Results", createPTree("Results"));
    IPropertyTree *r = p->queryPropTree("Results");
    IPropertyTree *s = r->addPropTree("Result", createPTree());

    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    results.append(*q);
    return q;
}

IWUResult* CLocalWorkUnit::updateResultByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getResultByName(qname);
    if (existing)
        return (IWUResult *) existing;
    IWUResult* q = createResult(); 
    q->setResultName(qname);
    return q;
}

IWUResult* CLocalWorkUnit::updateResultBySequence(unsigned seq)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getResultBySequence(seq);
    if (existing)
        return (IWUResult *) existing;
    IWUResult* q = createResult(); 
    q->setResultSequence(seq);
    return q;
}

IConstWUResultIterator& CLocalWorkUnit::getResults() const
{
    CriticalBlock block(crit);
    loadResults();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (results, 0, (IConstWorkUnit *) this);
}

IConstWUResult* CLocalWorkUnit::getResultByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadResults();
    ForEachItemIn(idx, results)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = results.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResult* CLocalWorkUnit::getResultBySequence(unsigned seq) const
{
    CriticalBlock block(crit);
    loadResults();
    ForEachItemIn(idx, results)
    {
        IConstWUResult &cur = results.item(idx);
        if (cur.getResultSequence() == seq)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResultIterator& CLocalWorkUnit::getVariables() const
{
    CriticalBlock block(crit);
    loadVariables();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (variables, 0, (IConstWorkUnit *) this);
}

IConstWUResult* CLocalWorkUnit::getGlobalByName(const char *qname) const
{
    CriticalBlock block(crit);
    if (strcmp(p->queryName(), GLOBAL_WORKUNIT)==0)
        return getVariableByName(qname);

    Owned <IWorkUnit> global = factory->ensureNamedWorkUnit(GLOBAL_WORKUNIT);
    return global->getVariableByName(qname);
}

IWUResult* CLocalWorkUnit::updateGlobalByName(const char *qname)
{
    CriticalBlock block(crit);
    if (strcmp(p->queryName(), GLOBAL_WORKUNIT)==0)
        return updateVariableByName(qname);

    Owned <IWorkUnit> global = factory->ensureNamedWorkUnit(GLOBAL_WORKUNIT);
    return global->updateVariableByName(qname);
}

IConstWUResult* CLocalWorkUnit::getVariableByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadVariables();
    ForEachItemIn(idx, variables)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = variables.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResult* CLocalWorkUnit::getTemporaryByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadTemporaries();
    ForEachItemIn(idx, temporaries)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = temporaries.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResultIterator& CLocalWorkUnit::getTemporaries() const
{
    CriticalBlock block(crit);
    loadTemporaries();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (temporaries, 0, (IConstWorkUnit *) this);
}

IWUResult* CLocalWorkUnit::updateTemporaryByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getTemporaryByName(qname);
    if (existing)
        return (IWUResult *) existing;
    if (!temporaries.length())
        p->addPropTree("Temporaries", createPTree("Temporaries"));
    IPropertyTree *vars = p->queryPropTree("Temporaries");
    IPropertyTree *s = vars->addPropTree("Variable", createPTree("Variable"));
    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    temporaries.append(*q);
    q->setResultName(qname);
    return q;
}

IWUResult* CLocalWorkUnit::updateVariableByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getVariableByName(qname);
    if (existing)
        return (IWUResult *) existing;
    if (!variables.length())
        p->addPropTree("Variables", createPTree("Variables"));
    IPropertyTree *vars = p->queryPropTree("Variables");
    IPropertyTree *s = vars->addPropTree("Variable", createPTree("Variable"));
    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    variables.append(*q);
    q->setResultName(qname);
    return q;
}

void CLocalWorkUnit::deleteTempFiles(const char *graph, bool deleteOwned, bool deleteJobOwned)
{
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files) return;
    Owned<IPropertyTreeIterator> iter = files->getElements("File");
    ICopyArrayOf<IPropertyTree> toRemove;
    ForEach (*iter)
    {
        IPropertyTree &file = iter->query();
        WUFileKind fileKind = (WUFileKind) file.getPropInt("@kind", WUFileStandard);
        if(file.getPropBool("@temporary")) fileKind = WUFileTemporary; // @temporary, legacy check
        bool needDelete;
        switch(fileKind)
        {
        case WUFileTemporary:
            if(graph==NULL)
                needDelete = true;
            else
            {
                const char *graphOwner = file.queryProp("@graph");
                needDelete = ((graphOwner==NULL) || (strcmp(graph, graphOwner)==0));
            }
            break;
        case WUFileJobOwned:
            needDelete = ((graph==NULL) && deleteJobOwned);
            break;
        case WUFileOwned:
            needDelete = ((graph==NULL) && deleteOwned);
            break;
        default:
            needDelete = false;
        }
        if(needDelete)
        {
            const char *name = file.queryProp("@name");
            LOG(MCdebugProgress, unknownJob, "Removing workunit file %s from DFS", name);
            queryDistributedFileDirectory().removeEntry(name, queryUserDescriptor());
            toRemove.append(file);
        }
    }
    ForEachItemIn(r, toRemove) files->removeTree(&toRemove.item(r));
}

static void _noteFileRead(IDistributedFile *file, IPropertyTree *filesRead)
{
    IDistributedSuperFile *super = file->querySuperFile();
    StringBuffer fname;
    file->getLogicalName(fname);
    StringBuffer path("File[@name=\"");
    path.append(fname).append("\"]");
    IPropertyTree *fileTree = filesRead->queryPropTree(path.str());
    if (fileTree)
        fileTree->setPropInt("@useCount", fileTree->getPropInt("@useCount")+1);
    else
    {
        StringBuffer cluster;
        file->getClusterName(0,cluster);
        fileTree = createPTree();
        fileTree->setProp("@name", fname.str());
        fileTree->setProp("@cluster", cluster.str());
        fileTree->setPropInt("@useCount", 1);
        fileTree = filesRead->addPropTree("File", fileTree);
    }
    
    if (super)
    {
        Owned<IDistributedFileIterator> iter = super->getSubFileIterator(false);
        ForEach (*iter)
        {
            IDistributedFile &file = iter->query();
            StringBuffer fname;
            file.getLogicalName(fname);
            Owned<IPropertyTree> subfile = createPTree();
            subfile->setProp("@name", fname.str());
            fileTree->addPropTree("Subfile", subfile.getClear());
            _noteFileRead(&file, filesRead);
        }
    }
}

void CLocalWorkUnit::noteFileRead(IDistributedFile *file)
{
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("FilesRead");
    if (!files)
        files = p->addPropTree("FilesRead", createPTree());
    _noteFileRead(file, files);
}

static void addFile(IPropertyTree *files, const char *fileName, const char *cluster, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
{
    StringBuffer path("File[@name=\"");
    path.append(fileName).append("\"]");
    if (cluster)
        path.append("[@cluster=\"").append(cluster).append("\"]");
    IPropertyTree *file = files->queryPropTree(path.str());
    if (file) files->removeTree(file);
    file = createPTree();
    file->setProp("@name", fileName);
    if (cluster)
        file->setProp("@cluster", cluster);
    if (graphOwner)
        file->setProp("@graph", graphOwner);
    file->setPropInt("@kind", (unsigned)fileKind);
    if (WUFileTemporary == fileKind)
        file->setPropInt("@usageCount", usageCount);
    files->addPropTree("File", file);
}

void CLocalWorkUnit::addFile(const char *fileName, StringArray *clusters, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
{
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files)
        files = p->addPropTree("Files", createPTree());
    if (!clusters)
        addFile(fileName, NULL, usageCount, fileKind, graphOwner);
    else
    {
        ForEachItemIn(c, *clusters)
            ::addFile(files, fileName, clusters->item(c), usageCount, fileKind, graphOwner);
    }
}

void CLocalWorkUnit::releaseFile(const char *fileName)
{
    StringBuffer path("File[@name=\"");
    path.append(fileName).append("\"]");
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files) return;
    Owned<IPropertyTreeIterator> fiter = files->getElements(path.str());
    ForEach (*fiter)
    {
        IPropertyTree *file = &fiter->query();
        unsigned usageCount = file->getPropInt("@usageCount");
        if (usageCount > 1)
            file->setPropInt("@usageCount", usageCount-1);
        else
        {
            StringAttr name(file->queryProp("@name"));
            files->removeTree(file);
            if (!name.isEmpty()&&(1 == usageCount))
            {
                if (queryDistributedFileDirectory().removeEntry(fileName, queryUserDescriptor()))
                    LOG(MCdebugProgress, unknownJob, "Removed (released) file %s from DFS", name.get());
            }
        }
    }
}

void CLocalWorkUnit::clearGraphProgress()
{
    CConstGraphProgress::deleteWuidProgress(p->queryName());
}

void CLocalWorkUnit::resetBeforeGeneration()
{
    CriticalBlock block(crit);
    //Remove all associated files
    Owned<IWUQuery> q = updateQuery();
    q->removeAssociatedFiles();

    //Remove any pre-existing workflow information
    workflowIterator.clear();
    p->removeProp("Workflow");
}

unsigned CLocalWorkUnit::queryFileUsage(const char *fileName) const
{
    StringBuffer path("Files/File[@name=\"");
    path.append(fileName).append("\"]/@usageCount");
    CriticalBlock block(crit);
    return p->getPropInt(path.str());
}

IPropertyTree *CLocalWorkUnit::getDiskUsageStats()
{
    return p->getPropTree("DiskUsageStats");
}

void CLocalWorkUnit::addDiskUsageStats(__int64 _avgNodeUsage, unsigned _minNode, __int64 _minNodeUsage, unsigned _maxNode, __int64 _maxNodeUsage, __int64 _graphId)
{
    IPropertyTree *stats = p->queryPropTree("DiskUsageStats");
    offset_t maxNodeUsage;
    if (stats)
        maxNodeUsage = stats->getPropInt64("@maxNodeUsage");
    else
    {
        stats = p->addPropTree("DiskUsageStats", createPTree());
        maxNodeUsage = 0;
    }

    if ((offset_t)_maxNodeUsage > maxNodeUsage)
    {
        // record all details at time of max node usage.
        stats->setPropInt("@minNode", _minNode);
        stats->setPropInt("@maxNode", _maxNode);
        stats->setPropInt64("@minNodeUsage", _minNodeUsage);
        stats->setPropInt64("@maxNodeUsage", _maxNodeUsage);
        stats->setPropInt64("@graphId", _graphId);
        if (_avgNodeUsage)
        {
            unsigned _skewHi = (unsigned)((100 * (_maxNodeUsage-_avgNodeUsage))/_avgNodeUsage);
            unsigned _skewLo = (unsigned)((100 * (_avgNodeUsage-_minNodeUsage))/_avgNodeUsage);
            stats->setPropInt("@skewHi", _skewHi);
            stats->setPropInt("@skewLo", _skewLo);
        }
    }
}

IPropertyTreeIterator & CLocalWorkUnit::getFileIterator() const
{
    CriticalBlock block(crit);
    return * p->getElements("Files/File");
}

IPropertyTreeIterator & CLocalWorkUnit::getFilesReadIterator() const
{
    CriticalBlock block(crit);
    return * p->getElements("FilesRead/File");
}

//=================================================================================================

IWUActivity * CLocalWorkUnit::updateActivity(__int64 id)
{
    CriticalBlock block(crit);
    IConstWUActivity *existing = getActivity(id);
    if (existing)
        return (IWUActivity *) existing;
    if (!activities.length())
        p->addPropTree("Activities", createPTree("Activities"));
    IPropertyTree *pl = p->queryPropTree("Activities");
    IPropertyTree *s = pl->addPropTree("Activity", createPTree("Activity"));
    IWUActivity * q = new CLocalWUActivity(LINK(s), id); 
    activities.append(*LINK(q));
    return q;
}

IConstWUActivity * CLocalWorkUnit::getActivity(__int64 id) const
{
    CriticalBlock block(crit);
    loadActivities();
    ForEachItemIn(idx, activities)
    {
        IConstWUActivity &cur = activities.item(idx);
        if (cur.getId() == id)
            return &OLINK(cur);
    }
    return NULL;
}

void CLocalWorkUnit::loadActivities() const
{
    CriticalBlock block(crit);
    if (!activitiesCached)
    {
        assertex(activities.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Activities/Activity");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            activities.append(*new CLocalWUActivity(rp));
        }
        activitiesCached = true;
    }
}

IConstWUActivityIterator& CLocalWorkUnit::getActivities() const
{
    CriticalBlock block(crit);
    loadActivities();
    return *new CArrayIteratorOf<IConstWUActivity,IConstWUActivityIterator> (activities, 0, (IConstWorkUnit *) this);
}

//=================================================================================================


bool CLocalWorkUnit::switchThorQueue(const char *cluster, IQueueSwitcher *qs)
{
    CriticalBlock block(crit);
    if (qs->isAuto()&&!getAllowAutoQueueSwitch())
        return false;
    Owned<IConstWUClusterInfo> newci = getTargetClusterInfo(cluster);
    if (!newci) 
        return false;
    StringBuffer currentcluster;
    if (!p->getProp("@clusterName",currentcluster))
        return false;
    Owned<IConstWUClusterInfo> curci = getTargetClusterInfo(currentcluster.str());
    if (!curci)
        return false;
    SCMStringBuffer curqname;
    curci->getThorQueue(curqname);
    const char *wuid = p->queryName();
    void *qi = qs->getQ(curqname.str(),wuid);
    if (!qi)
        return false;
    setClusterName(cluster);
    SCMStringBuffer newqname;
    newci->getThorQueue(newqname);
    qs->putQ(newqname.str(),wuid,qi);
    return true;
}


//=================================================================================================

IPropertyTree *CLocalWorkUnit::getUnpackedTree() const
{
    Owned<IPropertyTree> ret = createPTreeFromIPT(p);
    Owned<IConstWUGraphIterator> graphIter = &getGraphs(GraphTypeAny);
    ForEach(*graphIter)
    {
        IConstWUGraph &graph  = graphIter->query();
        Owned<IPropertyTree> graphTree = graph.getXGMMLTree(false);
        SCMStringBuffer gName;
        graph.getName(gName);
        StringBuffer xpath("Graphs/Graph[@name=\"");
        xpath.append(gName.s).append("\"]/xgmml");
        IPropertyTree *xgmml = ret->queryPropTree(xpath.str());
        if (xgmml) // don't know of any reason it shouldn't exist
        {
            xgmml->removeProp("graphBin");
            xgmml->setPropTree("graph", graphTree.getClear());
        }
    }
    return ret.getClear();
}

void CLocalWorkUnit::loadGraphs() const
{
    CriticalBlock block(crit);
    if (!cachedGraphs.get())
    {
        MemoryBuffer buf;
        IPropertyTree *t = p->queryPropTree("PackedGraphs");
        if (t&&t->getPropBin(NULL,buf)) {
            cachedGraphs.setown(createPTree(buf));
        }
        else
            cachedGraphs.set(p->queryPropTree("Graphs"));
        if (cachedGraphs.get())
        {
            Owned<IPropertyTreeIterator> iter = cachedGraphs->getElements("Graph");
            ForEach(*iter)
            {
                IPropertyTree &graph = iter->query();
                graphs.append(*new CLocalWUGraph(*this, LINK(&graph)));
            }
        }
    }
}

mapEnums graphTypes[] = {
   { GraphTypeAny, "unknown" },
   { GraphTypeProgress, "progress" },
   { GraphTypeEcl, "ECL" },
   { GraphTypeActivities, "activities" },
   { GraphTypeSubProgress, "subgraph" },
   { GraphTypeSize,  NULL },
};

CLocalWUGraph::CLocalWUGraph(const CLocalWorkUnit &_owner, IPropertyTree *props) : p(props), owner(_owner)
{
    SCMStringBuffer str;
    owner.getWuid(str);
    wuid.set(str.s.str());
    wuidVersion = owner.getWuidVersion();
}

IStringVal& CLocalWUGraph::getName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal& CLocalWUGraph::getLabel(IStringVal &str) const
{
    if (wuidVersion >= 2)
    {
        str.set(p->queryProp("@label"));
        return str;
    }
    else
    {
        Owned<IPropertyTree> xgmml = getXGMMLTree(false);
        str.set(xgmml->queryProp("@label"));
        return str;
    }
}


IStringVal& CLocalWUGraph::getXGMML(IStringVal &str, bool mergeProgress) const
{
    Owned<IPropertyTree> xgmml = getXGMMLTree(mergeProgress);
    if (xgmml)
    {
        StringBuffer x;
        toXML(xgmml, x);
        str.set(x.str());
    }
    return str;
}

unsigned CLocalWorkUnit::getGraphCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Graphs"))
    {
        return p->queryPropTree("Graphs")->numChildren();
    }
    return 0;
}

unsigned CLocalWorkUnit::getSourceFileCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("FilesRead"))
    {
        return p->queryPropTree("FilesRead")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getResultCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Results"))
    {
        return p->queryPropTree("Results")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getVariableCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Variables"))
    {
        return p->queryPropTree("Variables")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getTimerCount() const
{
    CriticalBlock block(crit);

    if (p->hasProp("Statistics"))
    {
        Owned<IPropertyTreeIterator> iter = p->getElements("Statistics/Statistic[@unit=\"ns\"]");
        unsigned cnt =0;
        if (iter)
        {
            ForEach(*iter)
                cnt++;
        }
        return cnt;
    }

    if (p->hasProp("Timings"))
        return p->queryPropTree("Timings")->numChildren();

    return 0;
}

unsigned CLocalWorkUnit::getApplicationValueCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Application"))
    {
        return p->queryPropTree("Application")->numChildren();
    }
    return 0;
    
}

IStringVal &CLocalWorkUnit::getXmlParams(IStringVal &str) const
{
    CriticalBlock block(crit);
    IPropertyTree *paramTree = p->queryPropTree("Parameters");
    if (paramTree)
    {
        StringBuffer temp;
        toXML(paramTree, temp);
        str.set(temp.str());
    }
    return str;
}

const IPropertyTree *CLocalWorkUnit::getXmlParams() const
{
    CriticalBlock block(crit);
    return p->getPropTree("Parameters");
}

void CLocalWorkUnit::setXmlParams(const char *params)
{
    CriticalBlock block(crit);
    p->setPropTree("Parameters", createPTreeFromXMLString(params));
}

void CLocalWorkUnit::setXmlParams(IPropertyTree *tree)
{
    CriticalBlock block(crit);
    p->setPropTree("Parameters", tree);
}

unsigned __int64 CLocalWorkUnit::getHash() const
{
    CriticalBlock block(crit);
    return p->getPropInt64("@hash");
}

void CLocalWorkUnit::setHash(unsigned __int64 hash)
{
    CriticalBlock block(crit);
    p->setPropInt64("@hash", hash);
}

IConstWUGraphIterator& CLocalWorkUnit::getGraphs(WUGraphType type) const
{
    CriticalBlock block(crit);
    loadGraphs();
    IConstWUGraphIterator *giter = new CArrayIteratorOf<IConstWUGraph,IConstWUGraphIterator> (graphs, 0, (IConstWorkUnit *) this);
    if (type!=GraphTypeAny) {
        class CConstWUGraphIterator: public CInterface, implements IConstWUGraphIterator
        {
            WUGraphType type;
            Owned<IConstWUGraphIterator> base;
            bool match()
            {
                return  base->query().getType()==type;
            }
        public:
            IMPLEMENT_IINTERFACE;
            CConstWUGraphIterator(IConstWUGraphIterator *_base,WUGraphType _type)
                : base(_base)
            {
                type = _type;
            }
            bool first()
            {
                if (!base->first())
                    return false;
                if (match())
                    return true;
                return next();
            }
            bool next()
            {
                while (base->next())
                    if (match())
                        return true;
                return false;
            }
            virtual bool isValid()
            {
                return base->isValid();
            }
            IConstWUGraph & query()
            {
                return base->query();
            }
        };
        giter = new CConstWUGraphIterator(giter,type);
    }
    return *giter;
}

IConstWUGraph* CLocalWorkUnit::getGraph(const char *qname) const
{
    CriticalBlock block(crit);
    loadGraphs();
    ForEachItemIn(idx, graphs)
    {
        SCMStringBuffer name;
        IConstWUGraph &cur = graphs.item(idx);
        cur.getName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IWUGraph* CLocalWorkUnit::createGraph()
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    ensureGraphsUnpacked();
    loadGraphs();
    if (!graphs.length())
        p->addPropTree("Graphs", createPTree("Graphs"));
    IPropertyTree *r = p->queryPropTree("Graphs");
    IPropertyTree *s = r->addPropTree("Graph", createPTree());
    s->Link();
    IWUGraph* q = new CLocalWUGraph(*this, s);
    q->Link();
    graphs.append(*q);
    return q;
}

IWUGraph * CLocalWorkUnit::updateGraph(const char * name)
{
    CriticalBlock block(crit);
    ensureGraphsUnpacked();
    IConstWUGraph *existing = getGraph(name);
    if (existing)
        return (IWUGraph *) existing;
    IWUGraph * q = createGraph();
    q->setName(name);
    return q;
}

IConstWUGraphProgress *CLocalWorkUnit::getGraphProgress(const char *name) const
{
    CriticalBlock block(crit);
    return new CConstGraphProgress(p->queryName(), name);
}

IStringVal& CLocalWUGraph::getDOT(IStringVal &str) const
{
    UNIMPLEMENTED;
}

void CLocalWUGraph::setName(const char *str)
{
    p->setProp("@name", str);
    progress.clear();
    progress.setown(new CConstGraphProgress(wuid, str));
}

void CLocalWUGraph::setLabel(const char *str)
{
    p->setProp("@label", str);
}

void CLocalWUGraph::setXGMML(const char *str)
{
    setXGMMLTree(createPTreeFromXMLString(str));
}

void CLocalWUGraph::setXGMMLTree(IPropertyTree *_graph, bool compress)
{
    assertex(strcmp(_graph->queryName(), "graph")==0);
    IPropertyTree *xgmml = p->setPropTree("xgmml", createPTree());
    if (compress)
    {
        MemoryBuffer mb;
        _graph->serialize(mb);
        xgmml->setPropBin("graphBin", mb.length(), mb.toByteArray());
        graph.setown(_graph);
    }
    else
        xgmml->setPropTree("graph", _graph);
}

void CLocalWUGraph::mergeProgress(IPropertyTree &rootNode, IPropertyTree &progressTree, const unsigned &progressV) const
{
    IPropertyTree *graphNode = rootNode.queryPropTree("att/graph");
    if (!graphNode) return;
    unsigned nodeId = rootNode.getPropInt("@id");
    StringBuffer progressNodePath("node[@id=\"");
    progressNodePath.append(nodeId).append("\"]");
    IPropertyTree *progressNode = progressTree.queryPropTree(progressNodePath.str());
    if (progressNode)
    {
        Owned<IPropertyTreeIterator> edges = progressNode->getElements("edge");
        ForEach (*edges)
        {
            IPropertyTree &edge = edges->query();
            StringBuffer edgePath("edge[@id=\"");
            edgePath.append(edge.queryProp("@id")).append("\"]");
            IPropertyTree *graphEdge = graphNode->queryPropTree(edgePath.str());
            if (graphEdge)
            {
                if (progressV < 1)
                    mergePTree(graphEdge, &edge);
                else
                { // must translate to XGMML format
                    Owned<IAttributeIterator> aIter = edge.getAttributes();
                    ForEach (*aIter)
                    {
                        const char *aName = aIter->queryName()+1;
                        if (0 != stricmp("id", aName)) // "id" reserved.
                        {
                            IPropertyTree *att = graphEdge->addPropTree("att", createPTree());
                            att->setProp("@name", aName);
                            att->setProp("@value", aIter->queryValue());
                        }
                    }
                    // This is really only here, so that our progress format can use non-attribute values, which have different efficiency qualifies (e.g. can be external by dali)
                    Owned<IPropertyTreeIterator> iter = edge.getElements("*");
                    ForEach (*iter)
                    {
                        IPropertyTree &t = iter->query();
                        IPropertyTree *att = graphEdge->addPropTree("att", createPTree());
                        att->setProp("@name", t.queryName());
                        att->setProp("@value", t.queryProp(NULL));
                    }
                }
            }
        }
        Owned<IPropertyTreeIterator> nodes = progressNode->getElements("node");
        ForEach (*nodes)
        {
            IPropertyTree &node = nodes->query();
            StringBuffer nodePath("node[@id=\"");
            nodePath.append(node.queryProp("@id")).append("\"]");
            IPropertyTree *_node = graphNode->queryPropTree(nodePath.str());
            if (_node)
            {
                if (progressV < 1)
                    mergePTree(_node, &node);
                else
                { // must translate to XGMML format
                    Owned<IAttributeIterator> aIter = node.getAttributes();
                    ForEach (*aIter)
                    {
                        const char *aName = aIter->queryName()+1;
                        if (0 != stricmp("id", aName)) // "id" reserved.
                        {
                            IPropertyTree *att = _node->addPropTree("att", createPTree());
                            att->setProp("@name", aName);
                            att->setProp("@value", aIter->queryValue());
                        }
                    }
                }
            }
        }
    }
    Owned<IPropertyTreeIterator> iter = graphNode->getElements("node");
    ForEach (*iter)
        mergeProgress(iter->query(), progressTree, progressV);
}

IPropertyTree * CLocalWUGraph::getXGMMLTreeRaw() const
{
    return p->getPropTree("xgmml");
}

IPropertyTree * CLocalWUGraph::getXGMMLTree(bool doMergeProgress) const
{
    if (!graph)
    {
        // NB: although graphBin introduced in wuidVersion==2,
        // daliadmin can retrospectively compress existing graphs, so need to check for all versions
        MemoryBuffer mb;
        if (p->getPropBin("xgmml/graphBin", mb))
            graph.setown(createPTree(mb));
        else
            graph.setown(p->getBranch("xgmml/graph"));
        if (!graph)
            return NULL;
    }
    if (!doMergeProgress)
        return graph.getLink();
    else
    {
        Owned<IPropertyTree> copy = createPTreeFromIPT(graph);
        Owned<IConstWUGraphProgress> _progress;
        if (progress) _progress.set(progress);
        else
            _progress.setown(new CConstGraphProgress(wuid, p->queryProp("@name")));

        unsigned progressV = _progress->queryFormatVersion();
        IPropertyTree *progressTree = _progress->queryProgressTree();
        Owned<IPropertyTreeIterator> nodeIterator = copy->getElements("node");
        ForEach (*nodeIterator)
            mergeProgress(nodeIterator->query(), *progressTree, progressV);
        return copy.getClear();
    }
}

bool CLocalWUGraph::isValid() const
{
    // JCSMORE - I can't really see why this is necessary, a graph cannot be empty.
    return p->hasProp("xgmml/graph/node") || p->hasProp("xgmml/graphBin");
}

WUGraphType CLocalWUGraph::getType() const
{
    return (WUGraphType) getEnum(p, "@type", graphTypes);
}

IStringVal & CLocalWUGraph::getTypeName(IStringVal &str) const
{
    str.set(p->queryProp("@type"));
    if (!str.length())
        str.set("unknown");
    return str;
}

void CLocalWUGraph::setType(WUGraphType _type)
{
    setEnum(p, "@type", _type, graphTypes);
}

//=================================================================================================

mapEnums queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeSize,  NULL },
};

CLocalWUAssociated::CLocalWUAssociated(IPropertyTree *props) : p(props)
{
}

WUFileType CLocalWUAssociated::getType() const
{
    return (WUFileType)getEnum(p, "@type", queryFileTypes);
}

IStringVal & CLocalWUAssociated::getDescription(IStringVal & str) const
{
    str.set(p->queryProp("@desc"));
    return str;
}

IStringVal & CLocalWUAssociated::getIp(IStringVal & str) const
{
    str.set(p->queryProp("@ip"));
    return str;
}

IStringVal & CLocalWUAssociated::getName(IStringVal & str) const
{
    str.set(p->queryProp("@filename"));
    return str;
}

IStringVal & CLocalWUAssociated::getNameTail(IStringVal & str) const
{
    str.set(pathTail(p->queryProp("@filename")));
    return str;
}

unsigned CLocalWUAssociated::getCrc() const
{
    return p->getPropInt("@crc", 0);
}



//=================================================================================================

CLocalWUQuery::CLocalWUQuery(IPropertyTree *props) : p(props)
{
    associatedCached = false;
}

mapEnums queryTypes[] = {
   { QueryTypeUnknown, "unknown" },
   { QueryTypeEcl, "ECL" },
   { QueryTypeSql, "SQL" },
   { QueryTypeXml, "XML" },
   { QueryTypeAttribute, "Attribute" },
   { QueryTypeSize,  NULL },
};

WUQueryType CLocalWUQuery::getQueryType() const
{
    return (WUQueryType) getEnum(p, "@type", queryTypes);
}

void CLocalWUQuery::setQueryType(WUQueryType qt) 
{
    setEnum(p, "@type", qt, queryTypes);
}

IStringVal& CLocalWUQuery::getQueryText(IStringVal &str) const
{
    str.set(p->queryProp("Text"));
    return str;
}

IStringVal& CLocalWUQuery::getQueryShortText(IStringVal &str) const
{
    const char * text = p->queryProp("Text");
    if (isArchiveQuery(text))
    {
        Owned<IPropertyTree> xml = createPTreeFromXMLString(text, ipt_caseInsensitive);
        const char * path = xml->queryProp("Query/@attributePath");
        if (path)
        {
            IPropertyTree * resolved = resolveDefinitionInArchive(xml, path);
            if (resolved)
                str.set(resolved->queryProp(NULL));
        }
        else
            str.set(xml->queryProp("Query"));
    }
    else
        str.set(text);
    return str;
}

IStringVal& CLocalWUQuery::getQueryName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal & CLocalWUQuery::getQueryMainDefinition(IStringVal & str) const
{
    str.set(p->queryProp("@main"));
    return str;
}

IStringVal& CLocalWUQuery::getQueryDllName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeDll, 0);
    if (entry)
        entry->getNameTail(str);
    return str;
}

IStringVal& CLocalWUQuery::getQueryCppName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeCpp, 0);
    if (entry)
        entry->getName(str);
    return str;
}

IStringVal& CLocalWUQuery::getQueryResTxtName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeResText, 0);
    if (entry)
        entry->getName(str);
    return str;
}

unsigned CLocalWUQuery::getQueryDllCrc() const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeDll, 0);
    if (entry)
        return entry->getCrc();
    return 0;
}

void CLocalWUQuery::setQueryText(const char *text)
{
    p->setProp("Text", text);
}

void CLocalWUQuery::setQueryName(const char *qname)
{
    p->setProp("@name", qname);
}

void CLocalWUQuery::setQueryMainDefinition(const char * str)
{
    p->setProp("@main", str);
}

void CLocalWUQuery::addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc)
{
    CriticalBlock block(crit);
    loadAssociated();
    if (!associated.length())
        p->addPropTree("Associated", createPTree("Associated"));
    IPropertyTree *pl = p->queryPropTree("Associated");
    IPropertyTree *s = pl->addPropTree("File", createPTree("File"));
    setEnum(s, "@type", type, queryFileTypes);
    s->setProp("@filename", name);
    s->setProp("@ip", ip);
    s->setProp("@desc", desc);

    if (crc)
        s->setPropInt("@crc", crc);
    IConstWUAssociatedFile * q = new CLocalWUAssociated(LINK(s)); 
    associated.append(*q);
}

void CLocalWUQuery::removeAssociatedFiles()
{
    associatedCached = false;
    associated.kill();
    p->removeProp("Associated");
}


IConstWUAssociatedFile * CLocalWUQuery::getAssociatedFile(WUFileType type, unsigned index) const
{
    CriticalBlock block(crit);
    loadAssociated();
    ForEachItemIn(idx, associated)
    {
        CLocalWUAssociated &cur = static_cast<CLocalWUAssociated &>(associated.item(idx));
        if (cur.getType() == type)
        {
            if (index-- == 0)
                return &OLINK(cur);
        }
    }
    return NULL;
}

void CLocalWUQuery::addSpecialCaseAssociated(WUFileType type, const char * propname, unsigned crc) const
{
    const char * name = p->queryProp(propname);
    if (name)
    {
        IPropertyTree *s = createPTree("File");
        setEnum(s, "@type", type, queryFileTypes);
        s->setProp("@filename", name);
        if (crc)
            s->setPropInt("@crc", crc);
        associated.append(*new CLocalWUAssociated(s));
    }
}

void CLocalWUQuery::loadAssociated() const
{
    CriticalBlock block(crit);
    if (!associatedCached)
    {
        assertex(associated.length() == 0);
        addSpecialCaseAssociated(FileTypeDll, "DllName", p->getPropInt("DllCrc", 0));
        addSpecialCaseAssociated(FileTypeCpp, "CppName", 0);
        addSpecialCaseAssociated(FileTypeResText, "ResTxtName", 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Associated/File");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            associated.append(*new CLocalWUAssociated(rp));
        }
        associatedCached = true;
    }
}

IConstWUAssociatedFileIterator& CLocalWUQuery::getAssociatedFiles() const
{
    CriticalBlock block(crit);
    loadAssociated();
    return *new CArrayIteratorOf<IConstWUAssociatedFile,IConstWUAssociatedFileIterator> (associated, 0, (IConstWUQuery *) this);
}

//========================================================================================

CLocalWUWebServicesInfo::CLocalWUWebServicesInfo(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUWebServicesInfo::getModuleName(IStringVal &str) const
{
    str.set(p->queryProp("@module"));
    return str;
}

IStringVal& CLocalWUWebServicesInfo::getAttributeName(IStringVal &str) const
{
    str.set(p->queryProp("@attribute"));
    return str;
}

IStringVal& CLocalWUWebServicesInfo::getDefaultName(IStringVal &str) const
{
    str.set(p->queryProp("@defaultName"));
    return str;
}

unsigned CLocalWUWebServicesInfo::getWebServicesCRC() const
{
    return (unsigned) p->getPropInt("@crc");
}

IStringVal& CLocalWUWebServicesInfo::getInfo(const char *name, IStringVal &str) const
{
    if (!name)
    {
        StringBuffer ws_info;
        ws_info.appendf("<%s ", p->queryName());
        Owned<IAttributeIterator> attrs = p->getAttributes();
        for(attrs->first(); attrs->isValid(); attrs->next())
        {
            const char *name = attrs->queryName()+1;
            const char *value = attrs->queryValue();
            ws_info.appendf("%s='%s' ", name, value);
        }
        ws_info.append("> \n");

        Owned<IPropertyTreeIterator> info = p->getElements("*");
        ForEach(*info)
        {
            IPropertyTree &item = info->query();
            const char *name = item.queryName();
            if (name)
            {
                MemoryBuffer mb;
                bool isbin = p->isBinary(name);
                if (isbin)
                {
                    p->getPropBin(name,mb);

                    if (mb.length())
                    {
                        unsigned len = 0;
                        mb.read(len);
                        StringBuffer encodedString;
                        StringBuffer val(len, (const char *) mb.readDirect(len));
                        encodeXML(val, encodedString);
                        ws_info.appendf("<%s>%s</%s>", name, encodedString.str(), name);
                    }
                }
                else
                {
                    StringBuffer tmp;
                    toXML(&item, tmp);
                    ws_info.append(tmp.str());
                }
            }
        }
        ws_info.appendf("</%s>", p->queryName());
        str.setLen(ws_info.str(), ws_info.length());
    }
    else
    {
        MemoryBuffer mb;
        p->getPropBin(name,mb);

        if (mb.length())
        {
            unsigned len;
            mb.read(len);
            str.setLen((const char *) mb.readDirect(len), len);
        }
    }

    return str;

}


void CLocalWUWebServicesInfo::setModuleName(const char *mname)
{
    p->setProp("@module", mname);
}

void CLocalWUWebServicesInfo::setAttributeName(const char *aname)
{
    p->setProp("@attribute", aname);
}

void CLocalWUWebServicesInfo::setDefaultName(const char *dname)
{
    p->setProp("@defaultName", dname);
}

void CLocalWUWebServicesInfo::setWebServicesCRC(unsigned crc)
{
    p->setPropInt("@crc", crc);
}

void CLocalWUWebServicesInfo::setInfo(const char *name, const char *info)
{
    MemoryBuffer m;
    unsigned len = (size32_t)strlen(info);
    serializeLPString(len, info, m);
    p->setPropBin(name, m.length(), m.toByteArray());
}


//========================================================================================

CLocalWURoxieQueryInfo::CLocalWURoxieQueryInfo(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWURoxieQueryInfo::getQueryInfo(IStringVal &str) const
{
    IPropertyTree *queryTree = p->queryPropTree("query");
    if (queryTree)
    {
        StringBuffer temp;
        toXML(queryTree, temp);
        str.set(temp.str());
    }

    return str;
}

IStringVal& CLocalWURoxieQueryInfo::getDefaultPackageInfo(IStringVal &str) const
{
    MemoryBuffer mb;
    p->getPropBin("RoxiePackages",mb);

    if (mb.length())
    {
        unsigned len;
        mb.read(len);
        str.setLen((const char *) mb.readDirect(len), len);
    }

    return str;
}

IStringVal& CLocalWURoxieQueryInfo::getRoxieClusterName(IStringVal &str) const
{
    const char *val = p->queryProp("@roxieClusterName");
    if (val)
        str.set(val);
    
    return str;
}

IStringVal& CLocalWURoxieQueryInfo::getWuid(IStringVal &str) const
{
    const char *val = p->queryProp("@wuid");
    if (val)
        str.set(val);

    return str;
}


void CLocalWURoxieQueryInfo::setQueryInfo(const char *info)
{
    IPropertyTree *queryTree = p->queryPropTree("query");
    if (queryTree)
        p->removeTree(queryTree);

    IPropertyTree * tempTree = p->addPropTree("query", createPTreeFromXMLString(info));

    if (!p->hasProp("@roxieClusterName"))
    {
        const char *roxieClusterName = tempTree->queryProp("@roxieName");
        if (roxieClusterName && *roxieClusterName)
            p->addProp("@roxieClusterName", roxieClusterName);
    }

    if (!p->hasProp("@wuid"))
    {
        const char *wuid = tempTree->queryProp("Query/@wuid");
        if (wuid && *wuid)
            p->addProp("@wuid", wuid);
    }

}

void CLocalWURoxieQueryInfo::setDefaultPackageInfo(const char *info, int len)
{
    MemoryBuffer m;
    serializeLPString(len, info, m);
    p->setPropBin("RoxiePackages", m.length(), m.toByteArray());
}

void CLocalWURoxieQueryInfo::setRoxieClusterName(const char *info)
{
    p->setProp("@roxieClusterName", info);
}

void CLocalWURoxieQueryInfo::setWuid(const char *info)
{
    p->setProp("@wuid", info);
}


//========================================================================================

CLocalWUResult::CLocalWUResult(IPropertyTree *props) : p(props)
{
}

mapEnums resultStatuses[] = {
   { ResultStatusUndefined, "undefined" },
   { ResultStatusCalculated, "calculated" },
   { ResultStatusSupplied, "supplied" },
   { ResultStatusFailed, "failed" },
   { ResultStatusPartial, "partial" },
   { ResultStatusSize, NULL }
};

WUResultStatus CLocalWUResult::getResultStatus() const
{
    return (WUResultStatus ) getEnum(p, "@status", resultStatuses);
}

IStringVal& CLocalWUResult::getResultName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

int CLocalWUResult::getResultSequence() const
{
    return p->getPropInt("@sequence", -1);
}

bool CLocalWUResult::isResultScalar() const
{
    return p->getPropInt("@isScalar", 1) != 0;
}

bool findSize(int size, IntArray &sizes)
{
    ForEachItemIn(idx, sizes)
    {
        if (sizes.item(idx)==size)
            return true;
    }
    return false;
}

void CLocalWUResult::getSchema(TypeInfoArray &types, StringAttrArray &names, IStringVal * eclText) const
{
    MemoryBuffer schema;
    p->getPropBin("SchemaRaw", schema);
    if (schema.length())
    {
        for (;;)
        {
            StringAttr name;
            schema.read(name);
            if (*schema.readDirect(0)==type_void)
                break;
            names.append(*new StringAttrItem(name));
            types.append(*deserializeType(schema));  // MORE - nested records!
        }
        schema.skip(1);

        if (schema.length() != schema.getPos())
        {
            unsigned eclLen;
            schema.read(eclLen);
            const char * schemaData = (const char *)schema.readDirect(eclLen);
            if (eclText)
            {
                eclText->setLen(schemaData, eclLen);
                if ((eclLen == 0) && names.ordinality())
                {
                    const char * firstName = names.item(0).text;

                    StringBuffer temp;
                    temp.append("RECORD ");
                    types.item(0).getECLType(temp);
                    temp.append(" value{NAMED('").append(firstName).append("')}").append("; END;");
                    eclText->set(temp.str());
                }
            }
        }
    }
}

void readRow(StringBuffer &out, MemoryBuffer &in, TypeInfoArray &types, StringAttrArray &names)
{
    ForEachItemIn(idx, types)
    {
        StringAttrItem &name = names.item(idx);
        ITypeInfo &type = types.item(idx);
        unsigned size = type.getSize();
        switch(type.getTypeCode())
        {
        case type_data:
            if (size==UNKNOWN_LENGTH)
            {
                if (in.remaining() < sizeof(int))
                    throw MakeStringException(WUERR_CorruptResult, "corrupt workunit information");
                in.read(size);
            }
            outputXmlData(size, in.readDirect(size), name.text, out);
            break;
        case type_string:
            if (size==UNKNOWN_LENGTH)
            {
                if (in.remaining() < sizeof(int))
                    throw MakeStringException(WUERR_CorruptResult, "corrupt workunit information");
                in.read(size);
            }
            outputXmlString(size, (const char *) in.readDirect(size), name.text, out);
            break;
        case type_varstring:
            {
                if (size == UNKNOWN_LENGTH)
                    size = (size32_t)strlen((const char *) in.readDirect(0))+1;
                const char * text = (const char *) in.readDirect(size);
                outputXmlString((size32_t)strlen(text), text, name.text, out);
                break;
            }
        case type_unicode:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                    in.read(len);
                outputXmlUnicode(len, (UChar const *) in.readDirect(len*2), name.text, out);
            }
            break;
        case type_utf8:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                {
                    in.read(len);
                    size = rtlUtf8Size(len, in.readDirect(0));
                }
                outputXmlUtf8(len, (const char *) in.readDirect(size), name.text, out);
            }
            break;
        case type_qstring:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                    in.read(len);
                unsigned outlen;
                char *outstr;
                rtlQStrToStrX(outlen, outstr, len, (const char *) in.readDirect(rtlQStrSize(len)));
                outputXmlString(outlen, outstr, name.text, out);
                free(outstr);
                break;
            }
        case type_int: 
        case type_swapint: 
            if (type.isSigned())
            {
                const unsigned char *raw = (const unsigned char *) in.readDirect(size);
                unsigned __int64 cval8 = 0;
                //MORE: I think this is wrong - swapped doesn't mean little/big/
                if (type.isSwappedEndian())
                {
                    unsigned idx = 0;
                    if (raw[idx] & 0x80)
                        cval8 = (__int64)-1;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[idx++];
                }
                else
                {
                    if (raw[size-1] & 0x80)
                        cval8 = (__int64)-1;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[size];
                }
                outputXmlInt((__int64) cval8, name.text, out);
            }
            else
            {
                const unsigned char *raw = (const unsigned char *) in.readDirect(size);
                unsigned __int64 cval8 = 0;
                if (type.isSwappedEndian())
                {
                    unsigned idx = 0;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[idx++];
                }
                else
                {
                    while (size--)
                        cval8 = (cval8 << 8) | raw[size];
                }
                outputXmlUInt(cval8, name.text, out);
            }
            break;
        case type_boolean:
            bool cvalb;
            in.read(cvalb);
            outputXmlBool(cvalb, name.text, out);
            break;
        case type_decimal:
            if (type.isSigned())
                outputXmlDecimal(in.readDirect(size), size, type.getPrecision(), name.text, out);
            else
                outputXmlUDecimal(in.readDirect(size), size, type.getPrecision(), name.text, out);
            break;
        case type_real:
            double cvald;
            switch(size)
            {
            case 4:
                float cvalf;
                in.read(cvalf);
                cvald = cvalf;
                break;
            case 8:
                in.read(cvald);
                break;
            }
            outputXmlReal(cvald, name.text, out);
            break;
        default:
            assertex(!"unexpected type in raw record");
            break;
        }
    }
}

IStringVal& CLocalWUResult::getResultXml(IStringVal &str) const
{
    TypeInfoArray types;
    StringAttrArray names;
    getSchema(types, names);

    StringBuffer xml;
    MemoryBuffer raw;
    p->getPropBin("Value", raw);
    const char * name = p->queryProp("@name");
    if (name)
        xml.appendf("<Dataset name=\'%s\'>\n", name);
    else
        xml.append("<Dataset>\n");

    unsigned __int64 numrows = getResultRowCount();
    while (numrows--)
    {
        xml.append(" <Row>");
        readRow(xml, raw, types, names);
        xml.append("</Row>\n");
    }
    xml.append("</Dataset>\n");
    str.set(xml.str());
    return str;
}

unsigned CLocalWUResult::getResultFetchSize() const
{
    return p->getPropInt("fetchSize", 100);
}

__int64 CLocalWUResult::getResultTotalRowCount() const
{
    return p->getPropInt64("totalRowCount", -1);
}

__int64 CLocalWUResult::getResultRowCount() const
{
    return p->getPropInt64("rowCount", 0);
}

void CLocalWUResult::getResultDataset(IStringVal & ecl, IStringVal & defs) const
{
    ecl.set(p->queryProp("datasetEcl"));
    defs.set(p->queryProp("datasetEclDefs"));
}

IStringVal& CLocalWUResult::getResultLogicalName(IStringVal & val) const
{
    val.set(p->queryProp("logicalName"));
    return val;
}

IStringVal& CLocalWUResult::getResultKeyField(IStringVal & ecl) const
{
    ecl.set(p->queryProp("keyField"));
    return ecl;
}

unsigned CLocalWUResult::getResultRequestedRows() const
{
    return p->getPropInt("requestedRows", 1);
}

IStringVal& CLocalWUResult::getResultEclSchema(IStringVal & str) const
{
    TypeInfoArray types;
    StringAttrArray names;
    getSchema(types, names, &str);
    return str;
}

IStringVal& CLocalWUResult::getResultRecordSizeEntry(IStringVal & str) const
{
    str.set(p->queryProp("@recordSizeEntry"));
    return str;
}

IStringVal& CLocalWUResult::getResultTransformerEntry(IStringVal & str) const
{
    str.set(p->queryProp("@transformerEntry"));
    return str;
}

__int64 CLocalWUResult::getResultRowLimit() const
{
    return p->getPropInt64("@rowLimit");
}

IStringVal& CLocalWUResult::getResultFilename(IStringVal & str) const
{
    str.set(p->queryProp("@tempFilename"));
    return str;
}

void CLocalWUResult::setResultStatus(WUResultStatus status)
{
    setEnum(p, "@status", status, resultStatuses);
    if (status==ResultStatusUndefined)
        p->removeProp("Value");
}

void CLocalWUResult::setResultName(const char *s)
{
    p->setProp("@name", s);
}
void CLocalWUResult::setResultSequence(unsigned seq)
{
    p->setPropInt("@sequence", seq);
}
void CLocalWUResult::setResultSchemaRaw(unsigned size, const void *schema)
{
    p->setPropBin("SchemaRaw", size, schema);
}
void CLocalWUResult::setResultScalar(bool isScalar)
{
    p->setPropInt("@isScalar", (int) isScalar);
    if (isScalar)
        setResultTotalRowCount(1);
}
void CLocalWUResult::setResultRaw(unsigned len, const void *data, WUResultFormat format)
{
    p->setPropBin("Value", len, data);
    setResultStatus(ResultStatusSupplied);
    setResultFormat(format);
}

void CLocalWUResult::setResultFormat(WUResultFormat format)
{
    switch (format)
    {
    case ResultFormatXml: 
        p->setProp("@format","xml"); 
        break;
    case ResultFormatXmlSet: 
        p->setProp("@format","xmlSet"); 
        break;
    case ResultFormatCsv: 
        p->setProp("@format","csv"); 
        break;
    default:
        p->removeProp("@format");
        break;
    }
}

void CLocalWUResult::setResultXML(const char *val)
{
    p->setProp("xmlValue", val);
}

void CLocalWUResult::addResultRaw(unsigned len, const void *data, WUResultFormat format)
{
    p->appendPropBin("Value", len, data);
    setResultStatus(ResultStatusPartial);
    const char *existingFormat = p->queryProp("@format");
    const char *formatStr = NULL;
    switch (format)
    {
    case ResultFormatXml: 
        formatStr = "xml"; 
        break;
    case ResultFormatXmlSet: 
        formatStr = "xmlSet";
        break;
    case ResultFormatCsv: 
        formatStr = "csv";
        break;
    default:
        p->removeProp("@format");
        break;
    }
    if (format)
    {
        if (existingFormat)
        {
            if (0 != stricmp(formatStr, existingFormat))
                throw MakeStringException(WUERR_ResultFormatMismatch, "addResult format %s, does not match existing format %s", formatStr, existingFormat);
        }
        else
            p->setProp("@format", formatStr);

    }
}

void CLocalWUResult::setResultFetchSize(unsigned rows)
{
    p->setPropInt("fetchSize", rows);
}

void CLocalWUResult::setResultTotalRowCount(__int64 rows)
{
    p->setPropInt64("totalRowCount", rows);
}

void CLocalWUResult::setResultRowCount(__int64 rows)
{
    p->setPropInt64("rowCount", rows);
}

void CLocalWUResult::setResultDataset(const char *ecl, const char *defs)
{
    p->setProp("datasetEcl", ecl);
    p->setProp("datasetEclDefs", defs);
}

void CLocalWUResult::setResultLogicalName(const char *logicalName)
{
    p->setProp("logicalName", logicalName);
}

void CLocalWUResult::setResultKeyField(const char *ecl)
{
    p->setProp("keyField", ecl);
}

void CLocalWUResult::setResultRequestedRows(unsigned rows)
{
    p->setPropInt("requestedRows", rows);
}

void CLocalWUResult::setResultRecordSizeEntry(const char * entry)
{
    p->setProp("@recordSizeEntry", entry);
}

void CLocalWUResult::setResultTransformerEntry(const char * entry)
{
    p->setProp("@transformerEntry", entry);
}


void CLocalWUResult::setResultRowLimit(__int64 value)
{
    p->setPropInt64("@rowLimit", value);
}

void CLocalWUResult::setResultFilename(const char * name)
{
    p->setProp("@tempFilename", name);
}

// MORE - it's an undetected error if we call getResult... of a type that does not match schema

__int64 CLocalWUResult::getResultInt() const
{
    __int64 result = 0;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
        result = p->getPropInt64("xmlValue");
    return result;
}

bool CLocalWUResult::getResultBool() const
{
    bool result = false;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
        result = p->getPropBool("xmlValue");
    return result;
}

double CLocalWUResult::getResultReal() const
{
    double result = 0;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
    {
        const char *xmlVal = p->queryProp("xmlValue");
        if (xmlVal)
            result = atof(xmlVal);
    }
    return result;
}

void CLocalWUResult::getResultDecimal(void * val, unsigned len, unsigned precision, bool isSigned) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        assertex(s.length() == len);
        s.read(len, val);
    }
    else
    {
        const char *xmlVal = p->queryProp("xmlValue");
        if (xmlVal)
        {
            Decimal d;
            d.setString(strlen(xmlVal), xmlVal);
            if (isSigned)
                d.getDecimal(len, precision, val);
            else
                d.getUDecimal(len, precision, val);
        }
        else
            memset(val, 0, len);
    }
}

IStringVal& CLocalWUResult::getResultString(IStringVal & str) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        unsigned len;
        s.read(len);
        str.setLen((const char *) s.readDirect(len), len);
    }
    else
    {
        p->getPropBin("xmlValue", s);
        if (p->isBinary("xmlValue"))
            str.setLen(s.toByteArray(), s.length());
        else
        {
            char *ascii = rtlUtf8ToVStr(rtlUtf8Length(s.length(), s.toByteArray()), s.toByteArray());
            str.set(ascii);
            rtlFree(ascii);
        }
    }
    return str;
}


WUResultFormat CLocalWUResult::getResultFormat() const
{
    const char * format = p->queryProp("@format");
    if (!format)
        return ResultFormatRaw;
    else if (strcmp(format, "xml") == 0)
        return ResultFormatXml;
    else if (strcmp(format, "xmlSet") == 0)
        return ResultFormatXmlSet;
    else if (strcmp(format, "csv") == 0)
        return ResultFormatCsv;
    else
        throw MakeStringException(WUERR_InvalidResultFormat, "Unrecognised result format %s", format);
}

IDataVal& CLocalWUResult::getResultRaw(IDataVal & data, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    unsigned len = s.length();
    if (len)
    {
        WUResultFormat format = getResultFormat();
        if (format == ResultFormatXml || format == ResultFormatXmlSet)
        {
            if (!xmlTransformer)
                throw MakeStringException(WUERR_MissingFormatTranslator, "No transformer supplied to translate XML format result");
            xmlTransformer->transform(data, len, s.readDirect(len), format == ResultFormatXml);
        }
        else if (format == ResultFormatCsv)
        {
            if (!csvTransformer)
                throw MakeStringException(WUERR_MissingFormatTranslator, "No transformer supplied to translate Csv format result");
            csvTransformer->transform(data, len, s.readDirect(len), true);
        }
        else
            data.setLen(s.readDirect(len), len);
    }
    else
        data.clear();
    return data;
}

unsigned CLocalWUResult::getResultHash() const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    unsigned len = s.length();
    const byte * data = (const byte *)s.toByteArray();
    return ~hashc(data, len, ~0);
}


IDataVal& CLocalWUResult::getResultUnicode(IDataVal & data) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        unsigned len;
        s.read(len);
        data.setLen(s.readDirect(len*2), len*2);
    }
    else
    {
        StringBuffer utf8;
        if (p->getProp("xmlValue", utf8))
        {
            unsigned outlen;
            UChar *out;
            rtlUtf8ToUnicodeX(outlen, out, utf8.length(), utf8.str());
            data.setLen(out, outlen*2);
            rtlFree(out);
        }
        else
            data.clear();
    }
    return data;
}

__int64 CLocalWUResult::getResultRawSize(IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    WUResultFormat format = getResultFormat();
    if (format == ResultFormatRaw)
    {
        //MORE: This should not load the whole property...
        MemoryBuffer s;
        p->getPropBin("Value", s);
        return s.length();
    }
    else
    {
        MemoryBuffer temp;
        MemoryBuffer2IDataVal adaptor(temp);
        getResultRaw(adaptor, xmlTransformer, csvTransformer);
        return temp.length();
    }
}

IDataVal& CLocalWUResult::getResultRaw(IDataVal & data, __int64 from, __int64 length, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    WUResultFormat format = getResultFormat();
    if (format != ResultFormatRaw)
    {
        MemoryBuffer temp;
        MemoryBuffer2IDataVal adaptor(temp);
        getResultRaw(adaptor, xmlTransformer, csvTransformer);

        unsigned len = temp.length();
        if (from > len) from = len;
        if (from + length > len) length = len - from;
        data.setLen(temp.readDirect(len) + from, (size32_t)length);
        return data;
    }
    else
    {
        //MORE: This should not load the whole property, and should be different from the code above...
        MemoryBuffer s;
        p->getPropBin("Value", s);
        unsigned len = s.length();
        if (from > len) from = len;
        if (from + length > len) length = len - from;
        data.setLen(s.readDirect(len) + from, (size32_t)length);
        return data;
    }
}

bool CLocalWUResult::getResultIsAll() const
{
    return p->getPropBool("@isAll", false);
}

// MORE - it's an undetected error if we call setResult... of a type that does not match schema

void CLocalWUResult::setResultInt(__int64 val)
{
    // Note: we always serialize scalar integer results as int8, and schema must reflect this
    MemoryBuffer m;
    serializeInt8(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultUInt(unsigned __int64 val)
{
    setResultInt((__int64) val);
}

void CLocalWUResult::setResultReal(double val)
{
    // Note: we always serialize scalar real results as real8, and schema must reflect this
    MemoryBuffer m;
    serializeReal8(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultBool(bool val)
{
    MemoryBuffer m;
    serializeBool(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultString(const char *val, unsigned len)
{
    // Note: we always serialize scalar strings with length prefix, and schema must reflect this
    MemoryBuffer m;
    serializeLPString(len, val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultUnicode(const void *val, unsigned len)
{
    // Note: we always serialize scalar strings with length prefix, and schema must reflect this
    MemoryBuffer m;
    m.append(len).append(len*2, val);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultData(const void *val, unsigned len)
{
    // Note: we always serialize scalar data with length prefix, and schema must reflect this
    MemoryBuffer m;
    serializeLPString(len, (const char *)val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultDecimal(const void *val, unsigned len)
{
    // Note: serialized as data but with length known from schema
    MemoryBuffer m;
    serializeFixedData(len, val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultRow(unsigned len, const void * data)
{
    p->setPropBin("Value", len, data);
    setResultRowCount(1);
    setResultTotalRowCount(1);
    setResultFormat(ResultFormatRaw);
}
void CLocalWUResult::setResultIsAll(bool value)
{
    p->setPropBool("@isAll", value);
}

//==========================================================================================

CLocalWUPlugin::CLocalWUPlugin(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUPlugin::getPluginName(IStringVal &str) const
{
    str.set(p->queryProp("@dllname"));
    return str;
}

IStringVal& CLocalWUPlugin::getPluginVersion(IStringVal &str) const
{
    str.set(p->queryProp("@version"));
    return str;
}

bool CLocalWUPlugin::getPluginThor() const
{
    return p->getPropInt("@thor") != 0;
}

bool CLocalWUPlugin::getPluginHole() const
{
    return p->getPropInt("@hole") != 0;
}

void CLocalWUPlugin::setPluginName(const char *str)
{
    p->setProp("@dllname", str);
}

void CLocalWUPlugin::setPluginVersion(const char *str)
{
    p->setProp("@version", str);
}

void CLocalWUPlugin::setPluginThor(bool on)
{
    p->setPropInt("@thor", (int) on);
}

void CLocalWUPlugin::setPluginHole(bool on)
{
    p->setPropInt("@hole", (int) on);
}

//==========================================================================================

class WULibraryActivityIterator : public CInterface, implements IConstWULibraryActivityIterator
{
public:
    WULibraryActivityIterator(IPropertyTree * tree) { iter.setown(tree->getElements("activity")); }
    IMPLEMENT_IINTERFACE;
    bool                first() { return iter->first(); }
    bool                isValid() { return iter->isValid(); }
    bool                next() { return iter->next(); }
    unsigned            query() const { return iter->query().getPropInt("@id"); }
private:
    Owned<IPropertyTreeIterator> iter;
};

CLocalWULibrary::CLocalWULibrary(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWULibrary::getName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IConstWULibraryActivityIterator * CLocalWULibrary::getActivities() const 
{ 
    return new WULibraryActivityIterator(p); 
}

void CLocalWULibrary::setName(const char *str)
{
    p->setProp("@name", str);
}

void CLocalWULibrary::addActivity(unsigned id)
{
    StringBuffer s;
    s.append("activity[@id=\"").append(id).append("\"]");
    if (!p->hasProp(s.str()))
        p->addPropTree("activity", createPTree())->setPropInt("@id", id);
}

//==========================================================================================

CLocalWUActivity::CLocalWUActivity(IPropertyTree *props, __int64 id) : p(props)
{
    if (id)
        p->setPropInt64("@id", id);
}

__int64 CLocalWUActivity::getId() const
{
    return p->getPropInt64("@id");
}

unsigned CLocalWUActivity::getKind() const
{
    return (unsigned)p->getPropInt("@kind");
}

IStringVal & CLocalWUActivity::getHelper(IStringVal & str) const
{
    str.set(p->queryProp("@helper"));
    return str;
}


void CLocalWUActivity::setKind(unsigned kind)
{
    p->setPropInt64("@kind", kind);
}

void CLocalWUActivity::setHelper(const char * str)
{
    p->setProp("@helper", str);
}

//==========================================================================================

CLocalWUException::CLocalWUException(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUException::getExceptionSource(IStringVal &str) const
{
    str.set(p->queryProp("@source"));
    return str;
}

IStringVal& CLocalWUException::getExceptionMessage(IStringVal &str) const
{
    str.set(p->queryProp(NULL));
    return str;
}

unsigned  CLocalWUException::getExceptionCode() const
{
    return p->getPropInt("@code", 0);
}

WUExceptionSeverity CLocalWUException::getSeverity() const
{
    return (WUExceptionSeverity)p->getPropInt("@severity", ExceptionSeverityError);
}

IStringVal & CLocalWUException::getTimeStamp(IStringVal & dt) const
{
    dt.set(p->queryProp("@time"));
    return dt;
}

IStringVal & CLocalWUException::getExceptionFileName(IStringVal & str) const
{
    str.set(p->queryProp("@filename"));
    return str;
}

unsigned CLocalWUException::getExceptionLineNo() const
{
    return p->getPropInt("@row", 0);
}

unsigned CLocalWUException::getExceptionColumn() const
{
    return p->getPropInt("@col", 0);
}

void CLocalWUException::setExceptionSource(const char *str)
{
    p->setProp("@source", str);
}

void CLocalWUException::setExceptionMessage(const char *str)
{
    p->setProp(NULL, str);
}

void CLocalWUException::setExceptionCode(unsigned code)
{
    p->setPropInt("@code", code);
}

void CLocalWUException::setSeverity(WUExceptionSeverity level)
{
    p->setPropInt("@severity", level);
}

void CLocalWUException::setTimeStamp(const char *str)
{
    p->setProp("@time", str);
}

void CLocalWUException::setExceptionFileName(const char *str)
{
    p->setProp("@filename", str);
}

void CLocalWUException::setExceptionLineNo(unsigned r)
{
    p->setPropInt("@row", r);
}

void CLocalWUException::setExceptionColumn(unsigned c)
{
    p->setPropInt("@col", c);
}

//==========================================================================================

CLocalWUTimeStamp::CLocalWUTimeStamp(IPropertyTree *props) : p(props)
{
}

IStringVal & CLocalWUTimeStamp::getApplication(IStringVal & str) const
{
    str.set(p->queryProp("@application"));
    return str;
}

IStringVal & CLocalWUTimeStamp::getEvent(IStringVal & str) const
{
    IPropertyTree* evt=p->queryPropTree("*[1]");
    if(evt)
        str.set(evt->queryName());
    return str;
}

IStringVal & CLocalWUTimeStamp::getDate(IStringVal & str) const
{
    str.set(p->queryProp("*[1]"));
    return str;
}

//==========================================================================================

CLocalWUAppValue::CLocalWUAppValue(IPropertyTree *props,unsigned child): p(props)
{
    prop.append("*[").append(child).append("]");
}

IStringVal & CLocalWUAppValue::getApplication(IStringVal & str) const
{
    str.set(p->queryName());
    return str;
}

IStringVal & CLocalWUAppValue::getName(IStringVal & str) const
{
    IPropertyTree* val=p->queryPropTree(prop.str());
    if(val)
        str.set(val->queryName());
    return str;
}

IStringVal & CLocalWUAppValue::getValue(IStringVal & str) const
{
    str.set(p->queryProp(prop.str()));
    return str;
}

//==========================================================================================

CLocalWUStatistic::CLocalWUStatistic(IPropertyTree *props) : p(props)
{
}

IStringVal & CLocalWUStatistic::getFullName(IStringVal & str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal & CLocalWUStatistic::getCreator(IStringVal & str) const
{
    const char * name = p->queryProp("@name");
    const char * sep1 = strchr(name, ';');
    assertex(sep1);
    str.setLen(name, sep1-name);
    return str;
}

IStringVal & CLocalWUStatistic::getDescription(IStringVal & str) const
{
    str.set(p->queryProp("@desc"));
    return str;
}

IStringVal & CLocalWUStatistic::getName(IStringVal & str) const
{
    const char * name = p->queryProp("@name");
    const char * sep1 = strchr(name, ';');
    assertex(sep1);
    const char * scope = sep1+1;
    const char * sep2 = strchr(scope, ';');
    assertex(sep2);
    str.set(sep2+1);
    return str;
}

IStringVal & CLocalWUStatistic::getScope(IStringVal & str) const
{
    const char * name = p->queryProp("@name");
    const char * sep1 = strchr(name, ';');
    assertex(sep1);
    const char * scope = sep1+1;
    const char * sep2 = strchr(scope, ';');
    assertex(sep2);
    str.setLen(scope, sep2-scope);
    return str;
}

StatisticMeasure CLocalWUStatistic::getKind() const
{
    return (StatisticMeasure)getEnum(p, "@unit", queryStatMeasure);
}

unsigned __int64 CLocalWUStatistic::getValue() const
{
    return p->getPropInt64("@value", 0);
}

unsigned __int64 CLocalWUStatistic::getCount() const
{
    return p->getPropInt64("@count", 0);
}

unsigned __int64 CLocalWUStatistic::getMax() const
{
    return p->getPropInt64("@max", 0);
}

//==========================================================================================

extern WORKUNIT_API ILocalWorkUnit * createLocalWorkUnit()
{
    Owned<CLocalWorkUnit> cw = new CLocalWorkUnit("W_LOCAL", NULL, (ISecManager*)NULL, NULL);
    ILocalWorkUnit* ret = QUERYINTERFACE(&cw->lockRemote(false), ILocalWorkUnit);
    return ret;
}

extern WORKUNIT_API StringBuffer &exportWorkUnitToXML(const IConstWorkUnit *wu, StringBuffer &str, bool unpack)
{
    const CLocalWorkUnit *w = QUERYINTERFACE(wu, const CLocalWorkUnit);
    if (!w)
    {
        const CLockedWorkUnit *wl = QUERYINTERFACE(wu, const CLockedWorkUnit);
        if (wl)
            w = wl->c;
    }
    if (w)
    {
        Linked<IPropertyTree> p;
        if (unpack)
            p.setown(w->getUnpackedTree());
        else
            p.set(w->p);
        toXML(p, str, 0, XML_Format|XML_SortTags);
    }
    else
        str.append("Unrecognized workunit format");
    return str;
}

extern WORKUNIT_API IStringVal& exportWorkUnitToXML(const IConstWorkUnit *wu, IStringVal &str, bool unpack)
{
    StringBuffer x;
    str.set(exportWorkUnitToXML(wu,x,unpack).str());
    return str;
}

extern WORKUNIT_API void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool unpack)
{
    const CLocalWorkUnit *w = QUERYINTERFACE(wu, const CLocalWorkUnit);
    if (!w)
    {
        const CLockedWorkUnit *wl = QUERYINTERFACE(wu, const CLockedWorkUnit);
        if (wl)
            w = wl->c;
    }
    if (w)
    {
        Linked<IPropertyTree> p;
        if (unpack)
            p.setown(w->getUnpackedTree());
        else
            p.set(w->p);
        saveXML(filename, p, 0, XML_Format|XML_SortTags|extraXmlFlags);
    }
}


extern WORKUNIT_API void submitWorkUnit(const char *wuid, const char *username, const char *password)
{
    MemoryBuffer buffer;
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0); // MORE - security token?
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
    assertex(workunit);

    SCMStringBuffer token;
    createToken(wuid, username, password, token);
    workunit->setSecurityToken(token.str());
    SCMStringBuffer clusterName;
    workunit->getClusterName(clusterName);
    if (!clusterName.length()) 
        throw MakeStringException(WUERR_InvalidCluster, "No target cluster specified");
    workunit->commit();
    workunit.clear();
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
    if (!clusterInfo) 
        throw MakeStringException(WUERR_InvalidCluster, "Unknown cluster %s", clusterName.str());
    SCMStringBuffer serverQueue;
    clusterInfo->getServerQueue(serverQueue);
    assertex(serverQueue.length());
    Owned<IJobQueue> queue = createJobQueue(serverQueue.str());
    if (!queue.get()) 
        throw MakeStringException(WUERR_InvalidQueue, "Could not create workunit queue");

    IJobQueueItem *item = createJobQueueItem(wuid);
    queue->enqueue(item);
}
extern WORKUNIT_API void abortWorkUnit(const char *wuid)
{
    StringBuffer xpath("/WorkUnitAborts/");
    xpath.append(wuid);
    Owned<IRemoteConnection> acon = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE, SDS_LOCK_TIMEOUT);
    acon->queryRoot()->setPropInt(NULL, 1);
}
extern WORKUNIT_API void secSubmitWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser)
{
    if (checkWuSecAccess(wuid, secmgr, &secuser, SecAccess_Write, "Submit", true, true))
        submitWorkUnit(wuid, secuser.getName(), secuser.credentials().getPassword());
}

extern WORKUNIT_API void secAbortWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser)
{
    if (checkWuSecAccess(wuid, secmgr, &secuser, SecAccess_Write, "Submit", true, true))
        abortWorkUnit(wuid);
}

extern WORKUNIT_API void submitWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return secSubmitWorkUnit(wuid, *secmgr, *secuser);
    if (secuser)
        return submitWorkUnit(wuid, secuser->getName(), secuser->credentials().getPassword());
    submitWorkUnit(wuid, "", "");
}

extern WORKUNIT_API void abortWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return secAbortWorkUnit(wuid, *secmgr, *secuser);
    abortWorkUnit(wuid);
}

bool CLocalWorkUnit::hasWorkflow() const
{
    return p->hasProp("Workflow");
}

unsigned CLocalWorkUnit::queryEventScheduledCount() const
{
    CriticalBlock block(crit);
    return p->getPropInt("Workflow/@eventScheduledCount", 0);
}

void CLocalWorkUnit::incEventScheduledCount()
{
    CriticalBlock block(crit);
    p->setPropInt("Workflow/@eventScheduledCount", p->getPropInt("Workflow/@eventScheduledCount", 0)+1);
}

IPropertyTree * CLocalWorkUnit::queryWorkflowTree() const
{
    CriticalBlock block(crit);
    return p->queryPropTree("Workflow");
}

IConstWorkflowItemIterator* CLocalWorkUnit::getWorkflowItems() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if(!workflowIteratorCached)
    {
        assertex(!workflowIterator);
        Owned<IPropertyTree> s = p->getPropTree("Workflow");
        if(s)
            workflowIterator.setown(createWorkflowItemIterator(s)); 
        workflowIteratorCached = true;
    }
    return workflowIterator.getLink();
}

IWorkflowItemArray * CLocalWorkUnit::getWorkflowClone() const
{
    unsigned count = 0;
    Owned<IConstWorkflowItemIterator> iter = getWorkflowItems();
    for(iter->first(); iter->isValid(); iter->next())
        count++;
    Owned<IWorkflowItemArray> array = createWorkflowItemArray(count);
    for(iter->first(); iter->isValid(); iter->next())
        array->addClone(iter->query());
    return array.getLink();
}

IWorkflowItem * CLocalWorkUnit::addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    workflowIterator.clear();
    workflowIteratorCached = false;
    IPropertyTree * s = p->queryPropTree("Workflow");
    if(!s)
        s = p->addPropTree("Workflow", createPTree("Workflow"));
    return createWorkflowItem(s, wfid, type, mode, success, failure, recovery, retriesAllowed, contingencyFor);
}

IWorkflowItemIterator * CLocalWorkUnit::updateWorkflowItems()
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if(!workflowIterator)
    {
        IPropertyTree * s = p->queryPropTree("Workflow");
        if(!s)
            s = p->addPropTree("Workflow", createPTree("Workflow"));
        workflowIterator.setown(createWorkflowItemIterator(s)); 
        workflowIteratorCached = true;
    }
    return workflowIterator.getLink();
}

void CLocalWorkUnit::syncRuntimeWorkflow(IWorkflowItemArray * array)
{
    Owned<IWorkflowItemIterator> iter = updateWorkflowItems();
    Owned<IWorkflowItem> item;
    for(iter->first(); iter->isValid(); iter->next())
    {
        item.setown(iter->get());
        item->syncRuntimeData(array->queryWfid(item->queryWfid()));
    }
    workflowIterator.clear();
    workflowIteratorCached = false;
}

void CLocalWorkUnit::resetWorkflow()
{
    if (hasWorkflow())
    {
        Owned<IWorkflowItemIterator> iter = updateWorkflowItems();
        Owned<IWorkflowItem> wf;
        for(iter->first(); iter->isValid(); iter->next())
        {
            wf.setown(iter->get());
            wf->reset();
        }
        workflowIterator.clear();
        workflowIteratorCached = false;
    }
}

void CLocalWorkUnit::schedule()
{
    CriticalBlock block(crit);
    if(queryEventScheduledCount() == 0) return;

    switch(getState())
    {
    case WUStateCompleted:
        setState(WUStateWait);
        break;
    case WUStateFailed:
    case WUStateArchived:
    case WUStateAborting:
    case WUStateAborted:
    case WUStateScheduled:
        throw MakeStringException(WUERR_CannotSchedule, "Cannot schedule workunit in this state");
    }

    StringBuffer rootPath;
    SCMStringBuffer clusterName;
    getClusterName(clusterName);
    rootPath.append("/Schedule/").append(clusterName.str());
    Owned<IRemoteConnection> conn = querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    Owned<IPropertyTree> root = conn->getRoot();
    if(!root->hasChildren())
    {
        StringBuffer addPath;
        addPath.append("/Schedulers/").append(clusterName.str());
        Owned<IRemoteConnection> addConn = querySDS().connect(addPath.str(), myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    }

    char const * wuid = p->queryName();
    StringBuffer xpath("*/*/");
    ncnameEscape(wuid, xpath);
    bool more;
    do more = root->removeProp(xpath.str()); while(more);
        
    Owned<IConstWorkflowItemIterator> iter = getWorkflowItems();
    Owned<IWorkflowEvent> event;
    Owned<IPropertyTree> branch1, branch2;
    for(iter->first(); iter->isValid(); iter->next())
    {
        event.setown(iter->query()->getScheduleEvent());
        if(!event) continue;
        ncnameEscape(event->queryName(), xpath.clear());
        ensurePTree(root, xpath.str());
        branch1.setown(root->getPropTree(xpath.str()));
        ncnameEscape(event->queryText(), xpath.clear());
        ensurePTree(branch1, xpath.str());
        branch2.setown(branch1->getPropTree(xpath.str()));
        ncnameEscape(wuid, xpath.clear());
        ensurePTree(branch2, xpath.str());
    }
}

void CLocalWorkUnit::deschedule()
{
    if(queryEventScheduledCount() == 0) return;
    if(getState() == WUStateWait)
        setState(WUStateCompleted);
    doDescheduleWorkkunit(p->queryName());
}

mapEnums localFileUploadTypes[] = {
    { UploadTypeFileSpray, "FileSpray" },
    { UploadTypeWUResult, "WUResult" },
    { UploadTypeWUResultCsv, "WUResultCsv" },
    { UploadTypeWUResultXml, "WUResultXml" },
    { UploadTypeSize, NULL }
};

class CLocalFileUpload : public CInterface, implements IConstLocalFileUpload
{
public:
    CLocalFileUpload(IPropertyTree * _tree) : tree(_tree) {}
    CLocalFileUpload(unsigned id, LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
    {
        tree.setown(createPTree());
        tree->setPropInt("@id", id);
        setEnum(tree, "@type", type, localFileUploadTypes);
        tree->setProp("@source", source);
        tree->setProp("@destination", destination);
        if (eventTag)
            tree->setProp("@eventTag", eventTag);
    }
    IMPLEMENT_IINTERFACE;
    IPropertyTree * getTree() { return tree.getLink(); }

    virtual unsigned queryID() const { return tree->getPropInt("@id"); }
    virtual LocalFileUploadType queryType() const { return (LocalFileUploadType)getEnum(tree, "@type", localFileUploadTypes); }
    virtual IStringVal & getSource(IStringVal & ret) const { ret.set(tree->queryProp("@source")); return ret; }
    virtual IStringVal & getDestination(IStringVal & ret) const { ret.set(tree->queryProp("@destination")); return ret; }
    virtual IStringVal & getEventTag(IStringVal & ret) const { if(tree->hasProp("@eventTag")) ret.set(tree->queryProp("@eventTag")); else ret.clear(); return ret; }

private:
    Owned<IPropertyTree> tree;
};

class CLocalFileUploadIterator : public CInterface, implements IConstLocalFileUploadIterator
{
public:
    CLocalFileUploadIterator(IPropertyTree * _tree) : tree(_tree), iter(tree->getElements("LocalFileUpload")) {}
    IMPLEMENT_IINTERFACE;
    bool first() { return iter->first(); }
    bool isValid() { return iter->isValid(); }
    bool next() { return iter->next(); }
    IConstLocalFileUpload * get() { return new CLocalFileUpload(&iter->get()); }

private:
    Owned<IPropertyTree> tree;
    Owned<IPropertyTreeIterator> iter;
};

IConstLocalFileUploadIterator * CLocalWorkUnit::getLocalFileUploads() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    Owned<IPropertyTree> s = p->getPropTree("LocalFileUploads");
    if(s)
        return new CLocalFileUploadIterator(s.getClear());
    else
        return NULL;
}

bool CLocalWorkUnit::requiresLocalFileUpload() const
{
    SCMStringBuffer dest;
    Owned<IConstWUResult> result;
    Owned<IConstLocalFileUploadIterator> iter(getLocalFileUploads());
    if(!iter)
        return false;
    for(iter->first(); iter->isValid(); iter->next())
    {
        Owned<IConstLocalFileUpload> upload(iter->get());
        switch(upload->queryType())
        {
        case UploadTypeWUResult:
        case UploadTypeWUResultCsv:
        case UploadTypeWUResultXml:
            upload->getDestination(dest);
            result.setown(getResultByName(dest.str()));
            if(!result)
                return true;
            break;
        default:
            throw MakeStringException(WUERR_InvalidUploadFormat, "Unsupported local file upload type %s", getEnumText(upload->queryType(), localFileUploadTypes));
        }
    }
    return false;
}

unsigned CLocalWorkUnit::addLocalFileUpload(LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    IPropertyTree * s = p->queryPropTree("LocalFileUploads");
    if(!s)
        s = p->addPropTree("LocalFileUploads", createPTree());
    unsigned id = s->numChildren();
    Owned<CLocalFileUpload> upload = new CLocalFileUpload(id, type, source, destination, eventTag);
    s->addPropTree("LocalFileUpload", upload->getTree());
    return id;
}

#if 0
void testConstWorkflow(IConstWorkflowItem * cwf, bool * okay, bool * dep)
{
    DBGLOG("Test workflow const iface %u", cwf->queryWfid());
    unsigned deps = 0;
    Owned<IWorkflowDependencyIterator> diter;
    switch(cwf->queryWfid())
    {
    case 1:
        assertex(!cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeNormal);
        assertex(cwf->queryState() == WFStateNull);
        diter.setown(cwf->getDependencies());
        for(diter->first(); diter->isValid(); diter->next())
            deps++;
        assertex(deps==0);
        okay[0] = true;
        break;
    case 2:
        assertex(!cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeRecovery);
        assertex(cwf->queryState() == WFStateSkip);
        okay[1] = true;
        break;
    case 3:
        assertex(cwf->queryContingencyFor() == 4);
        okay[2] = true;
        break;
    case 4:
        assertex(cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeNormal);
        assertex(cwf->queryState() == WFStateReqd);
        assertex(cwf->querySuccess() == 0);
        assertex(cwf->queryFailure() == 3);
        assertex(cwf->queryRecovery() == 2);
        assertex(cwf->queryRetriesAllowed() == 10);
        assertex(cwf->queryRetriesRemaining() == 10);
        diter.setown(cwf->getDependencies());
        for(diter->first(); diter->isValid(); diter->next())
        {
            dep[diter->query()-1] = true;
            deps++;
        }
        assertex(deps==2);
        assertex(dep[0]);
        assertex(dep[1]);
        okay[3] = true;
        break;
    case 5:
        assertex(cwf->isScheduled());
        assertex(!cwf->isScheduledNow());
        assertex(cwf->querySchedulePriority() == 75);
        assertex(cwf->queryScheduleCount() == 5);
        assertex(cwf->queryScheduleCountRemaining() == 5);
        okay[4] = true;
        break;
    case 6:
        assertex(cwf->isScheduled());
        assertex(!cwf->isScheduledNow());
        assertex(cwf->querySchedulePriority() == 25);
        assertex(!cwf->hasScheduleCount());
        okay[5] = true;
        break;
    default:
        assertex(!"unknown wfid in test");
    }
}

void testRuntimeWorkflow(IRuntimeWorkflowItem * rwf, bool * okay)
{
    DBGLOG("Test workflow runtime iface %u", rwf->queryWfid());
    switch(rwf->queryWfid())
    {
    case 1:
    case 2:
    case 3:
        okay[rwf->queryWfid()-1] = true;
        break;
    case 4:
        {
            unsigned tries = 0;
            while(rwf->testAndDecRetries())
                tries++;
            assertex(tries == 10);
            assertex(rwf->queryRetriesRemaining() == 0);
            rwf->setState(WFStateFail);
            assertex(rwf->queryState() == WFStateFail);
            rwf->reset();
            assertex(rwf->queryRetriesRemaining() == 10);
            assertex(rwf->queryState() == WFStateReqd);
        }
        okay[3] = true;
        break;
    case 5:
        {
            assertex(rwf->queryScheduleCountRemaining() == 5);
            unsigned count = 0;
            do count++; while(rwf->decAndTestScheduleCountRemaining());
            assertex(count == 5);
            assertex(rwf->queryScheduleCountRemaining() == 0);
            rwf->reset();
            assertex(rwf->queryScheduleCountRemaining() == 5);
        }
        okay[4] = true;
        break;
    case 6:
        {
            assertex(!rwf->hasScheduleCount());
            unsigned count;
            for(count=0; count<20; count++)
                assertex(rwf->decAndTestScheduleCountRemaining());
        }
        okay[5] = true;
        break;
    default:
        assertex(!"unknown wfid in test");
    }
}

void testWorkflow()
{
    DBGLOG("workunit.cpp : testWorkflow");
    CLocalWorkUnit wu("W-WF-TEST", 0, 0, 0);
    Owned<IWorkflowItem> wf;
    wf.setown(wu.addWorkflowItem(1, WFTypeNormal, 0, 0, 0, 0, 0));
    wf.setown(wu.addWorkflowItem(2, WFTypeRecovery, 0, 0, 0, 0, 0));
    wf.setown(wu.addWorkflowItem(3, WFTypeFailure, 0, 0, 0, 0, 4));
    wf.setown(wu.addWorkflowItem(4, WFTypeNormal, 0, 3, 2, 10, 0));
    wf->setScheduledNow();
    wf->addDependency(1);
    wf.setown(wu.addWorkflowItem(5, WFTypeNormal, 0, 0, 0, 0, 0));
    wf->setScheduledOn("test", "foo*");
    wf->setSchedulePriority(75);
    wf->setScheduleCount(5);
    wf.setown(wu.addWorkflowItem(6, WFTypeNormal, 0, 0, 0, 0, 0));
    wf->setScheduledOn("test", "bar*");
    wf->setSchedulePriority(25);

    unsigned const n = 6;
    bool okay[n];
    bool dep[n];
    unsigned i;
    for(i=0; i<n; i++)
        okay[i] = dep[i] = 0;

    Owned<IConstWorkflowItemIterator> citer(wu.getWorkflowItems());
    for(citer->first(); citer->isValid(); citer->next())
        testConstWorkflow(citer->query(), okay, dep);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }

    Owned<IWorkflowItemIterator> miter(wu.updateWorkflowItems());
    for(miter->first(); miter->isValid(); miter->next())
    {
        Owned<IRuntimeWorkflowItem> rwf(miter->get());
        testRuntimeWorkflow(rwf, okay);
    }

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = dep[i] = false;
    }

    Owned<IWorkflowItemArray> array(wu.getWorkflowClone());
    unsigned wfid;
    for(wfid = 1; array->isValid(wfid); wfid++)
        testConstWorkflow(&array->queryWfid(wfid), okay, dep);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }

    for(wfid = 1; array->isValid(wfid); wfid++)
        testRuntimeWorkflow(&array->queryWfid(wfid), okay);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }
}
#endif

//------------------------------------------------------------------------------------------

class WorkUnitWaiter : public CInterface, implements ISDSSubscription, implements IAbortHandler
{
    Semaphore changed;
    SubscriptionId change;
public:
    IMPLEMENT_IINTERFACE;

    WorkUnitWaiter(const char *xpath) 
    {
        change = querySDS().subscribe(xpath, *this, false);
        aborted = false; 
    }
    ~WorkUnitWaiter()
    {
        assertex(change==0);
    }

    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        changed.signal();
    }
    bool wait(unsigned timeout)
    {
        return changed.wait(timeout) && !aborted;
    }
    bool onAbort()
    {
        aborted = true;
        changed.signal();
        return false;
    }
    void unsubscribe()
    {
        querySDS().unsubscribe(change);
        change = 0;
    }
    bool aborted;
};

static WUState _waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, bool returnOnWaitState)
{
    StringBuffer wuRoot;
    getXPath(wuRoot, wuid);
    Owned<WorkUnitWaiter> waiter = new WorkUnitWaiter(wuRoot.str());
    LocalIAbortHandler abortHandler(*waiter);
    WUState ret = WUStateUnknown;
    Owned<IRemoteConnection> conn = querySDS().connect(wuRoot.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        unsigned start = msTick();
        loop
        {
            ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
            switch (ret)
            {
            case WUStateCompiled:
            case WUStateUploadingFiles:
                if (!compiled)
                    break;
                // fall into
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateAborted:
                waiter->unsubscribe();
                return ret;
            case WUStateWait:
                if(returnOnWaitState)
                {
                    waiter->unsubscribe();
                    return ret;
                }
                break;
            case WUStateCompiling:
            case WUStateRunning:
            case WUStateDebugPaused:
            case WUStateDebugRunning:
            case WUStateBlocked:
            case WUStateAborting:
                if (queryDaliServerVersion().compare("2.1")>=0)
                {
                    SessionId agent = conn->queryRoot()->getPropInt64("@agentSession", -1);
                    if((agent>0) && querySessionManager().sessionStopped(agent, 0))
                    {
                        waiter->unsubscribe();
                        conn->reload();
                        ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
                        bool isEcl = false;
                        switch (ret)
                        {
                            case WUStateCompiling:
                                isEcl = true;
                                // drop into
                            case WUStateRunning:
                            case WUStateBlocked:
                                ret = WUStateFailed;
                                break;
                            case WUStateAborting:
                                ret = WUStateAborted;
                                break;
                            default:
                                return ret;
                        }
                        WARNLOG("_waitForWorkUnit terminated: %"I64F"d state = %d",(__int64)agent,(int)ret);
                        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                        Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
                        wu->setState(ret);
                        Owned<IWUException> e = wu->createException();
                        e->setExceptionCode(isEcl ? 1001 : 1000);
                        e->setExceptionMessage(isEcl ? "EclServer terminated unexpectedly" : "Workunit terminated unexpectedly");
                        return ret;
                    }
                }
                break;
            }
            unsigned waited = msTick() - start;
            if (timeout==-1)
            {
                waiter->wait(20000);  // recheck state every 20 seconds even if no timeout, in case eclagent has crashed.
                if (waiter->aborted)
                {
                    ret = WUStateUnknown;  // MORE - throw an exception?
                    break;
                }
            }
            else if (waited > timeout || !waiter->wait(timeout-waited))
            {
                ret = WUStateUnknown;  // MORE - throw an exception?
                break;
            }
            conn->reload();
        }
    }
    waiter->unsubscribe();
    return ret;
}

extern WUState waitForWorkUnitToComplete(const char * wuid, int timeout, bool returnOnWaitState)
{
    return _waitForWorkUnit(wuid, (unsigned)timeout, false, returnOnWaitState);
}

extern WORKUNIT_API WUState secWaitForWorkUnitToComplete(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout, bool returnOnWaitState)
{
    if (checkWuSecAccess(wuid, secmgr, &secuser, SecAccess_Read, "Wait for Complete", false, true))
        return waitForWorkUnitToComplete(wuid, timeout, returnOnWaitState);
    return WUStateUnknown;
}

extern bool waitForWorkUnitToCompile(const char * wuid, int timeout)
{
    switch(_waitForWorkUnit(wuid, (unsigned)timeout, true, true))
    {
    case WUStateCompiled:
    case WUStateCompleted:
    case WUStateWait:
    case WUStateUploadingFiles:
        return true;
    default:
        return false;
    }
}

extern WORKUNIT_API bool secWaitForWorkUnitToCompile(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout)
{
    if (checkWuSecAccess(wuid, secmgr, &secuser, SecAccess_Read, "Wait for Compile", false, true))
        return waitForWorkUnitToCompile(wuid, timeout);
    return false;
}

extern WORKUNIT_API bool secDebugWorkunit(const char * wuid, ISecManager &secmgr, ISecUser &secuser, const char *command, StringBuffer &response)
{
    if (strnicmp(command, "<debug:", 7) == 0 && checkWuSecAccess(wuid, secmgr, &secuser, SecAccess_Read, "Debug", false, true))
    {
        Owned<IConstWorkUnit> wu = factory->secOpenWorkUnit(wuid, false, &secmgr, &secuser);
        SCMStringBuffer ip;
        unsigned port;
        port = wu->getDebugAgentListenerPort();
        wu->getDebugAgentListenerIP(ip);
        SocketEndpoint debugEP(ip.str(), port);
        Owned<ISocket> socket = ISocket::connect_timeout(debugEP, 1000);
        unsigned len = (size32_t)strlen(command);
        unsigned revlen = len;
        _WINREV(revlen);
        socket->write(&revlen, sizeof(revlen));
        socket->write(command, len);
        for (;;)
        {
            socket->read(&len, sizeof(len));
            _WINREV(len);                    
            if (len == 0)
                break;
            if (len & 0x80000000)
            {
                throwUnexpected();
            }
            char * mem = (char*) response.reserve(len);
            socket->read(mem, len);
        }
        return true;
    }
    return false;
}

IWUResult * updateWorkUnitResult(IWorkUnit * w, const char *name, unsigned sequence)
{
    switch ((int)sequence)
    {
    case ResultSequenceStored:
        return w->updateVariableByName(name);
    case ResultSequencePersist:
        return w->updateGlobalByName(name);
    case ResultSequenceInternal:
    case ResultSequenceOnce:
        return w->updateTemporaryByName(name);
    default:
        return w->updateResultBySequence(sequence);
    }
}

IConstWUResult * getWorkUnitResult(IConstWorkUnit * w, const char *name, unsigned sequence)
{
    switch ((int)sequence)
    {
    case ResultSequenceStored:
        return w->getVariableByName(name);

    case ResultSequencePersist:
        return w->getGlobalByName(name);

    case ResultSequenceInternal:
    case ResultSequenceOnce:
        return w->getTemporaryByName(name);

    default:
        if (name && name[0])
            return w->getResultByName(name);//name takes precedence over sequence
        else
            return w->getResultBySequence(sequence);
    }
}

extern WORKUNIT_API bool getWorkUnitCreateTime(const char *wuid,CDateTime &time)
{
    if (wuid) {
        char prefchar;
        unsigned year,month,day,hour,min,sec;
        if (sscanf(wuid, "%c%4u%2u%2u-%2u%2u%2u", &prefchar, &year, &month, &day, &hour, &min, &sec)==7) {
            time.set(year, month, day, hour, min, sec, 0, true);
//          time.setDate(year, month, day);
//          time.setTime(hour, min, sec, 0, true);  // for some reason time is local
            return true;
        }
    }
    return false;
}

extern WORKUNIT_API IStringVal& createToken(const char *wuid, const char *user, const char *password, IStringVal &str)
{
    StringBuffer wu, token("X");
    wu.append(wuid).append(';').append(user).append(';').append(password);
    encrypt(token,wu.str());
    str.set(token.str());
    return str;
}

// This will be replaced by something more secure!
extern WORKUNIT_API void extractToken(const char *token, const char *wuid, IStringVal &user, IStringVal &password)
{
    if (token && *token)
    {
        StringBuffer wu;
        decrypt(wu, token+1);
        const char *finger = strchr(wu.str(),';');
        if (finger && strnicmp(wuid, wu.str(), finger-wu.str())==0)
        {
            const char *finger1 = strchr(++finger,';');
            if(finger1)
            {
                user.setLen(finger, (size32_t)(finger1-finger));
                password.setLen(++finger1, (size32_t)(wu.str() + wu.length() - finger1));
                return;
            }
        }
        throw MakeStringException(WUERR_InvalidSecurityToken, "Invalid call to extractToken");
    }
}

extern WORKUNIT_API WUState getWorkUnitState(const char* state)
{
    return (WUState) getEnum(state, states);
}

const LogMsgCategory MCschedconn = MCprogress(1000);    // Category used to inform about schedule synchronization

class CWorkflowScheduleConnection : public CInterface, implements IWorkflowScheduleConnection
{
public:
    CWorkflowScheduleConnection(char const * wuid)
    {
        basexpath.append("/WorkflowSchedule/").append(wuid);
        flagxpath.append(basexpath.str()).append("/Active");
    }

    IMPLEMENT_IINTERFACE;

    virtual void lock()
    {
        LOG(MCschedconn, "Locking base schedule connection");
        baseconn.setown(querySDS().connect(basexpath.str(), myProcessSession(), RTM_CREATE_QUERY | RTM_LOCK_WRITE, INFINITE));
        if(!baseconn)
            throw MakeStringException(WUERR_ScheduleLockFailed, "Could not get base workflow schedule lock");
    }

    virtual void unlock()
    {
        LOG(MCschedconn, "Unlocking base schedule connection");
        baseconn.clear();
    }

    virtual void setActive()
    {
        LOG(MCschedconn, "Setting active flag in schedule connection");
        flagconn.setown(querySDS().connect(flagxpath.str(), myProcessSession(), RTM_CREATE | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, INFINITE));
        if(!flagconn)
            throw MakeStringException(WUERR_ScheduleLockFailed, "Could not get active workflow schedule lock");
    }

    virtual void resetActive()
    {
        LOG(MCschedconn, "Resetting active flag in schedule connection");
        flagconn.clear();
    }

    virtual bool queryActive()
    {
        return baseconn->queryRoot()->hasProp("Active");
    }

    virtual bool pull(IWorkflowItemArray * workflow)
    {
        assertex(baseconn);
        Owned<IPropertyTree> root = baseconn->getRoot();
        Owned<IPropertyTree> eventQueue = root->getPropTree("EventQueue");
        if(!eventQueue) return false;
        if(!eventQueue->hasProp("Item")) return false;
        {
            Owned<IPropertyTreeIterator> eventItems = eventQueue->getElements("Item");
            Owned<IPropertyTree> eventItem;
            Owned<IRuntimeWorkflowItemIterator> wfItems = workflow->getSequenceIterator();
            Owned<IRuntimeWorkflowItem> wfItem;
            for(eventItems->first(); eventItems->isValid(); eventItems->next())
            {
                eventItem.setown(&eventItems->get());
                const char * eventName = eventItem->queryProp("@name");
                const char * eventText = eventItem->queryProp("@text");
                for(wfItems->first(); wfItems->isValid(); wfItems->next())
                {
                    wfItem.setown(wfItems->get());
                    if(wfItem->queryState() != WFStateWait)
                        continue;
                    Owned<IWorkflowEvent> targetEvent = wfItem->getScheduleEvent();
                    if(!targetEvent || !targetEvent->matches(eventName, eventText))
                        continue;
                    wfItem->setEvent(eventName, eventText);
                    wfItem->setState(WFStateReqd);
                    resetDependentsState(workflow, *wfItem);
                }
            }
        }
        bool more;
        do
            more = eventQueue->removeProp("Item");
        while(more);
        return true;
    }

    virtual void push(char const * name, char const * text)
    {
        assertex(baseconn);
        Owned<IPropertyTree> root = baseconn->getRoot();
        ensurePTree(root, "EventQueue");
        Owned<IPropertyTree> eventQueue = root->getPropTree("EventQueue");
        Owned<IPropertyTree> eventItem = createPTree();
        eventItem->setProp("@name", name);
        eventItem->setProp("@text", text);
        eventQueue->addPropTree("Item", eventItem.getLink());
    }

private:
    void resetItemStateAndDependents(IWorkflowItemArray * workflow, unsigned wfid) const
    {
        if (wfid)
            resetItemStateAndDependents(workflow, workflow->queryWfid(wfid));
    }

    void resetItemStateAndDependents(IWorkflowItemArray * workflow, IRuntimeWorkflowItem & item) const
    {
        switch(item.queryState())
        {
        case WFStateDone:
        case WFStateFail:
            {
                item.setState(WFStateNull);
                resetItemStateAndDependents(workflow, item.queryPersistWfid());
                resetDependentsState(workflow, item);
                break;
            }
        }
    }

    void resetDependentsState(IWorkflowItemArray * workflow, IRuntimeWorkflowItem & item) const
    {
        Owned<IWorkflowDependencyIterator> iter(item.getDependencies());
        for(iter->first(); iter->isValid(); iter->next())
        {
            IRuntimeWorkflowItem & dep = workflow->queryWfid(iter->query());
            resetItemStateAndDependents(workflow, dep);
        }
    }

private:
    StringBuffer basexpath;
    StringBuffer flagxpath;
    Owned<IRemoteConnection> baseconn;
    Owned<IRemoteConnection> flagconn;
};

extern WORKUNIT_API IWorkflowScheduleConnection * getWorkflowScheduleConnection(char const * wuid)
{
    return new CWorkflowScheduleConnection(wuid);
}

extern WORKUNIT_API IExtendedWUInterface * queryExtendedWU(IWorkUnit * wu)
{
    return QUERYINTERFACE(wu, IExtendedWUInterface);
}


extern WORKUNIT_API void addExceptionToWorkunit(IWorkUnit * wu, WUExceptionSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column)
{
    Owned<IWUException> we = wu->createException();
    we->setSeverity(severity);
    we->setExceptionMessage(text);
    if (source)
        we->setExceptionSource(source);
    if (code)
        we->setExceptionCode(code);
    if (filename)
        we->setExceptionFileName(filename);
    if (lineno)
    {
        we->setExceptionLineNo(lineno);
        if (column)
            we->setExceptionColumn(lineno);
    }
}

const char * skipLeadingXml(const char * text)
{
    if (!text)
        return NULL;

    //skip utf8 BOM, probably excessive
    if (memcmp(text, UTF8_BOM, 3) == 0)
        text += 3;

    loop
    {
        if (isspace(*text))
            text++;
        else if (text[0] == '<' && text[1] == '?')
        {
            text += 2;
            loop
            {
                if (!*text) break;
                if (text[0] == '?' && text[1] == '>')
                {
                    text += 2;
                    break;
                }
                text++;
            }
        }
        else if (text[0] == '<' && text[1] == '!' && text[2] == '-' && text[3] == '-')
        {
            text += 4;
            loop
            {
                if (!*text) break;
                if (text[0] == '-' && text[1] == '-' && text[2] == '>')
                {
                    text += 3;
                    break;
                }
                text++;
            }
        }
        else
            break;
    }

    return text;
}

extern WORKUNIT_API bool isArchiveQuery(const char * text)
{
    text = skipLeadingXml(text);
    if (!text)
        return false;
    const char * archivePrefix = "<Archive";
    return memicmp(text, archivePrefix, strlen(archivePrefix)) == 0;
}

extern WORKUNIT_API bool isQueryManifest(const char * text)
{
    text = skipLeadingXml(text);
    if (!text)
        return false;
    const char * manifestPrefix = "<Manifest";
    return memicmp(text, manifestPrefix, strlen(manifestPrefix)) == 0;
}

//------------------------------------------------------------------------------
// Named Alias helper function

static IPropertyTree * resolveQueryByDll(IPropertyTree * queryRegistry, const char * dll)
{
    StringBuffer xpath;
    xpath.append("Query[@dll=\"").append(dll).append("\"]");
    return queryRegistry->getPropTree(xpath);
}


static IPropertyTree * resolveQueryByWuid(IPropertyTree * queryRegistry, const char * wuid)
{
    StringBuffer xpath;
    xpath.append("Query[@wuid=\"").append(wuid).append("\"]");
    return queryRegistry->getPropTree(xpath);
}


static void clearAliases(IPropertyTree * queryRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Alias[@id=\"").append(lcId).append("\"]");

    Owned<IPropertyTreeIterator> iter = queryRegistry->getElements(xpath);
    ForEach(*iter)
    {
        queryRegistry->removeProp(xpath.str());
    }
}

IPropertyTree * addNamedQuery(IPropertyTree * queryRegistry, const char * name, const char * wuid, const char * dll, bool library, const char *userid)
{
    StringBuffer lcName(name);
    lcName.toLowerCase();
    StringBuffer xpath;
    xpath.append("Query[@name=\"").append(lcName.str()).append("\"]");

    Owned<IPropertyTreeIterator> iter = queryRegistry->getElements(xpath);
    unsigned seq = 1;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        const char *thisWuid = item.queryProp("@wuid");
        if (strieq(wuid, thisWuid))
            return &item;
        unsigned thisSeq = item.getPropInt("@seq");
        if (thisSeq >= seq)
            seq = thisSeq + 1;
    }

    StringBuffer id;
    id.append(lcName).append(".").append(seq);
    IPropertyTree * newEntry = createPTree("Query", ipt_caseInsensitive);
    newEntry->setProp("@name", lcName);
    newEntry->setProp("@wuid", wuid);
    newEntry->setProp("@dll", dll);
    newEntry->setProp("@id", id);
    newEntry->setPropInt("@seq", seq);
    if (library)
        newEntry->setPropBool("@isLibrary", true);
    if (userid && *userid)
        newEntry->setProp("@publishedBy", userid);
    return queryRegistry->addPropTree("Query", newEntry);
}

void removeNamedQuery(IPropertyTree * queryRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    clearAliases(queryRegistry, lcId);
    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(lcId).append("\"]");
    queryRegistry->removeProp(xpath);
}


void removeDllFromNamedQueries(IPropertyTree * queryRegistry, const char * dll)
{
    Owned<IPropertyTree> match = resolveQueryByDll(queryRegistry, dll);
    if (!match)
        return;
    clearAliases(queryRegistry, match->queryProp("@id"));
    queryRegistry->removeTree(match);
}

void removeWuidFromNamedQueries(IPropertyTree * queryRegistry, const char * wuid)
{
    Owned<IPropertyTree> match = resolveQueryByWuid(queryRegistry, wuid);
    if (!match)
        return;
    clearAliases(queryRegistry, match->queryProp("@id"));
    queryRegistry->removeTree(match);
}

void removeAliasesFromNamedQuery(IPropertyTree * queryRegistry, const char * id)
{
    clearAliases(queryRegistry, id);
}

void setQueryAlias(IPropertyTree * queryRegistry, const char * name, const char * value)
{
    StringBuffer lcName(name);
    lcName.toLowerCase();

    StringBuffer xpath;
    xpath.append("Alias[@name=\"").append(lcName).append("\"]");
    IPropertyTree * match = queryRegistry->queryPropTree(xpath);
    if (!match)
    {
        IPropertyTree * newEntry = createPTree("Alias");
        newEntry->setProp("@name", lcName);
        match = queryRegistry->addPropTree("Alias", newEntry);
    }
    match->setProp("@id", value);
}

extern WORKUNIT_API IPropertyTree * getQueryById(IPropertyTree * queryRegistry, const char *queryid)
{
    if (!queryRegistry || !queryid)
        return NULL;
    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(queryid).append("\"]");
    return queryRegistry->getPropTree(xpath);
}

extern WORKUNIT_API IPropertyTree * getQueryById(const char *queryset, const char *queryid, bool readonly)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(queryset, readonly);
    return getQueryById(queryRegistry, queryid);
}

extern WORKUNIT_API IPropertyTree * resolveQueryAlias(IPropertyTree * queryRegistry, const char * alias)
{
    if (!queryRegistry || !alias)
        return NULL;

    StringBuffer xpath;
    unsigned cnt = 0;
    StringBuffer lc(alias);
    const char * search = lc.toLowerCase().str();
    loop
    {
        xpath.set("Alias[@name='").append(search).append("']/@id");
        const char * queryId = queryRegistry->queryProp(xpath);
        if (!queryId)
            break;
        //Check for too many alias indirections.
        if (cnt++ > 10)
            return NULL;
        search = lc.set(queryId).toLowerCase().str();
    }

    return getQueryById(queryRegistry, search);
}

extern WORKUNIT_API IPropertyTree * resolveQueryAlias(const char *queryset, const char *alias, bool readonly)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(queryset, readonly);
    return resolveQueryAlias(queryRegistry, alias);
}

void setQuerySuspendedState(IPropertyTree * queryRegistry, const char *id, bool suspend, const char *userid)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(lcId).append("\"]");
    IPropertyTree *tree = queryRegistry->queryPropTree(xpath);
    if (tree)
    {
        if (tree->getPropBool("@suspended", false) == suspend)
            return;
        if (suspend)
        {
            tree->addPropBool("@suspended", true);
            if (userid && *userid)
                tree->addProp("@suspendedBy", userid);
        }
        else
        {
            tree->removeProp("@suspended");
            tree->removeProp("@suspendedBy");
        }
    }
    else
        throw MakeStringException((suspend)? QUERRREG_SUSPEND : QUERRREG_UNSUSPEND, "Modifying query suspended state failed.  Could not find query %s", id);
}

void setQueryCommentForNamedQuery(IPropertyTree * queryRegistry, const char *id, const char *queryComment)
{
    if (queryComment)
    {
        StringBuffer lcId(id);
        lcId.toLowerCase();

        StringBuffer xpath;
        xpath.append("Query[@id=\"").append(lcId).append("\"]");
        IPropertyTree *tree = queryRegistry->queryPropTree(xpath);
        if (tree)
            tree->setProp("@queryComment", queryComment);
        else
            throw MakeStringException(QUERRREG_COMMENT,  "Could not find query %s", id);
    }
}
extern WORKUNIT_API IPropertyTree * getQueryRegistryRoot()

{
      Owned<IRemoteConnection> globalLock = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);

      if (!globalLock)

            return NULL;

      //Only lock the branch for the target we're interested in.

      StringBuffer xpath;

      xpath.append("/QuerySets");

      Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
      if (conn)
            return conn->getRoot();
      else
            return NULL;
}



extern WORKUNIT_API IPropertyTree * getQueryRegistry(const char * wsEclId, bool readonly)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/QuerySets/QuerySet[@id=\"").append(wsEclId).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        if (readonly)
            return NULL;
        Owned<IPropertyTree> querySet = createPTree();
        querySet->setProp("@id", wsEclId);
        globalLock->queryRoot()->addPropTree("QuerySet", querySet.getClear());
        globalLock->commit();

        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!conn)
            throwUnexpected();
    }

    return conn->getRoot();
}

IPropertyTree * addNamedPackageSet(IPropertyTree * packageRegistry, const char * name, IPropertyTree *packageInfo, bool overWrite)
{
    StringBuffer xpath;
    StringBuffer lcName(name);
    lcName.toLowerCase();
    // see if "name" already exists
    xpath.append("Package[@id='").append(name).append("']");
    IPropertyTree *pkgTree = packageRegistry->queryPropTree(xpath.str());
    if (pkgTree)
    {
        if (overWrite)
            packageRegistry->removeTree(pkgTree);
        else
            throw MakeStringException(WUERR_PackageAlreadyExists, "Package name %s already exists, either delete it or specify overwrite",name);
    }
    
    IPropertyTree *tree = packageRegistry->addPropTree("Package", packageInfo);
    tree->setProp("@id", lcName);
    return tree;
}

void removeNamedPackage(IPropertyTree * packageRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Package[@id=\"").append(lcId).append("\"]");
    packageRegistry->removeProp(xpath);
}

extern WORKUNIT_API IPropertyTree * getPackageSetRegistry(const char * wsEclId, bool readonly)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(wsEclId).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
    { 
        if (readonly)
            return NULL;
        Owned<IPropertyTree> querySet = createPTree();
        querySet->setProp("@id", wsEclId);
        globalLock->queryRoot()->addPropTree("PackageSet", querySet.getClear());
        globalLock->commit();

        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!conn)
            throwUnexpected();
    }

    return conn->getRoot();
}

void addQueryToQuerySet(IWorkUnit *workunit, const char *querySetName, const char *queryName, IPropertyTree *packageInfo, WUQueryActivationOptions activateOption, StringBuffer &newQueryId, const char *userid)
{
    StringBuffer cleanQueryName;
    appendUtf8XmlName(cleanQueryName, strlen(queryName), queryName);

    SCMStringBuffer dllName;
    Owned<IConstWUQuery> q = workunit->getQuery();
    q->getQueryDllName(dllName);
    if (!dllName.length())
        throw MakeStringException(WUERR_InvalidDll, "Cannot deploy query - no associated dll.");

    SCMStringBuffer wuid;
    workunit->getWuid(wuid);

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, false);

    StringBuffer currentTargetClusterType;
    queryRegistry->getProp("@targetclustertype", currentTargetClusterType); 

    SCMStringBuffer targetClusterType;
    workunit->getDebugValue("targetclustertype", targetClusterType);

    if (currentTargetClusterType.length() < 1) 
    {
        queryRegistry->setProp("@targetclustertype", targetClusterType.str());
    }
    else
    {
        if (strcmp(currentTargetClusterType.str(), "roxie") == 0 && strcmp(currentTargetClusterType.str(), targetClusterType.str())!=0)
        {
            throw MakeStringException(WUERR_MismatchClusterType, "TargetClusterTypes of workunit and queryset do not match.");
        }
    }

    IPropertyTree *newEntry = addNamedQuery(queryRegistry, cleanQueryName, wuid.str(), dllName.str(), isLibrary(workunit), userid);
    newQueryId.append(newEntry->queryProp("@id"));
    workunit->setIsQueryService(true); //will check querysets before delete
    workunit->commit();

    if (activateOption == ACTIVATE_SUSPEND_PREVIOUS|| activateOption == ACTIVATE_DELETE_PREVIOUS)
    {
        Owned<IPropertyTree> prevQuery = resolveQueryAlias(queryRegistry, cleanQueryName);
        setQueryAlias(queryRegistry, cleanQueryName, newQueryId);
        if (prevQuery)
        {
            if (activateOption == ACTIVATE_SUSPEND_PREVIOUS)
                setQuerySuspendedState(queryRegistry, prevQuery->queryProp("@id"), true, userid);
            else 
                removeNamedQuery(queryRegistry, prevQuery->queryProp("@id"));
        }
    }
    else if (activateOption == MAKE_ACTIVATE || activateOption == MAKE_ACTIVATE_LOAD_DATA_ONLY)
        setQueryAlias(queryRegistry, cleanQueryName, newQueryId.str());
}

bool removeQuerySetAlias(const char *querySetName, const char *alias)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    StringBuffer xpath;
    xpath.appendf("Alias[@name='%s']", alias);
    IPropertyTree *t = queryRegistry->queryPropTree(xpath);
    return queryRegistry->removeTree(t);
}

void addQuerySetAlias(const char *querySetName, const char *alias, const char *id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, false);
    setQueryAlias(queryRegistry, alias, id);
}

void setSuspendQuerySetQuery(const char *querySetName, const char *id, bool suspend, const char *userid)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    setQuerySuspendedState(queryRegistry, id, suspend, userid);
}

void deleteQuerySetQuery(const char *querySetName, const char *id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    removeNamedQuery(queryRegistry, id);
}

void removeQuerySetAliasesFromNamedQuery(const char *querySetName, const char * id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    clearAliases(queryRegistry, id);
}

void setQueryCommentForNamedQuery(const char *querySetName, const char *id, const char *queryComment)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    setQueryCommentForNamedQuery(queryRegistry, id, queryComment);
}

const char *queryIdFromQuerySetWuid(const char *querySetName, const char *wuid, IStringVal &id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, false);
    StringBuffer xpath;
    xpath.appendf("Query[@wuid='%s']", wuid);
    IPropertyTree *q = queryRegistry->queryPropTree(xpath.str());
    if (q)
    {
        id.set(q->queryProp("@id"));
    }
    return id.str();
}

extern WORKUNIT_API void gatherLibraryNames(StringArray &names, StringArray &unresolved, IWorkUnitFactory &workunitFactory, IConstWorkUnit &cw, IPropertyTree *queryset)
{
    IConstWULibraryIterator &wulibraries = cw.getLibraries();
    ForEach(wulibraries)
    {
        SCMStringBuffer libname;
        IConstWULibrary &wulibrary = wulibraries.query();
        wulibrary.getName(libname);
        if (names.contains(libname.str()) || unresolved.contains(libname.str()))
            continue;

        Owned<IPropertyTree> query = resolveQueryAlias(queryset, libname.str());
        if (query && query->getPropBool("@isLibrary"))
        {
            const char *wuid = query->queryProp("@wuid");
            Owned<IConstWorkUnit> libcw = workunitFactory.openWorkUnit(wuid, false);
            if (libcw)
            {
                names.appendUniq(libname.str());
                gatherLibraryNames(names, unresolved, workunitFactory, *libcw, queryset);
                continue;
            }
        }

        unresolved.appendUniq(libname.str());
    }
}

bool looksLikeAWuid(const char * wuid)
{
    if (!wuid)
        return false;
    if (wuid[0] != 'W')
        return false;
    if (!isdigit(wuid[1]) || !isdigit(wuid[2]) || !isdigit(wuid[3]) || !isdigit(wuid[4]))
        return false;
    if (!isdigit(wuid[5]) || !isdigit(wuid[6]) || !isdigit(wuid[7]) || !isdigit(wuid[8]))
        return false;
    return (wuid[9]=='-');
}

IPropertyTree * resolveDefinitionInArchive(IPropertyTree * archive, const char * path)
{
    IPropertyTree * module = archive;
    const char * dot = strrchr(path, '.');

    StringBuffer xpath;
    if (dot)
    {
        xpath.clear().append("Module[@key='").appendLower(dot-path, path).append("']");
        module = archive->queryPropTree(xpath);

        path = dot+1;
    }
    else
        module = archive->queryPropTree("Module[@key='']");

    if (!module)
        return NULL;

    xpath.clear().append("Attribute[@key='").appendLower(strlen(path), path).append("']");
    return module->queryPropTree(xpath);
}

extern WORKUNIT_API void associateLocalFile(IWUQuery * query, WUFileType type, const char * name, const char * description, unsigned crc)
{
    StringBuffer hostname;
    queryHostIP().getIpText(hostname);

    StringBuffer fullPathname;
    makeAbsolutePath(name, fullPathname);
    query->addAssociatedFile(type, fullPathname, hostname, description, crc);
}

extern WORKUNIT_API void descheduleWorkunit(char const * wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
    if(workunit)
        workunit->deschedule();
    else
        doDescheduleWorkkunit(wuid);
}

extern WORKUNIT_API void updateWorkunitTimeStat(IWorkUnit * wu, const char * component, const char * wuScope, const char * stat, const char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue)
{
    if (!wuScope)
        wuScope = "workunit";
    if (!stat)
        stat = "time";

    //The following line duplicates the statistics as timing information - preserved temporarily to show refactoring.
    //wu->setTimerInfo(description, (unsigned)(value/1000000), (unsigned)count, (unsigned)maxValue);
    wu->setStatistic(component, wuScope, stat, description, SMEASURE_TIME_NS, value, count, maxValue, false);
}

extern WORKUNIT_API void updateWorkunitTiming(IWorkUnit * wu, const char * component, const char * mangledScope, const char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue)
{
    StringAttr scopeText;
    StringAttr componentText;
    const char * wuScope = mangledScope;
    const char * stat = "time";

    //If the scope contains a semicolon then it is taken to mean (wuScope;stat or comonent;wuScope;stat)
    const char * sep1 = strchr(mangledScope, ';');
    if (sep1)
    {
        const char * sep2 = strchr(sep1+1, ';');
        if (sep2)
        {
            componentText.set(mangledScope, sep1 - mangledScope);
            scopeText.set(sep1+1, sep2-(sep1+1));
            component = componentText;
            wuScope = scopeText;
            stat = sep2+1;
        }
        else
        {
            scopeText.set(mangledScope, sep1-mangledScope);
            wuScope = scopeText.get();
            stat = sep1+1;
        }
    }

    updateWorkunitTimeStat(wu, component, wuScope, stat, description, value, count, maxValue);
}

extern WORKUNIT_API void updateWorkunitTimings(IWorkUnit * wu, ITimeReporter *timer, const char * component)
{
    StringBuffer description;
    StringBuffer scope;
    for (unsigned i = 0; i < timer->numSections(); i++)
    {
        timer->getDescription(i, description.clear());
        timer->getScope(i, scope.clear());
        updateWorkunitTiming(wu, component, scope, description, timer->getTime(i), timer->getCount(i), timer->getMaxTime(i));
    }
}
