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

#ifndef WORKUNIT_IPP_INCL
#include "seclib.hpp"
#include "dasess.hpp"  /// For IUserDescriptor
#include "workunit.hpp"

class CLocalWUAppValue : public CInterface, implements IConstWUAppValue
{
    Owned<IPropertyTree> p;
    StringAttr prop;
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

    virtual IStringVal & getCreator(IStringVal & str) const;
    virtual IStringVal & getDescription(IStringVal & str, bool createDefault) const;
    virtual IStringVal & getFormattedValue(IStringVal & str) const;
    virtual IStringVal & getType(IStringVal & str) const;
    virtual IStringVal & getScope(IStringVal & str) const;
    virtual StatisticMeasure getMeasure() const;
    virtual StatisticCreatorType getCreatorType() const;
    virtual StatisticScopeType getScopeType() const;
    virtual StatisticKind getKind() const;
    virtual unsigned __int64 getValue() const;
    virtual unsigned __int64 getCount() const;
    virtual unsigned __int64 getMax() const;
    virtual unsigned __int64 getTimestamp() const;

    virtual bool matches(const IStatisticsFilter * filter) const;
};

class CLocalWULegacyTiming : public CInterface, implements IConstWUStatistic
{
    Owned<IPropertyTree> p;
public:
    IMPLEMENT_IINTERFACE;
    CLocalWULegacyTiming(IPropertyTree *p);

    virtual IStringVal & getCreator(IStringVal & str) const;
    virtual IStringVal & getDescription(IStringVal & str, bool createDefault) const;
    virtual IStringVal & getFormattedValue(IStringVal & str) const;
    virtual IStringVal & getType(IStringVal & str) const;
    virtual IStringVal & getScope(IStringVal & str) const;
    virtual StatisticMeasure getMeasure() const;
    virtual StatisticCreatorType getCreatorType() const;
    virtual StatisticScopeType getScopeType() const;
    virtual StatisticKind getKind() const;
    virtual unsigned __int64 getValue() const;
    virtual unsigned __int64 getCount() const;
    virtual unsigned __int64 getMax() const;
    virtual unsigned __int64 getTimestamp() const;

    virtual bool matches(const IStatisticsFilter * filter) const;
};

//==========================================================================================

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
    unsigned ordinality() const { return tags.ordinality(); }

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

//==========================================================================================

class CLocalWorkUnit : public CInterface, implements IWorkUnit , implements IExtendedWUInterface
{
    friend StringBuffer &exportWorkUnitToXML(const IConstWorkUnit *wu, StringBuffer &str, bool decodeGraphs, bool includeProgress, bool hidePasswords);
    friend void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool decodeGraphs, bool includeProgress, bool hidePasswords);

protected:
    Owned<IPropertyTree> p;
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
    mutable IArrayOf<IWUPlugin> plugins;
    mutable IArrayOf<IWULibrary> libraries;
    mutable IArrayOf<IWUException> exceptions;
    mutable IArrayOf<IWUGraph> graphs;
    mutable IArrayOf<IWUResult> results;
    mutable IArrayOf<IWUResult> temporaries;
    mutable IArrayOf<IWUResult> variables;
    mutable CachedTags<CLocalWUAppValue,IConstWUAppValue> appvalues;
    mutable CachedTags<CLocalWUStatistic,IConstWUStatistic> statistics;
    mutable CachedTags<CLocalWULegacyTiming,IConstWUStatistic> legacyTimings;
    mutable Owned<IUserDescriptor> userDesc;
    Mutex locked;
    Owned<ISecManager> secMgr;
    Owned<ISecUser> secUser;
    mutable Owned<IPropertyTree> cachedGraphs;

protected:
    
public:
    IMPLEMENT_IINTERFACE;

    CLocalWorkUnit(ISecManager *secmgr, ISecUser *secuser);
    void loadPTree(IPropertyTree *ptree);
    void beforeDispose();
    
    IPropertyTree *getUnpackedTree(bool includeProgress) const;

    ISecManager *querySecMgr() { return secMgr.get(); }
    ISecUser *querySecUser() { return secUser.get(); }

    virtual bool aborting() const;
    virtual void forceReload() {};
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
    virtual const char *queryClusterName() const;
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
    virtual unsigned getApplicationValueCount() const;
    virtual IConstWUGraphIterator & getGraphs(WUGraphType type) const;
    virtual IConstWUGraphMetaIterator & getGraphsMeta(WUGraphType type) const;
    virtual IConstWUGraph * getGraph(const char *name) const;
    virtual IConstWUGraphProgress * getGraphProgress(const char * name) const;
    virtual const char *queryJobName() const;
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
    virtual IStringVal & getScope(IStringVal & str) const;
    virtual IStringVal & getSecurityToken(IStringVal & str) const;
    virtual WUState getState() const;
    virtual IStringVal & getStateEx(IStringVal & str) const;
    virtual __int64 getAgentSession() const;
    virtual unsigned getAgentPID() const;
    virtual const char *queryStateDesc() const;
    virtual IConstWUResult * getTemporaryByName(const char * name) const;
    virtual IConstWUResultIterator & getTemporaries() const;
    virtual IConstWUStatisticIterator & getStatistics(const IStatisticsFilter * filter) const;
    virtual IConstWUStatistic * getStatistic(const char * creator, const char * scope, StatisticKind kind) const;
    virtual IConstWUWebServicesInfo * getWebServicesInfo() const;
    virtual IConstWURoxieQueryInfo * getRoxieQueryInfo() const;
    virtual IStringVal & getXmlParams(IStringVal & params, bool hidePasswords) const;
    virtual const IPropertyTree *getXmlParams() const;
    virtual unsigned __int64 getHash() const;
    virtual IStringIterator *getLogs(const char *type, const char *component) const;
    virtual IStringIterator *getProcesses(const char *type) const;
    virtual IPropertyTreeIterator* getProcesses(const char *type, const char *instance) const;
    virtual IStringVal & getSnapshot(IStringVal & str) const;
    virtual ErrorSeverity getWarningSeverity(unsigned code, ErrorSeverity defaultSeverity) const;

    virtual const char *queryUser() const;
    virtual IStringVal & getWuScope(IStringVal & str) const;
    virtual IConstWUResult * getVariableByName(const char * name) const;
    virtual IConstWUResultIterator & getVariables() const;
    virtual const char *queryWuid() const;
    virtual bool getRunningGraph(IStringVal &graphName, WUGraphIDType &subId) const;
    virtual bool isProtected() const;
    virtual bool isPausing() const;
    virtual IWorkUnit& lock();
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
    void setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction);
    void setTracingValue(const char * propname, const char * value);
    void setTracingValueInt(const char * propname, int value);
    void setUser(const char * value);
    void setWarningSeverity(unsigned code, ErrorSeverity severity);
    void setWuScope(const char * value);
    void setSnapshot(const char * value);
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
    IWUGraph * createGraph(const char * name, WUGraphType type, IPropertyTree *xgmml);
    IWUGraph * updateGraph(const char * name);
    IWUQuery * updateQuery();
    IWUWebServicesInfo* updateWebServicesInfo(bool create);
    IWURoxieQueryInfo* updateRoxieQueryInfo(const char *wuid, const char *roxieClusterName);
    IWUPlugin * updatePluginByName(const char * name);
    IWULibrary * updateLibraryByName(const char * name);
    virtual IWUResult * updateResultByName(const char * name);
    virtual IWUResult * updateResultBySequence(unsigned seq);
    virtual IWUResult * updateTemporaryByName(const char * name);
    virtual IWUResult * updateVariableByName(const char * name);
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
    void unlockRemote();
    void abort();
    bool switchThorQueue(const char *cluster, IQueueSwitcher *qs);
    void setAllowedClusters(const char *value);
    IStringVal & getAllowedClusters(IStringVal & str) const;
    void remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const;
    void setAllowAutoQueueSwitch(bool val);
    bool getAllowAutoQueueSwitch() const;
    void setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash);

    virtual void cleanupAndDelete(bool deldll,bool deleteOwned, const StringArray *deleteExclusions=NULL);
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

protected:
    void clearCached(bool clearTree);
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
    void checkAgentRunning(WUState & state);

    void ensureGraphsUnpacked()
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

    // Implemented by derived classes
    virtual void _lockRemote() {};
    virtual void _unlockRemote() {};
    virtual void unsubscribe();
    virtual void _loadResults() const;
};

interface ISDSManager; // MORE - can remove once dali split out

class CWorkUnitFactory : public CInterface, implements IWorkUnitFactory
{
public:
    IMPLEMENT_IINTERFACE;

    CWorkUnitFactory();
    ~CWorkUnitFactory();

    // interface IWorkUnitFactory - some are left for derived classes

    virtual IWorkUnit * createWorkUnit(const char * app, const char * user, ISecManager *secmgr, ISecUser *secuser);
    virtual bool deleteWorkUnit(const char * wuid, ISecManager *secmgr, ISecUser *secuser);
    virtual IConstWorkUnit * openWorkUnit(const char * wuid, bool lock, ISecManager *secmgr, ISecUser *secuser);
    virtual IWorkUnit * updateWorkUnit(const char * wuid, ISecManager *secmgr, ISecUser *secuser);
    virtual int setTracingLevel(int newlevel);
    virtual IWorkUnit * createNamedWorkUnit(const char * wuid, const char * app, const char *scope, ISecManager *secmgr, ISecUser *secuser);
    virtual IWorkUnit * getGlobalWorkUnit(ISecManager *secmgr, ISecUser *secuser) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner, ISecManager *secmgr, ISecUser *secuser) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByState(WUState state, ISecManager *secmgr = NULL, ISecUser *secuser = NULL) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByECL(const char * ecl, ISecManager *secmgr = NULL, ISecUser *secuser = NULL) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByCluster(const char * cluster, ISecManager *secmgr = NULL, ISecUser *secuser = NULL) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByXPath(const char * xpath, ISecManager *secmgr = NULL, ISecUser *secuser = NULL) = 0;  // deprecated
    virtual IConstWorkUnitIterator* getWorkUnitsSorted(WUSortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                WUSortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                                const void *filterbuf,  // (appended) string values for filters
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                const char *queryowner,
                                                __int64 *cachehint,
                                                unsigned *total,
                                                ISecManager *secmgr,
                                                ISecUser *secuser) = 0;
    virtual unsigned numWorkUnits() = 0;
    virtual void descheduleAllWorkUnits(ISecManager *secmgr, ISecUser *secuser);
    virtual IConstQuerySetQueryIterator * getQuerySetQueriesSorted(WUQuerySortField *sortorder, WUQuerySortField *filters, const void *filterbuf, unsigned startoffset, unsigned maxnum, __int64 *cachehint, unsigned *total, const MapStringTo<bool> *subset);
    virtual bool isAborting(const char *wuid) const;
    virtual void clearAborting(const char *wuid);
    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, bool returnOnWaitState) = 0;

protected:
    // These need to be implemented by the derived classes
    virtual CLocalWorkUnit* _createWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser) = 0;
    virtual CLocalWorkUnit* _openWorkUnit(const char *wuid, bool lock, ISecManager *secmgr, ISecUser *secuser) = 0;  // for read access
    virtual CLocalWorkUnit* _updateWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser) = 0;  // for write access

};
#endif
