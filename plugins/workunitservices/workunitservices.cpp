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

/* TBD

FilesRead
FilesWritten
Errors
Plugins
Results
Timings

Persists changed?

*/

#pragma warning (disable : 4786)
#pragma warning (disable : 4297)  // function assumed not to throw an exception but does

#include "platform.h"



#include "workunit.hpp"
#include "agentctx.hpp"
#include "enginecontext.hpp"
#include "portlist.h"

#include "jio.hpp"
#include "jmisc.hpp"
#include "jstring.hpp"
#include "dasess.hpp"
#include "dasds.hpp"
#include "dautils.hpp"
#include "daaudit.hpp"

#include "sacmd.hpp"

#include "workunitservices.hpp"
#include "workunitservices.ipp"
#include "environment.hpp"

#define WORKUNITSERVICES_VERSION "WORKUNITSERVICES 1.0.1"

static const char * compatibleVersions[] = {
    "WORKUNITSERVICES 1.0 ",  // a version was released with a space here in signature... 
    "WORKUNITSERVICES 1.0.1", 
    NULL };

static const char * EclDefinition =
"export WsWorkunitRecord := record "
                            " string24 wuid;"
                            " string owner{maxlength(64)};"
                            " string cluster{maxlength(64)};"
                            " string roxiecluster{maxlength(64)};"
                            " string job{maxlength(256)};"
                            " string10 state;"
                            " string7 priority;"
                            " integer2 priorityvalue;"
                            " string20 created;"
                            " string20 modified;"
                            " boolean online;"
                            " boolean protected;"
                          " end;\n"
"export WsTimeStamp := record "
                            " string32 application;"
                            " string16 id;"
                            " string20 time;"
                            " string16 instance;"
                        " end;\n"
"export WsMessage := record "
                            " unsigned4 severity;"
                            " integer4  code;"
                            " string32  location;"
                            " unsigned4 row;"
                            " unsigned4 col;"
                            " string16  source;"
                            " string20  time;"
                            " string      message{maxlength(1024)};"
                        " end;\n"
"export WsFileRead := record "
                            " string name{maxlength(256)};"
                            " string cluster{maxlength(64)};"
                            " boolean isSuper;"
                            " unsigned4 usage;"
                        " end;\n"
"export WsFileWritten := record "
                            " string name{maxlength(256)};"
                            " string10 graph;"
                            " string cluster{maxlength(64)};"
                            " unsigned4 kind;"
                        " end;\n"
"export WsTiming := record "
                            " unsigned4 count;"
                            " unsigned4 duration;"
                            " unsigned4 max;"
                            " string name{maxlength(64)};"
                        " end;\n"
"export WsStatistic := record "
                            " unsigned8 value;"
                            " unsigned8 count;"
                            " unsigned8 maxValue;"
                            " string creatorType;"
                            " string creator;"
                            " string scopeType;"
                            " string scope;"
                            " string name;"
                            " string description;"
                            " string unit;"
                        " end;\n"
"export WorkunitServices := SERVICE\n"
"   boolean WorkunitExists(const varstring wuid, boolean online=true, boolean archived=false) : c,context,entrypoint='wsWorkunitExists'; \n"
"   dataset(WsWorkunitRecord) WorkunitList("
                                        " const varstring lowwuid," 
                                        " const varstring highwuid=''," 
                                        " const varstring username=''," 
                                        " const varstring cluster=''," 
                                        " const varstring jobname='',"
                                        " const varstring state=''," 
                                        " const varstring priority='',"
                                        " const varstring fileread='',"
                                        " const varstring filewritten='',"
                                        " const varstring roxiecluster='',"
                                        " const varstring eclcontains='',"
                                        " boolean online=true,"
                                        " boolean archived=false"
                                        ") : c,context,entrypoint='wsWorkunitList'; \n"
"  varstring WUIDonDate(unsigned4 year,unsigned4 month,unsigned4 day,unsigned4 hour, unsigned4 minute) : c,entrypoint='wsWUIDonDate'; \n"
"  varstring WUIDdaysAgo(unsigned4  daysago) : c,entrypoint='wsWUIDdaysAgo'; \n"
"  dataset(WsTimeStamp) WorkunitTimeStamps(const varstring wuid) : c,context,entrypoint='wsWorkunitTimeStamps'; \n"
"  dataset(WsMessage) WorkunitMessages(const varstring wuid) : c,context,entrypoint='wsWorkunitMessages'; \n"
"  dataset(WsFileRead) WorkunitFilesRead(const varstring wuid) : c,context,entrypoint='wsWorkunitFilesRead'; \n"
"  dataset(WsFileWritten) WorkunitFilesWritten(const varstring wuid) : c,context,entrypoint='wsWorkunitFilesWritten'; \n"
"  dataset(WsTiming) WorkunitTimings(const varstring wuid) : c,context,entrypoint='wsWorkunitTimings'; \n"
"  streamed dataset(WsStatistic) WorkunitStatistics(const varstring wuid, boolean includeActivities = false, const varstring _filter = '') : c,context,entrypoint='wsWorkunitStatistics'; \n"
    
"END;";

#define WAIT_SECONDS 30
#define SDS_LOCK_TIMEOUT (1000*60*5)
#define SASHA_TIMEOUT (1000*60*10)

WORKUNITSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = WORKUNITSERVICES_VERSION;
    pb->moduleName = "lib_WORKUNITSERVICES";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "WORKUNITSERVICES library";

    return true;
}

namespace nsWorkunitservices {

IPluginContext * parentCtx = NULL;


static void getSashaNodes(SocketEndpointArray &epa)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
    {
        ERRLOG("getSashaNodes: cannot connect to /Environment!");
        return;
    }

    Owned<IPropertyTree> root = &env->getPTree();
    StringBuffer tmp;
    Owned<IPropertyTreeIterator> siter = root->getElements("Software/SashaServerProcess/Instance");
    ForEach(*siter) {
        if (siter->query().getProp("@netAddress",tmp.clear())) {
            SocketEndpoint sashaep(tmp.str(),siter->query().getPropInt("@port",DEFAULT_SASHA_PORT));
            epa.append(sashaep);
        }
    }
}

static IWorkUnitFactory * getWorkunitFactory(ICodeContext * ctx)
{
    IEngineContext *engineCtx = ctx->queryEngineContext();
    if (engineCtx && !engineCtx->allowDaliAccess())
    {
        Owned<IException> e = MakeStringException(-1, "workunitservices cannot access Dali in this context - this normally means it is being called from a thor slave");
        EXCLOG(e, NULL);
        throw e.getClear();
    }

    //MORE: These should really be set up correctly - probably should be returned from IEngineContext
    ISecManager *secmgr = NULL;
    ISecUser *secuser = NULL;
    return getWorkUnitFactory(secmgr, secuser);
}

static bool securityDisabled = false;

static bool checkScopeAuthorized(IUserDescriptor *user, const char *scopename)
{
    if (securityDisabled)
        return true;
    unsigned auditflags = DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED;
    int perm = 255;
    if (scopename && *scopename)
    {
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0)
        {
            if (perm==-1)
            {
                perm = 255;
                securityDisabled = true;
            }
            else 
                perm = 0;
        }
        if (!HASREADPERMISSION(perm)) 
            return false;

    }
    return true;
}

static IConstWorkUnit * getWorkunit(ICodeContext * ctx, const char * wuid)
{
    StringBuffer _wuid(wuid);
    if (!_wuid.length())
        return NULL;
    wuid = _wuid.toUpperCase().str();
    Owned<IWorkUnitFactory> wuFactory = getWorkunitFactory(ctx);
    Owned<IConstWorkUnit> wu = wuFactory->openWorkUnit(wuid);
    if (wu)
    {
        if (!checkScopeAuthorized(ctx->queryUserDescriptor(), wu->queryWuScope()))
            wu.clear();
    }
    return wu.getClear();
}

static IConstWorkUnit *getWorkunit(ICodeContext * ctx)
{
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    // One assumes we have read access to our own wu
    return getWorkunit(ctx, wuid);
}

static StringBuffer &getWUIDonDate(StringBuffer &wuid,unsigned year,unsigned month,unsigned day,unsigned hour,unsigned minute)
{
    if ((year==0)||(month==0)||(day==0)) {
        CDateTime dt;
        dt.setNow();
        unsigned y;
        unsigned m;
        unsigned d;
        dt.getDate(y,m,d, true);
        if (year==0)
            year = y;
        if (month==0)
            month = m;
        if (day==0)
            day = d;
    }
    else if (year<100) 
        year+=2000;
    wuid.appendf("W%d%02d%02d-%02d%02d00",year,month,day,hour,minute);
    return wuid;
}

static StringBuffer &getWUIDdaysAgo(StringBuffer &wuid,int daysago)
{
    CDateTime dt;
    dt.setNow();
    dt.adjustTime(-(daysago*60*24));
    unsigned y;
    unsigned m;
    unsigned d;
    dt.getDate(y,m,d, true);
    unsigned h;
    unsigned mn;
    unsigned s;
    unsigned ns;
    dt.getTime(h,mn,s,ns,true);
    return getWUIDonDate(wuid,y,m,d,h,mn);
}

static bool addWUQueryFilter(WUSortField *filters, unsigned &count, MemoryBuffer &buff, const char *name, WUSortField value)
{
    if (!name || !*name)
        return false;
    filters[count++] = value;
    buff.append(name);
    return true;
}

static bool serializeWUInfo(IConstWorkUnitInfo &info,MemoryBuffer &mb)
{
    fixedAppend(mb,24,info.queryWuid());
    varAppendMax(mb,64,info.queryUser());
    varAppendMax(mb,64,info.queryClusterName());
    varAppendMax(mb,64,""); // roxiecluster is obsolete
    varAppendMax(mb,256,info.queryJobName());
    fixedAppend(mb,10,info.queryStateDesc());
    fixedAppend(mb,7,info.queryPriorityDesc());
    short int prioritylevel = info.getPriorityLevel();
    mb.append(prioritylevel);
    fixedAppend(mb,20,"");  // Created timestamp
    fixedAppend(mb,20,"");  // Modified timestamp
    mb.append(true);
    mb.append(info.isProtected());
    if (mb.length()>WORKUNIT_SERVICES_BUFFER_MAX) {
        mb.clear().append(WUS_STATUS_OVERFLOWED);
        return false;
    }
    return true;
}


}//namespace

using namespace nsWorkunitservices;

WORKUNITSERVICES_API void wsWorkunitList(
    ICodeContext *ctx,
    size32_t & __lenResult,
    void * & __result, 
    const char *lowwuid,
    const char *highwuid,
    const char *username,
    const char *cluster,
    const char *jobname,
    const char *state,
    const char *priority,
    const char *fileread,
    const char *filewritten,
    const char *roxiecluster,  // Not in use - retained for compatibility only
    const char *eclcontains,
    bool online,
    bool archived 
)
{
    MemoryBuffer mb;
    if (archived) {
        SocketEndpointArray sashaeps;
        getSashaNodes(sashaeps);
        ForEachItemIn(i,sashaeps) {
            Owned<ISashaCommand> cmd = createSashaCommand();    
            cmd->setAction(SCA_WORKUNIT_SERVICES_GET);                          
            cmd->setOnline(false);                              
            cmd->setArchived(true); 
            cmd->setWUSmode(true);
            if (lowwuid&&*lowwuid)
                cmd->setAfter(lowwuid);
            if (highwuid&&*highwuid)
                cmd->setBefore(highwuid);
            if (username&&*username)
                cmd->setOwner(username);
            if (cluster&&*cluster)
                cmd->setCluster(cluster);
            if (jobname&&*jobname)
                cmd->setJobName(jobname);
            if (state&&*state)
                cmd->setState(state);
            if (priority&&*priority)
                cmd->setPriority(priority);
            if (fileread&&*fileread)
                cmd->setFileRead(fileread);
            if (filewritten&&*filewritten)
                cmd->setFileWritten(filewritten);
            if (eclcontains&&*eclcontains)
                cmd->setEclContains(eclcontains);
            Owned<INode> sashanode = createINode(sashaeps.item(i));
            if (cmd->send(sashanode,SASHA_TIMEOUT)) {
                byte res = cmd->getWUSresult(mb);
                if (res==WUS_STATUS_OVERFLOWED)
                    throw MakeStringException(-1,"WORKUNITSERVICES: Result buffer overflowed");
                if (res!=WUS_STATUS_OK)
                    throw MakeStringException(-1,"WORKUNITSERVICES: Sasha get results failed (%d)",(int)res);
                break;
            }
            if (i+1>=sashaeps.ordinality()) {
               StringBuffer ips;
               sashaeps.item(0).getIpText(ips);
               throw MakeStringException(-1,"Time out to Sasha server on %s (server not running or query too complex)",ips.str());
            }
        }
    }
    if (online)
    {
        WUSortField filters[20];  // NOTE - increase if you add a LOT more parameters!
        unsigned filterCount = 0;
        MemoryBuffer filterbuf;

        if (state && *state)
        {
            filters[filterCount++] = WUSFstate;
            if (!strieq(state, "unknown"))
                filterbuf.append(state);
            else
                filterbuf.append("");
        }
        if (priority && *priority)
        {
            filters[filterCount++] = WUSFpriority;
            if (!strieq(state, "unknown"))
                filterbuf.append(state);
            else
                filterbuf.append("");
        }
        addWUQueryFilter(filters, filterCount, filterbuf, cluster, WUSFcluster);
        addWUQueryFilter(filters, filterCount, filterbuf, fileread, (WUSortField) (WUSFfileread | WUSFnocase));
        addWUQueryFilter(filters, filterCount, filterbuf, filewritten, (WUSortField) (WUSFfilewritten | WUSFnocase));
        addWUQueryFilter(filters, filterCount, filterbuf, username, (WUSortField) (WUSFuser | WUSFnocase));
        addWUQueryFilter(filters, filterCount, filterbuf, jobname, (WUSortField) (WUSFjob | WUSFwild | WUSFnocase));
        addWUQueryFilter(filters, filterCount, filterbuf, eclcontains, (WUSortField) (WUSFecl | WUSFwild));
        addWUQueryFilter(filters, filterCount, filterbuf, lowwuid, WUSFwuid);
        addWUQueryFilter(filters, filterCount, filterbuf, highwuid, WUSFwuidhigh);
        filters[filterCount] = WUSFterm;
        Owned<IWorkUnitFactory> wuFactory = getWorkunitFactory(ctx);
        Owned<IConstWorkUnitIterator> it = wuFactory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), filters, filterbuf.bufferBase(), 0, INT_MAX, NULL, NULL); // MORE - need security flags here!
        ForEach(*it)
        {
            if (!serializeWUInfo(it->query(), mb))
                throw MakeStringException(-1,"WORKUNITSERVICES: Result buffer overflowed");
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API bool wsWorkunitExists(ICodeContext *ctx, const char *wuid, bool online, bool archived)
{
    if (!wuid||!*wuid)
        return false;
    StringBuffer _wuid(wuid);
    wuid = _wuid.toUpperCase().str();
    if (online)
    {
        Owned<IWorkUnitFactory> wuFactory = getWorkunitFactory(ctx);
        Owned<IConstWorkUnit> wu = wuFactory->openWorkUnit(wuid);  // Note - we don't use getWorkUnit as we don't need read access
        return wu != NULL;
    }
    if (archived)
    {
        SocketEndpointArray sashaeps;
        getSashaNodes(sashaeps);
        ForEachItemIn(i,sashaeps) {
            Owned<ISashaCommand> cmd = createSashaCommand();    
            cmd->setAction(SCA_LIST);                           
            cmd->setOnline(false);                              
            cmd->setArchived(true); 
            cmd->addId(wuid);
            Owned<INode> sashanode = createINode(sashaeps.item(i));
            if (cmd->send(sashanode,SASHA_TIMEOUT)) {
                return cmd->numIds()>0;
            }
        }
    }
    return false;
}

WORKUNITSERVICES_API char * wsWUIDonDate(unsigned year,unsigned month,unsigned day,unsigned hour,unsigned minute)
{
    StringBuffer ret;
    return getWUIDonDate(ret,year,month,day,hour,minute).detach();
}

WORKUNITSERVICES_API char * wsWUIDdaysAgo(unsigned daysago)
{
    StringBuffer ret;
    return getWUIDdaysAgo(ret,(int)daysago).detach();
}

WORKUNITSERVICES_API void wsWorkunitTimeStamps(ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid)
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    MemoryBuffer mb;
    if (wu)
    {
/*
        // Workunit timestamps have not been stored like this for a while - so this code has not been working
        // Should look at fixing but perhaps as a separate Jira
        Owned<IPropertyTreeIterator> iter = pt->getElements("TimeStamp");
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            Owned<IPropertyTreeIterator> iter2 = item.getElements("*");
            ForEach(*iter2)
            {
                IPropertyTree &item2 = iter2->query();
                fixedAppend(mb, 32, item, "@application");              // item correct here
                fixedAppend(mb, 16, item2.queryName());                 // id
                fixedAppend(mb,  20, item2.queryProp(NULL));            // time
                fixedAppend(mb,16, item, "@instance");                  // item correct here
            }
        }
  */
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitMessages( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    MemoryBuffer mb;
    if (wu)
    {
        SCMStringBuffer s;
        Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
        ForEach(*exceptions)
        {
            IConstWUException &e = exceptions->query();
            mb.append((unsigned) e.getSeverity());
            mb.append((int) e.getExceptionCode());
            e.getExceptionFileName(s);
            fixedAppend(mb, 32, s.str(), s.length());
            mb.append((unsigned) e.getExceptionLineNo());
            mb.append((unsigned)  e.getExceptionColumn());
            e.getExceptionSource(s);
            fixedAppend(mb, 16, s.str(), s.length());
            e.getTimeStamp(s);
            fixedAppend(mb, 20, s.str(), s.length());
            e.getExceptionMessage(s);
            varAppendMax(mb, 1024, s.str(), s.length());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitFilesRead( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    if (wu)
    {
        Owned<IPropertyTreeIterator> sourceFiles = &wu->getFilesReadIterator();
        ForEach(*sourceFiles)
        {
            IPropertyTree &item = sourceFiles->query();
            varAppendMax(mb, 256, item, "@name");
            varAppendMax(mb, 64, item, "@cluster");
            mb.append(item.getPropBool("@super"));
            mb.append((unsigned) item.getPropInt("@useCount"));
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitFilesWritten( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    if (wu)
    {
        Owned<IPropertyTreeIterator> sourceFiles = &wu->getFileIterator();
        ForEach(*sourceFiles)
        {
            IPropertyTree &item = sourceFiles->query();
            varAppendMax(mb, 256, item, "@name");
            fixedAppend(mb, 10, item, "@graph");                
            varAppendMax(mb, 64, item, "@cluster");
            mb.append( (unsigned) item.getPropInt("@kind"));
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitTimings( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    MemoryBuffer mb;
    if (wu)
    {
        StatisticsFilter filter;
        filter.setScopeDepth(1, 2);
        filter.setMeasure(SMeasureTimeNs);

        SCMStringBuffer desc;
        Owned<IConstWUStatisticIterator> iter = &wu->getStatistics(&filter);
        ForEach(*iter)
        {
            IConstWUStatistic & cur = iter->query();

            unsigned __int64 value = cur.getValue();
            unsigned __int64 count = cur.getCount();
            unsigned __int64 max = cur.getMax();
            cur.getDescription(desc, true);

            mb.append((unsigned) count);
            mb.append((unsigned) (value / 1000000));
            mb.append((unsigned) max);
            varAppend(mb, desc.str());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


class StreamedStatistics : public CInterfaceOf<IRowStream>
{
public:
    StreamedStatistics(IConstWorkUnit * _wu, IEngineRowAllocator * _resultAllocator, IConstWUStatisticIterator * _iter)
    : wu(_wu), resultAllocator(_resultAllocator),iter(_iter)
    {
    }

    virtual const void *nextRow()
    {
        if (!iter || !iter->isValid())
            return NULL;

        IConstWUStatistic & cur = iter->query();

        unsigned __int64 value = cur.getValue();
        unsigned __int64 count = cur.getCount();
        unsigned __int64 max = cur.getMax();
        StatisticCreatorType creatorType = cur.getCreatorType();
        cur.getCreator(creator);
        StatisticScopeType scopeType = cur.getScopeType();
        cur.getScope(scope);
        cur.getDescription(description, true);
        StatisticMeasure measure = cur.getMeasure();
        StatisticKind kind = cur.getKind();

        MemoryBuffer mb;
        mb.append(sizeof(value),&value);
        mb.append(sizeof(count),&count);
        mb.append(sizeof(max),&max);
        varAppend(mb, queryCreatorTypeName(creatorType));
        varAppend(mb, creator.str());
        varAppend(mb, queryScopeTypeName(scopeType));
        varAppend(mb, scope.str());
        varAppend(mb, queryStatisticName(kind));
        varAppend(mb, description.str());
        varAppend(mb, queryMeasureName(measure));

        size32_t len = mb.length();
        size32_t newSize;
        void * row = resultAllocator->createRow(newSize);
        row = resultAllocator->resizeRow(len, row, newSize);
        memcpy(row, mb.bufferBase(), len);

        iter->next();
        return resultAllocator->finalizeRow(len, row, newSize);
    }
    virtual void stop()
    {
        iter.clear();
    }


protected:
    Linked<IConstWorkUnit> wu;
    Linked<IEngineRowAllocator> resultAllocator;
    Linked<IConstWUStatisticIterator> iter;
    SCMStringBuffer creator;
    SCMStringBuffer scope;
    SCMStringBuffer description;
};

WORKUNITSERVICES_API IRowStream * wsWorkunitStatistics( ICodeContext *ctx, IEngineRowAllocator * allocator, const char *wuid, bool includeActivities, const char * filterText)
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx, wuid);
    if (!wu)
        return createNullRowStream();
    //Filter needs to be allocated because the iterator outlasts it.
    Owned<StatisticsFilter> filter = new StatisticsFilter(filterText);
    if (!includeActivities)
        filter->setScopeDepth(1, 2);
    Owned<IConstWUStatisticIterator> stats = &wu->getStatistics(filter);
    return new StreamedStatistics(wu, allocator, stats);
}


WORKUNITSERVICES_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

WORKUNITSERVICES_API char * WORKUNITSERVICES_CALL fsGetBuildInfo(void)
{ 
    return CTXSTRDUP(parentCtx, WORKUNITSERVICES_VERSION);
}
