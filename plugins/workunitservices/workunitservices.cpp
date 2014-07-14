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
#include "portlist.h"

#include "jio.hpp"
#include "jmisc.hpp"
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
                            " string creator;"
                            " string scope;"
                            " string name;"
                            " string description;"
                            " string unit;"
                        " end;\n"
"export WorkunitServices := SERVICE\n"
"   boolean WorkunitExists(const varstring wuid, boolean online=true, boolean archived=false) : c,entrypoint='wsWorkunitExists'; \n"
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
"  streamed dataset(WsStatistic) WorkunitStatistics(const varstring wuid, boolean includeActivities = false) : c,context,entrypoint='wsWorkunitStatistics'; \n"
    
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


static IConstWorkUnit * getWorkunit(ICodeContext * ctx)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    return factory->openWorkUnit(wuid, false);
}

static IWorkUnit * updateWorkunit(ICodeContext * ctx)
{
    // following bit of a kludge, as 
    // 1) eclagent keeps WU locked, and 
    // 2) rtti not available in generated .so's to convert to IAgentContext
    IAgentContext * actx = dynamic_cast<IAgentContext *>(ctx);
    if (actx == NULL) { // fall back to pure ICodeContext
        // the following works for thor only 
        char * platform = ctx->getPlatform();
        if (strcmp(platform,"thor")==0) {  
            CTXFREE(parentCtx, platform);
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            StringAttr wuid;
            wuid.setown(ctx->getWuid());
            return factory->updateWorkUnit(wuid);
        }
        CTXFREE(parentCtx, platform);
        return NULL;
    }
    return actx->updateWorkUnit();
}




static bool checkScopeAuthorized(IUserDescriptor *user,IPropertyTree &pt,bool &securitydisabled)
{
    if (securitydisabled)
        return true;
    unsigned auditflags = DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED;
    int perm = 255;
    const char *scopename = pt.queryProp("@scope");
    if (scopename&&*scopename) {
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0) {
            if (perm==-1) {
                perm = 255;
                securitydisabled = true;
            }
            else 
                perm = 0;
        }
        if (!HASREADPERMISSION(perm)) 
            return false;

    }
    return true;
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



class COnlineWorkunitIterator: public CInterface, implements IPropertyTreeIterator
{
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTreeIterator> iter;
    Linked<IUserDescriptor> user;
    bool securitydisabled;
    StringAttr namehi;
    StringAttr namelo;


    bool postFilterOk()
    {
        IPropertyTree &t = query();
        const char *name = t.queryName();
        if (stricmp(name,namelo.get())<0)
            return false;
        if (stricmp(name,namehi.get())>0)
            return false;
        if (!checkScopeAuthorized(user,t,securitydisabled))
            return false;
        return true;
    }

public:
    IMPLEMENT_IINTERFACE;   
    
    COnlineWorkunitIterator (
                    IUserDescriptor* _user,
                    const char *_namelo,
                    const char *_namehi,
                    const char *user,
                    const char *cluster,
                    const char *jobname,
                    const char *state,
                    const char *priority,
                    const char *fileread,
                    const char *filewritten,
                    const char *roxiecluster,
                    const char *ecl

    ) : user(_user), namelo(_namelo), namehi(_namehi)
    {
        securitydisabled = false;
        if (namelo.isEmpty()) 
            namelo.set("W");
        if (namehi.isEmpty()) {
            StringBuffer tmp;
            namehi.set(getWUIDdaysAgo(tmp,-1).str());
        }
        const char *lo = namelo;
        const char *hi = namehi;
        StringBuffer query;
        while (*lo&&(toupper(*lo)==toupper(*hi))) {
            query.append((char)toupper(*lo));
            lo++;
            hi++;
        }
        if (*lo||*hi)
            query.append("*");
        if (user&&*user) 
            query.appendf("[@submitID=~?\"%s\"]",user);
        if (cluster&&*cluster) 
            query.appendf("[@clusterName=~?\"%s\"]",cluster);
        if (jobname&&*jobname) 
            query.appendf("[@jobName=~?\"%s\"]",jobname);
        if (state&&*state) 
            query.appendf("[@state=?\"%s\"]",state);
        if (priority&&*priority) 
            query.appendf("[@priorityClass=?\"%s\"]",priority);
        if (fileread&&*fileread) 
            query.appendf("[FilesRead/File/@name=~?\"%s\"]",fileread);
        if (filewritten&&*filewritten) 
            query.appendf("[Files/File/@name=~?\"%s\"]",filewritten);
        if (roxiecluster&&*roxiecluster) 
            query.appendf("[RoxieQueryInfo/@roxieClusterName=~?\"%s\"]",roxiecluster);
        if (ecl&&*ecl)
            query.appendf("[Query/Text=?~\"*%s*\"]",ecl);
        conn.setown(querySDS().connect("WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT));
        if (conn.get()) {
            iter.setown(conn->getElements(query.str()));
            if (!iter.get()) 
                conn.clear();
        }
    }


    bool first()
    {
        if (!iter.get()||!iter->first())
            return false;
        if (postFilterOk())
            return true;
        return next();
    }

    bool next()
    {
        while (iter.get()&&iter->next()) 
            if (postFilterOk())
                return true;
        return false;
    }

    bool isValid() 
    { 
        return iter&&iter->isValid(); 
    }

    IPropertyTree & query() 
    { 
        assertex(iter); 
        return iter->query(); 
    }


    bool serialize(MemoryBuffer &mb)
    {
        IPropertyTree &pt = query();
        return serializeWUSrow(pt,mb,true);
    }

};



static IPropertyTree *getWorkUnitBranch(ICodeContext *ctx,const char *wuid,const char *branch)
{
    if (!wuid||!*wuid)
        return NULL;
    StringBuffer _wuid(wuid);
    _wuid.trimRight();
    wuid = _wuid.toUpperCase().str();
    StringBuffer query;
    query.append("WorkUnits/").append(wuid);
    Owned<IRemoteConnection> conn =  querySDS().connect(query.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (conn) {
        IPropertyTree *t = conn->queryRoot();
        if (!t)
            return NULL;
        bool securitydisabled = false;
        if (!checkScopeAuthorized(ctx->queryUserDescriptor(),*t,securitydisabled))
            return NULL;
        IPropertyTree *ret = t->queryBranch(branch);
        if (!ret)
            return NULL;
        return createPTreeFromIPT(ret);
    }
    // look up in sasha - this could be improved with server support
    SocketEndpointArray sashaeps;
    getSashaNodes(sashaeps);
    ForEachItemIn(i,sashaeps) {
        Owned<ISashaCommand> cmd = createSashaCommand();    
        cmd->setAction(SCA_GET);
        cmd->setOnline(false);                              
        cmd->setArchived(true); 
        cmd->addId(wuid);
        Owned<INode> sashanode = createINode(sashaeps.item(i));
        if (cmd->send(sashanode,SASHA_TIMEOUT)) {
            if (cmd->numIds()) {
                StringBuffer res;
                if (cmd->getResult(0,res)) 
                    return createPTreeFromXMLString(res.str());
            }
            if (i+1>=sashaeps.ordinality()) 
                break;
        }
    }
    return NULL;
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
    const char *roxiecluster,
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
            if (roxiecluster&&*roxiecluster)
                cmd->setRoxieCluster(roxiecluster);
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
    if (online) {
        Owned<COnlineWorkunitIterator> oniter = new COnlineWorkunitIterator(ctx->queryUserDescriptor(),lowwuid,highwuid,username,cluster,jobname,state,priority,fileread,filewritten,roxiecluster,eclcontains);
        ForEach(*oniter) {
            if (!oniter->serialize(mb)) 
                throw MakeStringException(-1,"WORKUNITSERVICES: Result buffer overflowed");
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


WORKUNITSERVICES_API bool wsWorkunitExists(const char *wuid, bool online, bool archived)
{
    if (!wuid||!*wuid)
        return false;
    StringBuffer _wuid(wuid);
    wuid = _wuid.toUpperCase().str();
    if (online) {
        StringBuffer s("WorkUnits/");
        s.append(wuid);
        Owned<IRemoteConnection> conn = querySDS().connect(s.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
            if (conn.get()) 
                return true;
    }
    if (archived) {
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

WORKUNITSERVICES_API void wsWorkunitTimeStamps( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"TimeStamps");
    if (pt) {
        Owned<IPropertyTreeIterator> iter = pt->getElements("TimeStamp");
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (&item==NULL)
                continue; // paranoia
            Owned<IPropertyTreeIterator> iter2 = item.getElements("*");
            ForEach(*iter2) {
                IPropertyTree &item2 = iter2->query();
                if (&item2==NULL)
                    continue; // paranoia
                fixedAppend(mb, 32, item, "@application");              // item correct here
                fixedAppend(mb, 16, item2.queryName());                 // id
                fixedAppend(mb,  20, item2.queryProp(NULL));            // time
                fixedAppend(mb,16, item, "@instance");                  // item correct here
            }
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitMessages( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    unsigned tmpu;
    int tmpi;
    Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"Exceptions");
    if (pt) {
        Owned<IPropertyTreeIterator> iter = pt->getElements("Exception");
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (&item==NULL)
                continue; // paranoia
            tmpu = (unsigned)item.getPropInt("@severity");
            mb.append(sizeof(tmpu),&tmpu);
            tmpi = (int)item.getPropInt("@code");
            mb.append(sizeof(tmpi),&tmpi);
            fixedAppend(mb, 32, item, "@filename");             
            tmpu = (unsigned)item.getPropInt("@row");
            mb.append(sizeof(tmpu),&tmpu);
            tmpu = (unsigned)item.getPropInt("@col");
            mb.append(sizeof(tmpu),&tmpu);
            fixedAppend(mb, 16, item, "@source");               
            fixedAppend(mb, 20, item, "@time");             
            varAppend(mb, 1024, item, NULL);                
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitFilesRead( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"FilesRead");
    if (pt) {
        Owned<IPropertyTreeIterator> iter = pt->getElements("File");
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (&item==NULL)
                continue; // paranoia
            varAppend(mb, 256, item, "@name");              
            varAppend(mb, 64, item, "@cluster");                
            byte b = item.getPropBool("@super")?1:0;
            mb.append(sizeof(b),&b);
            unsigned uc = (unsigned)item.getPropInt("@useCount");
            mb.append(sizeof(uc),&uc);
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitFilesWritten( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    MemoryBuffer mb;
    Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"Files");
    if (pt) {
        Owned<IPropertyTreeIterator> iter = pt->getElements("File");
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (&item==NULL)
                continue; // paranoia
            varAppend(mb, 256, item, "@name");              
            fixedAppend(mb, 10, item, "@graph");                
            varAppend(mb, 64, item, "@cluster");                
            unsigned k = (unsigned)item.getPropInt("@kind");
            mb.append(sizeof(k),&k);
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

WORKUNITSERVICES_API void wsWorkunitTimings( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid )
{
    unsigned tmp;
    MemoryBuffer mb;
    Owned<IPropertyTree> st = getWorkUnitBranch(ctx,wuid,"Statistics");
    if (st)
    {
        Owned<IPropertyTreeIterator> iter = st->getElements("Statistic[@unit=\"ns\"]");
        ForEach(*iter) {
            IPropertyTree &item = iter->query();
            if (&item==NULL)
                continue; // paranoia
            tmp = (unsigned)item.getPropInt("@count");
            mb.append(sizeof(tmp),&tmp);
            tmp = (unsigned)(item.getPropInt64("@value") / 1000000);
            mb.append(sizeof(tmp),&tmp);
            tmp = (unsigned)item.getPropInt("@max");
            mb.append(sizeof(tmp),&tmp);
            varAppend(mb, 64, item, "@desc");
        }
    }
    else
    {
        Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"Timings");
        if (pt) {
            Owned<IPropertyTreeIterator> iter = pt->getElements("Timing");
            ForEach(*iter) {
                IPropertyTree &item = iter->query();
                if (&item==NULL)
                    continue; // paranoia
                tmp = (unsigned)item.getPropInt("@count");
                mb.append(sizeof(tmp),&tmp);
                tmp = (unsigned)item.getPropInt("@duration");
                mb.append(sizeof(tmp),&tmp);
                tmp = (unsigned)item.getPropInt("@max");
                mb.append(sizeof(tmp),&tmp);
                varAppend(mb, 64, item, "@name");
            }
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


class StreamedStatistics : public CInterfaceOf<IRowStream>
{
public:
    StreamedStatistics(IEngineRowAllocator * _resultAllocator, IPropertyTreeIterator * _iter)
    : resultAllocator(_resultAllocator),iter(_iter)
    {
    }

    virtual const void *nextRow()
    {
        if (!iter || !iter->isValid())
            return NULL;

        IPropertyTree & cur = iter->query();
        unsigned __int64 value = cur.getPropInt64("@value", 0);
        unsigned __int64 count = cur.getPropInt64("@count", 0);
        unsigned __int64 max = cur.getPropInt64("@max", 0);
        const char * uid = cur.queryProp("@name");
        const char * sep1 = strchr(uid, ';');
        const char * scope = sep1+1;
        const char * sep2 = strchr(scope, ';');
        const char * name = sep2+1;
        const char * desc = cur.queryProp("@desc");
        const char * unit = cur.queryProp("@unit");
        if (!desc) desc = "";

        size32_t lenComponent = sep1-uid;
        size32_t lenScope = sep2-scope;
        size32_t lenName = strlen(name);
        size32_t lenUnit = strlen(unit);

        MemoryBuffer mb;
        mb.append(sizeof(value),&value);
        mb.append(sizeof(count),&count);
        mb.append(sizeof(max),&max);
        varAppend(mb, lenComponent, uid);
        varAppend(mb, lenScope, scope);
        varAppend(mb, lenName, name);
        varAppend(mb, desc);
        varAppend(mb, unit);

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
    Linked<IEngineRowAllocator> resultAllocator;
    Linked<IPropertyTreeIterator> iter;
};

WORKUNITSERVICES_API IRowStream * wsWorkunitStatistics( ICodeContext *ctx, IEngineRowAllocator * allocator, const char *wuid, bool includeActivities)
{
    MemoryBuffer mb;
    Owned<IPropertyTree> pt = getWorkUnitBranch(ctx,wuid,"Statistics");
    Owned<IPropertyTreeIterator> iter;
    if (pt)
    {
        iter.setown(pt->getElements("Statistic"));

        //MORE - it includeActivities create an iterator over the progress information, and create a union iterator.
        iter->first();
    }
    return new StreamedStatistics(allocator, iter);
}


WORKUNITSERVICES_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

WORKUNITSERVICES_API char * WORKUNITSERVICES_CALL fsGetBuildInfo(void)
{ 
    return CTXSTRDUP(parentCtx, WORKUNITSERVICES_VERSION);
}
