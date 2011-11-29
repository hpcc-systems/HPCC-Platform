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

#ifndef _WSWU_HELPERS_HPP__
#define _WSWU_HELPERS_HPP__

#include "ws_workunits_esp.ipp"
#include "exception_util.hpp"

#include "jtime.ipp"
#include "workunit.hpp"
#include "hqlerror.hpp"

#include <list>
#include <vector>

namespace ws_workunits {

#define     OWN_WU_ACCESS      "OwnWorkunitsAccess"
#define     OTHERS_WU_ACCESS   "OthersWorkunitsAccess"

#define    File_Cpp "cpp"
#define    File_ThorLog "ThorLog"
#define    File_ThorSlaveLog "ThorSlaveLog"
#define    File_EclAgentLog "EclAgentLog"
#define    File_XML "XML"
#define    File_Res "res"
#define    File_DLL "dll"
#define    File_ArchiveQuery "ArchiveQuery"

#define    TEMPZIPDIR "tempzipfiles"

static const long MAXXLSTRANSFER = 5000000;
const unsigned DATA_SIZE = 16;
const unsigned AWUS_CACHE_SIZE = 16;
const unsigned AWUS_CACHE_MIN_DEFAULT = 15;

inline bool notEmpty(const char *val){return (val && *val);}
inline bool isEmpty(const char *val){return (!val || !*val);}

const char *getWuAccessType(IConstWorkUnit& cw, const char *user);

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, const char *owner, SecAccessFlags accessOwn, SecAccessFlags accessOthers);
SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, IConstWorkUnit& cw, SecAccessFlags accessOwn, SecAccessFlags accessOthers);
SecAccessFlags getWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw);

void getUserWuAccessFlags(IEspContext& context, SecAccessFlags& accessOwn, SecAccessFlags& accessOthers, bool except);
void ensureWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw, SecAccessFlags minAccess);
void ensureWsWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess);
void ensureWsWorkunitAccessByOwnerId(IEspContext& context, const char* owner, SecAccessFlags minAccess);
void ensureWsCreateWorkunitAccess(IEspContext& cxt);

const char *getGraphNum(const char *s,unsigned &num);

class WsWuDateTime : public CScmDateTime
{
public:
    IMPLEMENT_IINTERFACE;
    WsWuDateTime()
    {
        setSimpleLocal(0);
    }

    bool isValid()
    {
        unsigned year, month, day;
        cdt.getDate(year, month, day, true);
        return year>1969;
    }
};

void formatDuration(StringBuffer &s, unsigned ms);

struct WsWUExceptions
{
    WsWUExceptions(IConstWorkUnit& wu);

    operator IArrayOf<IEspECLException>&() { return errors; }
    int ErrCount() { return numerr; }
    int WrnCount() { return numwrn; }
    int InfCount() { return numinf; }

private:
    IArrayOf<IEspECLException> errors;
    int numerr;
    int numwrn;
    int numinf;
};

#define WUINFO_TruncateEclTo64k         0x001
#define WUINFO_IncludeExceptions        0x002
#define WUINFO_IncludeGraphs            0x004
#define WUINFO_IncludeResults           0x008
#define WUINFO_IncludeVariables         0x010
#define WUINFO_IncludeTimers            0x020
#define WUINFO_IncludeDebugValues       0x040
#define WUINFO_IncludeApplicationValues 0x080
#define WUINFO_IncludeWorkflows         0x100
#define WUINFO_IncludeEclSchemas        0x200
#define WUINFO_IncludeSourceFiles       0x400
#define WUINFO_IncludeResultsViewNames  0x800
#define WUINFO_All                      0xFFF

class WsWuInfo
{
public:
    WsWuInfo(IEspContext &ctx, IConstWorkUnit *cw_) :
      context(ctx), cw(cw_)
    {
        version = context.getClientVersion();
        cw->getWuid(wuid);
    }

    WsWuInfo(IEspContext &ctx, const char *wuid_) :
      context(ctx)
    {
        wuid.set(wuid_);
        version = context.getClientVersion();
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(ctx.querySecManager(), ctx.queryUser());
        cw.setown(factory->openWorkUnit(wuid_, false));
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", wuid_);
    }

    bool getResultViews(StringArray &resultViews, unsigned flags);

    void getCommon(IEspECLWorkunit &info, unsigned flags);
    void getInfo(IEspECLWorkunit &info, unsigned flags);

    bool getResults(IEspECLWorkunit &info, unsigned flags);
    bool getVariables(IEspECLWorkunit &info, unsigned flags);
    bool getDebugValues(IEspECLWorkunit &info, unsigned flags);
    bool getClusterInfo(IEspECLWorkunit &info, unsigned flags);
    bool getApplicationValues(IEspECLWorkunit &info, unsigned flags);
    void getExceptions(IEspECLWorkunit &info, unsigned flags);
    bool getSourceFiles(IEspECLWorkunit &info, unsigned flags);
    bool getTimers(IEspECLWorkunit &info, unsigned flags);
    bool getHelpers(IEspECLWorkunit &info, unsigned flags);
    bool getGraphInfo(IEspECLWorkunit &info, unsigned flags);
    void getGraphTimingData(IArrayOf<IConstECLTimingData> &timingData, unsigned flags);

    void getRoxieCluster(IEspECLWorkunit &info, unsigned flags);
    bool getWorkflow(IEspECLWorkunit &info, unsigned flags);

    void getHelpFiles(IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers);
    void getSubFiles(IPropertyTreeIterator* f, IEspECLSourceFile* eclSuperFile, StringArray& fileNames);
    void getEclSchemaChildFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    void getEclSchemaFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    bool getResultEclSchemas(IConstWUResult &r, IArrayOf<IEspECLSchemaItem>& schemas);
    void getResult(IConstWUResult &r, IArrayOf<IEspECLResult>& results, unsigned flags);

    void getWorkunitEclAgentLog(MemoryBuffer& buf);
    void getWorkunitThorLog(MemoryBuffer& buf);
    void getWorkunitThorSlaveLog(const char *slaveip, MemoryBuffer& buf);
    void getWorkunitResTxt(MemoryBuffer& buf);
    void getWorkunitArchiveQuery(MemoryBuffer& buf);
    void getWorkunitDll(MemoryBuffer& buf);
    void getWorkunitXml(const char* plainText, MemoryBuffer& buf);
    void getWorkunitCpp(const char* cppname, const char* description, const char* ipAddress, MemoryBuffer& buf);

public:
    IEspContext &context;
    Linked<IConstWorkUnit> cw;
    double version;
    SCMStringBuffer clusterName;
    SCMStringBuffer wuid;
};

void getSashaNode(SocketEndpoint &ep);

struct WsWuSearch
{
    WsWuSearch(IEspContext& context,const char* owner=NULL,const char* state=NULL,const char* cluster=NULL,const char* startDate=NULL,const char* endDate=NULL,const char* ecl=NULL,const char* jobname=NULL,const char* appname=NULL,const char* appkey=NULL,const char* appvalue=NULL);

    typedef std::vector<std::string>::iterator iterator;

    iterator begin() { return wuids.begin(); }
    iterator end()   { return wuids.end(); }

    iterator locate(const char* wuid)
    {
        if(wuids.size() && *wuids.begin()>wuid)
            return std::lower_bound(wuids.begin(),wuids.end(),wuid,std::greater<std::string>());
        return wuids.begin();
    }

     __int64 getSize() { return wuids.size(); }

private:

    StringBuffer& createWuidFromDate(const char* timestamp,StringBuffer& s);

    std::vector<std::string> wuids;
};

struct DataCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    DataCacheElement(const char* filter, const char* data, const char* name, const char* logicalName, const char* wuid,
        const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total):m_filter(filter),
        m_data(data), m_name(name), m_logicalName(logicalName), m_wuid(wuid), m_resultName(resultName),
        m_seq(seq), m_start(start), m_rowcount(count), m_requested(requested), m_total(total)
    {
        m_timeCached.setNow();
    }

    CDateTime m_timeCached;
    std::string m_filter;
    std::string m_data;
    std::string m_name;
    std::string m_logicalName;
    std::string m_wuid;
    std::string m_resultName;
    unsigned m_seq;
    __int64 m_start;
    unsigned m_rowcount;
    __int64 m_requested;
    __int64 m_total;
};

struct DataCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;

    DataCache(size32_t _cacheSize=0): cacheSize(_cacheSize){}

    DataCacheElement* lookup(IEspContext &context, const char* filter, unsigned timeOutMin);

    void add(const char* filter, const char* data, const char* name, const char* localName, const char* wuid,
    const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total);

    std::list<Linked<DataCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

struct ArchivedWuCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    ArchivedWuCacheElement(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, /*const char* data,*/ IArrayOf<IEspECLWorkunit>& wus):m_filter(filter),
        m_sashaUpdatedWhen(sashaUpdatedWhen), m_hasNextPage(hasNextPage)/*, m_data(data)*/
    {
        m_timeCached.setNow();
        if (wus.length() > 0)

        for (unsigned i = 0; i < wus.length(); i++)
        {
            Owned<IEspECLWorkunit> info= createECLWorkunit("","");
            IEspECLWorkunit& info0 = wus.item(i);
            info->copy(info0);

            m_results.append(*info.getClear());
        }
    }

    std::string m_filter;
    std::string m_sashaUpdatedWhen;
    bool m_hasNextPage;

    CDateTime m_timeCached;
    IArrayOf<IEspECLWorkunit> m_results;
};

struct ArchivedWuCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;

    ArchivedWuCache(size32_t _cacheSize=0): cacheSize(_cacheSize){}
    ArchivedWuCacheElement* lookup(IEspContext &context, const char* filter, const char* sashaUpdatedWhen, unsigned timeOutMin);

    void add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, IArrayOf<IEspECLWorkunit>& wus);

    std::list<Linked<ArchivedWuCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

class WsWuJobQueueAuditInfo
{
public:
    WsWuJobQueueAuditInfo() {  };
    WsWuJobQueueAuditInfo(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, const char *xls);

    void getAuditLineInfo(const char* line, unsigned& longestQueue, unsigned& maxConnected, unsigned maxDisplay, unsigned showAll, IArrayOf<IEspThorQueue>& items);
    bool checkSameStrings(const char* s1, const char* s2);
    bool checkNewThorQueueItem(IEspThorQueue* tq, unsigned showAll, IArrayOf<IEspThorQueue>& items);
};

StringBuffer &getWuidFromLogicalFileName(IEspContext &context, const char *logicalName, StringBuffer &wuid);

bool addToQueryString(StringBuffer &queryString, const char *name, const char *value, const char delim = '&');

void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret);

class WUSchedule : public Thread
{
    IEspContainer* m_container;

public:
    virtual int run();
    virtual void setContainer(IEspContainer * container)
    {
        m_container = container;
    }
};

}
#endif
