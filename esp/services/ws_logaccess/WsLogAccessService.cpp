#include <ws_logaccess/WsLogAccessService.hpp>

Cws_logaccessEx::Cws_logaccessEx()
{
}

Cws_logaccessEx::~Cws_logaccessEx()
{
}

bool Cws_logaccessEx::onGetLogAccessMeta(IEspContext &context, IEspGetLogAccessMetaRequest &req, IEspGetLogAccessMetaResponse &resp)
{
    return true;
}

void Cws_logaccessEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == nullptr)
        throw MakeStringException(-1, "Cannot initialize Cws_logaccessEx, cfg is NULL");

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    m_serviceCfg.setown(cfg->getPropTree(xpath.str()));
    LOG(MCdebugProgress,"WsLogAccessService loading remote log access plugin...");
    m_esLogAccessor.set(RemoteLogAccessLoader::queryRemoteLogAccessor<IRemoteLogAccess>());
    if (m_esLogAccessor == nullptr || stricmp(m_esLogAccessor->getRemoteLogAccessType(), "elasticstack")!=0)
        LOG(MCerror,"WsLogAccessService could not load remote log access plugin!");

    m_logMap.set(m_esLogAccessor->fetchLogMap());
}

LogAccessRange requestedRangeToLARange(IConstRange & reqrange)
{
    struct LogAccessRange range;
    CRangeType rangetype = reqrange.getType();

    switch (rangetype)
    {
        case CRangeType_All:
            range.rangeType = ACRTAllAvailable;
            break;
        case CRangeType_LastNMinutes:
            range.rangeType = ACRTLastNMinutes;
            break;
        case CRangeType_LastNHours:
            range.rangeType = ACRTLastNHours;
            break;
        case CRangeType_LastNDays:
            range.rangeType = ACRTLastNDays;
            break;
        case CRangeType_LastNWeeks:
            range.rangeType = ACRTLastNWeeks;
            break;
        case CRangeType_LastNMonths:
            range.rangeType = ACRTLastNMonths;
            break;
        case CRangeType_LastNYears:
            range.rangeType = ACRTLastNYears;
            break;
        case CRangeType_StartEndDate:
            range.rangeType = ACRTTimeRange;
            range.startDate.set(reqrange.getStartDate());
            range.endDate.set(reqrange.getEndDate());
            break;
        case CRangeType_FirstNEntries:
            range.rangeType = ACRTirstNRows;
            break;
        case CRangeType_LastNEntries:
            range.rangeType = ACRTLastNRows;
            break;
        case RangeType_Undefined:
        default:
            break;
    }
    range.quantity = reqrange.getQuantity(); //might not be needed, but easier to fetch/set once

    return range;
}

bool Cws_logaccessEx::onGetComponentLog(IEspContext &context, IEspGetComponentLogRequest &req, IEspGetComponentLogResponse &resp)
{
    const char * message = "success";
    bool success = true;

    const char * componentname = req.getName();
    if (!componentname || !*componentname)
    {
        message = "Must provide component name";
        success = false;
    }
    else
    {
        struct LogAccessRange range = requestedRangeToLARange(req.getRange());
        StringArray & cols = req.getColumns();

        StringBuffer logcontent;
        m_esLogAccessor->fetchComponentLog(logcontent, componentname, range, cols);
        resp.setLog(logcontent.str());
        resp.setColumns(cols);
        resp.setRange(req.getRange());
        resp.setName(componentname);
    }

    resp.setSuccess(success);
    if (message && *message)
        resp.setMessage(message);

    return success;
}
bool Cws_logaccessEx::onGetComponentLogMeta(IEspContext &context, IEspGetComponentLogMetaRequest &req, IEspGetComponentLogMetaResponse &resp)
{
    const char * compindex = nullptr;
    const char * primarycolumn = nullptr;
    const char * contentcolumn = nullptr;

    bool hascomp = m_logMap->hasProp("components");
    if (hascomp)
    {
        compindex = m_logMap->queryProp("components/@indexsearchpattern");
        primarycolumn = m_logMap->queryProp("components/@searchcolumn");
        contentcolumn = m_logMap->queryProp("components/@contentcolumn");
    }
    else
    {
        compindex = m_logMap->queryProp("global/@indexsearchpattern");
        primarycolumn = m_logMap->queryProp("global/@searchcolumn");
        contentcolumn = m_logMap->queryProp("global/@contentcolumn");
    }

    resp.setSearchColumn(primarycolumn);
    resp.setSourceStore(compindex);
    resp.setContentColumn(contentcolumn);
    StringArray columns;

    Owned<IPropertyTreeIterator> iter = m_logMap->getElements("fields/field");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        columns.append(cur.queryProp("@name"));
    }
    resp.setAvailableColumns(columns);

    return true;
}

bool Cws_logaccessEx::onGetWULogMeta(IEspContext &context, IEspGetWULogMetaRequest &req, IEspGetWULogMetaResponse &resp)
{

    const char * compindex = nullptr;
    const char * primarycolumn = nullptr;
    const char * contentcolumn = nullptr;

    bool hasWUMap = m_logMap->hasProp("workunits");
    if (hasWUMap)
    {
        compindex = m_logMap->queryProp("workunits/@indexsearchpattern");
        primarycolumn = m_logMap->queryProp("workunits/@searchcolumn");
        contentcolumn = m_logMap->queryProp("workunits/@contentcolumn");
    }
    else
    {
        compindex = m_logMap->queryProp("global/@indexsearchpattern");
        primarycolumn = m_logMap->queryProp("global/@searchcolumn");
        contentcolumn = m_logMap->queryProp("global/@contentcolumn");
    }

    resp.setSearchColumn(primarycolumn);
    resp.setSourceStore(compindex);
    resp.setContentColumn(contentcolumn);
    StringArray columns;

    Owned<IPropertyTreeIterator> iter = m_logMap->getElements("fields/field");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        columns.append(cur.queryProp("."));
    }
    resp.setAvailableColumns(columns);

    return true;
}

bool Cws_logaccessEx::onGetWULog(IEspContext &context, IEspGetWULogRequest &req, IEspGetWULogResponse &resp)
{
    const char * message = "success";
    bool success = true;

    const char * wuid = req.getWUID();
    if (!wuid || !*wuid)
    {
        message = "Must provide WUID";
        success = false;
    }
    else
    {
        struct LogAccessRange range = requestedRangeToLARange(req.getRange());
        StringArray & cols = req.getColumns();

        StringBuffer logcontent;
        m_esLogAccessor->fetchWULog(logcontent, wuid, range, cols);
        resp.setLog(logcontent.str());
        resp.setColumns(cols);
        resp.setRange(req.getRange());
        resp.setWUID(wuid);
    }

    resp.setSuccess(success);
    if (message && *message)
        resp.setMessage(message);

    return success;
}
