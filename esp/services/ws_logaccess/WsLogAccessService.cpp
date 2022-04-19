#include <ws_logaccess/WsLogAccessService.hpp>

Cws_logaccessEx::Cws_logaccessEx()
{
}

Cws_logaccessEx::~Cws_logaccessEx()
{
}

bool Cws_logaccessEx::onGetLogAccessInfo(IEspContext &context, IEspGetLogAccessInfoRequest &req, IEspGetLogAccessInfoResponse &resp)
{
    bool success = true;
    if (m_remoteLogAccessor)
    {
        resp.setRemoteLogManagerType(m_remoteLogAccessor->getRemoteLogAccessType());
        resp.setRemoteLogManagerConnectionString(m_remoteLogAccessor->fetchConnectionStr());
    }
    else
        success = false;

    return success;
}

void Cws_logaccessEx::init(const IPropertyTree *cfg, const char *process, const char *service)
{
    LOG(MCdebugProgress,"WsLogAccessService loading remote log access plug-in...");

    try
    {
        m_remoteLogAccessor.set(&queryRemoteLogAccessor());

        if (m_remoteLogAccessor == nullptr)
            LOG(MCerror,"WsLogAccessService could not load remote log access plugin!");
    }
    catch (IException * e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        LOG(MCoperatorWarning,"WsLogAccessService could not load remote log access plug-in: %s", msg.str());
        e->Release();
    }
}

LogAccessTimeRange requestedRangeToLARange(IConstTimeRange & reqrange)
{
    struct LogAccessTimeRange range;

    range.setStart(reqrange.getStartDate());
    range.setEnd(reqrange.getEndDate());

    return range;
}

bool Cws_logaccessEx::onGetLogs(IEspContext &context, IEspGetLogsRequest &req, IEspGetLogsResponse & resp)
{
    if (!m_remoteLogAccessor)
        throw makeStringException(-1, "WsLogAccess: Remote Log Access plug-in not available!");

    CLogAccessType searchByCategory = req.getLogCategory();
    const char * searchByValue = req.getSearchByValue();
    if (searchByCategory != CLogAccessType_All && isEmptyString(searchByValue))
        throw makeStringException(-1, "WsLogAccess::onGetLogs: Must provide log category");

    LogAccessConditions logFetchOptions;
    switch (searchByCategory)
    {
        case CLogAccessType_All:
            logFetchOptions.setFilter(getWildCardLogAccessFilter());
            break;
        case CLogAccessType_ByJobIdID:
            logFetchOptions.setFilter(getJobIDLogAccessFilter(searchByValue));
            break;
        case CLogAccessType_ByComponent:
            logFetchOptions.setFilter(getComponentLogAccessFilter(searchByValue));
            break;
        case CLogAccessType_ByLogType:
        {
            LogMsgClass logType = LogMsgClassFromAbbrev(searchByValue);
            if (logType == MSGCLS_unknown)
                throw makeStringExceptionV(-1, "Invalid Log Type 3-letter code encountered: '%s' - Available values: 'DIS,ERR,WRN,INF,PRO,MET'", searchByValue);

            logFetchOptions.setFilter(getClassLogAccessFilter(logType));
            break;
        }
        case CLogAccessType_ByTargetAudience:
        {
            MessageAudience targetAud = LogMsgAudFromAbbrev(searchByValue);
            if (targetAud == MSGAUD_unknown || targetAud == MSGAUD_all)
                throw makeStringExceptionV(-1, "Invalid Target Audience 3-letter code encountered: '%s' - Available values: 'OPR,USR,PRO,ADT'", searchByValue);

            logFetchOptions.setFilter(getAudienceLogAccessFilter(targetAud));
            break;
        }
        case CLogAccessType_BySourceInstance:
        {
            logFetchOptions.setFilter(getInstanceLogAccessFilter(searchByValue));
            break;
        }
        case CLogAccessType_BySourceNode:
        {
            logFetchOptions.setFilter(getHostLogAccessFilter(searchByValue));
            break;
        }
        case LogAccessType_Undefined:
        default:
            throw makeStringException(-1, "Invalid remote log access request type");
    }

    LogAccessTimeRange range = requestedRangeToLARange(req.getRange());
    double version = context.getClientVersion();
    if (version > 1.0)
    {
        switch (req.getSelectColumnMode())
        {
        case 0:
            logFetchOptions.setReturnColsMode(RETURNCOLS_MODE_min);
            break;
        case 1:
            logFetchOptions.setReturnColsMode(RETURNCOLS_MODE_default);
            break;
        case 2:
            logFetchOptions.setReturnColsMode(RETURNCOLS_MODE_all);
            break;
        case 3:
            logFetchOptions.setReturnColsMode(RETURNCOLS_MODE_custom);
            logFetchOptions.copyLogFieldNames(req.getColumns());
            break;
        default:
            logFetchOptions.setReturnColsMode(RETURNCOLS_MODE_default);
            break;
        }
    }

    unsigned limit = req.getLogLineLimit();

    __int64 startFrom = req.getLogLineStartFrom();
    if (startFrom < 0)
        throw makeStringExceptionV(-1, "WsLogAccess: Encountered invalid LogLineStartFrom value: '%lld'", startFrom);

    logFetchOptions.setTimeRange(range);
    logFetchOptions.setLimit(limit);
    logFetchOptions.setStartFrom((offset_t)startFrom);

    StringBuffer logcontent;
    m_remoteLogAccessor->fetchLog(logFetchOptions, logcontent, logAccessFormatFromName(req.getFormat()));

    resp.setLogLines(logcontent.str());

    return true;
}
