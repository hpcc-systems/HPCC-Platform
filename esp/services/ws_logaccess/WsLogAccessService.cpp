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

LogAccessFilterType cLogAccessFilterOperator2LogAccessFilterType(CLogAccessFilterOperator cLogAccessFilterOperator)
{
    switch (cLogAccessFilterOperator)
    {
    case CLogAccessFilterOperator_NONE:
        return LOGACCESS_FILTER_unknown;
    case CLogAccessFilterOperator_AND:
        return LOGACCESS_FILTER_and;
    case CLogAccessFilterOperator_OR:
        return LOGACCESS_FILTER_or;
    case LogAccessFilterOperator_Undefined:
    default:
        throw makeStringException(-1, "WsLogAccess: Cannot convert log filter operator!");
    }
}

ILogAccessFilter * buildLogFilterByFields(CLogAccessType searchByCategory, const char * searchByValue, const char * serchField)
{
    if (isEmptyString(searchByValue) && searchByCategory != CLogAccessType_All)
       throw makeStringException(-1, "WsLogAccess: Empty searchByValue detected");

    switch (searchByCategory)
    {
        case CLogAccessType_All:
            return getWildCardLogAccessFilter();
        case CLogAccessType_ByJobIdID:
            return getJobIDLogAccessFilter(searchByValue);
        case CLogAccessType_ByComponent:
            return getComponentLogAccessFilter(searchByValue);
        case CLogAccessType_ByLogType:
        {
            LogMsgClass logType = LogMsgClassFromAbbrev(searchByValue);
            if (logType == MSGCLS_unknown)
                throw makeStringExceptionV(-1, "Invalid Log Type 3-letter code encountered: '%s' - Available values: 'DIS,ERR,WRN,INF,PRO,MET'", searchByValue);

            return getClassLogAccessFilter(logType);
        }
        case CLogAccessType_ByTargetAudience:
        {
            MessageAudience targetAud = LogMsgAudFromAbbrev(searchByValue);
            if (targetAud == MSGAUD_unknown || targetAud == MSGAUD_all)
                throw makeStringExceptionV(-1, "Invalid Target Audience 3-letter code encountered: '%s' - Available values: 'OPR,USR,PRO,ADT'", searchByValue);

            return getAudienceLogAccessFilter(targetAud);
        }
        case CLogAccessType_BySourceInstance:
        {
            return getInstanceLogAccessFilter(searchByValue);
        }
        case CLogAccessType_BySourceNode:
        {
            return getHostLogAccessFilter(searchByValue);
            break;
        }
        case CLogAccessType_ByFieldName:
        {
            return getColumnLogAccessFilter(serchField, searchByValue);
        }
        case LogAccessType_Undefined:
        default:
            throw makeStringException(-1, "Invalid remote log access request type");
    }
}

ILogAccessFilter * buildLogFilter(IConstLogFilter * logFilter)
{
    if (logFilter)
        return buildLogFilterByFields(logFilter->getLogCategory(), logFilter->getSearchByValue(), logFilter->getSearchField());
    else
        return nullptr;
}

bool isLogFilterEmpty(IConstLogFilter * logFilter)
{
    if (!logFilter)
        return true;

    return isEmptyString(logFilter->getSearchByValue()) && logFilter->getLogCategory() == LogAccessType_Undefined && isEmptyString(logFilter->getSearchField());
}

ILogAccessFilter * buildBinaryLogFilter(IConstBinaryLogFilter * binaryfilter)
{
    if (!binaryfilter)
        return nullptr;
    
    ILogAccessFilter * leftFilter = nullptr;
    if (binaryfilter->getLeftBinaryFilter().ordinality() == 0)
    {
        leftFilter = buildLogFilter(&binaryfilter->getLeftFilter());
    }
    else
    {
        if (binaryfilter->getLeftBinaryFilter().ordinality() > 1)
            throw makeStringException(-1, "WsLogAccess: LeftBinaryFilter cannot contain multiple entries!");

        if (!isLogFilterEmpty(&binaryfilter->getLeftFilter()))
            throw makeStringException(-1, "WsLogAccess: Cannot submit leftFilter and leftBinaryFilter!");

        leftFilter = buildBinaryLogFilter(&binaryfilter->getLeftBinaryFilter().item(0));
    }

    if (!leftFilter)
        throw makeStringExceptionV(-1, "WsLogAccess: Empty LEFT filter encountered");

    switch (binaryfilter->getOperator())
    {
    case CLogAccessFilterOperator_NONE:
    case LogAccessFilterOperator_Undefined: //no operator found
        //if (rightFilter != nullptr)
        //     WARNLOG("right FILTER ENCOUNTERED but no valid operator");
        return leftFilter;
    case CLogAccessFilterOperator_AND:
    case CLogAccessFilterOperator_OR:
    {
        ILogAccessFilter * rightFilter = nullptr;
        if (binaryfilter->getRightBinaryFilter().ordinality() == 0)
        {
            rightFilter = buildLogFilter(&binaryfilter->getRightFilter());
        }
        else
        {
            if (binaryfilter->getRightBinaryFilter().ordinality() > 1)
                throw makeStringException(-1, "WsLogAccess: RightBinaryFilter cannot contain multiple entries!");

            if (!isLogFilterEmpty(&binaryfilter->getRightFilter()))
                throw makeStringException(-1, "WsLogAccess: Cannot submit rightFilter and rightBinaryFilter!");

            rightFilter = buildBinaryLogFilter(&binaryfilter->getRightBinaryFilter().item(0));
        }

        if (!rightFilter)
            throw makeStringExceptionV(-1, "WsLogAccess: Empty RIGHT filter encountered");

        return getBinaryLogAccessFilterOwn(leftFilter, rightFilter, cLogAccessFilterOperator2LogAccessFilterType(binaryfilter->getOperator()));
    }

    default:
        throw makeStringExceptionV(-1, "WsLogAccess: Invalid log access filter operator encountered '%d'", binaryfilter->getOperator());
    }
}

bool Cws_logaccessEx::onGetLogs(IEspContext &context, IEspGetLogsRequest &req, IEspGetLogsResponse & resp)
{
    if (!m_remoteLogAccessor)
        throw makeStringException(-1, "WsLogAccess: Remote Log Access plug-in not available!");

    double version = context.getClientVersion();
    LogAccessConditions logFetchOptions;
    if (version > 1.01)
        logFetchOptions.setFilter(buildBinaryLogFilter(&req.getFilter()));
    else
        logFetchOptions.setFilter(buildLogFilterByFields(req.getLogCategory(), req.getSearchByValue(), nullptr));

    LogAccessTimeRange range = requestedRangeToLARange(req.getRange());
    
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
