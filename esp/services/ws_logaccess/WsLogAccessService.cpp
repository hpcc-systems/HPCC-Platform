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
    if (queryRemoteLogAccessor())
    {
        double clientVer = context.getClientVersion();
        IPropertyTree *logColumnDefinitions = queryRemoteLogAccessor()->queryLogMap();
        if (!logColumnDefinitions)
        {
            success = false;
            OERRLOG("WsLogaccess: NULL col definitions - check logaccess logmap config!");
        }
        else
        {
            if (clientVer >= 1.04)
            {
                IArrayOf<IEspLogColumn> logColumns;
                Owned<IPropertyTreeIterator> columnIter = logColumnDefinitions->getElements("logMaps");
                ForEach(*columnIter)
                {
                    IPropertyTree &column = columnIter->query();
                    if (column.hasProp("@searchColumn"))
                    {
                        Owned<IEspLogColumn> espLogColumn = createLogColumn();

                        const char *csearchColumn = column.queryProp("@searchColumn");
                        const char *cprojectName = column.queryProp("@projectName");
                        espLogColumn->setName(!isEmptyString(cprojectName) ? cprojectName: csearchColumn);

                        if (column.hasProp("@type"))
                            espLogColumn->setLogType(column.queryProp("@type"));

                        if (column.hasProp("@columnMode"))
                            espLogColumn->setColumnMode(column.queryProp("@columnMode"));

                        if (column.hasProp("enumValues"))
                        {
                            Owned<IPropertyTreeIterator> enumValues = column.getElements("enumValues");
                            StringArray enumValuesArr;
                            ForEach(*enumValues)
                            {
                                IPropertyTree &enumVal = enumValues->query();
                                enumValuesArr.append(enumVal.queryProp("@code"));
                            }
                            espLogColumn->setEnumeratedValues(enumValuesArr);
                        }
                        if (column.hasProp("@columnType"))
                        {
                            try
                            {
                                espLogColumn->setColumnType(column.queryProp("@columnType"));
                            }
                            catch (IException *e)
                            {
                                VStringBuffer msg("Invalid col type found in logaccess logmap config for '%s'", csearchColumn);
                                EXCLOG(e, msg.str());
                                e->Release();
                            }
                            catch(...)
                            {
                                WARNLOG("Invalid col type found in logaccess logmap config");
                            }
                        }
                        else
                        {
                            espLogColumn->setColumnType("string");
                        }
                        logColumns.append(*espLogColumn.getClear());
                    }
                    else
                    {
                        DBGLOG("Encountered Column definition without NAME!");
                        OERRLOG("WsLogaccess: col definitions without @searchColumn - check logaccess logmap config!");
                    }
                }
                resp.setColumns(logColumns);
            }
        }

        if (clientVer >= 1.05)
        {
            resp.setSupportsResultPaging(queryRemoteLogAccessor()->supportsResultPaging());
        }

        resp.setRemoteLogManagerType(queryRemoteLogAccessor()->getRemoteLogAccessType());
        resp.setRemoteLogManagerConnectionString(queryRemoteLogAccessor()->fetchConnectionStr());
    }
    else
    {
        success = false;
        OERRLOG("WsLogaccess: No logAccess available - check logaccess config!");
    }

    return success;
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
        case CLogAccessType_ByJobID:
            return getJobIDLogAccessFilter(searchByValue);
        case CLogAccessType_ByComponent:
            return getComponentLogAccessFilter(searchByValue);
        case CLogAccessType_ByPod:
            return getPodLogAccessFilter(searchByValue);
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
        }
        case CLogAccessType_ByTraceID:
        {
            return getTraceIDLogAccessFilter(searchByValue);
        }
        case CLogAccessType_BySpanID:
        {
            return getSpanIDLogAccessFilter(searchByValue);
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
    if (!queryRemoteLogAccessor())
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

    if (version >= 1.03)
    {
        IArrayOf<IConstSortCondition> & sortby = req.getSortBy();
        for (int index = 0; index < sortby.length(); index++)
        {
            IConstSortCondition& condition = sortby.item(index);
            CSortDirection espdirection = condition.getDirection();
            SortByDirection direction = SORTBY_DIRECTION_none;
            if (espdirection == CSortDirection_ASC)
                direction = SORTBY_DIRECTION_ascending;
            else if (espdirection == CSortDirection_DSC)
                direction = SORTBY_DIRECTION_descending;

            if (condition.getBySortType() <= -1) //only known sortby types processed
                throw makeStringExceptionV(-1, "WsLogAccess: Unknown SortType encountered!");

            if (condition.getBySortType() != CSortColumType_ByFieldName && !isEmptyString(condition.getColumnName()))
                throw makeStringExceptionV(-1, "WsLogAccess: SortBy ColumnName not allowed unless coupled with ByFieldName BySortType!");

            LogAccessMappedField mappedField = LOGACCESS_MAPPEDFIELD_unmapped;
            switch (condition.getBySortType())
            {
            case CSortColumType_ByDate:
                mappedField = LOGACCESS_MAPPEDFIELD_timestamp;
                break;
            case CSortColumType_ByJobID:
                mappedField = LOGACCESS_MAPPEDFIELD_jobid;
                break;
            case CSortColumType_ByComponent:
                mappedField = LOGACCESS_MAPPEDFIELD_component;
                break;
            case CSortColumType_ByLogType:
                mappedField = LOGACCESS_MAPPEDFIELD_class;
                break;
            case CSortColumType_ByTargetAudience:
                mappedField = LOGACCESS_MAPPEDFIELD_audience;
                break;
            case CSortColumType_BySourceInstance:
                mappedField = LOGACCESS_MAPPEDFIELD_instance;
                break;
            case CSortColumType_BySourceNode:
                mappedField = LOGACCESS_MAPPEDFIELD_host;
                break;
            case CSortColumType_ByTraceID:
                mappedField = LOGACCESS_MAPPEDFIELD_traceid;
                break;
            case CSortColumType_BySpanID:
                mappedField = LOGACCESS_MAPPEDFIELD_spanid;
                break;
            case CSortColumType_ByFieldName:
                if (isEmptyString(condition.getColumnName()))
                    throw makeStringExceptionV(-1, "WsLogAccess: SortByFieldName option requires ColumnName!");
                break;
            default:
                throw makeStringExceptionV(-1, "WsLogAccess: Unknown SortType encountered!");
            }

            logFetchOptions.addSortByCondition(mappedField, condition.getColumnName(), direction);
        }
    }
    StringBuffer logcontent;
    LogQueryResultDetails LogQueryResultDetails;
    queryRemoteLogAccessor()->fetchLog(LogQueryResultDetails, logFetchOptions, logcontent, logAccessFormatFromName(req.getFormat()));

    if (version >= 1.02)
    {
        resp.setLogLineCount(LogQueryResultDetails.totalReceived);
        resp.setTotalLogLinesAvailable(LogQueryResultDetails.totalAvailable);
    }

    resp.setLogLines(logcontent.str());

    return true;
}
