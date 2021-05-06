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
    /*
     * enum AccessLogRangeType
{
    ACRTFirstPage = 0,
	ACRTirstNRows = 1,
	ACRTLastNDays = 2,
	ACRTLastNHours = 3,
	ACRTLastNMinutes = 4,
    ACRTLastPage = 5,
    ACRTGoToPage = 6,
    ACRTLastNRows = 7,
	ACRTTimeRange = 8,
};

struct LogAccessTimeFrame
{
	StringBuffer startDate;
	StringBuffer endDate;
	unsigned lastDays;
	unsigned lastHours;
	unsigned lastMinutes;
};

struct LogAccessRange
{
	AccessLogRangeType rangeType;
	LogAccessTimeFrame timeFrame;
	unsigned logViewSize;
	unsigned pageSize;
};
     * enum AccessLogFilterType
{
    ALF_ALLAvailable = 0,
    ALF_Component = 1,
	ALF_WorkUnit = 2,
	ALF_Audience = 3,
    ALF_Class = 4,
    ALF_NativeQuery = 5
};

struct LogAccessFilter
{
	AccessLogFilterType filterType;
	StringBuffer workUnit;
	StringArray targetAudience;
	StringArray messageClass;
	StringBuffer componentName;
};


struct LogAccessConditions
     */

    Owned<IPropertyTree> testTree = createPTreeFromXMLString(
                    "<LogAccess name='localES' type='elasticsearch' libName='libelasticstacklogaccess.so'>"
    		        " <Connection protocol='http' host='localhost'>"
    		        " <!--Connection protocol='http' host='somehost.somewhere' port='1234'-->"
    				"   <Credentials secret='somek8ssecretspath'/>"
    				"   <DefaultIndexName name='someIndexName' />"
            		" </Connection>"
                    " <LogMap>"
    		        "   <Components index='someelasticindex' searchcolumn='kubernetes.container.name' contentcolumn='message'>"
    		        "     <Component name='thor' index='thorindex'/>"
    		        "   </Components>"
    		        "   <Fields>"
    		        "     <Field name='message'/>"
    		        "     <Field name='kubernetes.container.name'/>"
    		        "     <Field name='container.image.name'/>"
    		        "   </Fields>"
    		        "   <Filters>"
    		        "    <Filter primary='true' name='hpcccontainer'>"
    		        "		<Field name='container.image.name'/>"
    		        "   	<Operator value='is'/>"
    		        "      <Value value='hpccplatform-core'/>"
    		        "    </Filter>"
    		        "   </Filters>"
    		        " </LogMap>"
                    "</LogAccess>");

    m_logMap.setown(testTree->getPropTree("LogMap"));
    const char * compindex = m_logMap->queryProp("Components/@index");

    m_logAccess.set(ILogAccessLoader::loadLogAccessPlugin<ILogAccess>(testTree.getClear()));

#ifdef _DEBUG
    StringBuffer thexml;
    toXML(m_serviceCfg, thexml,0,0);
    DBGLOG("^^^^^^%s", thexml.str());
#endif
}

bool Cws_logaccessEx::onGetComponentLog(IEspContext &context, IEspGetComponentLogRequest &req, IEspGetComponentLogResponse &resp)
{
	return true;
}
bool Cws_logaccessEx::onGetComponentLogMeta(IEspContext &context, IEspGetComponentLogMetaRequest &req, IEspGetComponentLogMetaResponse &resp)
{
	const char * compindex = m_logMap->queryProp("Components/@index");
	const char * primarycolumn = m_logMap->queryProp("Components/@searchcolumn");
	const char * contentcolumn = m_logMap->queryProp("Components/@contentcolumn");

	resp.setSearchColumn(primarycolumn);
	resp.setSourceStore(compindex);
	resp.setContentColumn(contentcolumn);
	StringArray columns;

	Owned<IPropertyTreeIterator> iter = m_logMap->getElements("Fields/Field");
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
	const char * compindex = m_logMap->queryProp("Components/@index");
	const char * primarycolumn = m_logMap->queryProp("Components/@searchcolumn");
	const char * contentcolumn = m_logMap->queryProp("Components/@contentcolumn");

	resp.setSearchColumn(primarycolumn);
	resp.setSourceStore(compindex);
	resp.setContentColumn(contentcolumn);
	StringArray columns;

	Owned<IPropertyTreeIterator> iter = m_logMap->getElements("Fields/Field");
	ForEach(*iter)
	{
		IPropertyTree & cur = iter->query();
		columns.append(cur.queryProp("@name"));
	}
	resp.setAvailableColumns(columns);

	return true;
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

bool Cws_logaccessEx::onGetWULog(IEspContext &context, IEspGetWULogRequest &req, IEspGetWULogResponse &resp)
{
	const char * message;
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
		m_logAccess->fetchWULog(logcontent, wuid, range, &cols);
		resp.setLog(logcontent.str());
		resp.setColumns(cols);
		resp.setRange(req.getRange());
	}

	resp.setSuccess(success);
	if (message && *message)
		resp.setMessage(message);

    return success;
}
