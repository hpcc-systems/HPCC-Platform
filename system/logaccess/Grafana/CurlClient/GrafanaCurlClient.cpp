/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#include "GrafanaCurlClient.hpp"

#include "platform.h"
#include <curl/curl.h>
#include <string>
#include <vector>

#include <cstdio>
#include <iostream>
#include <stdexcept>

#ifdef _CONTAINERIZED
//In containerized world, most likely localhost is not the target grafana host
static constexpr const char * DEFAULT_GRAFANA_HOST = "mycluster-grafana.default.svc.cluster.local";
#else
//In baremetal, localhost is good guess as any
static constexpr const char * DEFAULT_GRAFANA_HOST = "localhost";
#endif

static constexpr const char * DEFAULT_GRAFANA_PROTOCOL = "http";
static constexpr const char * DEFAULT_GRAFANA_PORT = "3000";
static constexpr const char * DEFAULT_DATASOURCE_ID = "1";

static constexpr const char * defaultNamespaceStream        = "default";
static constexpr const char * defaultExpectedLogFormat      = "table"; //"json";

static constexpr const char * logMapIndexPatternAtt =   "@storeName";
static constexpr const char * logMapSearchColAtt =      "@searchColumn";
static constexpr const char * logMapTimeStampColAtt =   "@timeStampColumn";
static constexpr const char * logMapKeyColAtt =         "@keyColumn";
static constexpr const char * logMapDisableJoinsAtt =   "@disableJoins";

static constexpr std::size_t  defaultMaxRecordsPerFetch = 100;

/*
* To be used as a callback for curl_easy_setopt to capture the response from a curl request
*/
size_t stringCallback(char *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

/*
* Constructs a curl based client request based on the provided connection string and targetURI
* The response is reported in the readBuffer
* Uses stringCallback to handle successfull curl requests
*/
void GrafanaLogAccessCurlClient::submitQuery(std::string & readBuffer, const char * targetURI)
{
    if (isEmptyString(m_grafanaConnectionStr.str()))
        throw makeStringExceptionV(-1, "%s Cannot submit query, empty connection string detected!", COMPONENT_NAME);

    if (isEmptyString(targetURI))
        throw makeStringExceptionV(-1, "%s Cannot submit query, empty request URI detected!", COMPONENT_NAME);

    OwnedPtrCustomFree<CURL, curl_easy_cleanup> curlHandle = curl_easy_init();
    if (curlHandle)
    {
        CURLcode                curlResponseCode;
        OwnedPtrCustomFree<curl_slist, curl_slist_free_all> headers = nullptr;
        char                    curlErrBuffer[CURL_ERROR_SIZE];
        curlErrBuffer[0] = '\0';

        VStringBuffer requestURL("%s%s%s", m_grafanaConnectionStr.str(), m_dataSourcesAPIURI.str(), targetURI);

        if (curl_easy_setopt(curlHandle, CURLOPT_URL, requestURL.str()) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_URL' (%s)!", COMPONENT_NAME, requestURL.str());
    
        int curloptretcode = curl_easy_setopt(curlHandle, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        if (curloptretcode != CURLE_OK)
        {
            if (curloptretcode == CURLE_UNKNOWN_OPTION)
                throw makeStringExceptionV(-1, "%s: Log query request: UNKNONW option 'CURLOPT_HTTPAUTH'!", COMPONENT_NAME);
            if (curloptretcode == CURLE_NOT_BUILT_IN)
                throw makeStringExceptionV(-1, "%s: Log query request: bitmask specified not built-in! 'CURLOPT_HTTPAUTH'/'CURLAUTH_BASIC'!", COMPONENT_NAME);

            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_HTTPAUTH':'CURLAUTH_BASIC'!", COMPONENT_NAME);
        }

        //allow annonymous connections??
        if (isEmptyString(m_grafanaUserName.str()))
            throw makeStringExceptionV(-1, "%s: Log query request: Empty user name detected!", COMPONENT_NAME);

        //allow non-secure connections??
        if (isEmptyString(m_grafanaPassword.str()))
            throw makeStringExceptionV(-1, "%s: Log query request: Empty password detected!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_USERNAME, m_grafanaUserName.str()))
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set  'CURLOPT_USERNAME' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_PASSWORD, m_grafanaPassword.str()))
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set  'CURLOPT_PASSWORD' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_POST, 0) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not disable 'CURLOPT_POST' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_HTTPGET, 1) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_HTTPGET' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_NOPROGRESS, 1) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_NOPROGRESS' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, stringCallback) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_WRITEFUNCTION' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, &readBuffer) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_WRITEDATA' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_USERAGENT, "HPCC Systems LogAccess client") != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_USERAGENT' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_ERRORBUFFER, curlErrBuffer) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Log query request: Could not set 'CURLOPT_ERRORBUFFER' option!", COMPONENT_NAME);

        //If we set CURLOPT_FAILONERROR, we'll miss the actual error message returned in the response
        //(curl_easy_setopt(curlHandle, CURLOPT_FAILONERROR, 1L) != CURLE_OK) // non HTTP Success treated as error

        try
        {
            curlResponseCode = curl_easy_perform(curlHandle);
        }
        catch (...)
        {
            throw makeStringExceptionV(-1, "%s LogQL request: Unknown libcurl error", COMPONENT_NAME);
        }

        long response_code;
        curl_easy_getinfo(curlHandle, CURLINFO_RESPONSE_CODE, &response_code);

        if (curlResponseCode != CURLE_OK || response_code != 200)
        {
            throw makeStringExceptionV(-1,"%s Error (%d): '%s'", COMPONENT_NAME, curlResponseCode, (readBuffer.length() != 0 ? readBuffer.c_str() : curlErrBuffer[0] ? curlErrBuffer : "Unknown Error"));
        }
        else if (readBuffer.length() == 0)
            throw makeStringExceptionV(-1, "%s LogQL request: Empty response!", COMPONENT_NAME);
    }
}

/*
 * This method consumes a JSON formatted data source response from a successful Grafana Loki query
 * It extracts the data source information and populates the m_targetDataSource structure and constructs
 * the URI to access the Loki API
 * 
 * If this operation fails, an exception is thrown
 */
void GrafanaLogAccessCurlClient::processDatasourceJsonResp(const std::string & retrievedDocument)
{
    Owned<IPropertyTree> tree = createPTreeFromJSONString(retrievedDocument.c_str());
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse data source query response!", COMPONENT_NAME);

    if (tree->hasProp("uid"))
        m_targetDataSource.uid.set(tree->queryProp("uid"));
    if (tree->hasProp("name"))
        m_targetDataSource.name.set(tree->queryProp("name"));
    if (tree->hasProp("type"))
        m_targetDataSource.type.set(tree->queryProp("type"));
    if (tree->hasProp("id"))
        m_targetDataSource.id.set(tree->queryProp("id"));

    //Other elements that could be extracted from the data source response:
    //basicAuthPassword, version, basicAuthUser, access=proxy, isDefault, withCredentials, readOnly, database
    //url=http://myloki4hpcclogs:3100, secureJsonFields, user, password, basicAuth, jsonData, typeLogoUrl

    if (isEmptyString(m_targetDataSource.id.get()))
        throw makeStringExceptionV(-1, "%s: DataSource query response does not include 'id'", COMPONENT_NAME);
    if (isEmptyString(m_targetDataSource.type.get()))
        throw makeStringExceptionV(-1, "%s: DataSource query response does not include 'type'", COMPONENT_NAME);

    //This URI is used to access the Loki API, if not properly populated, nothing will work!
    m_dataSourcesAPIURI.setf("/api/datasources/proxy/%s/%s/api/v1" , m_targetDataSource.id.get(), m_targetDataSource.type.get());
}

/*
 * This method consumes a logLine represented as a set of field names and values
 * The LogLine is wrapped in the requested output format
 */
void formatResultLine(StringBuffer & returnbuf, const IProperties * resultLine, LogAccessLogFormat format, bool & isFirstLine)
{
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            returnbuf.append("<line>");
            Owned<IPropertyIterator> fieldsIter = resultLine->getIterator();
            ForEach(*fieldsIter)
            {
                const char * prop = fieldsIter->queryPropValue();
                returnbuf.appendf("<%s>", fieldsIter->getPropKey());
                encodeXML(prop, returnbuf);
                returnbuf.appendf("</%s>", fieldsIter->getPropKey());
            }
            returnbuf.appendf("</line>");
            isFirstLine = false;
            break;
        }
        case LOGACCESS_LOGFORMAT_json:
        {
            if (!isFirstLine)
                returnbuf.append(", ");

            returnbuf.appendf("{\"fields\": [ ");
            bool firstField = true;
            Owned<IPropertyIterator> fieldsIter = resultLine->getIterator();
            ForEach(*fieldsIter)
            {
                if (!firstField)
                    returnbuf.append(", ");
                else
                    firstField = false;

                const char * prop = fieldsIter->queryPropValue();
                returnbuf.appendf("{\"%s\":\"", fieldsIter->getPropKey());
                encodeJSON(returnbuf,prop);
                returnbuf.append("\"}");
            }
            returnbuf.append(" ]}");

            isFirstLine = false;
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
        {
            bool firstField = true;
            Owned<IPropertyIterator> fieldsIter = resultLine->getIterator();
            ForEach(*fieldsIter)
            {
                if (!firstField)
                    returnbuf.append(", ");
                else
                    firstField = false;

                const char * fieldValue = resultLine->queryProp(fieldsIter->getPropKey());
                encodeCSVColumn(returnbuf, fieldValue);
            }

            returnbuf.newline();
            isFirstLine = false;
            break;
        }
        default:
            break;
    }
}

/*
 * This method consumes an Iterator of values elements from a successful Grafana Loki query
 * It extracts the appropriate "stream" values based on return column mode, and the values' 1st and 2nd children
 * which represent timestamp in ns, and the log line, and formats into the requested format
 */
void GrafanaLogAccessCurlClient::processValues(StringBuffer & returnbuf, IPropertyTreeIterator * valuesIter, IPropertyTree * stream, LogAccessLogFormat format, const LogAccessReturnColsMode retcolmode, bool & isFirstLine)
{
    if (!valuesIter)
        return;

    Owned<IProperties> fieldValues = createProperties(true);

    //extract the requested fields from the stream if it's available
    if (stream)
    {
        switch (retcolmode)
        {
        case RETURNCOLS_MODE_all:
        {
            fieldValues->setProp(m_nodeColumn.name, stream->queryProp(m_nodeColumn.name));
            fieldValues->setProp(m_containerColumn.name, stream->queryProp(m_containerColumn.name));
            fieldValues->setProp(m_instanceColumn.name, stream->queryProp(m_instanceColumn.name));
            [[fallthrough]];
        }
        case RETURNCOLS_MODE_default:
        {
            fieldValues->setProp(m_podColumn.name, stream->queryProp(m_podColumn.name));
            [[fallthrough]];
        }
        case RETURNCOLS_MODE_min:
        {
            fieldValues->setProp(m_logDateTimstampColumn.name, stream->queryProp(m_logDateTimstampColumn.name));
            fieldValues->setProp(m_messageColumn.name, stream->queryProp(m_messageColumn.name));
            break;
        }
        case RETURNCOLS_MODE_custom: //not supported yet
        default:
            break;
        }
    }

    ForEach(*valuesIter)
    {
        IPropertyTree & values = valuesIter->query();
        int numofvalues = values.getCount("values");
        if (values.getCount("values") == 2)
        {
            fieldValues->setProp(m_logDateTimstampColumn.name, values.queryProp("values[1]"));
            fieldValues->setProp(m_messageColumn.name, values.queryProp("values[2]"));
            formatResultLine(returnbuf, fieldValues, format, isFirstLine);
        }
        else
        {
            throw makeStringExceptionV(-1, "%s: Detected unexpected Grafana/Loki values response format!: %s", COMPONENT_NAME, values.queryProp("."));
        }
    }
}

/*
 * This starts the encapsulation of the logaccess response in the desired format
 */
inline void resultsWrapStart(StringBuffer & returnbuf, LogAccessLogFormat format, bool reportHeader)
{
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            returnbuf.append("<lines>");
            break;
        }
        case LOGACCESS_LOGFORMAT_json:
        {
            returnbuf.append("{\"lines\": [");
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
        default:
            break;
    }
}

/*
 * This finishes the encapsulation of the logaccess response in the desired format
 */
inline void resultsWrapEnd(StringBuffer & returnbuf, LogAccessLogFormat format)
{
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            returnbuf.append("</lines>");
            break;
        }
        case LOGACCESS_LOGFORMAT_json:
        {
            returnbuf.append("]}");
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
            break;
        default:
            break;
    }
}

/*
 * This method consumes the JSON response from a Grafana Loki query
 * It attempts to unwrap the response and extract the log payload, and reports it in the desired format
 */
void GrafanaLogAccessCurlClient::processQueryJsonResp(LogQueryResultDetails & resultDetails, const std::string & retrievedDocument, StringBuffer & returnbuf, const LogAccessLogFormat format, const LogAccessReturnColsMode retcolmode, bool reportHeader)
{
    Owned<IPropertyTree> tree = createPTreeFromJSONString(retrievedDocument.c_str());
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse log query response", COMPONENT_NAME);

    if (!tree->hasProp("data"))
        throw makeStringExceptionV(-1, "%s: Query respose did not contain data element!", COMPONENT_NAME);

    IPropertyTree * data = tree->queryPropTree("data");
    if (!data)
        throw makeStringExceptionV(-1, "%s: Could no parse data element!", COMPONENT_NAME);

    //process stats first, in case reported entries returned can help preallocate return buffer?
    if (data->hasProp("stats"))
    {
        if (data->hasProp("stats/summary/totalEntriesReturned"))
        {
            resultDetails.totalReceived = data->getPropInt64("stats/summary/totalEntriesReturned");
        }
    }
    //should any of these query stats be reported?
    /*"stats": {"summary": { "bytesProcessedPerSecond": 7187731, "linesProcessedPerSecond": 14201,
                "totalBytesProcessed": 49601, "totalLinesProcessed": 98, "execTime": 0.006900786, "queueTime": 0.000045301,
                "subqueries": 1, "totalEntriesReturned": 98},
      "querier": { "store": { "totalChunksRef": 1, "totalChunksDownloaded": 1,
                    "chunksDownloadTime": 916811, "chunk": {"headChunkBytes": 0,
                        "headChunkLines": 0, "decompressedBytes": 49601,
                        "decompressedLines": 98, "compressedBytes": 6571,"totalDuplicates": 0 }}},
      "ingester": {"totalReached": 0, "totalChunksMatched": 0, "totalBatches": 0, "totalLinesSent": 0,
                "store": {"totalChunksRef": 0, "totalChunksDownloaded": 0, "chunksDownloadTime": 0,
                    "chunk": {"headChunkBytes": 0,"headChunkLines": 0,"decompressedBytes": 0,
                        "decompressedLines": 0,"compressedBytes": 0, "totalDuplicates": 0 }}}*/

    if (data->hasProp("result")) //if no data, empty query rep
    {
        returnbuf.ensureCapacity(retrievedDocument.length());// this is difficult to predict, at least the size of the response?
        //Adds the format prefix to the return buffer
        resultsWrapStart(returnbuf, format, reportHeader);

        bool isFirstLine = true;
        Owned<IPropertyTreeIterator> resultIter = data->getElements("result");
        //many result elements can be returned, each with a unique set of labels and a common set of stream values
        ForEach(*resultIter)
        {
            IPropertyTree & result = resultIter->query();
            Owned<IPropertyTreeIterator> logLineIter;

            if (result.hasProp("values"))
            {
                logLineIter.setown(result.getElements("values")); // if no values elements found, will get NullPTreeIterator
                processValues(returnbuf, logLineIter, result.queryPropTree("stream"), format, retcolmode, isFirstLine);
            }
        }

        //Adds the format postfix to the return buffer
        resultsWrapEnd(returnbuf, format);
    }
}

/*
 * This method constructs a query string for Grafana to provide all info for a given data source
 * The method attemps to populate the m_targetDataSource structure with the data source information
 */
void GrafanaLogAccessCurlClient::fetchDatasourceByName(const char * targetDataSourceName)
{
    DBGLOG("%s: Fetching data source by name: '%s'", COMPONENT_NAME, targetDataSourceName);
    if (isEmptyString(targetDataSourceName))
        throw makeStringExceptionV(-1, "%s: fetchDatasourceByName: Empty data source name!", COMPONENT_NAME);

    std::string readBuffer;
    VStringBuffer targetURI("/api/datasources/name/%s", targetDataSourceName);
    submitQuery(readBuffer, targetURI.str());
    processDatasourceJsonResp(readBuffer);
}

/*
* sumbits a Grafana Loki query to fetch all available datasources
* The response is expected to be a JSON formatted list of datasources
*/
void GrafanaLogAccessCurlClient::fetchDatasources(std::string & readBuffer)
{
    submitQuery(readBuffer, "/");
}

/*
* sumbits a Grafana Loki query to fetch all labels
* The response is expected to be a JSON formatted list of labels
*/
void GrafanaLogAccessCurlClient::fetchLabels(std::string & readBuffer)
{
    submitQuery(readBuffer, "/label");
}

/*
 * Creates query filter and stream selector strings for the LogQL query based on the filter options provided
*/
void GrafanaLogAccessCurlClient::populateQueryFilterAndStreamSelector(StringBuffer & queryString, StringBuffer & streamSelector, const ILogAccessFilter * filter)
{
    if (filter == nullptr)
        throw makeStringExceptionV(-1, "%s: Null filter detected while creating LogQL query string", COMPONENT_NAME);

    const char * queryOperator = " |~ ";
    StringBuffer queryValue;
    StringBuffer streamField;
    StringBuffer queryField;

    filter->toString(queryValue);
    switch (filter->filterType())
    {
    case LOGACCESS_FILTER_jobid:
    {
        DBGLOG("%s: Searching log entries by jobid: '%s'...", COMPONENT_NAME, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_class:
    {
        DBGLOG("%s: Searching log entries by class: '%s'...", COMPONENT_NAME, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_audience:
    {
        DBGLOG("%s: Searching log entries by target audience: '%s'...", COMPONENT_NAME, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_component:
    {
        if (m_componentsColumn.isStream)
            streamField = m_componentsColumn.name;

        DBGLOG("%s: Searching '%s' component log entries...", COMPONENT_NAME, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_instance:
    {
        if (m_instanceColumn.isStream)
            streamField = m_instanceColumn.name;

        DBGLOG("%s: Searching log entries by HPCC component instance: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_wildcard:
    {
        DBGLOG("%s: Searching log entries by wildcard filter: '%s %s %s'...", COMPONENT_NAME, queryField.str(), queryOperator, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_or:
    case LOGACCESS_FILTER_and:
    {
        StringBuffer op(logAccessFilterTypeToString(filter->filterType()));
        queryString.append(" ( ");
        populateQueryFilterAndStreamSelector(queryString, streamSelector, filter->leftFilterClause());
        queryString.append(" ");
        queryString.append(op.toLowerCase()); //LogQL or | and
        queryString.append(" ");
        populateQueryFilterAndStreamSelector(queryString, streamSelector, filter->rightFilterClause());
        queryString.append(" ) ");
        return; // queryString populated, need to break out
    }
    case LOGACCESS_FILTER_pod:
    {
        if (m_podColumn.isStream)
            streamField = m_podColumn.name;

        DBGLOG("%s: Searching log entries by Pod: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_column:
    {
        if (filter->getFieldName() == nullptr)
            throw makeStringExceptionV(-1, "%s: empty field name detected in filter by column!", COMPONENT_NAME);
        break;
    }
    //case LOGACCESS_FILTER_trace:
    //case LOGACCESS_FILTER_span:
    default:
        throw makeStringExceptionV(-1, "%s: Unknown query criteria type encountered: '%s'", COMPONENT_NAME, queryValue.str());
    }

    //We're constructing two clauses, the stream selector and the query filter
    //the streamSelector is a comma separated list of key value pairs
    if (!streamField.isEmpty())
    {
        if (!streamSelector.isEmpty())
            streamSelector.append(", ");

        streamSelector.appendf(" %s=\"%s\" ", streamField.str(), queryValue.str());
    }
    else
    {
        //the query filter is a sequence of expressions seperated by a logical operator
        queryString.append(" ").append(queryField.str()).append(queryOperator);
        if (strcmp(m_expectedLogFormat, "table")==0)
            queryString.append(" \"").append(queryValue.str()).append("\" ");
        else
            queryString.append("\"").append(queryValue.str()).append("\"");
    }
}

/*
Translates LogAccess defined SortBy direction enum value to 
the LogQL/Loki counterpart
*/
const char * sortByDirection(SortByDirection direction)
{
    switch (direction)
    {
    case SORTBY_DIRECTION_ascending:
        return "FORWARD";
    case SORTBY_DIRECTION_descending:
    case SORTBY_DIRECTION_none:
    default:
        return "BACKWARD";
    }
}

/*
* Constructs LogQL query based on filter options, and sets Loki specific query parameters,
 submits query, processes responce and returns the log entries in the desired format
*/
bool GrafanaLogAccessCurlClient::fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    if (m_dataSourcesAPIURI.isEmpty())
        throw makeStringExceptionV(-1, "%s: Cannot query because Grafana datasource was not established, check logaccess configuration!", COMPONENT_NAME);

    try
    {
        resultDetails.totalReceived = 0;
        resultDetails.totalAvailable = 0;

        const LogAccessTimeRange & trange = options.getTimeRange();
        if (trange.getStartt().isNull())
            throw makeStringExceptionV(-1, "%s: start time must be provided!", COMPONENT_NAME);

        StringBuffer fullQuery;
        fullQuery.set("/query_range?");

        if (options.getSortByConditions().length() > 0)
        {
            if (options.getSortByConditions().length() > 1)
                UWARNLOG("%s: LogQL sorting is only supported by one field!", COMPONENT_NAME);

            SortByCondition condition = options.getSortByConditions().item(0);
            switch (condition.byKnownField)
            {
            case LOGACCESS_MAPPEDFIELD_timestamp:
                break;
            case LOGACCESS_MAPPEDFIELD_jobid:
            case LOGACCESS_MAPPEDFIELD_component:
            case LOGACCESS_MAPPEDFIELD_class:
            case LOGACCESS_MAPPEDFIELD_audience:
            case LOGACCESS_MAPPEDFIELD_instance:
            case LOGACCESS_MAPPEDFIELD_host:
            case LOGACCESS_MAPPEDFIELD_unmapped:
            default:
                throw makeStringExceptionV(-1, "%s: LogQL sorting is only supported by ingest timestamp!", COMPONENT_NAME);
            }

            const char * direction = sortByDirection(condition.direction);
            if (!isEmptyString(direction))
                fullQuery.appendf("direction=%s", direction);
        }

        fullQuery.append("&limit=").append(std::to_string(options.getLimit()).c_str());
        fullQuery.append("&query=");
        //At this point the log field appears as a detected field and is not formated
        //    Detected fields
        //if output is json:
        //    log	"{ \"MSG\": \"QueryFilesInUse.unsubscribe() called\", \"MID\": \"104\", \"AUD\": \"USR\", \"CLS\": \"PRO\", \"DATE\": \"2024-06-06\", \"TIME\": \"22:03:00.229\", \"PID\": \"8\", \"TID\": \"8\", \"JOBID\": \"UNK\" }\n"
        //if output is table:
        //    log	"00000174 USR PRO 2024-06-19 19:20:58.089     8   160 UNK     \"WUUpdate: W20240619-192058\"\n"
        //    stream	"stderr"
        //    time	"2024-06-06T22:03:00.230759942Z"
        //    ts	2024-06-06T22:03:00.382Z
        //    tsNs	1717711380382410602

        StringBuffer logLineParser;
        //from https://grafana.com/docs/loki/latest/query/log_queries/
        //Adding | json to your pipeline will extract all json properties as labels if the log line is a valid json document. Nested properties are flattened into label keys using the _ separator.
        logLineParser.set(" | json log"); //this parses the log entry and extracts the log field into a label
        logLineParser.append(" | line_format \"{{.log | trim}}\""); //Formats output line to only contain log label
                                                             //This drops the stream, and various insert timestamps

        //we're always going to get a stream container, and a the log line...
        //the stream container contains unnecessary, and redundant lines
        //there's documentation of a 'drop' command whch doesn't work in practice
        //online recomendation is to clear those stream entries...
        logLineParser.append(" | label_format log=\"\", filename=\"\"");//, namespace=\"\", job=\"\""// app=\"\", component=\"\", container=\"\", instance=\"\");

        /* we're not going to attempt to parse the log line for now,
           return the entire log line in raw format
        if (strcmp(m_expectedLogFormat.get(), "json") == 0)
        {
            logLineParser.append( " | json ");
            //at this point, the stream "log" looks like this:
            //	{ "MSG": "ESP server started.", "MID": "89", "AUD": "PRG", "CLS": "INF", "DATE": "2024-06-19", "TIME": "14:56:36.648", "PID": "8", "TID": "8", "JOBID": "UNK" }
            //no need to format "log" into json
            logLineParser.append(" | line_format \"{{.log}}\"");
        }
        else
        {
            //parses log into individual fields as labels
            logLineParser.append(" | pattern \"<MID> <AUD> <cls> <hpccdate> <hpcctime>     <pid>     <tid> <jobid> <MSG>\"");
            //the "pattern" parser is not reliable, sensitive to number of spaces, and the order of the fields

            //do we want to manually format the return format at the server?
            logLineParser.append(" | line_format \"{ \\\"MID\\\":\\\"{{.MID}}\\\", \\\"AUD\\\":\\\"{{.AUD}}\\\", \\\"MSG\\\":\\\"{{.MSG}}\\\" }\"");
        }
        */

        //if we parse the logline as above, We could control the individual fields returned
        //HPCC_LOG_TYPE="CLS", HPCC_LOG_MESSAGE="MSG", HPCC_LOG_JOBID="JOBID" | HPCC_LOG_JOBID="UNK"

        //"All LogQL queries contain a log stream selector." - https://grafana.com/docs/loki/latest/query/log_queries/
        StringBuffer streamSelector;
        StringBuffer queryFilter;
        populateQueryFilterAndStreamSelector(queryFilter, streamSelector, options.queryFilter());
        if (!streamSelector.isEmpty())
            streamSelector.append(", ");

        streamSelector.appendf("namespace=\"%s\"", m_targetNamespace.get());

        fullQuery.append("{");
        encodeURL(fullQuery, streamSelector.str());
        fullQuery.append("}");
        encodeURL(fullQuery, queryFilter.str());
        encodeURL(fullQuery, logLineParser.str());

        fullQuery.appendf("&start=%s000000000", std::to_string(trange.getStartt().getSimple()).c_str());
        if (!trange.getEndt().isNull()) //aka 'to' has been initialized
        {
            fullQuery.appendf("&end=%s000000000", std::to_string(trange.getEndt().getSimple()).c_str());
        }

        DBGLOG("FetchLog query: %s", fullQuery.str());

        std::string readBuffer;
        submitQuery(readBuffer, fullQuery.str());

        processQueryJsonResp(resultDetails, readBuffer, returnbuf, format, options.getReturnColsMode(), true);
    }
    catch(IException * e)
    {
        StringBuffer description;
        IERRLOG("%s: query exception: (%d) - %s", COMPONENT_NAME, e->errorCode(), e->errorMessage(description).str());
        e->Release();
    }
    return false;
}

void processLogMapConfig(const IPropertyTree * logMapConfig, LogField * targetField)
{
    if (!logMapConfig || !targetField)
        return;

    if (logMapConfig->hasProp(logMapIndexPatternAtt))
        if (strcmp(logMapConfig->queryProp(logMapIndexPatternAtt), "stream")==0)
            targetField->isStream = true;

    if (logMapConfig->hasProp(logMapSearchColAtt))
        targetField->name = logMapConfig->queryProp(logMapSearchColAtt);
}

GrafanaLogAccessCurlClient::GrafanaLogAccessCurlClient(IPropertyTree & logAccessPluginConfig)
{
    m_pluginCfg.set(&logAccessPluginConfig);

    const char * protocol = logAccessPluginConfig.queryProp("connection/@protocol");
    const char * host = logAccessPluginConfig.queryProp("connection/@host");
    const char * port = logAccessPluginConfig.queryProp("connection/@port");

    m_grafanaConnectionStr = isEmptyString(protocol) ? DEFAULT_GRAFANA_PROTOCOL : protocol;
    m_grafanaConnectionStr.append("://");
    m_grafanaConnectionStr.append(isEmptyString(host) ? DEFAULT_GRAFANA_HOST : host);
    m_grafanaConnectionStr.append(":").append((!port || !*port) ? DEFAULT_GRAFANA_PORT : port);

    m_targetDataSource.id.set(logAccessPluginConfig.hasProp("datasource/@id") ? logAccessPluginConfig.queryProp("datasource/@id") : DEFAULT_DATASOURCE_ID);
    m_targetDataSource.name.set(logAccessPluginConfig.hasProp("datasource/@name") ?  logAccessPluginConfig.queryProp("datasource/@name") : DEFAULT_DATASOURCE_NAME);

    if (logAccessPluginConfig.hasProp("namespace/@name"))
    {
        m_targetNamespace.set(logAccessPluginConfig.queryProp("namespace/@name"));
    }

    if (isEmptyString(m_targetNamespace.get()))
    {
        m_targetNamespace.set(defaultNamespaceStream);
        OWARNLOG("%s: No namespace specified! Loki logaccess should target non-default namespaced logs!!!", COMPONENT_NAME);
    }

    Owned<const IPropertyTree> secretTree = getSecret("esp", "grafana-logaccess");
    if (secretTree)
    {
        DBGLOG("Grafana LogAccess: loading esp/grafana-logaccess secret");

        getSecretKeyValue(m_grafanaUserName.clear(), secretTree, "username");
        if (isEmptyString(m_grafanaUserName.str()))
            throw makeStringExceptionV(-1, "%s: Empty Grafana user name detected!", COMPONENT_NAME);

        getSecretKeyValue(m_grafanaPassword.clear(), secretTree, "password");
        if (isEmptyString(m_grafanaPassword.str()))
            throw makeStringExceptionV(-1, "%s: Empty Grafana password detected!", COMPONENT_NAME);
    }
    else
    {
        DBGLOG("%s: could not load esp/grafana-logaccess secret", COMPONENT_NAME);
    }

    if (isEmptyString(m_grafanaUserName.str()) || isEmptyString(m_grafanaPassword.str()))
    {
        OWARNLOG("%s: Grafana credentials not found in secret, searching in grafana logaccess configuration", COMPONENT_NAME);

        if (logAccessPluginConfig.hasProp("connection/@username"))
            m_grafanaUserName.set(logAccessPluginConfig.queryProp("connection/@username"));

        if (logAccessPluginConfig.hasProp("connection/@password"))
            m_grafanaPassword.set(logAccessPluginConfig.queryProp("connection/@password"));
    }

    try
    {
        //this is very important, without this, we can't target the correct datasource
        fetchDatasourceByName(m_targetDataSource.name.get());
    }
    catch(IException * e)
    {
        StringBuffer description;
        OERRLOG("%s: Exception fetching Loki/Grafana datasource!!: (%d) - %s", COMPONENT_NAME, e->errorCode(), e->errorMessage(description).str());
        e->Release();
    }

    try
    {
        std::string availableLabels;
        fetchLabels(availableLabels);
        DBGLOG("%s: Available labels on target loki/grafana: %s", COMPONENT_NAME, availableLabels.c_str());
    }
    catch(IException * e)
    {
        StringBuffer description;
        OERRLOG("%s: Exception fetching available labels: (%d) - %s", COMPONENT_NAME, e->errorCode(), e->errorMessage(description).str());
        e->Release();
    }

    m_expectedLogFormat = defaultExpectedLogFormat;
    if (logAccessPluginConfig.hasProp("logFormat/@type"))
    {
        m_expectedLogFormat.set(logAccessPluginConfig.queryProp("logFormat/@type"));
    }

    Owned<IPropertyTreeIterator> logMapIter = m_pluginCfg->getElements("logMaps");
    ForEach(*logMapIter)
    {
        IPropertyTree & logMap = logMapIter->query();
        const char * logMapType = logMap.queryProp("@type");
        if (streq(logMapType, "global"))
            processLogMapConfig(&logMap, &m_globalSearchCol);
        else if (streq(logMapType, "workunits"))
            processLogMapConfig(&logMap, &m_workunitsColumn);
        else if (streq(logMapType, "components"))
            processLogMapConfig(&logMap, &m_componentsColumn);
        else if (streq(logMapType, "class"))
            processLogMapConfig(&logMap, &m_classColumn);
        else if (streq(logMapType, "audience"))
            processLogMapConfig(&logMap, &m_audienceColumn);
        else if (streq(logMapType, "instance"))
            processLogMapConfig(&logMap, &m_instanceColumn);
        else if (streq(logMapType, "node"))
            processLogMapConfig(&logMap, &m_nodeColumn);
        else if (streq(logMapType, "host"))
            OWARNLOG("%s: 'host' LogMap entry is NOT supported!", COMPONENT_NAME);
        else if (streq(logMapType, "pod"))
            processLogMapConfig(&logMap, &m_podColumn);
        else if (streq(logMapType, "message"))
            processLogMapConfig(&logMap, &m_messageColumn);
        else if (streq(logMapType, "timestamp"))
            processLogMapConfig(&logMap, &m_logDateTimstampColumn);
        else
            ERRLOG("Encountered invalid LogAccess field map type: '%s'", logMapType);
    }

    DBGLOG("%s: targeting: '%s' - datasource: '%s'", COMPONENT_NAME, m_grafanaConnectionStr.str(), m_dataSourcesAPIURI.str());
}

class GrafanaLogaccessStream : public CInterfaceOf<IRemoteLogAccessStream>
{
public:
    virtual bool readLogEntries(StringBuffer & record, unsigned & recsRead) override
    {
        DBGLOG("%s: GrafanaLogaccessStream readLogEntries called", COMPONENT_NAME);
        LogQueryResultDetails  resultDetails;
        m_remoteLogAccessor->fetchLog(resultDetails, m_options, record, m_outputFormat);
        recsRead = resultDetails.totalReceived;
        DBGLOG("%s: GrafanaLogaccessStream readLogEntries returned %d records", COMPONENT_NAME, recsRead);

        return false;
    }

    GrafanaLogaccessStream(IRemoteLogAccess * grafanaQueryClient, const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
    {
        DBGLOG("%s: GrafanaLogaccessStream created", COMPONENT_NAME);
        m_remoteLogAccessor.set(grafanaQueryClient);
        m_outputFormat = format;
        m_pageSize = pageSize;
        m_options = options;
    }

private:
    unsigned int m_pageSize;
    bool m_hasBeenScrolled = false;
    LogAccessLogFormat m_outputFormat;
    LogAccessConditions m_options;
    Owned<IRemoteLogAccess> m_remoteLogAccessor;
};

IRemoteLogAccessStream * GrafanaLogAccessCurlClient::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format)
{
    return getLogReader(options, format, defaultMaxRecordsPerFetch);
}

IRemoteLogAccessStream * GrafanaLogAccessCurlClient::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
{
    return new GrafanaLogaccessStream(this, options, format, pageSize);
}

extern "C" IRemoteLogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
    return new GrafanaLogAccessCurlClient(logAccessPluginConfig);
}