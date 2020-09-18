/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "ws_workunits_esp.ipp"
#include "exception_util.hpp"
#include "ws_wuresult.hpp"
#include "ws_workunitsHelpers.hpp"

FlushingWUResultBuffer::~FlushingWUResultBuffer()
{
    try
    {
        if (!outIOS)
        {
            if (!s.isEmpty())
                response->sendChunk(s);
        }
    }
    catch (IException *e)
    {
        // Ignore any socket errors that we get at termination - nothing we can do about them anyway...
        e->Release();
    }
}

void FlushingWUResultBuffer::flushXML(StringBuffer& current, bool closing)
{
    if (outIOS)
    {
        outIOS->write(current.length(), current.str());
        current.clear();
        return;
    }

    s.append(current);
    current.clear();

    unsigned threshold = closing ? 1 : wuResultDownloadFlushThreshold;
    if (s.length() < threshold)
        return;

    response->sendChunk(s);
    s.clear();
}

static const char* wuResultXMLStartStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Result>";
static const char* wuResultXMLEndStr = "</Result>";

bool CWsWuResultOutHelper::getWUResultStreaming(CHttpRequest* request, CHttpResponse* _response, unsigned _flushThreshold)
{
    context = request->queryContext();
    reqParams = request->queryParameters();
    response = _response;
    downloadFlushThreshold = _flushThreshold;

    if (!canStream())
        return false;

    readReq();

#ifdef _USE_ZLIB
    if ((outFormat == WUResultOutZIP) || (outFormat == WUResultOutGZIP))
        createResultIOS();
#endif

    //Based on the code inside CWsWorkunitsEx::onWUResultBin
    if (!wuid.isEmpty() && resultName.get())
    {
        PROGLOG("Call getWsWuResult(): wuid %s, ResultName %s", wuid.str(), resultName.get());
        getWsWuResult(resultName, nullptr, 0);
    }
    else if (!wuid.isEmpty() && (sequence >= 0))
    {
        PROGLOG("Call getWsWuResult(): wuid %s, Sequence %d", wuid.str(), sequence);
        getWsWuResult(nullptr, nullptr, sequence);
    }
    else if (logicalName.get())
    {
        if (!isEmptyString(cluster))
        {
            getFileResults();
        }
        else
        {
            //Find out wuid
            getWuidFromLogicalFileName(*context, logicalName, wuid);
            if (wuid.isEmpty())
                throw makeStringExceptionV(ECLWATCH_CANNOT_GET_WORKUNIT, "Cannot find the workunit for file %s.", logicalName.get());

            //Find out cluster
            SCMStringBuffer wuCluster;
            getWorkunitCluster(*context,  wuid, wuCluster, true);
            cluster.set(wuCluster.str());

            if (cluster.length() > 0)
            {
                getFileResults();
            }
            else
            {
                PROGLOG("Call getWsWuResult(): wuid %s, logicalName %s", wuid.str(), logicalName.str());
                getWsWuResult(nullptr, logicalName, 0);
            }
        }
    }
    else
        throw makeStringException(ECLWATCH_CANNOT_GET_WU_RESULT, "Cannot get workunit result due to invalid input.");

#ifdef _USE_ZLIB
    if ((outFormat == WUResultOutZIP) || (outFormat == WUResultOutGZIP))
    {
        resultIOS.clear();

        StringBuffer zipFileNameWithPath(resultFileNameWithPath);
        zipFileNameWithPath.append((outFormat == WUResultOutGZIP) ? ".gz" : ".zip");
        zipResultFile(zipFileNameWithPath);

        CWsWuFileHelper helper(nullptr);
        response->setContent(helper.createIOStreamWithFileName(zipFileNameWithPath, IFOread));
        if (outFormat == WUResultOutGZIP)
            response->setContentType("application/x-gzip");
        else
            response->setContentType("application/zip");
        response->send();
        recursiveRemoveDirectory(workingFolder);
    }
#endif

    return true;
}

bool CWsWuResultOutHelper::canStream()
{
    const char* format = reqParams->queryProp("Format");
    if (strieq(format, "raw")) //need to read the whole result for an encryption 
        return false;
    if (strieq(format, "xls")) //need to read the whole result for xslt transform 
        return false;
    return true;
}

void CWsWuResultOutHelper::readReq()
{
    readWUIDReq();
    resultName.set(reqParams->queryProp("ResultName"));
    sequence = reqParams->getPropInt("Sequence");
    logicalName.set(reqParams->queryProp("LogicalName"));
    cluster.set(reqParams->queryProp("Cluster"));

    readOutFormatReq();

    const char* ptr = reqParams->queryProp("Start");
    if (!isEmptyString(ptr))
    {
        start = atol(ptr);
        if (start < 0)
            start = 0;
    }
    count = reqParams->getPropInt("Count");

    readFilterByReq();
}

void CWsWuResultOutHelper::readWUIDReq()
{
    wuid.set(reqParams->queryProp("Wuid"));
    wuid.trim();
    if (wuid.isEmpty())
        return;
    if (!looksLikeAWuid(wuid, 'W'))
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid.str());
    ensureWsWorkunitAccess(*context, wuid, SecAccess_Read);
}

void CWsWuResultOutHelper::readOutFormatReq()
{
    const char* format = reqParams->queryProp("Format");
    if (isEmptyString(format))
        return; //default to WUResultOutXML

    if (strieq(format, "csv"))
    {
        outFormat = WUResultOutCSV;
    }
    else if (strieq(format, "json"))
    {
        outFormat = WUResultOutJSON;
    }
    else if (strieq(format, "zip")) //zip on XML
    {
        outFormat = WUResultOutZIP;
    }
    else if (strieq(format, "gzip")) //gzip on XML
    {
        outFormat = WUResultOutGZIP;
    }

#ifndef _USE_ZLIB
    if ((outFormat = WUResultOutZIP) || (outFormat = WUResultOutGZIP))
        throw makeStringException(ECLWATCH_INVALID_INPUT, "The zip format not supported");
#endif
}

void CWsWuResultOutHelper::readFilterByReq()
{
    Owned<IPropertyIterator> iter = reqParams->getIterator();
    ForEach(*iter)
    {
        const char* keyname = iter->getPropKey();
        const char* keyValue = reqParams->queryProp(iter->getPropKey());
        if (isEmptyString(keyname) || isEmptyString(keyValue) || strncmp(keyname, "FilterBys", 9))
            continue;

        Owned<IEspNamedValue> nv = createNamedValue();
        nv->setName(keyname);
        nv->setValue(keyValue);
        filterBy.append(*nv.getClear());
    }
}

void CWsWuResultOutHelper::createResultIOS()
{
    unsigned currentTime = msTick();
    workingFolder.appendf("%s%sT%xAT%x", TEMPZIPDIR, PATHSEPSTR, (unsigned)(memsize_t)GetCurrentThreadId(), currentTime);
    resultFileNameWithPath.appendf("%s%sWUResult.xml", workingFolder.str(), PATHSEPSTR);
    recursiveCreateDirectoryForFile(resultFileNameWithPath);

    OwnedIFile resultIFile = createIFile(resultFileNameWithPath);
    OwnedIFileIO resultIOW = resultIFile->open(IFOcreaterw);
    if (!resultIOW)
        throw makeStringExceptionV(ECLWATCH_CANNOT_OPEN_FILE, "Failed to open %s.", resultFileNameWithPath.str());
    resultIOS.setown(createIOStream(resultIOW));
}

void CWsWuResultOutHelper::zipResultFile(const char* zipFileNameWithPath)
{
    StringBuffer zipCommand;
    if (outFormat == WUResultOutGZIP)
        zipCommand.setf("tar -czf %s -C %s %s", zipFileNameWithPath, workingFolder.str(), "WUResult.xml");
    else
        zipCommand.setf("zip -j %s %s", zipFileNameWithPath, resultFileNameWithPath.str());

    if (system(zipCommand) != 0)
        throw makeStringException(ECLWATCH_CANNOT_COMPRESS_DATA, "Failed to execute system command 'zip'. Please make sure that zip utility is installed.");
}

void CWsWuResultOutHelper::addCustomerHeader()
{
    if (outFormat == WUResultOutCSV)
    {
        context->setResponseFormat(ESPSerializationCSV);
        context->addCustomerHeader("Content-disposition", "attachment;filename=WUResult.csv");
    }
    else if (outFormat == WUResultOutJSON)
    {
        context->setResponseFormat(ESPSerializationJSON);
        context->addCustomerHeader("Content-disposition", "attachment;filename=WUResult.json");
    }
    else if (outFormat == WUResultOutXML)
    {
        context->setResponseFormat(ESPSerializationXML);
        context->addCustomerHeader("Content-disposition", "attachment;filename=WUResult.xml");
    }
    else
    {
        StringBuffer headerStr("attachment;filename=WUResult.xml");
        if (outFormat == WUResultOutGZIP)
            headerStr.append(".gz");
        else
            headerStr.append(".zip");
        context->addCustomerHeader("Content-disposition", headerStr.str());
    }
}

void CWsWuResultOutHelper::startStreaming()
{
    if (resultIOS)
        resultIOS->write(strlen(wuResultXMLStartStr), wuResultXMLStartStr);
    else
    {
        response->setStatus(HTTP_STATUS_OK);
        response->startSend();
        if (outFormat == WUResultOutXML)
            response->sendChunk(wuResultXMLStartStr);
    }
}

void CWsWuResultOutHelper::finalXMLStreaming()
{
    if (resultIOS)
        resultIOS->write(strlen(wuResultXMLEndStr), wuResultXMLEndStr);
    else
        response->sendFinalChunk(wuResultXMLEndStr);
}

//Based on CWsWorkunitsEx::getWsWuResult
void CWsWuResultOutHelper::getWsWuResult(const char* resultName, const char* logicalName, unsigned sequence)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context->querySecManager(), context->queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    if (!cw)
        throw makeStringExceptionV(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Cannot open workunit %s.", wuid.str());

    Owned<IConstWUResult> result;
    if (notEmpty(resultName))
        result.setown(cw->getResultByName(resultName));
    else if (notEmpty(logicalName))
    {
        Owned<IConstWUResultIterator> it = &cw->getResults();
        ForEach(*it)
        {
            IConstWUResult& r = it->query();
            SCMStringBuffer filename;
            if (strieq(r.getResultLogicalName(filename).str(), logicalName))
            {
                result.setown(LINK(&r));
                break;
            }
        }
    }
    else
        result.setown(cw->getResultBySequence(sequence));
    
    if (!result)
        throw makeStringException(ECLWATCH_CANNOT_GET_WU_RESULT, "Cannot open the workunit result.");

    SCMStringBuffer logicalNameBuf;
    result->getResultLogicalName(logicalNameBuf);

    Owned<INewResultSet> rs;
    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context->querySecManager(),
        context->queryUser(), context->queryUserId(), context->queryPassword());
    if (logicalNameBuf.length())
    {
        rs.setown(resultSetFactory->createNewFileResultSet(logicalNameBuf.str(), cw->queryClusterName())); //MORE is this wrong cluster?
    }
    else
        rs.setown(resultSetFactory->createNewResultSet(result, wuid));

    filterAndAppendResultSet(rs, resultName, result->queryResultXmlns());
}

//Based on CWsWorkunitsEx::getFileResults
void CWsWuResultOutHelper::getFileResults()
{
    PROGLOG("Call getFileResults(): wuid %s, logicalName %s, cluster %s", wuid.str(), logicalName.str(), cluster.str());
    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context->querySecManager(),
        context->queryUser(), context->queryUserId(), context->queryPassword());
    Owned<INewResultSet> result(resultSetFactory->createNewFileResultSet(logicalName, cluster));
    filterAndAppendResultSet(result, "", nullptr);
}

void CWsWuResultOutHelper::filterAndAppendResultSet(INewResultSet* result, const char* resultName, const IProperties* xmlns)
{
    if (!filterBy.length())
        appendResultSetStreaming(result, resultName, xmlns);
    else
    {
        Owned<INewResultSet> filteredResult = createFilteredResultSet(result, &filterBy);
        appendResultSetStreaming(filteredResult, resultName, xmlns);
    }
}

//Similar to the appendResultSet in CWsWorkunitsEx
void CWsWuResultOutHelper::appendResultSetStreaming(INewResultSet* result, const char* resultName, const IProperties* xmlns)
{
    if (!result)
        return; //Should not happen

    addCustomerHeader();//Must be called before the startSend() which may be called on startStreaming().
    startStreaming();
    if (outFormat == WUResultOutCSV)
    {
        Owned<FlushingWUResultBuffer> flusher = new FlushingWUResultBuffer(response, nullptr, downloadFlushThreshold);
        count = getResultCSVStreaming(result, resultName, flusher);
    }
    else if (outFormat == WUResultOutJSON)
    {
        Owned<FlushingWUResultBuffer> flusher = new FlushingWUResultBuffer(response, nullptr, downloadFlushThreshold);
        count = getResultJSONStreaming(result, resultName, "myschema", flusher);
    }
    else
    {
        Owned<FlushingWUResultBuffer> flusher = new FlushingWUResultBuffer(response, resultIOS, downloadFlushThreshold);
        count = getResultXmlStreaming(result, resultName, "myschema", xmlns, flusher);
        finalXMLStreaming();
    }
}

//Similar to the getResultCSV in CWsWorkunitsEx
unsigned CWsWuResultOutHelper::getResultCSVStreaming(INewResultSet* result, const char* resultName, IXmlStreamFlusher* flusher)
{
    CSVOptions csvOptions;
    csvOptions.delimiter.set(",");
    csvOptions.terminator.set("\n");
    csvOptions.includeHeader = true;

    Owned<CommonCSVWriter> writer = new CommonCSVWriter(XWFtrim, csvOptions, flusher);
    const IResultSetMetaData & meta = result->getMetaData();
    unsigned headerLayer = 0;
    getCSVHeaders(meta, writer, headerLayer);
    writer->finishCSVHeaders();
    writer->flushContent(false);

    Owned<IResultSetCursor> cursor = result->createCursor();
    return writeResultCursorXml(*writer, cursor, resultName, start, count, nullptr, nullptr, true);
}

//Similar to the getResultJSON in fileview2
unsigned CWsWuResultOutHelper::getResultJSONStreaming(INewResultSet* result, const char* resultName, const char* schemaName, IXmlStreamFlusher* flusher)
{
    Owned<CommonJsonWriter> writer = new CommonJsonWriter(0, 0, flusher);
    writer->outputBeginRoot();
    writer->flushContent(false);

    Owned<IResultSetCursor> cursor = result->createCursor();
    unsigned rc = writeResultCursorXml(*writer, cursor, resultName, start, count, schemaName, nullptr, true);
    writer->outputEndRoot();
    return rc;
}

unsigned CWsWuResultOutHelper::getResultXmlStreaming(INewResultSet* result, const char* resultName, const char* schemaName, const IProperties* xmlns, IXmlStreamFlusher* flusher)
{
    Owned<IResultSetCursor> cursor = result->createCursor();
    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty, 0, flusher);
    return writeResultCursorXml(*writer, cursor, resultName, start, count, schemaName, xmlns, true);
}


