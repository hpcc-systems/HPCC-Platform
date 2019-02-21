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

#pragma warning (disable : 4786)

#include "math.h"
#include "ws_topologyService.hpp"
#include "workunit.hpp"

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "swapnodelib.hpp"
#include "dalienv.hpp"
#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif
#include "exception_util.hpp"
#include "jwrapper.hpp"

#define SDS_LOCK_TIMEOUT 30000

static const char* FEATURE_URL = "ClusterTopologyAccess";
static const char* MACHINE_URL = "MachineInfoAccess";

static const unsigned THORSTATUSDETAILS_REFRESH_MINS = 1;
static const long LOGFILESIZELIMIT = 100000; //Limit page size to 100k
static const long AVERAGELOGROWSIZE = 10000;
const char* TEMPZIPDIR = "tempzipfiles";

void CWsTopologyEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;

    if (!daliClientActive())
    {
        OERRLOG("No Dali Connection Active.");
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "No Connection to Dali server is active. Please specify a Dali server in the configuration file.");
    }

    //load threshold values for monitoring cpu load, disk/memory usage
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> pServiceNode = cfg->getPropTree(xpath.str());

    m_cpuThreshold = pServiceNode->getPropInt("@warnIfCpuLoadOver", 95);
    loadThresholdValue(pServiceNode, "@warnIfFreeStorageUnder", m_diskThreshold, m_bDiskThresholdIsPercentage);
    loadThresholdValue(pServiceNode, "@warnIfFreeMemoryUnder", m_memThreshold, m_bMemThresholdIsPercentage);

    m_bEncapsulatedSystem = false;
    StringBuffer systemUseRewrite;
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/SystemUseRewrite", process, service);
    cfg->getProp(xpath.str(), systemUseRewrite);
    if (streq(systemUseRewrite.str(), "true"))
        m_bEncapsulatedSystem = true;

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/PreflightProcessFilter", process, service);
    cfg->getProp(xpath.str(), m_preflightProcessFilter);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/DefaultTargetCluster/@name", process, service);
    if (cfg->hasProp(xpath.str()))
    {
        defaultTargetClusterName.set(cfg->queryProp(xpath.str()));
        xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/DefaultTargetCluster/@prefix", process, service);
        if (cfg->hasProp(xpath.str()))
            defaultTargetClusterPrefix.set(cfg->queryProp(xpath.str()));
    }

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService", process);
    Owned<IPropertyTreeIterator> it= cfg->getElements(xpath.str());
    ForEach(*it)
    {
        IPropertyTree& espService = it->query();
        const char* shortName = espService.queryProp("@shortName");
        const char* longName = espService.queryProp("@longName");
        const char* folderName = espService.queryProp("@folderName");
        const char* widgetName = espService.queryProp("@widgetName");
        if ((!shortName || !*shortName) && (!longName || !*longName) &&
            (!folderName || !*folderName) && (!widgetName || !*widgetName))
            continue;
        Owned<IEspTpEspServicePlugin> espServicePlugin= createTpEspServicePlugin();
        if (shortName && *shortName)
            espServicePlugin->setShortName(shortName);
        if (longName && *longName)
            espServicePlugin->setLongName(longName);
        if (folderName && *folderName)
            espServicePlugin->setFolderName(folderName);
        if (widgetName && *widgetName)
            espServicePlugin->setWidgetName(widgetName);

        espServicePlugins.append(*espServicePlugin.getClear());
    }

    m_enableSNMP = false;
}

StringBuffer& CWsTopologyEx::getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage)
{
    context.getAcceptLanguage(acceptLanguage);
    if (!acceptLanguage.length())
    {
        acceptLanguage.set("en");
        return acceptLanguage;
    }
    acceptLanguage.setLength(2);
    VStringBuffer languageFile("%ssmc_xslt/nls/%s/hpcc.xml", getCFD(), acceptLanguage.str());
    if (!checkFileExists(languageFile.str()))
        acceptLanguage.set("en");
    return acceptLanguage;
}

void CWsTopologyEx::loadThresholdValue(IPropertyTree* pServiceNode, const char* attrName, unsigned int& thresholdValue, 
                                                    bool& bThresholdIsPercentage)
{
    const char* threshold = pServiceNode->queryProp(attrName);
    if (threshold && *threshold)
    {
        thresholdValue = atoi(threshold);
        StringBuffer buf(threshold);
        buf.toUpperCase();
        bThresholdIsPercentage = strstr(buf.str(), "MB") == NULL;
    }
    else
    {
        thresholdValue = 95;
        bThresholdIsPercentage = true;
    }
}

bool CWsTopologyEx::onTpSwapNode(IEspContext &context,IEspTpSwapNodeRequest  &req, IEspTpSwapNodeResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpSwapNode: Permission denied.");

        bool res = swapNode(req.getCluster(),req.getOldIP(),req.getNewIP());

        resp.setTpSwapNodeResult(res);


        StringBuffer path;
        path.appendf("/Environment/Software/ThorCluster[@name='%s']", req.getCluster());

        StringBuffer encodedXpath;
        JBASE64_Encode(path, path.length(), encodedXpath, false);

        path.clear().append("/WsTopology/TpMachineQuery?Type=THORMACHINES&Cluster=");
        path.append(req.getCluster()).append("&Path=").append(encodedXpath);
       
        resp.setRedirectUrl(path.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpSetMachineStatus(IEspContext &context,IEspTpSetMachineStatusRequest  &req, IEspTpSetMachineStatusResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Write, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpSetMachineStatus: Permission denied.");

        resp.setTpSetMachineStatusResult(true);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpLogFileDisplay(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp)
{
    onTpLogFile(context, req, resp);

    return true;
}

bool CWsTopologyEx::onTpLogFile(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpLogFile: Permission denied.");

        const char* name = req.getName();
        const char* type = req.getType();
        if (!name || !*name)
            throw MakeStringException(ECLWATCH_INVALID_FILE_NAME,"File name not specified.");
        if (!type || !*type)
            throw MakeStringException(ECLWATCH_INVALID_FILE_NAME,"File type not specified.");

        double version = context.getClientVersion();
        if (version >= 1.20)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
        if (streq(type,"thormaster_log") || streq(type,"tpcomp_log"))
        {
            readTpLogFile(context, name, type, req, resp);
        }
        else if (streq(type,"xml"))
        {
            StringBuffer redirect;
            redirect.append("/WsTopology/TpXMLFile");
            redirect.appendf("?Name=%s", req.getName());
            resp.setRedirectUrl(redirect.str());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onSystemLog(IEspContext &context,IEspSystemLogRequest  &req, IEspSystemLogResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::SystemLog: Permission denied.");

        const char* name = req.getName();
        if (!name || !*name)
            throw MakeStringException(ECLWATCH_INVALID_FILE_NAME,"File name not specified.");

        StringBuffer logname;
        const char* type = req.getType();
        if (type && !strcmp(type,"thormaster_log"))
        {
            logname.append(CCluster(name)->queryRoot()->queryProp("LogFile"));
        }
        else
        {
            logname = name;
        }

        int nZip = req.getZip();

        //Remove path from file name
        char* ppStr = (char*) logname.str();
        char* pStr = strchr(ppStr, '/');
        while (pStr)
        {
            ppStr = pStr+1;
            pStr = strchr(ppStr, '/');
        }
        pStr = strchr(ppStr, '\\');
        while (pStr)
        {
            ppStr = pStr+1;
            pStr = strchr(ppStr, '\\');
        }

        StringBuffer fileName, headerStr;
        if (ppStr && *ppStr)
        {
            fileName.append(ppStr);
        }
        else
        {
            fileName.append("SystemLog");
        }

        headerStr.appendf("attachment;filename=%s", fileName.str());
        if (nZip > 2)
            headerStr.append(".gz");
        else if (nZip > 1)
            headerStr.append(".zip");

        Owned<IFile> rFile = createIFile(logname.str());
        if (!rFile)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.", logname.str());

        OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
        if (!rIO)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logname.str());

        offset_t fileSize = rFile->size();

        StringBuffer tmpBuf;
        tmpBuf.ensureCapacity((unsigned)fileSize);
        tmpBuf.setLength((unsigned)fileSize);

        size32_t nRead = rIO->read(0, (size32_t) fileSize, (char*)tmpBuf.str());
        if (nRead != fileSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

        if (nZip < 2)
        {
            MemoryBuffer membuff;
            membuff.setBuffer(tmpBuf.length(), (void*)tmpBuf.str());
            resp.setThefile(membuff);
            resp.setThefile_mimetype(HTTP_TYPE_TEXT_PLAIN);
            context.addCustomerHeader("Content-disposition", headerStr.str());
        }
        else
        {
#ifndef _USE_ZLIB
            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
#else
            StringBuffer ifname;
            unsigned threadID = (unsigned) (memsize_t) GetCurrentThreadId();
            if (nZip > 2)
                ifname.appendf("%s%sT%xAT%x", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());
            else
                ifname.appendf("%s%sT%xAT%x.zip", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());

            int ret = 0;
            IZZIPor* Zipor = createZZIPor();
            if (nZip > 2)
                ret = Zipor->gzipToFile(tmpBuf.length(), (void*)tmpBuf.str(), ifname.str());
            else
                ret = Zipor->zipToFile(tmpBuf.length(), (void*)tmpBuf.str(), fileName.str(), ifname.str());

            releaseIZ(Zipor);

            if (ret < 0)
            {
                Owned<IFile> rFile = createIFile(ifname.str());
                if (rFile->exists())
                    rFile->remove();

                throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
            }

            int outlen = 0;
            unsigned char* outd = NULL;
            ret = loadFile(ifname.str(), outlen, outd); 
            if(ret < 0 || outlen < 1 || !outd || !*outd)
            {
                Owned<IFile> rFile = createIFile(ifname.str());
                if (rFile->exists())
                    rFile->remove();

                if (outd)
                    free(outd);

                throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
            }

            MemoryBuffer membuff;
            membuff.setBuffer(outlen, (void*)outd);
            resp.setThefile(membuff);
            if (nZip > 2)
                resp.setThefile_mimetype("application/x-gzip");
            else
                resp.setThefile_mimetype("application/zip");
            context.addCustomerHeader("Content-disposition", headerStr.str());

            Owned<IFile> rFile1 = createIFile(ifname.str());
            if (rFile1->exists())
                rFile1->remove();
            if (outd)
                free(outd);
#endif
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpXMLFile(IEspContext &context,IEspTpXMLFileRequest  &req, IEspTpXMLFileResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpXMLFile: Permission denied.");

        StringBuffer strBuff, xmlBuff;
        strBuff.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>");
        getThorXml(req.getName(),xmlBuff);
        strBuff.append(xmlBuff);
        
        MemoryBuffer membuff;
        membuff.setBuffer(strBuff.length(), (void*)strBuff.str());

        resp.setThefile_mimetype(HTTP_TYPE_APPLICATION_XML);
        resp.setThefile(membuff);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsTopologyEx::getThorXml(const char *cluster,StringBuffer& returnStr)
{
    CCluster conn(cluster);
    toXML(conn->queryRoot(), returnStr);
}

void CWsTopologyEx::getThorLog(const char *cluster,MemoryBuffer& returnbuff)
{
    StringBuffer logname;
    logname.append(CCluster(cluster)->queryRoot()->queryProp("LogFile"));

    Owned<IFile> rFile = createIFile(logname.str());
    if (!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());

    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logname.str());
    read(rIO, 0, (size32_t)-1, returnbuff);
}

int CWsTopologyEx::loadFile(const char* fname, int& len, unsigned char* &buf, bool binary)
{
    len = 0;
    buf = NULL;

    FILE* fp = fopen(fname, binary?"rb":"rt");
    if (fp)
    {
        char* buffer[1024];
        int bytes;
        for (;;)
        {
            bytes = fread(buffer, 1, sizeof(buffer), fp);
            if (!bytes)
                break;
            buf = (unsigned char*)realloc(buf, len + bytes + 1);
            memcpy(buf + len, buffer, bytes);
            len += bytes;
        }
        fclose(fp);
    }
    else
    {
        printf("unable to open file %s\n", fname);
        return -1;
    }

    if(buf)
        buf[len] = '\0';

    return 0;
}

unsigned CWsTopologyEx::findLineTerminator(const char* dataPtr, const size32_t dataSize)
{
    char* pTr = (char*) dataPtr;
    size32_t bytesToCheck = dataSize;
    while(bytesToCheck > 0)
    {
        if ((bytesToCheck > 1) && (pTr[0] == '\r') && (pTr[1] == '\n'))
            return 2;

        if (pTr[0] == '\r' || pTr[0] == '\n')
            return 1;

        pTr++;
        bytesToCheck--;
    }

    return 0;
}

bool CWsTopologyEx::isLineTerminator(const char* dataPtr, const size32_t dataSize, unsigned ltLength)
{
    if (ltLength > 1)
    {
        if ((dataSize > 1) && (dataPtr[0] == '\r') && (dataPtr[1] == '\n'))
            return true;
    }
    else if (dataPtr[0] == '\r' || dataPtr[0] == '\n')
        return true;

    return false;
}

bool CWsTopologyEx::readLogLineID(char* linePtr, unsigned long& lineID)
{
    char *epTr;
    lineID = strtoul(linePtr, &epTr, 16);
    if (epTr - linePtr < 8)  //LineID is the first 8 bytes of a log line
        return false;

    return true;
}

bool CWsTopologyEx::readLogTime(char* pTr, int start, int length, CDateTime& dt)
{
    bool bRet = false;
    try
    {
        StringBuffer strBuf;
        strBuf.append(pTr, start, length);
        strBuf.setCharAt(10, 'T');
        dt.setString(strBuf.str(), NULL, false);
        bRet = true;
    }
    catch(IException* e)
    {
        e->Release();
    }

    return bRet;
}

bool CWsTopologyEx::findTimestampAndLT(StringBuffer logname, IFile* rFile, ReadLog& readLogReq, CDateTime& latestLogTime)
{
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot read file %s.",logname.str());

    size32_t fileSize = (size32_t) rFile->size();
    size32_t readSize = 64000;
    if (readSize > fileSize)
        readSize = fileSize;

    //Read the first chuck of file to find out a timestamp and line terminator
    StringBuffer dataBuffer;
    size32_t bytesRead = rIO->read(0, readSize, dataBuffer.reserve(readSize));
    if (bytesRead != readSize)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

    char* pTr = (char*) dataBuffer.str();
    readLogReq.ltBytes = findLineTerminator(pTr, bytesRead);

    if (rFile->size() < 28) //lineID + timestamp
        return false;//no timestamp in this log
    if (!readLogTime(pTr, 9, 19, latestLogTime))
        return false;//no timestamp in this log

    if ((readLogReq.filterType != GLOLastNHours) && (readLogReq.filterType != GLOTimeRange))
        return true;//Not interested in the last timestamp this time

    //Search the last chuck to find out the latest timestamp
    if (readSize < fileSize)
    {
        bytesRead = rIO->read(fileSize - readSize, readSize, dataBuffer.clear().reserve(readSize));
        if (bytesRead != readSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());
        pTr = (char*) dataBuffer.str();
    }

    //skip the first line which may not contain the entire log line
    StringBuffer logLine;
    size32_t bytesRemaining = bytesRead;
    bool hasAnotherLine = false;
    pTr = readALogLine(pTr, bytesRemaining, readLogReq.ltBytes, logLine, hasAnotherLine);
    if (!hasAnotherLine || (bytesRemaining < 28))
        return true;

    //Find out the last timestamp
    while (hasAnotherLine && bytesRemaining > 27)
    {
        CDateTime dt;
        pTr = readALogLine(pTr, bytesRemaining, readLogReq.ltBytes, logLine, hasAnotherLine);
        if ((logLine.length() > 27) && readLogTime((char*) logLine.str(), 9, 19, dt))
            latestLogTime = dt;
    }

    return true;
}

char* CWsTopologyEx::readALogLine(char* dataPtr, size32_t& bytesRemaining, unsigned ltLength, StringBuffer& logLine, bool& hasLineTerminator)
{
    char* pTr = dataPtr;

    CDateTime dt;
    hasLineTerminator = false;
    while(bytesRemaining > 0)
    {
        hasLineTerminator = isLineTerminator(pTr, bytesRemaining, ltLength);
        if (hasLineTerminator && (bytesRemaining > ltLength + 27) && readLogTime(pTr+ltLength, 9, 19, dt))
            break;

        pTr++;
        bytesRemaining--;
    }

    if (hasLineTerminator && (bytesRemaining > 0))
    {
        pTr += ltLength;
        bytesRemaining -= ltLength;
    }

    logLine.clear();
    if (pTr > dataPtr)
        logLine.append(dataPtr, 0, pTr - dataPtr);

    return pTr;
}

void CWsTopologyEx::addALogLine(offset_t& readFrom, unsigned& locationFlag, StringBuffer dataRow, ReadLog& readLogReq, StringArray& returnbuff)
{
    if (readLogReq.filterType == GLOFirstNRows)
    {
        locationFlag = 1; //enter the area to be retrieved
        returnbuff.append(dataRow);
        if (returnbuff.length() == readLogReq.firstRows)
            locationFlag = 2; //stop now since we have enough rows
    }
    else if (readLogReq.filterType == GLOLastNRows)
    {
        if (returnbuff.length() == readLogReq.lastRows)
            returnbuff.remove(readLogReq.lastRows - 1);

        returnbuff.add(dataRow, 0);
        locationFlag = 1; //always in the area to be retrieved, but extra lines may be pushed out later
    }
    else
    {
        unsigned long rowID;
        if (!readLogLineID((char*)dataRow.str(), rowID)) //row id (and timestamp) not found in this log line
        {
            if (locationFlag > 0)
                returnbuff.append(dataRow);
            readFrom += dataRow.length();
            return;
        }

        StringBuffer str;
        str.append(dataRow.str(), 20, 8); //Read time
        if (readLogReq.endDate.length() > 0 && strcmp(str.str(), readLogReq.endDate.str()) > 0)
            locationFlag = 2; //out of the area to be retrieved
        else if (readLogReq.startDate.length() > 1 && strcmp(str.str(), readLogReq.startDate.str()) < 0)
            readFrom += dataRow.length(); //skip this line
        else
        {
            returnbuff.append(dataRow);
            if ((locationFlag < 1) && (readLogReq.filterType == GLOTimeRange))
                readLogReq.pageFrom = readFrom;

            readFrom += dataRow.length();
            locationFlag = 1; //enter the area to be retrieved
        }
    }
    return;
}

void CWsTopologyEx::readLogFileToArray(StringBuffer logname, OwnedIFileIO rIO, ReadLog& readLogReq, StringArray& returnbuf)
{
    bool firstChuck = true;
    bool lastChuck = false;

    offset_t readFrom = 0;
    offset_t logLineFrom = 0;
    unsigned locationFlag = 0; //0: before the interested log lines are reached, 1: found the log lines, 2: out
    StringBuffer logLine;

    offset_t bytesLeft = readLogReq.fileSize;
    while (bytesLeft > 0)
    {
        size32_t readSize;
        StringBuffer dataBuffer;

        //Find out where to read and how many bytes to read
        if (!firstChuck || (readLogReq.filterType != GLOLastNRows))
        {
            if (bytesLeft > LOGFILESIZELIMIT)
                readSize = LOGFILESIZELIMIT;
            else
            {
                readSize = (size32_t) bytesLeft;
                lastChuck = true;
            }
            bytesLeft -= readSize;
        }
        else
        {
            //read the last chuck since the file is too big
            size32_t estimateSize = AVERAGELOGROWSIZE*readLogReq.lastRows;
            if (readLogReq.fileSize < estimateSize)
                readSize = (size32_t) readLogReq.fileSize;
            else
                readSize = estimateSize;
            readFrom = (size32_t) readLogReq.fileSize-readSize;
            bytesLeft = 0;
            lastChuck = true;
        }

        //read a chuck of log to a buffer
        if (logLine.length() > 0) //check any left from a previous chunk
            dataBuffer.append(logLine.str());
        size32_t nRead = rIO->read(readFrom, readSize, dataBuffer.reserve(readSize));
        if (nRead != readSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

        logLineFrom = readFrom;
        readFrom += nRead;

        //get the log lines from the buffer
        size32_t bytesRemaining = dataBuffer.length();
        char* pTr = (char*) dataBuffer.str();
        bool hasAnotherLine = true;
        while (hasAnotherLine && (bytesRemaining > 0) && (locationFlag < 2))
        {
            pTr = readALogLine(pTr, bytesRemaining, readLogReq.ltBytes, logLine.clear(), hasAnotherLine);
            if (logLine.length() < 1)
                continue;
            if (lastChuck || hasAnotherLine)
                addALogLine(logLineFrom, locationFlag, logLine, readLogReq, returnbuf);
        }

        if (locationFlag > 1) //interested log lines have been finished
            break;

        firstChuck = false;
    }

    return;
}

void CWsTopologyEx::readLogFile(StringBuffer logname, IFile* rFile, ReadLog& readLogReq, StringBuffer& returnbuff)
{
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot read file %s.",logname.str());

    if ((readLogReq.filterType == GLOFirstPage) || (readLogReq.filterType == GLOLastPage) || (readLogReq.filterType == GLOGoToPage)) //by page number
    {
        size32_t fileSize = (size32_t) (readLogReq.pageTo - readLogReq.pageFrom);
        size32_t nRead = rIO->read(readLogReq.pageFrom, fileSize, returnbuff.reserve(fileSize));
        if (nRead != fileSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());
    }
    else
    {
        StringArray logLines;
        readLogFileToArray(logname, rIO, readLogReq, logLines);
        ForEachItemIn(i, logLines)
        {
            StringBuffer logLine = logLines.item(i);
            if ((!readLogReq.reverse && (readLogReq.filterType != 5)) ||
                (readLogReq.reverse && (readLogReq.filterType == 5)))
                returnbuff.append(logLine.str());
            else
                returnbuff.insert(0, logLine);
        }

        //Update page information based on log content
        if (returnbuff.length() > LOGFILESIZELIMIT)
        {
            if (readLogReq.filterType == GLOFirstNRows)
                readLogReq.pageTo = LOGFILESIZELIMIT;
            else if (readLogReq.filterType == GLOTimeRange)
                readLogReq.pageTo = readLogReq.pageFrom + LOGFILESIZELIMIT;
            else if ((readLogReq.filterType == GLOLastNHours) || (readLogReq.filterType == GLOLastNRows))
            {
                readLogReq.pageFrom = readLogReq.fileSize - returnbuff.length();
                readLogReq.pageTo = readLogReq.pageFrom + LOGFILESIZELIMIT;
            }
        }
        else
        {
            if (readLogReq.filterType == GLOFirstNRows)
                readLogReq.pageTo = returnbuff.length();
            else if (readLogReq.filterType == GLOTimeRange)
                readLogReq.pageTo = readLogReq.pageFrom + returnbuff.length();
            else if ((readLogReq.filterType == GLOLastNHours) || (readLogReq.filterType == GLOLastNRows))
            {
                readLogReq.pageFrom = readLogReq.fileSize - returnbuff.length();
                readLogReq.pageTo = readLogReq.fileSize;
            }
        }
    }
}

void CWsTopologyEx::readTpLogFileRequest(IEspContext &context, const char* fileName, IFile* rFile, IEspTpLogFileRequest  &req, ReadLog& readLogReq)
{
    readLogReq.filterType = (GetLogOptions) req.getFilterType();
    readLogReq.pageNumber = req.getPageNumber();
    readLogReq.startDate = req.getStartDate();
    readLogReq.endDate = req.getEndDate();
    readLogReq.firstRows = req.getFirstRows();
    readLogReq.lastRows = req.getLastRows();
    readLogReq.reverse = req.getReversely();
    readLogReq.loadContent = req.getLoadData();

    readLogReq.fileSize = rFile->size();
    readLogReq.TotalPages = (int) ceil(((double)readLogReq.fileSize)/LOGFILESIZELIMIT);
    readLogReq.prevPage = -1;
    readLogReq.nextPage = -1;
    readLogReq.lastHours = 0;

    CDateTime latestLogTime;
    readLogReq.hasTimestamp = findTimestampAndLT(fileName, rFile, readLogReq, latestLogTime);

    switch (readLogReq.filterType)
    {
    case GLOFirstPage:
    case GLOGoToPage:
    {
        if (readLogReq.pageNumber > readLogReq.TotalPages - 1)
            readLogReq.pageNumber = readLogReq.TotalPages - 1;

        readLogReq.pageFrom = LOGFILESIZELIMIT * readLogReq.pageNumber;
        if (readLogReq.pageFrom > 0)
            readLogReq.prevPage = readLogReq.pageNumber - 1;
        if (readLogReq.pageNumber < readLogReq.TotalPages - 1)
        {
            readLogReq.nextPage = readLogReq.pageNumber + 1;
            readLogReq.pageTo = (long) (readLogReq.pageFrom + LOGFILESIZELIMIT);
        }
        else
            readLogReq.pageTo = (long) readLogReq.fileSize;
        break;
    }
    case GLOLastPage:
    {
        int pageCount = 1;
        readLogReq.pageFrom = 0;
        offset_t fileSize = readLogReq.fileSize;
        while (fileSize > LOGFILESIZELIMIT)
        {
            fileSize -= LOGFILESIZELIMIT;
            readLogReq.pageFrom += LOGFILESIZELIMIT;
            pageCount++;
        }

        readLogReq.pageTo = readLogReq.pageFrom + fileSize;
        readLogReq.pageNumber = pageCount - 1;
        if (readLogReq.pageFrom > 0)
            readLogReq.prevPage = pageCount - 2;
        break;
    }
    case GLOFirstNRows:
    {
        if (readLogReq.firstRows < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "'First rows' field should be defined.");
        readLogReq.pageFrom = 0;
        break;
    }
    case GLOLastNRows:
    {
        if (readLogReq.lastRows < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "'Last rows' field should be defined.");
        break;
    }
    case GLOLastNHours:
    {
        if (!readLogReq.hasTimestamp)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "This log file has no timestamp.");

        if  (req.getLastHours_isNull())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invlid 'Hours' field.");

        readLogReq.lastHours = req.getLastHours();
        unsigned hour, minute, second, nano;
        latestLogTime.getTime(hour, minute, second, nano, false);
        int hour2 = hour - readLogReq.lastHours;
        if (hour2 < 0)
            readLogReq.startDate.set("00:00:00");
        else
            readLogReq.startDate.clear().appendf("%02d:%02d:%02d", hour2, minute, second);
        readLogReq.endDate.clear();
        break;
    }
    case GLOTimeRange:
    {
        if (!readLogReq.hasTimestamp)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "This log file has no timestamp.");

        if ((readLogReq.startDate.length() < 8) && (readLogReq.endDate.length() < 8))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invlid 'Time' field.");
        break;
    }
    }

    return;
}

void CWsTopologyEx::setTpLogFileResponse(IEspContext &context, ReadLog& readLogReq, const char* fileName,
                                         const char* fileType, StringBuffer& returnbuf, IEspTpLogFileResponse &resp)
{
    if (readLogReq.loadContent)
    {
        if (returnbuf.length() <= LOGFILESIZELIMIT)
            resp.setLogData(returnbuf.str());
        else
        {
            StringBuffer returnbuf0;
            returnbuf0.append(returnbuf.str(), 0, LOGFILESIZELIMIT);
            returnbuf0.appendf("\r\n****** Warning: cannot display all. The page size is limited to %ld bytes. ******", LOGFILESIZELIMIT);
            resp.setLogData(returnbuf0.str());
        }
    }

    resp.setHasDate(readLogReq.hasTimestamp);
    if (readLogReq.lastHours > 0)
        resp.setLastHours(readLogReq.lastHours);
    if (readLogReq.startDate.length() > 0)
        resp.setStartDate(readLogReq.startDate.str());
    if (readLogReq.endDate.length() > 0)
        resp.setEndDate(readLogReq.endDate.str());
    if (readLogReq.lastRows > 0)
        resp.setLastRows(readLogReq.lastRows);
    if (readLogReq.firstRows > 0)
        resp.setFirstRows(readLogReq.firstRows);

    double version = context.getClientVersion();
    if (version > 1.05)
        resp.setTotalPages( readLogReq.TotalPages );

    if (readLogReq.fileSize > 0)
        resp.setFileSize(readLogReq.fileSize);
    if (readLogReq.pageNumber > 0)
        resp.setPageNumber(readLogReq.pageNumber);
    if (readLogReq.pageFrom > 0)
        resp.setPageFrom(readLogReq.pageFrom);
    if (readLogReq.pageTo > 0)
        resp.setPageTo(readLogReq.pageTo);
    resp.setPrevPage(readLogReq.prevPage);
    if (readLogReq.nextPage > 0)
        resp.setNextPage(readLogReq.nextPage);

    resp.setName(fileName);
    resp.setType(fileType);
    resp.setFilterType(readLogReq.filterType);
    resp.setReversely(readLogReq.reverse);

    return;
}

void CWsTopologyEx::readTpLogFile(IEspContext &context,const char* fileName, const char* fileType, IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp)
{
    StringBuffer logname;
    if (strcmp(fileType,"thormaster_log"))
        logname = fileName;
    else
        logname.append(CCluster(fileName)->queryRoot()->queryProp("LogFile"));

    Owned<IFile> rFile = createIFile(logname.str());
    if (!rFile || !rFile->exists())
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());

    ReadLog readLogReq;
    readTpLogFileRequest(context, fileName, rFile, req, readLogReq);

    StringBuffer returnbuf;
    if (readLogReq.loadContent)
        readLogFile(logname, rFile, readLogReq, returnbuf);

    setTpLogFileResponse(context, readLogReq, fileName, fileType, returnbuf, resp);

    return;
}

bool CWsTopologyEx::onTpClusterQuery(IEspContext &context, IEspTpClusterQueryRequest &req, IEspTpClusterQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpClusterQuery: Permission denied.");

        IArrayOf<IEspTpCluster> clusters;
        const char* type = req.getType();
        if (!type || !*type || (strcmp(eqRootNode,type) == 0) || (strcmp(eqAllClusters,type) == 0))
        {
            m_TpWrapper.getClusterProcessList(eqHoleCluster, clusters);
            m_TpWrapper.getClusterProcessList(eqThorCluster, clusters);
            m_TpWrapper.getClusterProcessList(eqRoxieCluster,clusters);
        }
        else
        {
            m_TpWrapper.getClusterProcessList(type,clusters);
        }
        double version = context.getClientVersion();
        if (version > 1.07)
        {       
            resp.setEnableSNMP(m_enableSNMP);
        }
        if (version >= 1.20)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }

        resp.setTpClusters(clusters);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpListTargetClusters(IEspContext &context, IEspTpListTargetClustersRequest &req, IEspTpListTargetClustersResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpListTargetClusters: Permission denied.");

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> env = factory->openEnvironment();
        Owned<IPropertyTree> root = &env->getPTree();
        bool foundDefault = false;
        bool hasHThor = false;
        bool hasThor = false;
        const char* defaultClusterName = defaultTargetClusterName.get();
        const char* defaultClusterPrefix = defaultTargetClusterPrefix.get();
        IArrayOf<IEspTpClusterNameType> clusters;
        Owned<IPropertyTreeIterator> targets = root->getElements("Software/Topology/Cluster");
        ForEach(*targets)
        {
            IPropertyTree &target = targets->query();
            const char* name = target.queryProp("@name");
            const char* prefix = target.queryProp("@prefix");
            if (!name || !*name || !prefix || !*prefix)
                continue;//invalid entry

            Owned<IEspTpClusterNameType> tc = new CTpClusterNameType("", "");
            tc->setName(name);
            tc->setType(prefix);

            if (defaultClusterName && defaultClusterPrefix && strieq(defaultClusterName, name) &&
                strieq(defaultClusterPrefix, prefix))
            {
                foundDefault = true;
                tc->setIsDefault(true);
            }

            if (!foundDefault && !hasHThor)
            {
                ClusterType targetClusterType = HThorCluster;
                getClusterType(prefix, targetClusterType);
                if (targetClusterType == HThorCluster)
                        hasHThor = true;
                else if (targetClusterType == ThorLCRCluster)
                        hasThor = true;
            }

            clusters.append(*tc.getLink());
        }
        if (!foundDefault)
        {  //No default target is specified or the default target does not match with any. Use the
            //following rules to decide a default target: if an hthor is found, it will be the 'default';
            //if no hthor, the first thor cluster will be the 'default';
            //If no hthor and no thor, the first roxie cluster will be the 'default'.
            ForEachItemIn(i, clusters)
            {
                IEspTpClusterNameType& tc = clusters.item(i);
                if (hasHThor)
                {
                    ClusterType targetClusterType = HThorCluster;
                    if (HThorCluster == getClusterType(tc.getType(), targetClusterType))
                    {
                        tc.setIsDefault(true);
                        break;
                    }
                }
                else if (hasThor)
                {
                    ClusterType targetClusterType = HThorCluster;
                    if (ThorLCRCluster == getClusterType(tc.getType(), targetClusterType))
                    {
                        tc.setIsDefault(true);
                        break;
                    }
                }
                else
                {
                    tc.setIsDefault(true);
                    break;
                }
            }
        }
        resp.setTargetClusters(clusters);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpTargetClusterQuery(IEspContext &context, IEspTpTargetClusterQueryRequest &req, IEspTpTargetClusterQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpTargetClusterQuery: Permission denied.");

        double version = context.getClientVersion();

        IArrayOf<IEspTpTargetCluster> clusters;
        const char* type = req.getType();
        const char* name = req.getName();
        if (!type || !*type || (strcmp(eqRootNode,type) == 0) || (strcmp(eqAllClusters,type) == 0))
        {
            m_TpWrapper.queryTargetClusters(version, eqAllClusters, NULL, clusters);
        }
        else if (!name || !*name)
        {
            m_TpWrapper.queryTargetClusters(version, type, NULL, clusters);
        }
        else
        {
            m_TpWrapper.queryTargetClusters(version, type, name, clusters);
        }

        resp.setMemThreshold( m_memThreshold );
        resp.setDiskThreshold( m_diskThreshold );
        resp.setCpuThreshold( m_cpuThreshold );
        resp.setMemThresholdType( m_bMemThresholdIsPercentage ? "0" : "1");
        resp.setDiskThresholdType( m_bDiskThresholdIsPercentage ? "0" : "1");

        resp.setTpTargetClusters(clusters);

        resp.setShowDetails(req.getShowDetails());
        if ((version > 1.12) && (m_preflightProcessFilter.length() > 0))
        {       
            resp.setPreflightProcessFilter(m_preflightProcessFilter);
        }
        if (version >= 1.20)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpLogicalClusterQuery(IEspContext &context, IEspTpLogicalClusterQueryRequest &req, IEspTpLogicalClusterQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpLogicalClusterQuery: Permission denied.");

        IArrayOf<IEspTpLogicalCluster> clusters;
        CConstWUClusterInfoArray wuClusters;
        getEnvironmentClusterInfo(wuClusters);
        ForEachItemIn(c, wuClusters)
        {
            IConstWUClusterInfo& wuCluster = wuClusters.item(c);
            SCMStringBuffer name;
            wuCluster.getName(name);

            Owned<IEspTpLogicalCluster> cluster = createTpLogicalCluster();
            cluster->setName(name.str());
            cluster->setType(clusterTypeString(wuCluster.getPlatform(), false));
            cluster->setLanguageVersion("3.0.0");
            clusters.append(*cluster.getClear());
        }
        resp.setTpLogicalClusters(clusters);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    
    return true;
}
    
bool CWsTopologyEx::onTpGroupQuery(IEspContext &context, IEspTpGroupQueryRequest &req, IEspTpGroupQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpGroupQuery: Permission denied.");

        IArrayOf<IEspTpGroup> Groups;
        m_TpWrapper.getGroupList(context.getClientVersion(), req.getKind(), Groups);
        resp.setTpGroups(Groups);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsTopologyEx::onTpClusterInfo(IEspContext &context, IEspTpClusterInfoRequest &req, IEspTpClusterInfoResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpClusterInfo: Permission denied.");

        Owned<IRemoteConnection> conn = querySDS().connect("/Status/Servers/", myProcessSession(),RTM_SUB,SDS_LOCK_TIMEOUT);
        if (conn)
        {
            Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(req.getName());
            SCMStringBuffer thorQueues;
            clusterInfo->getThorQueue(thorQueues);
            resp.setName(req.getName());
            StringArray qlist;
            qlist.appendListUniq(thorQueues.str(), ",");
            IArrayOf<IEspTpQueue> Queues;
            // look for the thor processes which are listening to this clusters queue, some may be listening to other clusters queues as well.
            ForEachItemIn(q, qlist)
            {
                const char *queueName = qlist.item(q);
                StringBuffer xpath("Server[@name=\"ThorMaster\"]");
                Owned<IPropertyTreeIterator> iter = conn->getElements(xpath.str());
                ForEach(*iter)
                {
                    IPropertyTree &server = iter->query();
                    const char *queues = server.queryProp("@queue");
                    StringArray thorqlist;
                    thorqlist.appendListUniq(queues, ",");
                    if (NotFound != thorqlist.find(queueName))
                    {
                        IEspTpQueue* pQueue = createTpQueue("","");
                        pQueue->setName(server.queryProp("@thorname"));
                        pQueue->setWorkUnit(server.queryProp("WorkUnit"));
                        Queues.append(*pQueue);
                    }
                }
            }
            resp.setTpQueues(Queues);
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsTopologyEx::onTpServiceQuery(IEspContext &context, IEspTpServiceQueryRequest &req, IEspTpServiceQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpServiceQuery: Permission denied.");

        double version = context.getClientVersion();
        const char* type = req.getType();
        if (!type || !*type || (strcmp(eqAllServices,type) == 0))
        {
            IEspTpServices& ServiceList = resp.updateServiceList();

            m_TpWrapper.getTpDaliServers( version, ServiceList.getTpDalis() );
            m_TpWrapper.getTpEclServers( ServiceList.getTpEclServers() );
            m_TpWrapper.getTpEclCCServers( ServiceList.getTpEclCCServers() );
            m_TpWrapper.getTpEclAgents( ServiceList.getTpEclAgents() );
            m_TpWrapper.getTpEspServers( ServiceList.getTpEspServers() );
            m_TpWrapper.getTpDfuServers( ServiceList.getTpDfuServers() );   
            m_TpWrapper.getTpSashaServers( ServiceList.getTpSashaServers() );   
            m_TpWrapper.getTpGenesisServers( ServiceList.getTpGenesisServers() );
            m_TpWrapper.getTpLdapServers( ServiceList.getTpLdapServers() );
            m_TpWrapper.getTpDropZones(version, nullptr, true, ServiceList.getTpDropZones() );
            m_TpWrapper.getTpFTSlaves( ServiceList.getTpFTSlaves() );
            m_TpWrapper.getTpDkcSlaves( ServiceList.getTpDkcSlaves() );

            if (version > 1.15)
            {       
                m_TpWrapper.getTpEclSchedulers( ServiceList.getTpEclSchedulers() );
            }
            if (version >= 1.28)
            {       
                m_TpWrapper.getTpSparkThors(version, nullptr, ServiceList.getTpSparkThors() );
            }
        }

        resp.setMemThreshold( m_memThreshold );
        resp.setDiskThreshold( m_diskThreshold );
        resp.setCpuThreshold( m_cpuThreshold );
        resp.setMemThresholdType( m_bMemThresholdIsPercentage ? "0" : "1");
        resp.setDiskThresholdType( m_bDiskThresholdIsPercentage ? "0" : "1");

        if (version > 1.06 && m_bEncapsulatedSystem)
        {       
            resp.setEncapsulatedSystem( m_bEncapsulatedSystem );
        }
        if (version > 1.07)
        {       
            resp.setEnableSNMP(m_enableSNMP);
        }
        if ((version > 1.12) && (m_preflightProcessFilter.length() > 0))
        {       
            resp.setPreflightProcessFilter(m_preflightProcessFilter);
        }
        if (version >= 1.20)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpMachineQuery(IEspContext &context, IEspTpMachineQueryRequest &req, IEspTpMachineQueryResponse &resp)
{ 
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpMachineQuery: Permission denied.");

        double version = context.getClientVersion();

        IArrayOf<IEspTpMachine> MachineList;
        const char* path      = req.getPath();
        const char* directory = req.getDirectory();

        bool hasThorSpareProcess = false;
        const char* type = req.getType();
        if (!type || !*type || (strcmp(eqAllNodes,type) == 0))
        {
            m_TpWrapper.getClusterMachineList(version, eqTHORMACHINES, path, directory, MachineList, hasThorSpareProcess);
            m_TpWrapper.getClusterMachineList(version, eqHOLEMACHINES, path, directory, MachineList, hasThorSpareProcess);
            m_TpWrapper.getClusterMachineList(version, eqROXIEMACHINES,path, directory, MachineList, hasThorSpareProcess);
        }
        else
        {
            m_TpWrapper.getClusterMachineList(version, type, path, directory, MachineList, hasThorSpareProcess, req.getCluster());
        }
        resp.setTpMachines(MachineList);
        resp.setType( req.getType() );
        resp.setCluster( req.getCluster() );
        resp.setOldIP( req.getOldIP() );
        resp.setPath( req.getPath() );
        resp.setLogDirectory( req.getLogDirectory() );

        resp.setMemThreshold( m_memThreshold );
        resp.setDiskThreshold( m_diskThreshold );
        resp.setCpuThreshold( m_cpuThreshold );
        resp.setMemThresholdType( m_bMemThresholdIsPercentage ? "0" : "1");
        resp.setDiskThresholdType( m_bDiskThresholdIsPercentage ? "0" : "1");

        SecAccessFlags access;
        bool bEnablePreflightInfo = context.authorizeFeature(MACHINE_URL, access) &&
                                    access >= SecAccess_Read;
        resp.setEnablePreflightInfo( bEnablePreflightInfo );

        if (version > 1.07)
        {       
            resp.setEnableSNMP(m_enableSNMP);
        }
        if ((version > 1.12) && (m_preflightProcessFilter.length() > 0))
        {       
            resp.setPreflightProcessFilter(m_preflightProcessFilter);
        }       
        if (version > 1.14 && hasThorSpareProcess)
        {
            resp.setHasThorSpareProcess( hasThorSpareProcess );
        }
        if (version >= 1.20)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpMachineInfo(IEspContext &context, IEspTpMachineInfoRequest &req, IEspTpMachineInfoResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpMachineInfo: Permission denied.");

        m_TpWrapper.getMachineInfo(context.getClientVersion(), req.getName(), req.getNetAddress(), resp.updateMachineInfo());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpGetComponentFile(IEspContext &context, IEspTpGetComponentFileRequest &req, 
    IEspTpGetComponentFileResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpGetComponentFile: Permission denied.");

        const char* fileType = req.getFileType();
        if (!fileType || (0!=stricmp(fileType, "cfg") && 0!=stricmp(fileType, "log")))
            throw MakeStringException(ECLWATCH_INVALID_FILE_TYPE, "A valid file type requested.  Only configuration and log files are supported!");

        const char* compType   = req.getCompType();
        if (!compType || !*compType)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_COMPONENT_TYPE, "Component type must be specified!");

        const char* compName   = req.getCompName();
        const char* directory  = req.getDirectory();
        const char* netAddress = req.getNetAddress();
        const char* fileName = NULL;
        OS_TYPE osType = (OS_TYPE) req.getOsType();
        bool bCluster = false;

        if (!stricmp(fileType, "cfg"))
        {
            if (!stricmp(compType, eqDali))
                fileName = "daliconf.xml";
            else if (!stricmp(compType, eqDfu))
                fileName = "dfuserver.xml";
            else if (!stricmp(compType, eqEclServer))
                fileName = "eclserver.xml";
            else if (!stricmp(compType, eqEclCCServer))
                fileName = "eclccserver.xml";
            else if (!stricmp(compType, eqEclScheduler))
                fileName = "eclscheduler.xml";
            else if (!stricmp(compType, eqEclAgent))
                fileName = "agentexec.xml";
            else if (!stricmp(compType, eqEsp))
                fileName = "esp.xml";
            else if (!stricmp(compType, eqSashaServer))
                fileName = "sashaconf.xml";
            else 
            {
                const unsigned int len = strlen(compType);
                if (len>4)
                {
                    if (!strnicmp(compType, "Roxie", 5))
                        compType = "RoxieCluster", fileName = "RoxieTopology.xml", bCluster = true;
                    else if (!strnicmp(compType, "Thor", 4))
                        compType = "ThorCluster", fileName = "thor.xml", bCluster = true;
                    else if (!strnicmp(compType, "Hole", 4))
                        compType = "HoleCluster", fileName = "edata.ini", bCluster = true;
                }
            }
        }
        else
        {
            fileName = "";
            if (strlen(compType)>4)
            {
                if (!strnicmp(compType, "Roxie", 5))
                    compType = "RoxieCluster", bCluster = true;
                else if (!strnicmp(compType, "Thor", 4))
                    compType = "ThorCluster", bCluster = true;
                else if (!strnicmp(compType, "Hole", 4))
                    compType = "HoleCluster", bCluster = true;
            }
        }

        if (!fileName)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_COMPONENT_OR_FILE_TYPE, "Unsupported component or file type specified!");
                        
        
        //the paths are all windows or samba network shares so construct windows network path
        StringBuffer netAddressStr;
        SCMStringBuffer scmNetAddress;
        StringAttr      sDirectory;
        if (bCluster && !(netAddress && *netAddress))
        {
            Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
            Owned<IConstEnvironment> constEnv = factory->openEnvironment();
            Owned<IPropertyTree> pRoot = &constEnv->getPTree();

            StringBuffer xpath;
            xpath.appendf("Software/%s[@name='%s']", compType, compName);

            IPropertyTree* pCluster = pRoot->queryPropTree( xpath.str() );
            if (!pCluster)
                throw MakeStringException(ECLWATCH_COMPONENT_NOT_IN_ENV_INFO, "%s '%s' is not defined!", compType, compName);

            if (!directory || !*directory)
            {
                sDirectory.set( pCluster->queryProp("@directory") );
                directory = sDirectory.get();
            }

            xpath.clear();
            if (!stricmp(compType, "RoxieCluster"))
                xpath.append("RoxieServerProcess[1]");
            else 
                if (!stricmp(compType, "ThorCluster"))
                    xpath.append("ThorMasterProcess");
                else//HoleCluster
                    xpath.append("HoleControlProcess");

            xpath.append("@computer");
            const char* computer = pCluster->queryProp(xpath.str());
            if (computer && *computer)
            {
                Owned<IConstMachineInfo> pMachine = constEnv->getMachine(computer);
                if (pMachine)
                {
                    pMachine->getNetAddress(scmNetAddress);
                    netAddressStr = scmNetAddress.str();
                    if (!strcmp(netAddressStr.str(), "."))
                    {
                        StringBuffer ipStr;
                        IpAddress ipaddr = queryHostIP();
                        ipaddr.getIpText(ipStr);
                        if (ipStr.length() > 0)
                        {
                            netAddressStr = ipStr.str();
                        }
                    }
                }
            }
        }

        if (netAddressStr.length() > 0)
            netAddress = netAddressStr.str();

        if (!directory || !*directory)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_FILE_FOLDER, "Directory must be specified!");

        if (!netAddress || !*netAddress)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_IP, "Network address must be specified!");

        StringBuffer sDir(directory);
        const char pathSepChar = osType == OS_WINDOWS ? '\\' : '/';
        if (*directory != pathSepChar)
            sDir.insert(0, osType == OS_WINDOWS ? "\\" : "/");

        if (osType == OS_WINDOWS)
            sDir.replace(':', '$');

        if (*fileName)
        {
            const unsigned int dirLen = sDir.length();
            if (*fileName && *(sDir.str() + dirLen - 1) != pathSepChar)
                sDir.append(pathSepChar);
        }

        //access remote path as \\ip\dir\file if we are running on windows, otherwise as //ip/dir/file
        sDir.replace(pathSepChar == '/' ? '\\' : '/', pathSepChar);

        StringBuffer uncPath;
        uncPath.append(pathSepChar).append(pathSepChar).append(netAddress).append(sDir).append(fileName);

        if (stricmp(fileType, "log") == 0)
        {
            StringBuffer url("/WsTopology/TpLogFile/");
            if (bCluster)
                url.appendf("%s?Name=%s&Type=tpcomp_log", compType, uncPath.str());
            else
                url.appendf("%s?Name=%s&Type=tpcomp_log&Reversely=true&FilterType=5&LastRows=300", compType, uncPath.str());
            resp.setRedirectUrl(url.str());
        }
        else
        {
            Owned<IFile> pFile = createIFile(uncPath.str());

            if (!pFile->exists())
            {
                if (!stricmp(fileType, "cfg") && !stricmp(compType, "DfuServerProcess"))
                {
                    uncPath.clear().append(pathSepChar).append(pathSepChar).append(netAddress).append(sDir).append("dfuserver.ini");
                    pFile.setown(createIFile(uncPath.str()));
                    if (!pFile->exists())
                        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "The file '%s' does not exist!", uncPath.str());
                }
                else if (!stricmp(fileType, "cfg") && !stricmp(compType, "ThorCluster"))
                {
                    uncPath.clear().append(pathSepChar).append(pathSepChar).append(netAddress).append(sDir).append("thor.ini");
                    pFile.setown(createIFile(uncPath.str()));
                    if (!pFile->exists())
                        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "The file '%s' does not exist!", uncPath.str());
                }
                else
                {
                    throw MakeStringException(ECLWATCH_INVALID_FILE_TYPE, "The file '%s' does not exist!", uncPath.str());
                }
            }

            Owned<IFileIO> pFileIO = pFile->openShared(IFOread, IFSHfull);
            if (!pFileIO)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "The file '%s' could not be opened!", uncPath.str());

            offset_t fileSize = pFile->size();

            const long FILESIZELIMIT = 10000000; //In case of a huge file
            if (fileSize > FILESIZELIMIT)
                fileSize = FILESIZELIMIT;

            MemoryBuffer buf;
            size32_t nRead = read(pFileIO, 0, (size32_t)fileSize, buf);
            if (nRead != fileSize)
                throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", uncPath.str());

            const char* pchBuf = buf.toByteArray();
            const char* pchExt = strrchr(fileName, '.');
            if (!pchExt || (pchBuf[0] != '<') || stricmp(++pchExt, "xml"))
            {
                resp.setFileContents_mimetype(HTTP_TYPE_TEXT_PLAIN);
                resp.setFileContents(buf);
            }
            else
            {
                const char* plainText = req.getPlainText();
                if (plainText && (!stricmp(plainText, "yes")))
                {
                    if (!checkFileExists("xslt/xmlformatter.xsl"))
                        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Could not find stylesheet xmlformatter.xsl");

                    Owned<IXslProcessor> proc  = getXslProcessor();
                    Owned<IXslTransform> trans = proc->createXslTransform();

                    trans->setXmlSource(pchBuf, buf.length());
                    trans->loadXslFromFile("xslt/xmlformatter.xsl");
                    
                    StringBuffer htmlBuf;
                    trans->transform(htmlBuf);

                    MemoryBuffer buf0;
                    buf0.append(htmlBuf.str());
                    resp.setFileContents(buf0);
                    resp.setFileContents_mimetype(HTTP_TYPE_TEXT_HTML);
                }
                else
                {
                    //if this is an xml file and is missing the xml tag at the top then add it
                    if (strncmp(pchBuf, "<?", 2))
                    {
                        const char* header="<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";
                        const unsigned int headerLen = strlen(header);

                        buf.insertDirect(0, headerLen);
                        buf.writeDirect(0, headerLen, header);
                    }
                    else
                    {
                        const char* pBuf = strstr(pchBuf+2, "?>"); 
                        if (pBuf)
                        {
                            const char* header="<?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";
                            const unsigned int headerLen = strlen(header);

                            unsigned pos = pBuf - pchBuf + 2;
                            buf.insertDirect(pos, headerLen);
                            buf.writeDirect(pos, headerLen, header);
                        }
                    }
                    resp.setFileContents(buf);
                    resp.setFileContents_mimetype(HTTP_TYPE_APPLICATION_XML);
                }
            }
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpThorStatus(IEspContext &context, IEspTpThorStatusRequest &req, IEspTpThorStatusResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpThorStatus: Permission denied.");

        const char* name   = req.getName();
        if (!name || !*name)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Thor name not specified.");

        CCluster conn(name);
        IPropertyTree *root = conn->queryRoot();
        if (!root)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Failed to access Thor status.");

        resp.setName( name );
        resp.setQueue( root->queryProp("@queue") );
        resp.setGroup( root->queryProp("@nodeGroup") );
        resp.setThorMasterIPAddress( root->queryProp("@node"));
        resp.setPort( root->getPropInt("@mpport", -1));
        resp.setStartTime( root->queryProp("@started") );
        const char *LogFile = root->queryProp("LogFile");
        if (LogFile && *LogFile)
            resp.setLogFile( LogFile );
        const char *wuid = root->queryProp("WorkUnit");
        if (wuid && *wuid)
        {
            resp.setWuid( wuid );
            const char *graph = root->queryProp("@graph");
            if (graph && *graph)
            {
                resp.setGraph( graph );
                int subgraph = root->getPropInt("@subgraph", -1);
                if (subgraph > -1)
                {
                    resp.setSubGraph( subgraph );
                }
                int duration = root->getPropInt("@sg_duration", -1);
                if (duration > -1)
                {
                    resp.setSubGraphDuration( duration );
                }
            }
        }
        resp.setAutoRefresh(THORSTATUSDETAILS_REFRESH_MINS);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpGetServicePlugins(IEspContext &context, IEspTpGetServicePluginsRequest &req, IEspTpGetServicePluginsResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpGetServicePlugins: Permission denied.");

        IArrayOf<IEspTpEspServicePlugin> plugins;
        ForEachItemIn(i,espServicePlugins)
        {
            IEspTpEspServicePlugin& servicePlugin = espServicePlugins.item(i);

            Owned<IEspTpEspServicePlugin> plugin= createTpEspServicePlugin();
            plugin->setWidgetName(servicePlugin.getWidgetName());
            plugin->setShortName(servicePlugin.getShortName());
            plugin->setLongName(servicePlugin.getLongName());
            plugin->setFolderName(servicePlugin.getFolderName());
            plugins.append(*plugin.getClear());
        }
        resp.setPlugins(plugins);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsTopologyEx::onTpDropZoneQuery(IEspContext &context, IEspTpDropZoneQueryRequest &req, IEspTpDropZoneQueryResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_TOPOLOGY_ACCESS_DENIED, "WsTopology::TpDropZoneQuery: Permission denied.");

        m_TpWrapper.getTpDropZones(context.getClientVersion(), req.getName(), req.getECLWatchVisibleOnly(), resp.getTpDropZones());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}
