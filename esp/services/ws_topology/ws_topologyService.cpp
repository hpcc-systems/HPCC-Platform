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
static const long AVERAGELOGROWSIZE = 2000;
const char* TEMPZIPDIR = "tempzipfiles";

void CWsTopologyEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;

    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "No Connection to Dali server is active. Please specify a Dali server in the configuration file.");
    }
    m_envFactory.setown( getEnvironmentFactory() );

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

    m_enableSNMP = false;
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to Swap Node. Permission denied.");

        //another client (like configenv) may have updated the constant environment so reload it
        m_envFactory->validateCache();

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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to Set Machine Status. Permission denied.");

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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to get Log File. Permission denied.");

        MemoryBuffer membuff;
        const char* name = req.getName();
        const char* type = req.getType();
        if (type && *type && ((strcmp(type,"thormaster_log") == 0) || (strcmp(type,"tpcomp_log") == 0)))
        {
            ReadLog readLogReq;
            readLogReq.pageNumber = req.getPageNumber();
            readLogReq.startDate = req.getStartDate();
            readLogReq.endDate = req.getEndDate();
            readLogReq.firstRows = req.getFirstRows();
            readLogReq.lastRows = req.getLastRows();
            readLogReq.filterType = req.getFilterType();
            readLogReq.reverse = req.getReversely();
            readLogReq.zip = req.getZip();
            readLogReq.fileSize = -1;
            readLogReq.prevPage = -1;
            readLogReq.nextPage = -1;

            int lastHours = -1;
            if (readLogReq.filterType == 2) //in the last n hours
            {
                if (readLogReq.startDate.length() < 19 || readLogReq.endDate.length() < 19)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invlid 'Hours' field.");

                char fromHour[3], toHour[3];
                fromHour[0] = readLogReq.startDate.charAt(11);
                fromHour[1] = readLogReq.startDate.charAt(12);
                toHour[0] = readLogReq.endDate.charAt(11);
                toHour[1] = readLogReq.endDate.charAt(12);
                lastHours = atoi(toHour)-atoi(fromHour);
            }
            else if (readLogReq.filterType == 6) //from date/time to date/time
            {
                if (readLogReq.startDate.length() < 19 && readLogReq.endDate.length() < 19)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invlid 'Date' field.");
            }

            bool hasDate = false;
            StringBuffer startDate, endDate, logname, returnbuff;

            if (strcmp(type,"thormaster_log"))
            {
                logname = name;
            }
            else
            {
                logname.append(CCluster(name)->queryRoot()->queryProp("LogFile"));
            }

            Owned<IFile> rFile = createIFile(logname.str());
            if (!rFile || !rFile->exists())
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());

            readLogReq.fileSize = rFile->size();
            readLogReq.TotalPages = (int) ceil(((double)readLogReq.fileSize)/LOGFILESIZELIMIT);
            if (readLogReq.filterType == 4) //by page number: 0 to n-1
            {
                if (readLogReq.pageNumber > readLogReq.TotalPages - 1)
                    readLogReq.pageNumber = readLogReq.TotalPages - 1;
            }

            if (!req.getLoadData())
            {
                hasDate = true;
                if (readLogReq.startDate.length() > 0)
                    resp.setStartDate(readLogReq.startDate.str());
                if (readLogReq.endDate.length() > 0)
                    resp.setEndDate(readLogReq.endDate.str());

                if (readLogReq.filterType == 0 || readLogReq.filterType == 4)
                {
                    offset_t readFrom = LOGFILESIZELIMIT * readLogReq.pageNumber;
                    if (readFrom > readLogReq.fileSize) 
                        readFrom = 0;

                    offset_t fileSize = readLogReq.fileSize - readFrom;
                    if (fileSize > LOGFILESIZELIMIT)
                    {
                        readLogReq.nextPage = readLogReq.pageNumber + 1;
                    }
                    if (readFrom > 0)
                    {
                        readLogReq.prevPage = readLogReq.pageNumber - 1;
                    }
                }
                else if (readLogReq.filterType == 3) //Last page
                {
                    int pageCount = 1;
                    offset_t readFrom = 0;
                    offset_t fileSize = readLogReq.fileSize;
                    while (fileSize > LOGFILESIZELIMIT)
                    {
                        fileSize -= LOGFILESIZELIMIT;
                        readFrom += LOGFILESIZELIMIT;
                        pageCount++;
                    }
                    if (readFrom > 0)
                    {
                        readLogReq.prevPage = pageCount - 2;
                    }

                    readLogReq.pageNumber = pageCount - 1;
                }
                else if (readLogReq.filterType == 6) //from date/time to date/time
                {
                    if (readLogReq.startDate.length() > 18)
                    {
                        CDateTime startdt;
                        StringBuffer fromStr = readLogReq.startDate;
                        fromStr.setCharAt(10, 'T');
                        startdt.setString(fromStr.str(), NULL, true);

                        StringBuffer startStr;
                        unsigned year, month, day, hour, minute, second, nano;
                        startdt.getDate(year, month, day, true);
                        startdt.getTime(hour, minute, second, nano, true);
                        startStr.appendf("%02d/%02d/%4d %02d:%02d:%02d", month, day, year, hour, minute, second);

                        startDate.append(startStr.str());
                    }

                    if (readLogReq.endDate.length() > 18)
                    {
                        CDateTime enddt;
                        StringBuffer toStr = readLogReq.endDate;
                        toStr.setCharAt(10, 'T');
                        enddt.setString(toStr.str(), NULL, true);

                        StringBuffer endStr;
                        unsigned year, month, day, hour, minute, second, nano;
                        enddt.getDate(year, month, day, true);
                        enddt.getTime(hour, minute, second, nano, true);
                        endStr.appendf("%02d/%02d/%4d %02d:%02d:%02d", month, day, year, hour, minute, second);

                        endDate.append(endStr.str());
                    }
                }
            }
            else if (type && *type && strcmp(type,"thormaster_log"))
            {
                readLogFile(logname, readLogReq, startDate, endDate, hasDate, returnbuff);
            }
            else
            {
                readLogFile(logname, readLogReq, startDate, endDate, hasDate, returnbuff);
            }

            resp.setHasDate(hasDate);
            if (lastHours > 0)
                resp.setLastHours(lastHours);
            if (startDate.length() > 0)
                resp.setStartDate(startDate.str());
            if (endDate.length() > 0)
                resp.setEndDate(endDate.str());
            if (readLogReq.lastRows > 0)
                resp.setLastRows(readLogReq.lastRows);
            if (readLogReq.firstRows > 0)
                resp.setFirstRows(readLogReq.firstRows);

            double version = context.getClientVersion();
            if (version > 1.05)
            {       
                resp.setTotalPages( readLogReq.TotalPages );
            }

            if (returnbuff.length() > 0)
            {
                if (returnbuff.length() > LOGFILESIZELIMIT)
                {
                    StringBuffer returnbuff0;
                    returnbuff0.append(returnbuff.str(), 0, LOGFILESIZELIMIT);
                    returnbuff0.appendf("\r\n****** Warning: cannot display all. The page size is limited to %ld bytes. ******", LOGFILESIZELIMIT);
                    resp.setLogData(returnbuff0.str());
                    if (readLogReq.filterType == 1)
                    {
                        readLogReq.pageFrom = 0;
                        readLogReq.pageTo = LOGFILESIZELIMIT;
                    }
                    else if ((readLogReq.filterType == 2) || (readLogReq.filterType == 5))
                    {
                        readLogReq.pageFrom = readLogReq.fileSize - returnbuff.length();
                        readLogReq.pageTo = readLogReq.pageFrom + LOGFILESIZELIMIT;
                    }
                    else if (readLogReq.filterType == 6)
                    {
                        readLogReq.pageTo = readLogReq.pageFrom + LOGFILESIZELIMIT;
                    }
                }
                else
                {
                    resp.setLogData(returnbuff.str());
                    if (readLogReq.filterType == 1)
                    {
                        readLogReq.pageFrom = 0;
                        readLogReq.pageTo = returnbuff.length();
                    }
                    else if ((readLogReq.filterType == 2) || (readLogReq.filterType == 5))
                    {
                        readLogReq.pageFrom = readLogReq.fileSize - returnbuff.length();
                        readLogReq.pageTo = readLogReq.fileSize;
                    }
                    else if (readLogReq.filterType == 6)
                    {
                        readLogReq.pageTo = readLogReq.pageFrom + returnbuff.length();
                    }
                }
            }
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

            resp.setName(req.getName());
            resp.setType(type);
            resp.setFilterType(readLogReq.filterType);
            resp.setReversely(readLogReq.reverse);
            resp.setZip(readLogReq.zip);
        }
        else if (type && *type && (strcmp(type,"xml") == 0))
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to get Log File. Permission denied.");

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
            throw MakeStringException(ERRORID_ECLWATCH_TOPOLOGY+109,"The data cannot be compressed.");
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to get Configuration File. Permission denied.");

        StringBuffer strBuff, xmlBuff;
        strBuff.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>");
        getThorXml(req.getName(),xmlBuff);
        strBuff.append(xmlBuff);
        
        MemoryBuffer membuff;
        membuff.setBuffer(strBuff.length(), (void*)strBuff.toCharArray());

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

void CWsTopologyEx::readLogFile(StringBuffer logname, ReadLog& readLogReq, StringBuffer& startDate, StringBuffer& endDate,  
                                         bool& hasDate, StringBuffer& returnbuff)
{
    Owned<IFile> rFile = createIFile(logname.str());
    if (!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());

    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot read file %s.",logname.str());

    if (readLogReq.filterType == 1 && readLogReq.firstRows < 1) //last n rows
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "'First' field should be defined.");

    if (readLogReq.filterType == 5 && readLogReq.lastRows < 1) //last n rows
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "'Last' field should be defined.");

    readLogReq.fileSize = rFile->size();

    if (readLogReq.filterType == 0 || readLogReq.filterType == 4) //by page number: 0 to n-1
    {
        offset_t readFrom = LOGFILESIZELIMIT * readLogReq.pageNumber;
        if (readFrom > readLogReq.fileSize) 
            readFrom = 0;

        offset_t fileSize = readLogReq.fileSize - readFrom;
        if (fileSize > LOGFILESIZELIMIT)
        {
            fileSize = LOGFILESIZELIMIT;
            readLogReq.nextPage = readLogReq.pageNumber + 1;
        }
        if (readFrom > 0)
        {
            readLogReq.prevPage = readLogReq.pageNumber - 1;
        }

        returnbuff.ensureCapacity((unsigned)fileSize);
        returnbuff.setLength((unsigned)fileSize);

        size32_t nRead = rIO->read(readFrom, (size32_t) fileSize, (char*)returnbuff.str());
        if (nRead != fileSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

        readLogReq.pageFrom = (long) readFrom;
        readLogReq.pageTo = (long) (readFrom + nRead);

        if (readFrom < 1)
        {
            if (nRead > 28)
            {
                char* pTr = (char*) returnbuff.str();
                CDateTime dt;
                hasDate = readLogTime(pTr, 9, 19, dt);
            }
        }
        else
        {
            offset_t fileSize0 = 29;
            StringBuffer returnbuff0;
            returnbuff0.ensureCapacity((unsigned)fileSize0);
            returnbuff0.setLength((unsigned)fileSize0);

            size32_t nRead0 = rIO->read(0, (size32_t)fileSize0, (char*)returnbuff0.str());
            if (nRead0 > 28)
            {
                char* pTr = (char*) returnbuff0.str();
                CDateTime dt;
                hasDate = readLogTime(pTr, 9, 19, dt);
            }
        }
    }
    else if (readLogReq.filterType == 3) //Last page
    {
        int pageCount = 1;
        offset_t readFrom = 0;
        offset_t fileSize = readLogReq.fileSize;
        while (fileSize > LOGFILESIZELIMIT)
        {
            fileSize -= LOGFILESIZELIMIT;
            readFrom += LOGFILESIZELIMIT;
            pageCount++;
        }
        if (readFrom > 0)
        {
            readLogReq.prevPage = pageCount - 2;
        }

        returnbuff.ensureCapacity((unsigned)fileSize);
        returnbuff.setLength((unsigned)fileSize);

        size32_t nRead = rIO->read(readFrom, (size32_t)fileSize, (char*)returnbuff.str());
        if (nRead != fileSize)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

        readLogReq.pageFrom = readFrom;
        readLogReq.pageTo = readFrom + nRead;

        offset_t fileSize0 = 29;
        StringBuffer returnbuff0;
        returnbuff0.ensureCapacity((unsigned)fileSize0);
        returnbuff0.setLength((unsigned)fileSize0);

        size32_t nRead0 = rIO->read(0, (size32_t)fileSize0, (char*)returnbuff0.str());
        if (nRead0 > 28)
        {
            char* pTr = (char*) returnbuff0.str();
            CDateTime dt;
            hasDate = readLogTime(pTr, 9, 19, dt);
        }
    }
    else
    {
        if (!readLogReq.zip)
        {
            StringArray rowList;
            readLogFile(logname, rIO, readLogReq, hasDate, rowList);
            if (rowList.length() > 0)
            {
                for (unsigned i = 0; i < rowList.length(); i++)
                {
                    StringBuffer item = rowList.item(i);
                    if (readLogReq.filterType != 5)
                    {
                        if (!readLogReq.reverse)
                        {
                            returnbuff.append(item);
                        }
                        else
                        {
                            returnbuff.insert(0, item);
                        }
                    }
                    else
                    {
                        if (readLogReq.reverse)
                        {
                            returnbuff.append(item);
                        }
                        else
                        {
                            returnbuff.insert(0, item);
                        }
                    }
                }
            }
        }
    }
}

void CWsTopologyEx::readLogFile(StringBuffer logname, OwnedIFileIO rIO, ReadLog& readLogReq, bool& hasDate, StringArray& returnbuff)
{
    bool returnNow = false;

    int ltBytes = 1; //how many bytes used for Line Terminator
    bool firstChuck = true;
    bool lastChuck = false;
    bool hasLineID = false;

    offset_t readFrom = 0;
    offset_t readFrom0 = 0;
    unsigned locationFlag = 0; //Not in the area to be retrieved
    StringBuffer dataLeft;

    offset_t fileSize = readLogReq.fileSize;
    while (fileSize > 0)
    {
        StringBuffer dataBuffer;
        lastChuck = readToABuffer(logname, rIO, fileSize, readFrom, dataLeft, dataBuffer);

        offset_t readPtr = 0;
        offset_t totalBytes = dataBuffer.length();
        char* pTr = (char*) dataBuffer.str();

        long firstOrLastRowID = -1;
        if (firstChuck) //first time
        {
            firstChuck = false;

            ltBytes = checkLineTerminator(pTr);

            //if rowID < 0, no row id found 
            long rowID = readLogLineID(pTr);
            if ((totalBytes > 8) && (rowID > -1))
                hasLineID = true;

            if (totalBytes > 28)
            {
                CDateTime dt;
                hasDate = readLogTime(pTr, 9, 19, dt);
            }

            if (hasLineID && (readLogReq.filterType == 1)) //first n rows
            {
                firstOrLastRowID = rowID;
            }
            else if (readLogReq.filterType == 5) //last n rows
            {
                offset_t estimateSize = AVERAGELOGROWSIZE*readLogReq.lastRows;
                if (readLogReq.fileSize > 5 * estimateSize) //try a short cut since the file is too big
                {
                    int n = 1;

                    dataLeft.clear();
                    dataBuffer.clear();

                    fileSize = readLogReq.fileSize;
                    readFrom = fileSize-estimateSize;
                    fileSize = estimateSize;
                    readToABuffer(logname, rIO, fileSize, readFrom, dataLeft, dataBuffer);

                    readPtr = 0;
                    totalBytes = dataBuffer.length();
                    pTr = (char*) dataBuffer.str();

                    //Find out a start point to check the data rows
                    bool bLT = false;
                    while (!bLT && (readPtr < totalBytes))
                    {
                        bLT = readLineTerminator(pTr, ltBytes);
                        pTr++;
                        readPtr++;
                    }

                    //Find out the row number of the last data row
                    StringBuffer dataRow;
                    char* pTr1 = pTr;
                    offset_t readPtr1 = readPtr;
                    
                    bLT = false;
                    while (readPtr1 < totalBytes)
                    {
                        dataRow.append(pTr1[0]);

                        //Check if this is the end of the row
                        bLT = readLineTerminator(pTr1, ltBytes);
                        if (!bLT)
                        {
                            pTr1++;
                            readPtr1++;
                            continue;
                        }

                        if (dataRow.length() > 8)
                        {
                            long id = readLogLineID((char*) dataRow.str());
                            if (id > -1)
                                firstOrLastRowID = id;
                        }

                        dataRow.clear();
                        pTr1++;
                        readPtr1++;
                    }

                    if (dataRow.length() > 8)
                    {
                        long id = readLogLineID((char*) dataRow.str());
                        if (id > -1)
                            firstOrLastRowID = id;
                    }
                }
            }
        }

        char* pTr0 = pTr;
        StringBuffer dataRow;
        while (totalBytes - readPtr > 27)
        {
            //Try to read a line
            bool bLT = false;
            while (readPtr < totalBytes)
            {
                dataRow.append(pTr[0]);

                //Check if this is the end of the row
                bLT = readLineTerminator(pTr, ltBytes);
                if (!bLT)
                {
                    pTr++;
                    readPtr++;
                    continue;
                }

                if (dataRow.length() > 0)
                {
                    addALogLine(readFrom0, locationFlag, firstOrLastRowID, dataRow, readLogReq, returnbuff);
                    dataRow.clear();
                }

                readPtr++;
                pTr++;
                if (readPtr < totalBytes)
                    pTr0 = pTr;
                else
                    pTr0 = NULL;
                break;
            }

            if (locationFlag > 1)
                break;
        }
        
        if (locationFlag > 1)
            break;

        dataLeft.clear();
        if (pTr0)
            dataLeft.append(pTr0);

        if (lastChuck)
        {
            addALogLine(readFrom0, locationFlag, firstOrLastRowID, dataLeft, readLogReq, returnbuff);
            break;
        }
    }

    return;
}

bool CWsTopologyEx::readToABuffer(StringBuffer logname, OwnedIFileIO rIO, offset_t& fileSize, offset_t& readFrom, 
                                             StringBuffer dataLeft, StringBuffer& dataBuffer)
{
    bool lastPage = true;
    offset_t readSize = fileSize;
    if (readSize > LOGFILESIZELIMIT)
    {
        readSize = LOGFILESIZELIMIT;
        lastPage = false;
    }

    StringBuffer buf;
    buf.ensureCapacity((unsigned)readSize);
    buf.setLength((unsigned)readSize);

    size32_t nRead = rIO->read(readFrom, (size32_t)readSize, (char*)buf.str());
    if (nRead != readSize)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Failed to read file %s.", logname.str());

    dataBuffer.clear();
    if (dataLeft.length() > 0)
        dataBuffer.append(dataLeft);

    dataBuffer.append(buf);

    readFrom += nRead;
    fileSize -= nRead;

    return lastPage;
}

long CWsTopologyEx::readLogLineID(char* pTr)
{
    long id = -1;
    StringBuffer lineID;

    int i = 0;
    while (i < 8)
    {
        if ((pTr[i] < 48) || ((pTr[i] > 57) && (pTr[i] < 65)) || (pTr[i] > 70))
        {
            lineID.clear();
            break;
        }

        lineID.append(pTr[i]);
        i++;
    }

    if (lineID.length() > 0)
    {
        id = strtol(lineID, NULL, 16);
    }

    return id;
}

bool CWsTopologyEx::readLogTime(char* pTr, int start, int length, CDateTime& dt)
{
    bool bRet = false;
    try
    {
        char str[20];
        memset(str, 0, 20);
        strncpy(str, pTr+start, length);

        StringBuffer strBuf = str;
        strBuf.setCharAt(10, 'T');
        dt.setString(strBuf.str(), NULL, true);
        bRet = true;
    }
    catch(IException* e)
    {   
        e->Release();
    }

    return bRet;
}

int CWsTopologyEx::checkLineTerminator(char* pTr)
{
    char* ppTr = pTr;
    while(ppTr)
    {
        if (ppTr[0] == '\r' && ppTr[1] == '\n')
        {
            return 2;
        }

        if (ppTr[0] == '\r' || ppTr[0] == '\n')
        {
            return 1;
        }

        ppTr++;
    }

    return 0;
}

bool CWsTopologyEx::readLineTerminator(char* pTr, int& byteCount)
{
    bool bFoundLineEnd = false;
    if (byteCount > 1)
    {
        if (pTr[0] == '\n') 
        {
            bFoundLineEnd = true;
        }
    }
    else if (pTr[0] == '\r' || pTr[0] == '\n')
    {
        bFoundLineEnd = true;
    }

    return bFoundLineEnd;
}

void CWsTopologyEx::addALogLine(offset_t& readFrom, unsigned& locationFlag, long firstOrLastRowID, StringBuffer dataRow, ReadLog& readLogReq, StringArray& returnbuff)
{
    long rowID = readLogLineID((char*)dataRow.str());
    if (readLogReq.filterType == 1) //first n rows
    {
        if (firstOrLastRowID > -1)
        {//there is row id to be used
            locationFlag = 1; //enter the area to be retrieved
            if ((rowID < 0) || (rowID - firstOrLastRowID < (long) readLogReq.firstRows)) //no row ID or the row ID is less than readLogReq.firstRows
            {
                returnbuff.append(dataRow);
            }
            else
            {
                locationFlag = 2; //out of the area to be retrieved
            }
        }
        else
        {
            locationFlag = 1; //enter the area to be retrieved
            returnbuff.append(dataRow); 
            if (returnbuff.length() == readLogReq.firstRows)
                locationFlag = 2; //stop now since we have enough rows
        }
    }
    else if (readLogReq.filterType == 5) //last n rows
    {
        if (firstOrLastRowID > -1)
        {//there is row id to be used
            if ((locationFlag < 1) && (rowID > -1) && (firstOrLastRowID - rowID < (long) readLogReq.lastRows))
                locationFlag = 1;

            if (locationFlag > 0) //Add the rest of rows
                returnbuff.add(dataRow, 0); 
        }
        else
        {
            if (returnbuff.length() == readLogReq.lastRows)
                returnbuff.remove(readLogReq.lastRows - 1);

            returnbuff.add(dataRow, 0); 
            locationFlag = 1; //always in the area to be retrieved, but may be pushed out later
        }
    }
    else
    {
        if (rowID < 0) //row id not found
        {
            if (locationFlag > 0)
            {
                returnbuff.append(dataRow);
            }
            readFrom += dataRow.length();
            return;
        }

        char str[20];
        memset(str, 0, 20);
        dataRow.getChars(9, 28, str);

        if (readLogReq.endDate.length() > 0 && strcmp(str, readLogReq.endDate.str()) > 0)
            locationFlag = 2; //out of the area to be retrieved
        else if (readLogReq.startDate.length() < 1 || strcmp(str, readLogReq.startDate.str()) >= 0)
        {
            returnbuff.append(dataRow);
            if ((locationFlag < 1) && (readLogReq.filterType == 6))
            {
                readLogReq.pageFrom = readFrom;
            }
            readFrom += dataRow.length();
            locationFlag = 1; //enter the area to be retrieved
        }
        else
        {
            readFrom += dataRow.length();
        }
    }
    return;
}

bool CWsTopologyEx::onTpClusterQuery(IEspContext &context, IEspTpClusterQueryRequest &req, IEspTpClusterQueryResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Cluster Query. Permission denied.");

        //another client (like configenv) may have updated the constant environment so reload it
        m_envFactory->validateCache();

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

        resp.setTpClusters(clusters);
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Cluster Query. Permission denied.");

        //another client (like configenv) may have updated the constant environment so reload it
        m_envFactory->validateCache();

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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Cluster Query. Permission denied.");

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = factory->openEnvironmentByFile();
        Owned<IPropertyTree> root = &constEnv->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

        IArrayOf<IEspTpLogicalCluster> clusters;
        Owned<IPropertyTreeIterator> clusterIterator = root->getElements("Software/Topology/Cluster");
        if (clusterIterator->first()) 
        {
            do {
                IPropertyTree &cluster0 = clusterIterator->query();
                StringBuffer processName;
                const char* clusterName0 = cluster0.queryProp("@name");
                if (!clusterName0 || !*clusterName0)
                    continue;

                IEspTpLogicalCluster* pService = createTpLogicalCluster("","");
                pService->setName(clusterName0);
                pService->setLanguageVersion("3.0.0");
                clusters.append(*pService);

            } while (clusterIterator->next());
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Group Query. Permission denied.");

        IArrayOf<IEspTpGroup> Groups;
        m_TpWrapper.getGroupList(Groups);
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to get Cluster Information. Permission denied.");

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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Service Query. Permission denied.");

        double version = context.getClientVersion();
        const char* type = req.getType();
        if (!type || !*type || (strcmp(eqAllServices,type) == 0))
        {
            //another client (like configenv) may have updated the constant environment so reload it
            m_envFactory->validateCache();

            IEspTpServices& ServiceList = resp.updateServiceList();

            m_TpWrapper.getTpDaliServers( ServiceList.getTpDalis() );
            m_TpWrapper.getTpEclServers( ServiceList.getTpEclServers() );
            m_TpWrapper.getTpEclCCServers( ServiceList.getTpEclCCServers() );
            m_TpWrapper.getTpEclAgents( ServiceList.getTpEclAgents() );
            m_TpWrapper.getTpEspServers( ServiceList.getTpEspServers() );   
            m_TpWrapper.getTpDfuServers( ServiceList.getTpDfuServers() );   
            m_TpWrapper.getTpSashaServers( ServiceList.getTpSashaServers() );   
            m_TpWrapper.getTpGenesisServers( ServiceList.getTpGenesisServers() );
            m_TpWrapper.getTpLdapServers( ServiceList.getTpLdapServers() );
            m_TpWrapper.getTpDropZones( ServiceList.getTpDropZones() );
            m_TpWrapper.getTpFTSlaves( ServiceList.getTpFTSlaves() );
            m_TpWrapper.getTpDkcSlaves( ServiceList.getTpDkcSlaves() );

            if (version > 1.15)
            {       
                m_TpWrapper.getTpEclSchedulers( ServiceList.getTpEclSchedulers() );
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
        //another client (like configenv) may have updated the constant environment so reload it
        m_envFactory->validateCache();

        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to do Machine Query. Permission denied.");
        
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
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsTopologyEx::onTpGetComponentFile(IEspContext &context, 
                                                                 IEspTpGetComponentFileRequest &req, 
                                                                 IEspTpGetComponentFileResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Permission denied.");

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
            else if (!stricmp(compType, eqAgentExec))
                fileName = "agentexec.xml";
            else if (!stricmp(compType, eqEsp))
                fileName = "esp.xml";
            else if (!stricmp(compType, eqSashaServer))
                fileName = "sashaconf.xml";
            else if (!stricmp(compType, eqEclAgent))
                fileName = osType==OS_WINDOWS ? "setvars.bat" : NULL;
            else 
            {
                const unsigned int len = strlen(compType);
                if (len>4)
                    if (!strnicmp(compType, "Roxie", 5))
                        compType = "RoxieCluster", fileName = "RoxieTopology.xml", bCluster = true;
                    else if (!strnicmp(compType, "Thor", 4))
                        compType = "ThorCluster", fileName = "thor.xml", bCluster = true;
                    else if (!strnicmp(compType, "Hole", 4))
                        compType = "HoleCluster", fileName = "edata.ini", bCluster = true;
            }
        }
        else
        {
            if (!stricmp(compType, eqDali))
                fileName = "DaServer.log";
            else if (!stricmp(compType, eqDfu))
                fileName = "dfuserver.log";
            else if (!stricmp(compType, eqEclServer))
                fileName = "eclserver.log";
            else if (!stricmp(compType, eqEclCCServer))
                fileName = "eclccserver.log";
            else if (!stricmp(compType, eqEclScheduler))
                fileName = "eclscheduler.log";
            else if (!stricmp(compType, eqEsp))
                fileName = "esp.log";
            else if (!stricmp(compType, eqSashaServer))
                fileName = "saserver.log";
            else if (!stricmp(compType, eqEclAgent))
                fileName = "";
            else 
            {
                const unsigned int len = strlen(compType);
                if (len>4)
                    if (!strnicmp(compType, "Roxie", 5))
                        compType = "RoxieCluster", fileName = "", bCluster = true;
                    else if (!strnicmp(compType, "Thor", 4))
                        compType = "ThorCluster", fileName = "", bCluster = true;
                    else if (!strnicmp(compType, "Hole", 4))
                        compType = "HoleCluster", fileName = "", bCluster = true;
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
            //another client (like configenv) may have updated the constant environment so reload it
            m_envFactory->validateCache();
            Owned<IConstEnvironment> constEnv = m_envFactory->openEnvironmentByFile();
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
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_TOPOLOGY_ACCESS_DENIED, "Failed to access Thor status. Permission denied.");

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
