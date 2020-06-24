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

#ifndef _ESPWIZ_ws_topology_HPP__
#define _ESPWIZ_ws_topology_HPP__

#include "ws_topology_esp.ipp"
#include "environment.hpp"
#include "dasds.hpp"

#include "TpWrapper.hpp"

enum GetLogOptions
{
    GLOFirstPage = 0,
    GLOFirstNRows = 1,
    GLOLastNHours = 2,
    GLOLastPage = 3,
    GLOGoToPage = 4,
    GLOLastNRows = 5,
    GLOTimeRange = 6,
};

struct ReadLog
{
    StringBuffer startDate;
    StringBuffer endDate;
    unsigned lastHours;
    unsigned ltBytes;
    bool hasTimestamp;
    bool loadContent;
    unsigned firstRows;
    unsigned lastRows;
    unsigned pageNumber;
    GetLogOptions filterType;
    bool reverse;
    offset_t fileSize;
    offset_t pageFrom;
    offset_t pageTo;
    unsigned prevPage;
    unsigned nextPage;
    unsigned TotalPages;
    unsigned logfields;
    unsigned columnNumDate;
    unsigned columnNumTime;
    StringArray logFieldNames;
    bool includeLogFieldNameLine = true;
    unsigned readLogFrom = 0;
};

class CWsTopologySoapBindingEx : public CWsTopologySoapBinding
{
public:
    CWsTopologySoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsTopologySoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
        IPropertyTree *folder = ensureNavFolder(data, "Topology", NULL, NULL, false, 4);
        ensureNavLink(*folder, "Target Clusters", "/WsTopology/TpTargetClusterQuery?Type=ROOT", "View details about target clusters and optionally run preflight activities", NULL, NULL, 1);
        ensureNavLink(*folder, "Cluster Processes", "/WsTopology/TpClusterQuery?Type=ROOT", "View details about clusters and optionally run preflight activities", NULL, NULL, 2);
        ensureNavLink(*folder, "System Servers", "/WsTopology/TpServiceQuery?Type=ALLSERVICES", "View details about System Support Servers clusters and optionally run preflight activities", NULL, NULL, 3);
    }
};



class CWsTopologyEx : public CWsTopology
{
private:

    CTpWrapper m_TpWrapper;
    bool         m_displayRoxieCluster;
    StringBuffer                    m_preflightProcessFilter;
    unsigned int                    m_cpuThreshold;
    unsigned int                    m_memThreshold;
    unsigned int                    m_diskThreshold;
    bool                                m_bMemThresholdIsPercentage;
    bool                                m_bDiskThresholdIsPercentage;
    bool                                m_bEncapsulatedSystem;
    bool                                m_enableSNMP;
    StringAttr                          defaultTargetClusterName;
    StringAttr                          defaultTargetClusterPrefix;
    IArrayOf<IEspTpEspServicePlugin>    espServicePlugins;

    void getThorXml(const char *cluster,StringBuffer& strBuff);
    void getThorLog(const char *cluster,MemoryBuffer& returnbuff);
    //void getThorLog(StringBuffer logname,StringBuffer& returnbuff);
    int loadFile(const char* fname, int& len, unsigned char* &buf, bool binary=true);
    void readLogFile(const char * logname, IFile* pFile, ReadLog& readLogReq, StringBuffer& returnbuff);
    void readLogFileToArray(const char * logname, OwnedIFileIO rIO, ReadLog& readLogReq, StringArray& returnbuff);
    bool readLogLineID(const char* pTr, unsigned long& lineID);
    bool readLogTime(char* pTr, int start, int length, CDateTime& dt);
    bool findTimestampAndLT(const char * logname, IFile* pFile, ReadLog& readLogReq, CDateTime& latestLogTime);
    unsigned findLineTerminator(const char* dataPtr, const size32_t dataSize);
    bool isLineTerminator(const char* dataPtr, const size32_t dataSize, unsigned ltLength);
    char* readALogLine(char* dataPtr, size32_t& dataSize, ReadLog& readLogReq, StringBuffer& logLine, bool& hasLineTerminator);
    void addALogLine(offset_t& readFrom, unsigned& locationFlag, const char *dataRow, ReadLog& readLogReq, StringBuffer& logTimeString, StringArray& returnbuff);
    void readTpLogFileRequest(IEspContext &context, const char* fileName, IFile* rFile, IEspTpLogFileRequest  &req, ReadLog& readLogReq);
    void setTpLogFileResponse(IEspContext &context, ReadLog& readLogReq, const char* fileName,
                                         const char* fileType, StringBuffer& returnbuf, IEspTpLogFileResponse &resp);
    void readTpLogFile(IEspContext &context,const char* fileName, const char* fileType, IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp);

    void loadThresholdValue(IPropertyTree* pServiceNode, const char* attrName, unsigned int& thresholdValue, 
                                    bool& bThresholdIsPercentage);

    StringBuffer& getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage);
    void readLogMessageFields(char* logStart, size32_t bytesRemaining, ReadLog& readLogReq);
    const char* readLastLogDateTimeFromContentBuffer(StringBuffer& content, ReadLog& readLogReq, CDateTime& latestLogTime);
    bool readLastLogDateTime(const char* logName, IFileIO* rIO, size32_t fileSize, size32_t readSize,
        ReadLog& readLogReq, CDateTime& latestLogTime);
    void readLogField(const char* lineStart, const unsigned lineLength, const unsigned columnNum,
        const unsigned columnLength, StringBuffer& logField);
    bool readLogDateTimeFromLogLine(const char* lineStart, const unsigned lineLength, ReadLog& readLogReq,
        CDateTime& dt, StringBuffer& logFieldTime);
    void readLastNRowsToArray(const char* logName, OwnedIFileIO rIO, ReadLog& readLogReq, StringArray& returnBuf);

public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsTopologyEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onTpClusterQuery(IEspContext &context, IEspTpClusterQueryRequest &req, IEspTpClusterQueryResponse &resp);

    bool onTpListTargetClusters(IEspContext &context, IEspTpListTargetClustersRequest &req, IEspTpListTargetClustersResponse &resp);

    bool onTpTargetClusterQuery(IEspContext &context, IEspTpTargetClusterQueryRequest &req, IEspTpTargetClusterQueryResponse &resp);

    bool onTpLogicalClusterQuery(IEspContext &context, IEspTpLogicalClusterQueryRequest &req, IEspTpLogicalClusterQueryResponse &resp);

    bool onTpGroupQuery(IEspContext &context, IEspTpGroupQueryRequest &req, IEspTpGroupQueryResponse &resp);

    bool onTpClusterInfo(IEspContext &context, IEspTpClusterInfoRequest &req, IEspTpClusterInfoResponse& resp);

    bool onTpMachineQuery(IEspContext &context, IEspTpMachineQueryRequest &req, IEspTpMachineQueryResponse &resp);

    bool onTpMachineInfo(IEspContext &context, IEspTpMachineInfoRequest &req, IEspTpMachineInfoResponse &resp);

    bool onTpDropZoneQuery(IEspContext &context, IEspTpDropZoneQueryRequest &req, IEspTpDropZoneQueryResponse &resp);

    bool onTpSetMachineStatus(IEspContext &context,IEspTpSetMachineStatusRequest  &req, IEspTpSetMachineStatusResponse &resp);

    bool onTpSwapNode(IEspContext &context,IEspTpSwapNodeRequest  &req, IEspTpSwapNodeResponse &resp);

    bool onTpXMLFile(IEspContext &context,IEspTpXMLFileRequest  &req, IEspTpXMLFileResponse &resp);

    bool onTpLogFile(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp);

    bool onSystemLog(IEspContext &context,IEspSystemLogRequest  &req, IEspSystemLogResponse &resp);

    bool onTpLogFileDisplay(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp);

    bool onTpServiceQuery(IEspContext &context, IEspTpServiceQueryRequest &req, IEspTpServiceQueryResponse &resp);
    bool onTpGetServicePlugins(IEspContext &context, IEspTpGetServicePluginsRequest &req, IEspTpGetServicePluginsResponse &resp);
    bool onTpGetComponentFile(IEspContext &context, IEspTpGetComponentFileRequest &req, IEspTpGetComponentFileResponse &resp);

    bool onTpThorStatus(IEspContext &context, IEspTpThorStatusRequest &req, IEspTpThorStatusResponse &resp);
};




#endif //_ESPWIZ_ws_topology_HPP__

