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

#ifndef _ESPWIZ_ws_topology_HPP__
#define _ESPWIZ_ws_topology_HPP__

#include "ws_topology_esp.ipp"
#include "environment.hpp"
#include "dasds.hpp"

#include "TpWrapper.hpp"

struct ReadLog
{
    StringBuffer startDate;
    StringBuffer endDate;
    unsigned firstRows;
    unsigned lastRows;
    unsigned pageNumber;
    int filterType; //0: first page, 1:first n rows, 2: last n hours; 3: last page, 4: page number, 5: last n rows; 6: from date/time to date/time
    bool reverse;
    bool zip;
    offset_t fileSize;
    offset_t pageFrom;
    offset_t pageTo;
    unsigned prevPage;
    unsigned nextPage;
    unsigned TotalPages;
};

class CWsTopologySoapBindingEx : public CWsTopologySoapBinding
{
public:
    CWsTopologySoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsTopologySoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
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
    Owned<IEnvironmentFactory> m_envFactory;
    StringBuffer                    m_preflightProcessFilter;
    unsigned int                    m_cpuThreshold;
    unsigned int                    m_memThreshold;
    unsigned int                    m_diskThreshold;
    bool                                m_bMemThresholdIsPercentage;
    bool                                m_bDiskThresholdIsPercentage;
    bool                                m_bEncapsulatedSystem;
    bool                                m_enableSNMP;

    void getThorXml(const char *cluster,StringBuffer& strBuff);
    void getThorLog(const char *cluster,MemoryBuffer& returnbuff);
    //void getThorLog(StringBuffer logname,StringBuffer& returnbuff);
    int loadFile(const char* fname, int& len, unsigned char* &buf, bool binary=true);
    void readLogFile(StringBuffer logname, ReadLog& readLogReq, StringBuffer& startDate, StringBuffer& endDate, 
        bool& hasDate, StringBuffer& returnbuff);
    void readLogFile(StringBuffer logname, OwnedIFileIO rIO, ReadLog& readLogReq, bool& hasDate, StringArray& returnbuff);
    bool readToABuffer(StringBuffer logname, OwnedIFileIO rIO, offset_t& fileSize, offset_t& readFrom, StringBuffer dataLeft, 
                                             StringBuffer& dataBuffer);
    long readLogLineID(char* pTr);
    bool readLogTime(char* pTr, int start, int length, CDateTime& dt);
    int checkLineTerminator(char* pTr);
    bool readLineTerminator(char* pTr, int& byteCount);
    void addALogLine(offset_t& readFrom, unsigned& locationFlag, long firstOrLastRowID, StringBuffer dataRow, ReadLog& readLogReq, StringArray& returnbuff);

    void loadThresholdValue(IPropertyTree* pServiceNode, const char* attrName, unsigned int& thresholdValue, 
                                    bool& bThresholdIsPercentage);

    //void getThorXml(const char* ClusterName req.getName(),returnStr);
public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsTopologyEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onTpClusterQuery(IEspContext &context, IEspTpClusterQueryRequest &req, IEspTpClusterQueryResponse &resp);

    bool onTpTargetClusterQuery(IEspContext &context, IEspTpTargetClusterQueryRequest &req, IEspTpTargetClusterQueryResponse &resp);

    bool onTpLogicalClusterQuery(IEspContext &context, IEspTpLogicalClusterQueryRequest &req, IEspTpLogicalClusterQueryResponse &resp);

    bool onTpGroupQuery(IEspContext &context, IEspTpGroupQueryRequest &req, IEspTpGroupQueryResponse &resp);

    bool onTpClusterInfo(IEspContext &context, IEspTpClusterInfoRequest &req, IEspTpClusterInfoResponse& resp);

    bool onTpMachineQuery(IEspContext &context, IEspTpMachineQueryRequest &req, IEspTpMachineQueryResponse &resp);

    bool onTpSetMachineStatus(IEspContext &context,IEspTpSetMachineStatusRequest  &req, IEspTpSetMachineStatusResponse &resp);

    bool onTpSwapNode(IEspContext &context,IEspTpSwapNodeRequest  &req, IEspTpSwapNodeResponse &resp);

    bool onTpXMLFile(IEspContext &context,IEspTpXMLFileRequest  &req, IEspTpXMLFileResponse &resp);

    bool onTpLogFile(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp);

    bool onSystemLog(IEspContext &context,IEspSystemLogRequest  &req, IEspSystemLogResponse &resp);

    bool onTpLogFileDisplay(IEspContext &context,IEspTpLogFileRequest  &req, IEspTpLogFileResponse &resp);

    bool onTpServiceQuery(IEspContext &context, IEspTpServiceQueryRequest &req, IEspTpServiceQueryResponse &resp);

    bool onTpGetComponentFile(IEspContext &context, IEspTpGetComponentFileRequest &req, IEspTpGetComponentFileResponse &resp);

    bool onTpThorStatus(IEspContext &context, IEspTpThorStatusRequest &req, IEspTpThorStatusResponse &resp);
};




#endif //_ESPWIZ_ws_topology_HPP__

