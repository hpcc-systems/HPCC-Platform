/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        IPropertyTree *folder = ensureNavFolder(data, "Topology", "Topology", NULL, false, 4);
        ensureNavLink(*folder, "Target Clusters", "/WsTopology/TpTargetClusterQuery?Type=ROOT", "Target Clusters", NULL, NULL, 1);
        ensureNavLink(*folder, "Cluster Processes", "/WsTopology/TpClusterQuery?Type=ROOT", "Cluster Processes", NULL, NULL, 2);
        ensureNavLink(*folder, "System Servers", "/WsTopology/TpServiceQuery?Type=ALLSERVICES", "System Servers", NULL, NULL, 3);
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
};




#endif //_ESPWIZ_ws_topology_HPP__

