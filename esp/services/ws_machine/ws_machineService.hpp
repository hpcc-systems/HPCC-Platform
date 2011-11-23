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

#ifndef _ESPWIZ_ws_machine_HPP__
#define _ESPWIZ_ws_machine_HPP__

#pragma warning (disable : 4786)

#include "ws_machine_esp.ipp"
#include "environment.hpp"
#include "dasds.hpp"
#include "thirdparty.h"
#include <set>
#include <map>

class CMachineInfoThreadParam;
class CMetricsThreadParam;
class CRemoteExecThreadParam;

struct CEnvironmentConfData
{
    StringBuffer m_configsPath;
    StringBuffer m_executionPath;
    StringBuffer m_runtimePath;
    StringBuffer m_lockPath;
    StringBuffer m_pidPath;
    StringBuffer m_user;
};

static CEnvironmentConfData     environmentConfData;

struct CField
{
   double Value;
   bool  Warn;
   bool  Undefined;
   bool  Hide;

   CField()
      : Value(0), Warn(0), Undefined(0)
   {      
   }
    void serialize(StringBuffer& xml) const
    {
        xml.append("<Field>");
        xml.appendf("<Value>%f</Value>", Value);
        if (Warn)
            xml.append("<Warn>1</Warn>");
        if (Hide)
            xml.append("<Hide>1</Hide>");
        if (Undefined)
            xml.append("<Undefined>1</Undefined>");
        xml.append("</Field>");
    }
};

struct CFieldMap : public map<string, CField*>
{
    virtual ~CFieldMap()
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            delete (*i).second;
    }
    void serialize(StringBuffer& xml)
    {
        xml.append("<Fields>");
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            (*i).second->serialize(xml);
        xml.append("</Fields>");
    }
};

struct CFieldInfo
{
   unsigned Count; //N
   double   SumSquaredDeviations; //SSD
   double Mean;
   double StandardDeviation;
   bool  Hide;

   CFieldInfo() 
      : Count(0),
        SumSquaredDeviations(0),
          Mean(0),
          StandardDeviation(0),
          Hide(true)
   {
   }
    void serialize(StringBuffer& xml, const char* fieldName) const
    {
        const char* fieldName0 = fieldName;
        if (!strncmp(fieldName, "ibyti", 5))
            fieldName += 5;

        xml.append("<FieldInfo>");
            xml.appendf("<Name>%s</Name>", fieldName0);
            xml.append("<Caption>");
            const char* pch = fieldName;
         if (!strncmp(pch, "lo", 2))
         {
            xml.append("Low");
            pch += 2;
         }
         else if (!strncmp(pch, "hi", 2))
         {
            xml.append("High");
            pch += 2;
         }
         else if (!strncmp(pch, "tot", 3))
         {
            xml.append("Total");
            pch += 3;
         }
         else xml.append( (char)toupper( *pch++) );

            while (*pch)
            {
                if (isupper(*pch))
                    xml.append(' ');
                xml.append(*pch++);     
            }
            xml.append("</Caption>");
            xml.appendf("<Mean>%f</Mean>", Mean);
            xml.appendf("<StandardDeviation>%f</StandardDeviation>", StandardDeviation);
            if (Hide)
                xml.appendf("<Hide>1</Hide>");
        xml.append("</FieldInfo>");
    }
};

struct CFieldInfoMap : public map<string, CFieldInfo*>
{
   Mutex    m_mutex;

    virtual ~CFieldInfoMap()
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            delete (*i).second;
    }

    void serialize(StringBuffer& xml) const
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
        {
            const char* fieldName = (*i).first.c_str();
            (*i).second->serialize(xml, fieldName);
        }
    }
};

class CMetricsParam : public CInterface
{
public:
   IMPLEMENT_IINTERFACE;

   CMetricsParam( const char* pszAddress) : m_sAddress(pszAddress){}
   virtual ~CMetricsParam() {}

   StringBuffer       m_sAddress;
   CFieldMap          m_fieldMap; 
};

static map<string, const char*>  s_processTypeToProcessMap;

struct CMachineInfo
{
   StringBuffer m_sID;
   StringBuffer m_sCPUIdle;
   StringBuffer m_sProcessUptime;
   StringBuffer m_sComputerUptime;
   StringBuffer m_sSpace;

    void setMachineInfo( const char* id, const char* space, const char* CPUIdle, const char* processUptime, const char* computerUptime)
    {
      m_sID = id;
        m_sCPUIdle = CPUIdle;
        m_sProcessUptime = processUptime;
        m_sComputerUptime = computerUptime;
        m_sSpace = space;
   }
};

//---------------------------------------------------------------------------------------------

class Cws_machineEx : public Cws_machine
{
public:
   IMPLEMENT_IINTERFACE;

    enum OpSysType { OS_Windows, OS_Solaris, OS_Linux };

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
   ~Cws_machineEx();

    bool onGetMachineInfo(IEspContext &context, 
        IEspGetMachineInfoRequest &req, IEspGetMachineInfoResponse &resp);

    bool onGetTargetClusterInfo(IEspContext &context, 
        IEspGetTargetClusterInfoRequest &req, IEspGetTargetClusterInfoResponse &resp);

    bool onGetMachineInfoEx(IEspContext &context, 
        IEspGetMachineInfoRequestEx &req, IEspGetMachineInfoResponseEx &resp);

    bool onGetMetrics(IEspContext &context, IEspMetricsRequest &req, 
                             IEspMetricsResponse &resp);
    bool onStartStop( IEspContext &context, IEspStartStopRequest &req,  IEspStartStopResponse &resp);
    bool onStartStopBegin( IEspContext &context, IEspStartStopBeginRequest &req,  IEspStartStopBeginResponse &resp);
    bool onStartStopDone( IEspContext &context, IEspStartStopDoneRequest &req,  IEspStartStopResponse &resp);

    void doGetMachineInfo(IEspContext& context, CMachineInfoThreadParam* pReq);
    void doGetMetrics(CMetricsThreadParam* pParam);

    bool doStartStop(IEspContext &context, StringArray& addresses, char* userName, char* password, bool bStop, IEspStartStopResponse &resp);
   void getAccountAndPlatformInfo(const char* address, StringBuffer& userId, StringBuffer& password, bool& bLinux);
    //IConstEnvironment* getConstEnvironment() const { return m_constEnv.getLink(); }
    IConstEnvironment* getConstEnvironment();
    IPropertyTree* getComponent(const char* compType, const char* compName);

    bool excludePartition(const char* partition) const;
//data members
    static map<string, const char*> s_oid2CompTypeMap;

private:
    void setAttPath(StringBuffer& Path,const char* PathToAppend,const char* AttName,const char* AttValue);
    int checkProcess(const char* type, const char* name, StringArray& typeArray, StringArray& nameArray);
    void getMachineList(IConstEnvironment* constEnv, IPropertyTree* envRoot, const char* machineName,
                                              const char* machineType, const char* status, const char* directory,
                                StringArray& processAddresses,
                                set<string>* pMachineNames=NULL);
    const char* getProcessTypeFromMachineType(const char* machineType);
    void getTargetClusterProcesses(StringArray& targetClusters, StringArray& processTypes, StringArray& processNames, StringArray& processAddresses, IPropertyTree* pTargetClusterTree);
    void setTargetClusterInfo(IPropertyTree* pTargetClusterTree, IArrayOf<IEspMachineInfoEx>& machineArray, IArrayOf<IEspTargetClusterInfo>& targetClusterInfoList);
    const char* getEnvironmentConf(const char* confFileName);
    void doGetSecurityString (const char* address, StringBuffer& securityString);
    bool applySoftwareFilters(const char* program, const char* ProcessType);
    void addIpAddressesToBuffer( void** buffer, unsigned& count, const char* address);

    void RunMachineQuery(IEspContext& context, StringArray &addresses,IEspRequestInfoStruct&  reqInfo,
                                    IArrayOf<IEspMachineInfoEx>& machineArray,StringArray& columnArray);
    void determineRequredProcesses(CMachineInfoThreadParam* pParam, const char* pszProcessType, 
                                             bool bMonitorDaliFileServer, const StringArray& additionalProcesses, 
                                             set<string>& requiredProcesses);
    void parseProperties(const char* info, StringBuffer& processType, StringBuffer& sCompName, OpSysType& os, StringBuffer& path);
    int lookupSnmpComponentIndex(const StringBuffer& sProcessType);
    const char* GetDisplayProcessName(const char* processName, char* buf);

    void getTimeStamp(char* timeStamp);
    void ConvertAddress( const char* originalAddress, StringBuffer& newAddress);
    void updatePathInAddress(const char* address, StringBuffer& addrStr);
    void getUpTime(CMachineInfoThreadParam* pParam, StringBuffer& out);
    void getRoxieClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port);
    void doPostProcessing(CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap);
    void processValue(const char *oid, const char *value, const bool bShow, CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap);
    int remoteGetMachineInfo(IEspContext& context, const char *address, const char *configAddress, const char *preflightCommand, const char* user, const char* password, StringBuffer& sResponse, CMachineInfo& machineInfo);
    void getRunningProcesses(IEspContext& context, const char* address, const char* configAddress, const char* userId, const char* securityString, IArrayOf<IEspProcessInfo>& runningProcesses);
    int runCommand(IEspContext& context, const char* sAddress, const char *configAddress, const char* sCommand, const char* sUserId, const char* sPassword, StringBuffer& sResponse);
    int invokeProgram(const char *command_line, StringBuffer& response);
    int readMachineInfo(const char *response, CMachineInfo& machineInfo);
    void readAString(const char *orig, const char *begin, const char *end, StringBuffer& strReturn, bool bTrim);
    void readTwoStrings(const char *orig, const char *begin, const char *middle, const char *end, StringBuffer& strReturn1, StringBuffer& strReturn2, bool bTrim);
    void readSpace(const char *line, char* title, __int64& free, __int64& total, int& percentage);
    void doGetStorageInfo(CMachineInfoThreadParam* pParam, IArrayOf<IEspStorageInfo> &output, CMachineInfo machineInfo);
    void doGetProcessorInfo(CMachineInfoThreadParam* pParam, IArrayOf<IEspProcessorInfo> &output, CMachineInfo machineInfo);
    void doGetSWRunInfo(IEspContext& context, CMachineInfoThreadParam* pParam, IArrayOf<IEspSWRunInfo> &output, CMachineInfo machineInfo, 
        IArrayOf<IEspProcessInfo>& runningProcesses, const char* pszProcessType, bool bFilterProcesses, bool bMonitorDaliFileServer, const StringArray& additionalProcesses);
    const char* lookupProcessname(const StringBuffer& sProcessType);
    void enumerateRunningProcesses(CMachineInfoThreadParam* pParam, IArrayOf<IEspProcessInfo>& runningProcesses, bool bLinuxInstance,
            bool bFilterProcesses, map<string, Linked<IEspSWRunInfo> >* processMap, map<int, Linked<IEspSWRunInfo> >& pidMap,
                                                             set<string>* pRequiredProcesses);
    char* skipChar(const char* sBuf, char c);
    void readRunningProcess(const char* lineBuf, IArrayOf<IEspProcessInfo>& runningProcesses);
    void checkRunningProcessesByPID(IEspContext& context, CMachineInfoThreadParam* pParam, set<string>* pRequiredProcesses);

    //data members
    StringBuffer m_sTestStr1;
    StringBuffer m_sTestStr2;

    bool m_useDefaultHPCCInit;

    Owned<IPropertyTree>     m_monitorCfg;
    Owned<IPropertyTree>     m_processFilters;
    Owned<IThreadPool>       m_threadPool;
    Owned<IEnvironmentFactory> m_envFactory;
    bool                     m_bMonitorDaliFileServer;
    static map<string, int>  s_processTypeToSnmpIdMap;
    set<string>                  m_excludePartitions;
    set<string>                  m_excludePartitionPatterns;
    StringBuffer                 m_machineInfoFile;
};

//---------------------------------------------------------------------------------------------
class CWsMachineThreadParam : public CInterface
{
public:
   IMPLEMENT_IINTERFACE;

   virtual ~CWsMachineThreadParam() {}

   StringBuffer          m_sAddress;
    StringBuffer          m_sSecurityString; 
    StringBuffer          m_sUserName; 
   StringBuffer          m_sPassword; 
    Linked<Cws_machineEx> m_pService;

   virtual void doWork() = 0;

protected:

   CWsMachineThreadParam( const char* pszAddress, 
                          const char* pszSecurityString, Cws_machineEx* pService)
      : m_sAddress(pszAddress), m_sSecurityString(pszSecurityString), m_pService(pService)
   {
   }
   CWsMachineThreadParam( const char* pszAddress, 
                          const char* pszUserName, const char* pszPassword, Cws_machineEx* pService)
      : m_sAddress(pszAddress), m_sUserName(pszUserName), m_sPassword(pszPassword), m_pService(pService)
   {
   }
};

//---------------------------------------------------------------------------------------------

//the following class implements a worker thread
//
class CWsMachineThread : public CInterface, 
                         implements IPooledThread
{
public:
   IMPLEMENT_IINTERFACE;

   CWsMachineThread()
   {
   }
   virtual ~CWsMachineThread()
   {
   }

    void init(void *startInfo) 
    {
        m_pParam.setown((CWsMachineThreadParam*)startInfo);
    }
    void main()
    {
      m_pParam->doWork();
        m_pParam.clear();
   }

    bool canReuse()
    {
        return true;
    }
    bool stop()
    {
        return true;
    }
   
private:
   Owned<CWsMachineThreadParam> m_pParam;
};

//---------------------------------------------------------------------------------------------

class CWsMachineThreadFactory : public CInterface, public IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CWsMachineThread();
    }
};

#endif //_ESPWIZ_ws_machine_HPP__

