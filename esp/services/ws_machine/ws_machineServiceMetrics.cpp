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

#include <map>
#include <vector>
#include <math.h>
#include "ws_machineService.hpp"
#include "jarray.hpp"
#include "jmisc.hpp"
#include "thirdparty.h"

#include "portlist.h"
#include "roxiecommlibscm.hpp"
#include "exception_util.hpp"

//---------------------------------------------------------------------------------------------
//NOTE: PART II of implementation for Cws_machineEx
//      PART I and III are in ws_machineService.cpp and ws_machineServiceRexec.cpp resp.
//---------------------------------------------------------------------------------------------

static const char* METRICS_FEATURE_URL = "MetricsAccess";
static const char* OID = "1.3.6.1.4.1.12723.6.16.1.4.1.2";

// We need to compute standard deviation for metrics information.
// The classical algorithm to do so requires two passes of data - 
// first pass to compute mean and the second to compute deviations
// as follows:
//
// SD = SQRT( SUM( (Xi-M)^2 ) / (n-1) ) 
//
// where Xi is X1, X2...Xn and M is their mean.
//
// To compute SD in a single pass of data, we use the following algorithm: 
// We maintain 3 variables (for each field) n (count), M (mean), and SSD (Sum of 
// squared deviations i.e. SUM((Xi-Mean)^2 ). 
//
// Begin by setting N=1, M=X1 (first sample), and SSD=0. 
// For every subsequent sample X, we update these values incrementallyas follows:
// 
// N++ 
// compute deviation, D:=(X-M)/N. This is how much the mean will change with new data. 
// M += D 
// SSD += (N-1) * D^2 + (X-M)^2. 
//
// We now have the updated mean and sum of squared deviations, SSD. 
// After all of the data is digested in this way, just calculate variance:=SSD/(n-1) 
// and standard_deviation := sqrt(variance). 
// It requires no memory to hold all of the data, only one pass through the data, formula, 
// and it gives the correct results for a much wider range of data. 
//
#ifdef OLD
struct CField
{
   float Value;
   bool  Warn;
   bool  Undefined;

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
   float   SumSquaredDeviations; //SSD
   float Mean;
   float StandardDeviation;
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
        xml.append("<FieldInfo>");
            xml.appendf("<Name>%s</Name>", fieldName);
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

//---------------------------------------------------------------------------------------------

class CSnmpWalkerCallback : public CInterface,
                            implements ISnmpWalkerCallback
{
private:
public:
   IMPLEMENT_IINTERFACE;

   CSnmpWalkerCallback(CFieldInfoMap& fieldInfoMap, CFieldMap& fieldMap)
      : m_fieldInfoMap(fieldInfoMap), m_fieldMap(fieldMap)
      {}
   virtual ~CSnmpWalkerCallback(){}

    virtual void processValue(const char *oid, const char *value)
   {
      double val = atof(value);
      CField* pField = new CField;
      pField->Value = val;

      if (!strncmp(oid, "ibyti", 5))
         oid += 5;
       
      m_fieldMap.insert(pair<const char*, CField*>( oid, pField) );

      synchronized block(m_fieldInfoMap.m_mutex);
        CFieldInfoMap::iterator i = m_fieldInfoMap.find(oid);

      if (i == m_fieldInfoMap.end())
      {
         CFieldInfo* pFieldInfo = new CFieldInfo;
         pFieldInfo->Count = 1;
         pFieldInfo->Mean = val;
         pFieldInfo->SumSquaredDeviations = 0;

         m_fieldInfoMap.insert( pair<const char*, CFieldInfo*>(oid, pFieldInfo) );
      }
      else
      {
            CFieldInfo* pFieldInfo = (*i).second;
         pFieldInfo->Count++;
         double deviation = (val - pFieldInfo->Mean) / pFieldInfo->Count;
         pFieldInfo->Mean =  pFieldInfo->Mean + deviation;

         pFieldInfo->SumSquaredDeviations += (pFieldInfo->Count-1) * (deviation * deviation);
         double temp = val - pFieldInfo->Mean;
         pFieldInfo->SumSquaredDeviations += temp * temp;
      }
   }
private:
   CSnmpWalkerCallback(const CSnmpWalkerCallback&);

   CFieldInfoMap& m_fieldInfoMap;
   CFieldMap&     m_fieldMap;
};

//---------------------------------------------------------------------------------------------

class CMetricsThreadParam : public CWsMachineThreadParam
{
public:
   IMPLEMENT_IINTERFACE;

   CFieldInfoMap&       m_fieldInfoMap;
   CFieldMap            m_fieldMap;
   PooledThreadHandle   m_threadHandle;
   bool                 m_bPostProcessing; //use mean & std deviation to set warnings, etc.

   CMetricsThreadParam( const char* pszAddress, const char* pszSecString,
                        CFieldInfoMap& fieldInfoMap, 
                        Cws_machineEx* pService)
      : CWsMachineThreadParam(pszAddress, pszSecString, pService),
        m_fieldInfoMap(fieldInfoMap),
        m_bPostProcessing(false)
   {
   }
    virtual ~CMetricsThreadParam()
    {
    }
   virtual void doWork()
   {
      if (m_bPostProcessing == false)
         m_pService->doGetMetrics(this);
      else
         doPostProcessing();
   }

   void doPostProcessing()
   {
       //DBGLOG("Post processing for %s", m_sAddress.str());

      //for each field in the field info map (some of these fields may not be defined
      //in our field map)
      //
        CFieldInfoMap::iterator i;
        CFieldInfoMap::iterator iEnd = m_fieldInfoMap.end();
       for (i=m_fieldInfoMap.begin(); i!=iEnd; i++)
       {
            const char *fieldName = (*i).first.c_str();
            const CFieldInfo* pFieldInfo = (*i).second;

            CFieldMap::iterator iField = m_fieldMap.find(fieldName);
         if (iField == m_fieldMap.end())
         {
            CField* pField = new CField;
            pField->Undefined = true;
            m_fieldMap.insert(pair<const char*, CField*>( fieldName, pField ));
         }
         else
         {
                CField* pField = (*iField).second;
            //set warnings based on mean and standard deviation
            double z = fabs((pField->Value - pFieldInfo->Mean) / pFieldInfo->StandardDeviation);
            if (z > 2)
               pField->Warn = true;
         }
       }
   }
};

void Cws_machineEx::doGetMetrics(CMetricsThreadParam* pParam)
{
    //DBGLOG("Getting Metrics for %s", pParam->m_sAddress.str());
   StringBuffer sSecurityString = pParam->m_sSecurityString;

   if (!*sSecurityString.str())
      doGetSecurityString(pParam->m_sAddress.str(), sSecurityString);

#ifdef _DEBUG
   if (!*sSecurityString.str())
      sSecurityString.append("M0n1T0r");
#endif

    //serialized for now
}

#else

//-------------------------------------------------METRICS------------------------------------------------------
void Cws_machineEx::getRoxieClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port)
{
#if 0
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> environment = factory->openEnvironment();
    Owned<IPropertyTree> pRoot = &environment->getPTree();
#else
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pRoot = &constEnv->getPTree();
    if (!pRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Failed to get environment information.");
#endif

    StringBuffer xpath;
    xpath.appendf("Software/%s[@name='%s']", clusterType, clusterName);

    IPropertyTree* pCluster = pRoot->queryPropTree( xpath.str() );
    if (!pCluster)
        throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "'%s %s' is not defined!", clusterType, clusterName);

    xpath.clear().append(processName);
    xpath.append("@computer");
    const char* computer = pCluster->queryProp(xpath.str());
    if (!computer || strlen(computer) < 1)
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "'%s %s: %s' is not defined!", clusterType, clusterName, processName);

    xpath.clear().append(processName);
    xpath.append("@port");
    const char* portStr = pCluster->queryProp(xpath.str());
    port = ROXIE_SERVER_PORT;
    if (portStr && *portStr)
    {
        port = atoi(portStr);
    }

#if 0
    Owned<IConstMachineInfo> pMachine = environment->getMachine(computer);
    if (pMachine)
    {
        SCMStringBuffer scmNetAddress;
        pMachine->getNetAddress(scmNetAddress);
        netAddress = scmNetAddress.str();
    }
#else
    xpath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computer);
    IPropertyTree* pMachine = pRoot->queryPropTree( xpath.str() );
    if (pMachine)
    {
        const char* addr = pMachine->queryProp("@netAddress");
        if (addr && *addr)
            netAddress.append(addr);
    }
#endif
    
    return;
}

void Cws_machineEx::processValue(const char *oid, const char *value, const bool bShow, CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap)
{
  double val = atof(value);
  CField* pField = new CField;
  pField->Value = val;

  //const char *oid0 = oid;
  //if (!strncmp(oid, "ibyti", 5))
  //   oid += 5;
   
  pField->Hide = !bShow;

  myfieldMap.insert(pair<const char*, CField*>( oid, pField) );

  synchronized block(myfieldInfoMap.m_mutex);
  CFieldInfoMap::iterator i = myfieldInfoMap.find(oid);

  if (i == myfieldInfoMap.end())
  {
     CFieldInfo* pFieldInfo = new CFieldInfo;
     pFieldInfo->Count = 1;
     pFieldInfo->Mean = val;
     pFieldInfo->SumSquaredDeviations = 0;
      pFieldInfo->Hide = !bShow;

     myfieldInfoMap.insert( pair<const char*, CFieldInfo*>(oid, pFieldInfo) );
  }
  else
  {
        CFieldInfo* pFieldInfo = (*i).second;
     pFieldInfo->Count++;
     double deviation = (val - pFieldInfo->Mean) / pFieldInfo->Count;
     pFieldInfo->Mean =  pFieldInfo->Mean + deviation;
      pFieldInfo->Hide = !bShow;

     pFieldInfo->SumSquaredDeviations += (pFieldInfo->Count-1) * (deviation * deviation);
     double temp = val - pFieldInfo->Mean;
     pFieldInfo->SumSquaredDeviations += temp * temp;
  }
}

void Cws_machineEx::doPostProcessing(CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap)
{
    //DBGLOG("Post processing for %s", m_sAddress.str());

    //for each field in the field info map (some of these fields may not be defined
    //in our field map)
    //
    CFieldInfoMap::iterator i;
    CFieldInfoMap::iterator iEnd = myfieldInfoMap.end();
    for (i=myfieldInfoMap.begin(); i!=iEnd; i++)
    {
        const char *fieldName = (*i).first.c_str();
        const CFieldInfo* pFieldInfo = (*i).second;

        CFieldMap::iterator iField = myfieldMap.find(fieldName);
        if (iField == myfieldMap.end())
        {
            CField* pField = new CField;
            pField->Undefined = true;
            myfieldMap.insert(pair<const char*, CField*>( fieldName, pField ));
        }
        else
        {
            CField* pField = (*iField).second;
            //set warnings based on mean and standard deviation
            double z = fabs((pField->Value - pFieldInfo->Mean) / pFieldInfo->StandardDeviation);
            if (z > 2)
                pField->Warn = true;
        }
    }
}
#endif

bool Cws_machineEx::onGetMetrics(IEspContext &context, IEspMetricsRequest &req, 
                                         IEspMetricsResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(METRICS_FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_METRICS_ACCESS_DENIED, "Failed to Get Metrics. Permission denied.");

        //insert entries in an array - one per IP address, sorted by IP address
        //
        const char* clusterName = req.getCluster();
        if (!clusterName || !*clusterName)
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Cluster name not defined.");

        resp.setCluster(clusterName);

        StringArray &addresses = req.getAddresses();
        unsigned int ordinality= addresses.ordinality();
        unsigned int addressCount = 0;
        unsigned* buffer = NULL;

        unsigned index;
        for (index=0; index<ordinality; index++)
        {
            const char *address = addresses.item(index);
            const char *colon   = strchr(address, ':');
            char* address2 = NULL;

            if (colon)
            {
                address2 = strdup(address);
                address2[colon-address] = '\0';
                address = address2;
            }

            char* configAddress = NULL;
            char* props1 = (char*) strchr(address, '|');
            if (props1)
            {
                configAddress = props1+1;
                *props1 = '\0';
            }
            else
            {
                configAddress = (char*) address;
            }

            addIpAddressesToBuffer( (void**)&buffer, addressCount, address);

            if (address2)
                free(address2);
        }

        CFieldInfoMap fieldInfoMap; //shared across all threads processing this request
#ifdef OLD
        CIArrayOf<CMetricsThreadParam> threadParamArray;

        //process this array (sorted by IP address)
        //
        // TBD IPV6 (cannot use long for netaddress)
        StringBuffer ipBuf;
        unsigned* lptr = buffer;
        for (index=0; index<addressCount; index++)
        {
            IpAddress ip;
            ip.setNetAddress(sizeof(unsigned),lptr++);
            ip.getIpText(ipBuf.clear());
            
            CMetricsThreadParam* pThreadReq = 
                    new CMetricsThreadParam(ipBuf.str(), req.getSecurityString(), 
                fieldInfoMap, this);
            threadParamArray.append(*::LINK(pThreadReq));
            pThreadReq->m_threadHandle = m_threadPool->start( pThreadReq );
        }

        if (buffer)
            ::free(buffer);

        //block for worker theads to finish, if necessary and then collect results
        //
        CMetricsThreadParam** pThreadParam = (CMetricsThreadParam**) threadParamArray.getArray();
        unsigned count=threadParamArray.ordinality();
        unsigned i;
        for (i = 0; i < count; i++, pThreadParam++) 
            m_threadPool->join((*pThreadParam)->m_threadHandle);

        //collect field information for all fields
        CFieldInfoMap::iterator iInfo;
        CFieldInfoMap::iterator iInfoEnd = fieldInfoMap.end();
        for (iInfo=fieldInfoMap.begin(); iInfo!=iInfoEnd; iInfo++)
        {
            CFieldInfo* pFieldInfo = (*iInfo).second;
            pFieldInfo->StandardDeviation = ( pFieldInfo->Count > 1 ? sqrt(pFieldInfo->SumSquaredDeviations / (pFieldInfo->Count-1)) : 0);
        }

        //respect user's wishes to only show some columns
        //
        StringArray& showColumns = req.getShowColumns();
        unsigned int columnsToShow = showColumns.ordinality();
        if (columnsToShow == 0)
        {
            static const char* defaultColumns[] = {
                "heapBlocksAllocated", "hiQueryActive", "hiQueryAverage", "hiQueryCount", "hiMax", "hiMin", 
                "lastQueryDate", "lastQueryTime", "loMax", "loMin", "loQueryActive", "loQueryAverage", 
                "loQueryCount", "retriesNeeded", "slavesActive"
            };

            columnsToShow = sizeof(defaultColumns)/sizeof(defaultColumns[0]);
            for (unsigned int i=0; i<columnsToShow; i++)
            {
                iInfo = fieldInfoMap.find(defaultColumns[i]);
                if (iInfo != iInfoEnd)
                    (*iInfo).second->Hide = 0;          
            }
        }
        else
            for (index=0; index<columnsToShow; index++)
            {
                const char *columnName = showColumns.item(index);
                iInfo = fieldInfoMap.find(columnName);
                if (iInfo != iInfoEnd)
                    (*iInfo).second->Hide = 0;
            }

        //create a separate thread to do post processing i.e. serialize field map
        //to field array while filling in any absent fields and set warnings for fields 
        //with very high deviation
        //
        pThreadParam = (CMetricsThreadParam**) threadParamArray.getArray();
        for (i = 0; i < count; i++, pThreadParam++) 
        {
            (*pThreadParam)->m_bPostProcessing = true;
            (*pThreadParam)->m_threadHandle = m_threadPool->start( ::LINK(*pThreadParam) );
        }

        StringBuffer xml;
        fieldInfoMap.serialize(xml);
        resp.setFieldInformation(xml);

        xml.clear();
        pThreadParam = (CMetricsThreadParam**) threadParamArray.getArray();
        for (i = 0; i < count; i++, pThreadParam++) 
        {
            xml.append("<MetricsInfo><Address>");
            xml.append( (*pThreadParam)->m_sAddress );
            xml.append("</Address>");

            //block for worker theads to finish, if necessary, and then collect results
            //
            m_threadPool->join((*pThreadParam)->m_threadHandle);
            (*pThreadParam)->m_fieldMap.serialize(xml);

            xml.append("</MetricsInfo>");
        }

        resp.setMetrics(xml);

#else
        //process this array (sorted by IP address)
        //
        // TBD IPV6 (cannot use long for netaddress)
        StringArray ipList;
        StringBuffer ipBuf;
        unsigned* lptr = buffer;
        for (index=0; index<addressCount; index++)
        {
            IpAddress ip;
            ip.setNetAddress(sizeof(unsigned),lptr++);
            ip.getIpText(ipBuf.clear());
            ipList.append(ipBuf.str());
        }

        if (buffer)
            ::free(buffer);

        int port;
        StringBuffer netAddress;
        SocketEndpoint ep;
        getRoxieClusterConfig("RoxieCluster", clusterName, "RoxieServerProcess[1]", netAddress, port);

        StringArray& showColumns = req.getShowColumns();
        unsigned int columnsToShow = showColumns.ordinality();

        ep.set(netAddress.str(), port);
        Owned<IRoxieCommunicationClient> roxieClient = createRoxieCommunicationClient(ep, 5000);
        Owned<IPropertyTree> result = roxieClient->retrieveRoxieMetrics(ipList);

         CIArrayOf<CMetricsParam> fieldMapArray;
        Owned<IPropertyTreeIterator> endpoints = result->getElements("Endpoint");
        ForEach(*endpoints)
        {
            IPropertyTree &endpoint = endpoints->query();

            CFieldMap fieldMap;
            const char* ep = endpoint.queryProp("@ep");
            if (!ep || !*ep)
                continue;

            char ip[32];
            strcpy(ip, ep);
            const char* ip0 = strchr(ep, ':');
            if (ip0)
            {
                ip[ip0 - ep] = 0;
            }

            Owned<CMetricsParam> pMetricsParam = new CMetricsParam(ip);
            Owned<IPropertyTreeIterator> metrics = endpoint.getElements("Metrics/Metric");
            ForEach(*metrics)
            {
                IPropertyTree &metric = metrics->query();
                const char* name = metric.queryProp("@name");
                const char* value = metric.queryProp("@value");
                if (!name || !*name || !value || !*value)
                    continue;

                //const char* name0 = name;
                //if (!strncmp(name0, "ibyti", 5))
                //  name0 += 5; 

                bool bShow = false;
                if (columnsToShow == 0)
                {
                    static const char* defaultColumns[] = {
                        "heapBlocksAllocated", "hiQueryActive", "hiQueryAverage", "hiQueryCount", "hiMax", "hiMin", 
                        "lastQueryDate", "lastQueryTime", "loMax", "loMin", "loQueryActive", "loQueryAverage", 
                        "loQueryCount", "retriesNeeded", "slavesActive"
                    };

                    unsigned int columnsToShow0 = sizeof(defaultColumns)/sizeof(defaultColumns[0]);
                    for (unsigned int i=0; i<columnsToShow0; i++)
                    {
                        if (!stricmp(defaultColumns[i], name))
                        {
                            bShow = true;
                            break;
                        }
                    }
                }
                else
                {
                    for (index=0; index<columnsToShow; index++)
                    {
                        const char *columnName = showColumns.item(index);
                        if (!stricmp(columnName, name))
                        {
                            bShow = true;
                            break;
                        }
                    }
                }

                processValue(name, value, bShow, fieldInfoMap, pMetricsParam->m_fieldMap);
            }
            fieldMapArray.append(*pMetricsParam.getLink());
        }

        int count=fieldMapArray.ordinality();
        
        //collect field information for all fields
        CFieldInfoMap::iterator iInfo;
        CFieldInfoMap::iterator iInfoEnd = fieldInfoMap.end();
        for (iInfo=fieldInfoMap.begin(); iInfo!=iInfoEnd; iInfo++)
        {
            CFieldInfo* pFieldInfo = (*iInfo).second;
            pFieldInfo->StandardDeviation = ( pFieldInfo->Count > 1 ? sqrt(pFieldInfo->SumSquaredDeviations / (pFieldInfo->Count-1)) : 0);
        }

        //respect user's wishes to only show some columns
        //
        if (columnsToShow == 0)
        {
            static const char* defaultColumns[] = {
                "heapBlocksAllocated", "hiQueryActive", "hiQueryAverage", "hiQueryCount", "hiMax", "hiMin", 
                "lastQueryDate", "lastQueryTime", "loMax", "loMin", "loQueryActive", "loQueryAverage", 
                "loQueryCount", "retriesNeeded", "slavesActive"
            };

            columnsToShow = sizeof(defaultColumns)/sizeof(defaultColumns[0]);
            for (unsigned int i=0; i<columnsToShow; i++)
            {
                iInfo = fieldInfoMap.find(defaultColumns[i]);
                if (iInfo != iInfoEnd)
                    (*iInfo).second->Hide = 0;          
            }
        }
        else
            for (index=0; index<columnsToShow; index++)
            {
                const char *columnName = showColumns.item(index);
                iInfo = fieldInfoMap.find(columnName);
                if (iInfo != iInfoEnd)
                    (*iInfo).second->Hide = 0;
            }

        //create a separate thread to do post processing i.e. serialize field map
        //to field array while filling in any absent fields and set warnings for fields 
        //with very high deviation
        //
        int i = 0;
        CMetricsParam** pMetricsParam = (CMetricsParam**) fieldMapArray.getArray();
        for (i = 0; i < count; i++, pMetricsParam++) 
        {
            doPostProcessing(fieldInfoMap, (*pMetricsParam)->m_fieldMap);
        }

        StringBuffer xml;
        fieldInfoMap.serialize(xml);
        resp.setFieldInformation(xml);

        xml.clear();
        pMetricsParam = (CMetricsParam**) fieldMapArray.getArray();
        for (i = 0; i < count; i++, pMetricsParam++) 
         {
            xml.append("<MetricsInfo><Address>");
            xml.append( (*pMetricsParam)->m_sAddress);
            xml.append("</Address>");
            
            (*pMetricsParam)->m_fieldMap.serialize(xml);
            xml.append("</MetricsInfo>");
        }

        resp.setMetrics(xml);
#endif
        double version = context.getClientVersion();
        if (version > 1.05)
        {
            resp.setSelectAllChecked( req.getSelectAllChecked() );
        }
        if (version > 1.06)
        {
            resp.setAutoUpdate( req.getAutoUpdate() );
        }
        if (version >= 1.12)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
        resp.setAutoRefresh( req.getAutoRefresh() );//loop back requested auto refresh timeout to output so javascript sets timeout
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


// TBD IPv6

static int compareNumericIp(const void* ip1, const void* ip2)
{
   const unsigned numIP1 = *(unsigned*)ip1;
   const unsigned numIP2 = *(unsigned*)ip2;

   return numIP1 == numIP2 ? 0 : numIP1 < numIP2 ? -1 : 1;
}

void Cws_machineEx::addIpAddressesToBuffer( void** buffer, unsigned& count, const char* address)
{
    IpAddress fromIp;
    unsigned  numIPs = fromIp.ipsetrange(address);
    if (numIPs==0)
        throw MakeStringException(ECLWATCH_INVALID_IP_RANGE, "Invalid IP address range '%s'.", address);

    
    //resize the array
    *buffer = realloc(*buffer, sizeof(unsigned) * (count+numIPs));
    
    //insert first address in the array
    bool bAdded;
    
    
    // TBD IPv6
    if (!fromIp.isIp4())
        IPV6_NOT_IMPLEMENTED();
    unsigned ip;
    if (fromIp.getNetAddress(sizeof(ip),&ip)!=sizeof(ip))
        IPV6_NOT_IMPLEMENTED(); // Not quite same exception, but re-use when IPv4 hack fails sanity check
    unsigned* pos = (unsigned*) binary_add((void*)&ip, *buffer, count, sizeof(ip), 
        compareNumericIp, &bAdded);
    
    //now insert all subsequent addresses, if any, in the address range as contiguous
    //memory assuming the ranges in the buffer don't overlap
    //
    if (bAdded)
    {
        count++;
        
        if (--numIPs > 0)
        {
            //at this point, one element has been inserted at position 'pos' in the buffer
            //so we need to make room for subsequent elements in the range by pushing the 
            //existing elements behind the position 'pos' by (numIPs-1) places
            //
            pos++; //points to position after first inserted element
            unsigned index = pos - (unsigned*)*buffer;//index of next item
            unsigned itemsToMove = count - index;
            memmove(pos+numIPs, pos, itemsToMove*sizeof(ip));
            count += numIPs;  
            while (numIPs--)
            {
                fromIp.ipincrement(1);
                if (fromIp.getNetAddress(sizeof(*pos),pos)!=sizeof(unsigned))
                    IPV6_NOT_IMPLEMENTED();
                pos++;
            }
        }
    }
}
