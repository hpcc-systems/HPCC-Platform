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

//-------------------------------------------------METRICS------------------------------------------------------
void Cws_machineEx::getRoxieClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port)
{
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pRoot = &constEnv->getPTree();
    if (!pRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Failed to get environment information.");

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

    xpath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computer);
    IPropertyTree* pMachine = pRoot->queryPropTree( xpath.str() );
    if (pMachine)
    {
        const char* addr = pMachine->queryProp("@netAddress");
        if (addr && *addr)
            netAddress.append(addr);
    }
    
    return;
}

void Cws_machineEx::processValue(const char *oid, const char *value, const bool bShow, CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap)
{
  double val = atof(value);
  CField* pField = new CField;
  pField->Value = val;

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
    //for each field in the field info map (some of these fields may not be defined
    //in our field map)
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

bool Cws_machineEx::onGetMetrics(IEspContext &context, IEspMetricsRequest &req, 
                                         IEspMetricsResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(METRICS_FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_METRICS_ACCESS_DENIED, "Failed to Get Metrics. Permission denied.");

        //insert entries in an array - one per IP address, sorted by IP address
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

        //process this array (sorted by IP address)
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

            CMetricsParam* pMetricsParam = new CMetricsParam(ip);
            Owned<IPropertyTreeIterator> metrics = endpoint.getElements("Metrics/Metric");
            ForEach(*metrics)
            {
                IPropertyTree &metric = metrics->query();
                const char* name = metric.queryProp("@name");
                const char* value = metric.queryProp("@value");
                if (!name || !*name || !value || !*value)
                    continue;

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
            fieldMapArray.append(*::LINK(pMetricsParam));
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

        double version = context.getClientVersion();
        if (version > 1.05)
        {
            resp.setSelectAllChecked( req.getSelectAllChecked() );
        }
        if (version > 1.06)
        {
            resp.setAutoUpdate( req.getAutoUpdate() );
        }
        resp.setAutoRefresh( req.getAutoRefresh() );//loop back requested auto refresh timeout to output so javascript sets timeout
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

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
    
    if (!fromIp.isIp4())
        IPV6_NOT_IMPLEMENTED();
    unsigned ip;
    if (fromIp.getNetAddress(sizeof(ip),&ip)!=sizeof(ip))
        IPV6_NOT_IMPLEMENTED(); // Not quite same exception, but re-use when IPv4 hack fails sanity check
    unsigned* pos = (unsigned*) binary_add((void*)&ip, *buffer, count, sizeof(ip), 
        compareNumericIp, &bAdded);
    
    //now insert all subsequent addresses, if any, in the address range as contiguous
    //memory assuming the ranges in the buffer don't overlap
    if (bAdded)
    {
        count++;
        
        if (--numIPs > 0)
        {
            //at this point, one element has been inserted at position 'pos' in the buffer
            //so we need to make room for subsequent elements in the range by pushing the 
            //existing elements behind the position 'pos' by (numIPs-1) places
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
