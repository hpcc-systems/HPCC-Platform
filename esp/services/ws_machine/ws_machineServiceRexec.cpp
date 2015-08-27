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

#include "ws_machineService.hpp"
#include "jarray.hpp"
#include "jmisc.hpp"
#include "exception_util.hpp"
#include "rmtssh.hpp"

//---------------------------------------------------------------------------------------------
//NOTE: PART III of implementation for Cws_machineEx
//      PART I and II are in ws_machineService.cpp and ws_machineServiceMetrics.cpp resp.
//---------------------------------------------------------------------------------------------

static const char* EXEC_FEATURE_URL = "ExecuteAccess";

//---------------------------------------------------------------------------------------------

class CRemoteExecThreadParam : public CWsMachineThreadParam
{
public:
   IMPLEMENT_IINTERFACE;

   Owned<IEspRemoteExecResult> m_pExecResult;
   StringBuffer m_sCommand;
   StringBuffer m_userId;
   StringBuffer m_password;
    StringBuffer m_sConfigAddress;
   bool m_bWait;        
   IEspContext& m_context;

    CRemoteExecThreadParam( const char* pszAddress, const char* cmd, bool wait,
                           Cws_machineEx* pService, IEspContext &context)
                                 : CWsMachineThreadParam(pszAddress, "", pService),
                                   m_sCommand(cmd), m_bWait(wait), m_context(context)
   {
   }

    CRemoteExecThreadParam( const char* pszAddress, const char* pszConfigAddress, const char* cmd, bool wait, 
                                    Cws_machineEx* pService, IEspContext &context)
                                 : CWsMachineThreadParam(pszAddress, "", "", pService),
                                   m_sCommand(cmd), m_bWait(wait), m_context(context)
   {
        m_sConfigAddress = pszConfigAddress;
   }
    virtual void setResultObject(void* pExecResult)
    {
        m_pExecResult.set( static_cast<IEspRemoteExecResult*>( pExecResult ));
    }

    virtual void setResponse( const char* resp )
    {
        m_pExecResult->setResponse( resp );
    }

    virtual void setResultCode( int code )
    {
        m_pExecResult->setResultCode( code );
    }

    virtual void setUserID( const char* userID )
    {
        m_userId.clear().append( userID );
    }

    virtual void setPassword( const char* password )
    {
        m_password.clear().append( password );
    }

    virtual void doWork()
    {
        try
        {
            StringBuffer userId;
            StringBuffer password;
            bool bLinux;

            if (m_sConfigAddress.length() < 1)
            {
                m_pService->getAccountAndPlatformInfo(m_sAddress.str(), userId, password, bLinux);
            }
            else
            {
                m_pService->getAccountAndPlatformInfo(m_sConfigAddress.str(), userId, password, bLinux);
            }

            if (!m_userId.length() || !m_password.length())
            {
                //BUG: 9825 - remote execution on linux needs to use individual accounts
                //use userid/password in ESP context for remote execution...
                if (bLinux)
                {
                    userId.clear();
                    password.clear();
                    m_context.getUserID(userId);
                    m_context.getPassword(password);
                }
            }
            else
            {
                userId.clear().append(m_userId);
                password.clear().append(m_password);
            }

            if (!m_sCommand.length())
                setResultCode(-1);
            else
            {
                IFRunSSH * connection = createFRunSSH();
                connection->init(m_sCommand.str(),NULL,userId.str(),password.str(),5,0);
                connection->exec(m_sAddress.str(),NULL,true);
            }
        }
        // CFRunSSH uses a MakeStringExceptionDirect throw to pass code and result string to caller
        catch(IException* e)
        {
            // errorCode == -1 on successful CFRunSSH execution
            if(e->errorCode() == -1)
                setResultCode(0);
            else
                setResultCode(e->errorCode());
            StringBuffer buf;
            e->errorMessage(buf);
            if (buf.length() && buf.charAt(buf.length() - 1) == '\n') // strip newline
                buf.setLength(buf.length() - 1);
            // set regardless of return
            setResponse(buf.str());
            e->Release();
        }
#ifndef NO_CATCHALL
        catch(...)
        {
            setResponse("An unknown exception occurred!");
            setResultCode(-1);
        }
#endif
    }//doWork()
};

//-------------------------------------------------StartStop--------------------------------------------------

class CStartStopThreadParam : public CRemoteExecThreadParam
{
public:
   IMPLEMENT_IINTERFACE;

   Owned<IEspStartStopResult> m_pResult;
    bool m_bStop;
    bool m_useHPCCInit;

#ifndef OLD_START_STOP
   CStartStopThreadParam( const char* pszAddress, const char* pszConfigAddress, bool bStop, bool useHPCCInit, Cws_machineEx* pService, IEspContext &context)
                        : CRemoteExecThreadParam(pszAddress, pszConfigAddress, "", true, pService, context),
                                  m_bStop(bStop), m_useHPCCInit(useHPCCInit)
   {
   }
#else
    CStartStopThreadParam( const char* pszAddress, bool bStop, bool useHPCCInit, Cws_machineEx* pService, IEspContext &context)
                        : CRemoteExecThreadParam(pszAddress, "", true, pService, context),
                                  m_bStop(bStop), m_useHPCCInit(useHPCCInit)
   {
   }
#endif

    virtual void setResultObject(void* pResult)
    {
        m_pResult.set( static_cast<IEspStartStopResult*>( pResult ));
    }

    virtual void setResponse( const char* resp )
    {
        m_pResult->setResponse( resp );
    }

    virtual void setResultCode( int code )
    {
        m_pResult->setResultCode( code );
    }

   virtual void doWork()
   {
        if (m_useHPCCInit)
        {
            //address specified can be either IP or name of component
            const char* address = m_sAddress.str();
            const char* configAddress = m_sConfigAddress.str();
            if (!address || !*address)
                throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "Invalid address or component name was specified!");

            if (!strchr(address, '.')) //not an IP address
            {
                const char* compType = m_pResult->getCompType();
                const char* compName = address;

                if (!compType || !*compType)
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "No component type specified!");

                StringBuffer xpath;
                if (!strcmp(compType, "RoxieCluster"))
                {
                    xpath.append("RoxieServer");
                }
                else if (!strcmp(compType, "ThorCluster"))
                {
                    xpath.append("ThorMaster");
                }
                else if (!strcmp(compType, "HoleCluster"))
                {
                    xpath.append("HoleControl");
                }
                else
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "Failed to resolve component type '%s'", compType);

                Owned<IPropertyTree> pComponent = m_pService->getComponent(compType, compName);
                xpath.append("Process[1]/@computer");
                const char* computer = pComponent->queryProp(xpath.str());

                if (!computer || !*computer)
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_INFO, "Failed to resolve computer for %s '%s'!", compType, compName);

                Owned<IConstEnvironment> pConstEnv = m_pService->getConstEnvironment();
                Owned<IConstMachineInfo> pConstMc  = pConstEnv->getMachine(computer);

                SCMStringBuffer sAddress;
                pConstMc->getNetAddress(sAddress);

                if (!stricmp(m_sAddress.str(), m_sConfigAddress.str()))
                {
                    m_sAddress.clear().append(sAddress.str());
                    m_sConfigAddress = m_sAddress;
                    m_pResult->setAddress( sAddress.str() );
                }
                else
                {
                    m_sAddress.clear().append(sAddress.str());
                    m_pResult->setAddress( sAddress.str() );
                    if (configAddress && !strchr(configAddress, '.')) //not an IP address
                    {
                        Owned<IPropertyTree> pComponent = m_pService->getComponent(compType, configAddress);
                        xpath.append("Process[1]/@computer");
                        const char* computer = pComponent->queryProp(xpath.str());

                        if (!computer || !*computer)
                            throw MakeStringException(ECLWATCH_INVALID_COMPONENT_INFO, "Failed to resolve computer for %s '%s'!", compType, configAddress);

                        Owned<IConstEnvironment> pConstEnv = m_pService->getConstEnvironment();
                        Owned<IConstMachineInfo> pConstMc  = pConstEnv->getMachine(computer);

                        SCMStringBuffer sAddress;
                        pConstMc->getNetAddress(sAddress);
                        m_sConfigAddress.clear().append(sAddress.str());
                    }
                }
            }

            if ((m_sAddress.length() > 0) && !stricmp(m_sAddress.str(), "."))
            {
                StringBuffer ipStr;
                IpAddress ipaddr = queryHostIP();
                ipaddr.getIpText(ipStr);
                if (ipStr.length() > 0)
                {
#ifdef MACHINE_IP
                    m_sAddress.clear().append(MACHINE_IP);
#else
                    m_sAddress.clear().append(ipStr.str());
#endif
                    m_pResult->setAddress( m_sAddress.str() );
                }
            }

#ifdef OLD_START_STOP
            int OS = m_pResult->getOS();

            StringBuffer sPath( m_pResult->getPath() );
            if (OS == 0)
                sPath.replace('$', ':');
            else
                if (sPath.charAt(0) != '/')
                    sPath.insert(0, '/');
            m_sCommand.clear().append(sPath).append( OS==0 ? '\\' : '/');
            m_sCommand.append(m_bStop ? "stop" : "startup");
            if (OS == 0)
                m_sCommand.append(".bat");
            m_sCommand.append(' ');

            m_sCommand.append(sPath);
#else
            StringBuffer sPath( m_pResult->getPath() );
            if (sPath.charAt(sPath.length() - 1) == '/')
                sPath.setLength(sPath.length() - 1);
            if (sPath.length() > 0)
            {
                char* pStr = (char*) sPath.str();
                char* ppStr = strchr(pStr, '/');
                while (ppStr)
                {
                    ppStr++;
                    pStr = ppStr;
                    ppStr = strchr(pStr, '/');
                }

                if (!m_bStop)
                    m_sCommand.appendf("sudo /etc/init.d/hpcc-init -c %s start", pStr);
                else
                    m_sCommand.appendf("sudo /etc/init.d/hpcc-init -c %s stop", pStr);
            }
#endif
            m_pResult->setCommand( m_sCommand.str() );
        }
        else
        {
            //address specified can be either IP or name of component
            const char* address = m_sAddress.str();
            if (!address || !*address)
                throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "Invalid address or component name was specified!");

            if (!strchr(address, '.')) //not an IP address
            {
                const char* compType = m_pResult->getCompType();
                const char* compName = address;

                if (!compType || !*compType)
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "No component type specified!");

                StringBuffer xpath;
                if (!strcmp(compType, "RoxieCluster"))
                {
                    xpath.append("RoxieServer");
                }
                else if (!strcmp(compType, "ThorCluster"))
                {
                    xpath.append("ThorMaster");
                }
                else if (!strcmp(compType, "HoleCluster"))
                {
                    xpath.append("HoleControl");
                }
                else
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "Failed to resolve component type '%s'", compType);

                Owned<IPropertyTree> pComponent = m_pService->getComponent(compType, compName);
                xpath.append("Process[1]/@computer");
                const char* computer = pComponent->queryProp(xpath.str());

                if (!computer || !*computer)
                    throw MakeStringException(ECLWATCH_INVALID_COMPONENT_INFO, "Failed to resolve computer for %s '%s'!", compType, compName);

                Owned<IConstEnvironment> pConstEnv = m_pService->getConstEnvironment();
                Owned<IConstMachineInfo> pConstMc  = pConstEnv->getMachine(computer);

                SCMStringBuffer sAddress;
                pConstMc->getNetAddress(sAddress);

                m_sAddress.clear().append(sAddress.str());
                m_pResult->setAddress( sAddress.str() );
            }

            int OS = m_pResult->getOS();

            StringBuffer sPath( m_pResult->getPath() );
            if (OS == 0)
                sPath.replace('$', ':');
            else
                if (sPath.charAt(0) != '/')
                    sPath.insert(0, '/');

            m_sCommand.clear().append(sPath).append( OS==0 ? '\\' : '/');
            m_sCommand.append(m_bStop ? "stop" : "startup");
            if (OS == 0)
                m_sCommand.append(".bat");
            m_sCommand.append(' ');

            m_sCommand.append(sPath);
            m_pResult->setCommand( m_sCommand.str() );
        }
        CRemoteExecThreadParam::doWork();
    }
};

void Cws_machineEx::ConvertAddress( const char* originalAddress, StringBuffer& newAddress)
{
    if (!originalAddress || !*originalAddress)
        throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "No network address or computer name specified!");

    StringArray sArray;
    sArray.appendList(originalAddress, ":");

    if (sArray.ordinality() < 4)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Incomplete arguments");

    const char* address = sArray.item(0);
    const char* compType= sArray.item(1);
    const char* compName= sArray.item(2);
    const char* OS        = sArray.item(3);
    const char* path      = sArray.item(4);

    StringBuffer process;
    if (sArray.ordinality() > 5)
    {
        const char* ClusterType   = sArray.item(5);
        if (ClusterType && *ClusterType)
        {
            if (strcmp("THORMACHINES",ClusterType) == 0)
            {
                process.append("ThorMasterProcess");
            }
            else if (strcmp("ROXIEMACHINES",ClusterType) == 0)
            {
                process.append("RoxieServerProcess");
            }
        }
    }

    if (strchr(address, '.')) //have an IP address
    {
        newAddress.clear().append(originalAddress);
        return;
    }

    StringBuffer xpath;
    if (!strcmp(compType, "RoxieCluster"))
    {
        xpath.append("RoxieServer");
    }
    else if (!strcmp(compType, "ThorCluster"))
    {
        xpath.append("ThorMaster");
    }
    else if (!strcmp(compType, "HoleCluster"))
    {
        xpath.append("HoleControl");
    }
    else
        throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "Failed to resolve address for component type '%s'", compType);

    Owned<IPropertyTree> pComponent = getComponent(compType, address);
    xpath.append("Process[1]/@computer");
    const char* computer = pComponent->queryProp(xpath.str());

    if (!computer || !*computer)
        throw MakeStringException(ECLWATCH_INVALID_COMPONENT_INFO, "Failed to resolve computer for %s '%s'!", compType, address);

    Owned<IConstEnvironment> pConstEnv = getConstEnvironment();
    Owned<IConstMachineInfo> pConstMc  = pConstEnv->getMachine(computer);

    SCMStringBuffer sAddress;
    pConstMc->getNetAddress(sAddress);
#ifndef OLD_START_STOP
    {
        StringBuffer sConfigAddress;
        sConfigAddress.append(sAddress.str());

        if (!strcmp(sAddress.str(), "."))
        {
            StringBuffer ipStr;
            IpAddress ipaddr = queryHostIP();
            ipaddr.getIpText(ipStr);
            if (ipStr.length() > 0)
            {
#ifdef MACHINE_IP
                sAddress.set(MACHINE_IP);
#else
                sAddress.set(ipStr.str());
#endif
            }
        }

        if (process.length() > 0)
            newAddress.clear().appendf("%s|%s:%s:%s:%s:%s", sAddress.str(), sConfigAddress.str(), process.str(), compName, OS, path);
        else
            newAddress.clear().appendf("%s|%s:%s:%s:%s:%s", sAddress.str(), sConfigAddress.str(), compType, compName, OS, path);
    }
#else
        if (process.length() > 0)
            newAddress.clear().appendf("%s:%s:%s:%s:%s", sAddress.str(), process.str(), compName, OS, path);
        else
            newAddress.clear().appendf("%s:%s:%s:%s:%s", sAddress.str(), compType, compName, OS, path);
        //newAddress.clear().appendf("%s:ThorMasterProcess:%s:%s:%s", sAddress.str(), compName, OS, path);
#endif
    return;
}

bool Cws_machineEx::doStartStop(IEspContext &context, StringArray& addresses, char* userName, char* password, bool bStop,
                                                     IEspStartStopResponse &resp)
{
    bool containCluster = false;
    double version = context.getClientVersion();
    const int ordinality= addresses.ordinality();

    UnsignedArray threadHandles;
    IArrayOf<IEspStartStopResult> resultsArray;

    for (int index=0; index<ordinality; index++)
    {
        const char* address0 = addresses.item(index);

        //address passed in is of the form "192.168.1.4:EspProcess:2:path1"
        StringArray sArray;
        sArray.appendList(addresses.item(index), ":");

        if (sArray.ordinality() < 4)
            throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Incomplete arguments");

        Owned<IEspStartStopResult> pResult = static_cast<IEspStartStopResult*>(new CStartStopResult(""));
        const char* address = sArray.item(0);
        const char* compType= sArray.item(1);
        const char* OS        = sArray.item(3);//index 2 is component name
        const char* path      = sArray.item(4);

        if (!(address && *address && compType && *compType && OS && *OS && path && *path))
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "Invalid input");

        if (!stricmp(compType, "ThorCluster") || !stricmp(compType, "RoxieCluster"))
            containCluster = true;

#ifndef OLD_START_STOP
        {
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

            StringBuffer newAddress;
            ConvertAddress(address0, newAddress);
            pResult->setAddressOrig ( newAddress.str() );//can be either IP or name of component
            pResult->setAddress ( address );//can be either IP or name of component
            pResult->setCompType( compType );
            if (version > 1.04)
            {       
                pResult->setName( path );
                const char* pStr2 = strstr(path, "LexisNexis");
                if (pStr2)
                {
                    char name[256];
                    const char* pStr1 = strchr(pStr2, '|');
                    if (!pStr1)
                    {
                        strcpy(name, pStr2+11);
                    }
                    else
                    {
                        strncpy(name, pStr2+11, pStr1 - pStr2 -11);
                        name[pStr1 - pStr2 -11] = 0;
                    }
                    pResult->setName( name );
                }   
            }
            
            pResult->setOS( atoi(OS) ); 
            pResult->setPath( path );

            resultsArray.append(*pResult.getLink());

            CStartStopThreadParam* pThreadReq;
            pThreadReq = new CStartStopThreadParam(address, configAddress, bStop, m_useDefaultHPCCInit, this, context);
            pThreadReq->setResultObject( pResult );

            if (userName && *userName)
                pThreadReq->setUserID( userName );
            if (password && *password)
                pThreadReq->setPassword( password );

            PooledThreadHandle handle = m_threadPool->start( pThreadReq );
            threadHandles.append(handle);
        }
#else
        {
            StringBuffer newAddress;
            ConvertAddress(address0, newAddress);
            char* pStr = (char*) strchr(address, '|');;
            if (pStr)
                pStr[0] = 0;

            pResult->setAddressOrig ( newAddress.str() );//can be either IP or name of component
            pResult->setAddress ( address );//can be either IP or name of component
            pResult->setCompType( compType );
            pResult->setOS( atoi(OS) ); 
            pResult->setPath( path );

            resultsArray.append(*pResult.getLink());

            CStartStopThreadParam* pThreadReq;
            pThreadReq = new CStartStopThreadParam(address, bStop, this, context);
            pThreadReq->setResultObject( pResult );

            if (userName && *userName)
                pThreadReq->setUserID( userName );
            if (password && *password)
                pThreadReq->setPassword( password );

            PooledThreadHandle handle = m_threadPool->start( pThreadReq );
            threadHandles.append(handle);
        }
#endif
    }

    //block for worker theads to finish, if necessary, and then collect results
    //
    PooledThreadHandle* pThreadHandle = threadHandles.getArray();
    unsigned i=threadHandles.ordinality();
    while (i--) 
    {
        m_threadPool->join(*pThreadHandle, 30000);//abort after 30 secs in remote possibility that the command blocks
        pThreadHandle++;
    }

    resp.setStartStopResults(resultsArray);
    resp.setStop(bStop);

    if (version > 1.08)
    {
        resp.setContainCluster(containCluster);
    }
    return true;
}

bool Cws_machineEx::onStartStop( IEspContext &context, IEspStartStopRequest &req, 
                                         IEspStartStopResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(EXEC_FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_EXECUTION_ACCESS_DENIED, "Permission denied.");

        char* userName = (char*) m_sTestStr1.str();
        char* password = (char*) m_sTestStr2.str();
        doStartStop(context, req.getAddresses(), userName, password, req.getStop(), resp);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void Cws_machineEx::updatePathInAddress(const char* address, StringBuffer& addrStr)
{
    addrStr.append(address);

    StringArray sArray;
    sArray.appendList(address, ":");
    const char* OS    = sArray.item(3);
    const char* Dir  = sArray.item(4);
    if (OS && *OS && Dir && *Dir)
    {
        char oldC1 = '/';
        char oldC2 = '$';
        char newC1 = '\\';
        char newC2 = ':';
        int os = atoi(OS);      
        if (os == 2) //2: linux
        {
            oldC1 = '\\';
            oldC2 = ':';
            newC1 = '/';
            newC2 = '$';
        }
        StringBuffer dirStr(Dir);
        dirStr.replace(oldC1, newC1);
        dirStr.replace(oldC2, newC2);
        if ((os == 2) && (dirStr.charAt(0) != '/'))
            dirStr.insert(0, '/');

        addrStr.clear();
        for (unsigned i = 0; i < sArray.length(); i++)
        {
            const char* item  = sArray.item(i);
            if (i == 4)
                addrStr.appendf(":%s", dirStr.str());
            else if (item && *item)
            {
                if (i == 0)
                    addrStr.append(item);
                else
                    addrStr.appendf(":%s", item);
            }
        }
    }

    return;
}

bool Cws_machineEx::onStartStopBegin( IEspContext &context, IEspStartStopBeginRequest &req, 
                                         IEspStartStopBeginResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(EXEC_FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_EXECUTION_ACCESS_DENIED, "Permission denied.");

        StringBuffer addresses;
        StringArray& addresses0 = req.getAddresses();
        for(unsigned i = 0; i < addresses0.length(); i++)
        {
            StringBuffer addrStr;
            const char* address = addresses0.item(i);
            updatePathInAddress(address, addrStr);
            if (i > 0)
                addresses.appendf("|Addresses_i%d=%s", i+1, addrStr.str());
            else
                addresses.appendf("Addresses_i1=%s", addrStr.str());
        }

        resp.setAddresses(addresses);
        resp.setKey1(req.getKey1());
        resp.setKey2(req.getKey2());
        resp.setStop(req.getStop());
        double version = context.getClientVersion();
        if (version > 1.07)
        {
            resp.setAutoRefresh( req.getAutoRefresh() );
            resp.setMemThreshold(req.getMemThreshold());
            resp.setDiskThreshold(req.getDiskThreshold());
            resp.setCpuThreshold(req.getCpuThreshold());
            resp.setMemThresholdType(req.getMemThresholdType());
            resp.setDiskThresholdType(req.getDiskThresholdType());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_machineEx::onStartStopDone( IEspContext &context, IEspStartStopDoneRequest &req, 
                                         IEspStartStopResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(EXEC_FEATURE_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_EXECUTION_ACCESS_DENIED, "Permission denied.");

        const char*addresses0 = req.getAddresses();
        bool bStop = req.getStop();

        char* userName = (char*) m_sTestStr1.str();
        char* password = (char*) m_sTestStr2.str();

        StringArray addresses;
        char* pAddr = (char*) addresses0;
        while (pAddr)
        {
            char* ppAddr = strstr(pAddr, "|Addresses_");
            if (!ppAddr)
            {
                char* ppAddr0 = strchr(pAddr, '=');
                if (!ppAddr0)
                    addresses.append(pAddr);
                else
                    addresses.append(ppAddr0+1);
                break;
            }
            else
            {
                char addr[1024];
                strncpy(addr, pAddr, ppAddr - pAddr);
                addr[ppAddr - pAddr] = 0;
                char* ppAddr0 = strchr(addr, '=');
                if (!ppAddr0)
                    addresses.append(addr);
                else
                    addresses.append(ppAddr0+1);

                pAddr = ppAddr + 1;
            }
        }

        doStartStop(context, addresses, userName, password, bStop, resp);

        double version = context.getClientVersion();
        if (version > 1.07)
        {
            resp.setAutoRefresh( req.getAutoRefresh() );
            resp.setMemThreshold(req.getMemThreshold());
            resp.setDiskThreshold(req.getDiskThreshold());
            resp.setCpuThreshold(req.getCpuThreshold());
            resp.setMemThresholdType(req.getMemThresholdType());
            resp.setDiskThresholdType(req.getDiskThresholdType());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

