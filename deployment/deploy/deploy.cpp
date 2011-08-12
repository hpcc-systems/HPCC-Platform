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
#include "deploy.hpp"
#include "environment.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jencrypt.hpp"
#include "xslprocessor.hpp"
#include "DeploymentEngine.hpp"
#include "ThorDeploymentEngine.hpp"
#include "EspDeploymentEngine.hpp"
#include "dalideploymentengine.hpp"
#include "RoxieDeploymentEngine.hpp"
#include "configgenengine.hpp"
#include "espconfiggenengine.hpp"
#include "thorconfiggenengine.hpp"


//---------------------------------------------------------------------------
//  CEnvironmentDeploymentEngine
//---------------------------------------------------------------------------
class CEnvironmentDeploymentEngine : public CInterface, implements IEnvDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    //---------------------------------------------------------------------------
    //  CEnvironmentDeploymentEngine
    //---------------------------------------------------------------------------
    CEnvironmentDeploymentEngine(IConstEnvironment &environment, IDeploymentCallback& callback, 
        IPropertyTree* pSelectedComponents)
        : m_environment(environment), 
        m_transform(NULL), 
        m_abort(false),
        m_espModuleCount(0), 
        m_tempFileCount(0),
        m_bLinuxDeployment(false),
        m_bInteractiveMode(true)
    {
        m_pCallback.set(&callback);

    if (pSelectedComponents)
    {
      initXML(pSelectedComponents);
        
          Owned<IPropertyTreeIterator> it = pSelectedComponents->getElements("*");
          ForEach(*it)
          {
              IPropertyTree* pComponent = &it->query();
              IDeploymentEngine* pEngine = addProcess(pComponent->queryName(), pComponent->queryProp("@name"));
            
              Owned<IPropertyTreeIterator> iter = pComponent->getElements("*");
              if (iter->first())
              {
                  pEngine->resetInstances();
                  ForEach(*iter)
                  {
                      IPropertyTree* pChild = &iter->query();
                      const char* tagName  = pChild->queryName();
                      const char* instName = pChild->queryProp("@name");
                      pEngine->addInstance(tagName, instName);
                    
                      //determine if this is linux deployment
                      if (!m_bLinuxDeployment)
                      {
                          const char* computer = pChild->queryProp("@computer");
                          IConstMachineInfo* pMachine = environment.getMachine(computer);
                          if (pMachine->getOS() == MachineOsLinux)
                              m_bLinuxDeployment = true;
                      }
                  }
              }
              else if (!m_bLinuxDeployment)//another previously added engine already does not have linux instance
              {
                  //some components like thor and hole clusters don't show their instances in the 
                  //deployment wizard so detect if they have any linux instance.
                  const IArrayOf<IPropertyTree>& instances = pEngine->getInstances();
                  if (instances.ordinality() > 0)
                  {
                      Owned<IConstMachineInfo> machine = m_environment.getMachine(instances.item(0).queryProp("@computer"));
                      if (machine && machine->getOS() == MachineOsLinux)
                          m_bLinuxDeployment = true;
                  }
              }
          }
    }

#ifdef _WINDOWS
        EnumerateNetworkConnections();
#endif
    }
    
    //---------------------------------------------------------------------------
    //  ~CEnvironmentDeploymentEngine
    //---------------------------------------------------------------------------
    virtual ~CEnvironmentDeploymentEngine()
    {
        //delete all temporary files generated during deployemnt
        int count = m_tempFiles.length();
        int i;
        for (i = 0; i < count; i++)
            DeleteFile(m_tempFiles.item(i));
        
        count = m_tempDirs.length();
        for (i = 0; i < count; i++)
            deleteRecursive(m_tempDirs.item(i));
        
        m_processes.kill(); // must do this before destroying deplyCallback and deployLog
        m_pDeployLog.clear(); // this causes the log file to be written
        m_pCallback.clear();
        
        termXML();
    }
    
    void deleteRecursive(const char* path)
    {
        Owned<IFile> pDir = createIFile(path);
        if (pDir->exists())
        {
            if (pDir->isDirectory())
            {
                Owned<IDirectoryIterator> it = pDir->directoryFiles(NULL, false, true);
                ForEach(*it)
                {               
                    StringBuffer name;
                    it->getName(name);
                    
                    StringBuffer childPath(path);
                    childPath.append(PATHSEPCHAR);
                    childPath.append(name);
                    
                    deleteRecursive(childPath.str());
                }
            }
            pDir->remove();
        }
    }
    
    //---------------------------------------------------------------------------
    //  addProcess
    //---------------------------------------------------------------------------
    IDeploymentEngine* addProcess(const char* processType, const char* processName)
    {
        assertex(processType);
        assertex(processName);
        StringBuffer xpath;
        xpath.appendf("Software/%s[@name='%s']", processType, processName);
        
        Owned<IPropertyTree> tree = &m_environment.getPTree();
        IPropertyTree* pComponent = tree->queryPropTree(xpath.str());
        if (!pComponent)
            throw MakeStringException(0, "%s with name %s was not found!", processType, processName);
        
        IDeploymentEngine* deployEngine;
        
        if (strcmp(processType, "DaliServerProcess")==0)
            deployEngine = new CDaliDeploymentEngine(*this, *m_pCallback, *pComponent);
        else if (strcmp(processType, "ThorCluster")==0)
            deployEngine = new CThorDeploymentEngine(*this, *m_pCallback, *pComponent);
        else if (strcmp(processType, "RoxieCluster")==0)
            deployEngine = new CRoxieDeploymentEngine(*this, *m_pCallback, *pComponent);
        else if (strcmp(processType, "EspProcess")==0)
            deployEngine = new CEspDeploymentEngine(*this, *m_pCallback, *pComponent);
        else
            deployEngine = new CDeploymentEngine(*this, *m_pCallback, *pComponent, "Instance", true);
        
        assertex(deployEngine);
        deployEngine->setXsl(m_processor, m_transform);
        m_processes.append(*deployEngine); // array releases members when destroyed
        return deployEngine;
    }
    
    //---------------------------------------------------------------------------
    //  setSshAccount
    //---------------------------------------------------------------------------
    void setSshAccount(const char* userid, const char* password)
    {
        m_sSshUserid   = userid;
        m_sSshPassword = password;
    }
    
    //---------------------------------------------------------------------------
    //  isLinuxDeployment
    //---------------------------------------------------------------------------
    bool isLinuxDeployment() const
    {
        return m_bLinuxDeployment;
    }
    
    //---------------------------------------------------------------------------
    //  start
    //---------------------------------------------------------------------------
    void start()
    {
        ForEachItemIn(idx, m_processes)
        {
            m_processes.item(idx).start();
        }
    }
    
    //---------------------------------------------------------------------------
    //  stop
    //---------------------------------------------------------------------------
    void stop()
    {
        ForEachItemInRev(idx, m_processes)
        {
            m_processes.item(idx).stop();
        }
    }
    
    //---------------------------------------------------------------------------
    //  stripXsltMessage
    //---------------------------------------------------------------------------
    void stripXsltMessage(StringBuffer& msg)
    {
        //for better readability of msg, remove redundant prefix of "[XSLT warning: ", if present
        const char* pattern = "[ElemMessageTerminateException: ";
        const int len =sizeof("[ElemMessageTerminateException: ")-1;
        if (!strncmp(msg, pattern, len))
            msg.remove(0, len);
        
        //remove the excessive info about XSLT context when this was thrown
        const char* begin = msg.str();
        const char* end   = strstr(begin, "(file:");
        
        if (end)
            msg.setLength(end-begin);
    }
    
    //---------------------------------------------------------------------------
    //  check
    //---------------------------------------------------------------------------
    void check()
    {
        if (m_pCallback->getAbortStatus())
            throw MakeStringException(0, "User abort");
        
        bool valid = false;
        m_nValidationErrors = 0;
        
        StringBuffer outputXml;
        
        Owned<IXslFunction>  externalFunction;
        externalFunction.setown(m_transform->createExternalFunction("validationMessage", validationMessageFromXSLT));
        m_transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction.get(), true);
        
        m_transform->setXslSource("validateAll.xsl");
        m_transform->setUserData(this);
        
        try
        {
            m_transform->transform( outputXml );
            m_transform->closeResultTarget();
            
            const char* msg = m_transform->getMessages();
            if (msg && *msg)
            {
                /*
                //there may be multiple warnings messages bundled here so process each of them:
                StringArray msgs;
                DelimToStringArray(msg, msgs, "\n");
                
                ForEachItemIn(idx, msgs)
                {
                    msg = msgs.item(idx);
                    if (msg && *msg)
                    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, msg);
                }
                */
                m_sValidationErrors.append(msg);
                m_nValidationErrors++;
            }

            if (!m_nValidationErrors)//this may get filled in by the external function
                valid = true;
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();

            stripXsltMessage(msg);
            m_sValidationErrors.append(msg);
        }
        catch (...)
        {
            m_sValidationErrors.appendf("Validation failed: Unspecified XSL error!");
        }

        m_transform->setExternalFunction(SEISINT_NAMESPACE, externalFunction.get(), false);
        m_transform->setUserData(NULL);

        if (!valid)
        {
            const char* errors = m_sValidationErrors.str();
            if (!errors || !*errors)
                errors = "Continue?";

            m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Preliminary validation failed!");
            while ( !m_pCallback->processException(NULL, NULL, NULL, NULL, errors, "Preliminary validation failed!", NULL) )
                ;
            valid = true; //ignore validation errors                
        }

        if (valid)
        {
            ForEachItemIn(idx, m_processes)
                m_processes.item(idx).check();
            
            m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
        }
    }
    
    
    //---------------------------------------------------------------------------
    //  compare
    //---------------------------------------------------------------------------
    void compare(unsigned mode)
    {
        ForEachItemIn(idx, m_processes)
        {
            m_processes.item(idx).compare(mode);
        }
    }
    
    //---------------------------------------------------------------------------
    //  deploy
    //---------------------------------------------------------------------------
    void deploy(unsigned flags, bool useTempDir)
    {
        m_tempFileCount = m_espModuleCount = 0;
        if (flags != DEFLAGS_NONE)
        {
            ForEachItemIn(idx, m_processes)
            {
                m_processes.item(idx).deploy(flags, useTempDir);
            }
        }
    }
    
    //---------------------------------------------------------------------------
    //  deploy
    //---------------------------------------------------------------------------
    void deploy(unsigned flags, BackupMode backupMode, bool bStop, bool bStart)
    {
        switch (backupMode)
        {
        case DEBACKUP_NONE:
            if (bStop)
                stop();
            deploy(flags, false);
            if (bStart)
                start();
            break;
            
        case DEBACKUP_COPY:
            backupDirs();
            if (bStop)
                stop();
            deploy(flags, false);
            if (bStart)
                start();
            break;
            
        case DEBACKUP_RENAME:
            deploy(flags, true);
            if (bStop)
                stop();
            renameDirs();
            if (bStart)
                start();
            break;
            
        default:
            assertex(false);
        }
    }
    
    //---------------------------------------------------------------------------
    //  renameDirs
    //---------------------------------------------------------------------------
    void renameDirs()
    {
        ForEachItemIn(idx, m_processes)
        {
            m_processes.item(idx).renameDirs();
        }
    }
    
    //---------------------------------------------------------------------------
    //  backupDirs
    //---------------------------------------------------------------------------
    void backupDirs()
    {
        ForEachItemIn(idx, m_processes)
        {
            m_processes.item(idx).backupDirs();
        }
    }
    
    //---------------------------------------------------------------------------
    //  abort
    //---------------------------------------------------------------------------
    void abort()
    {
        m_abort = true;
        ForEachItemIn(idx, m_processes)
        {
            m_processes.item(idx).abort();
        }
    }
    
    //---------------------------------------------------------------------------
    //  archive
    //---------------------------------------------------------------------------
    void archive(const char* filename)
    {
        if (!filename || !*filename) return;
        if (m_abort)
        {
            m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Aborted!");
            throw MakeStringException(0, "User abort");
        }
        
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Archiving environment data to %s...", filename);
        Owned<IPropertyTree> tree = &m_environment.getPTree();
        StringBuffer xml;
        toXML(tree, xml);
        Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Archive File", NULL, NULL, NULL, NULL, 
            filename, "", "", "", false);
        task->createFile(xml.str());
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
        if (task->getAbort())
            throw MakeStringException(0, "User abort");
        
        Owned<IFile> pFile = createIFile(filename);
        pFile->setReadOnly(true);
    }
    
    //---------------------------------------------------------------------------
    //  setLog
    //---------------------------------------------------------------------------
    void setLog(const char* filename, const char* envname)
    {
        if (!filename || !*filename) return;
        m_pDeployLog.setown(createDeployLog(*m_pCallback, filename, envname));
    }
    
    //---------------------------------------------------------------------------
    //  initXML
    //---------------------------------------------------------------------------
    void initXML(IPropertyTree* pSelectedComponents)
    {
        if (m_abort)
            throw MakeStringException(0, "User abort");
        
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Loading environment...");
        m_processor.setown(getXslProcessor());
        m_transform.setown(m_processor->createXslTransform());
        
        //decrypt external function is no longer used by any xslt
        //
        //m_externalFunction.setown(m_transform->createExternalFunction("decrypt", decrypt));
        //m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction.get(), true);
        
        Owned<IPropertyTree> tree = &m_environment.getPTree();
        
        IPropertyTree* pDeploy = tree->queryPropTree("DeployComponents");
        if (pDeploy)
            tree->removeTree(pDeploy);
        pDeploy = tree->addPropTree("DeployComponents", createPTreeFromIPT(pSelectedComponents));
        
        StringBuffer xml;
        toXML(tree, xml);
        
        tree->removeTree(pDeploy);
        
        if (m_transform->setXmlSource(xml.str(), xml.length()) != 0)
            throw MakeStringException(0, "Invalid environment XML string");
    }
    //---------------------------------------------------------------------------
    //  termXML
    //---------------------------------------------------------------------------
    void termXML()
    {
        //decrypt external function is no longer used by any xslt
        //
        //m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction.get(), false);
        m_externalFunction.clear();
        m_transform.clear();
        m_processor.clear();
    }
    
    //---------------------------------------------------------------------------
    //  incrementTempFileCount
    //---------------------------------------------------------------------------
    virtual int incrementTempFileCount() 
    {
        return ++m_tempFileCount;
    }
    
    //---------------------------------------------------------------------------
    //  incrementEspModuleCount
    //---------------------------------------------------------------------------
    virtual int  incrementEspModuleCount()
    {
        return ++m_espModuleCount;
    }
    
    //---------------------------------------------------------------------------
    //  validationMessageFromXSLT
    //---------------------------------------------------------------------------
    static void validationMessageFromXSLT(StringBuffer &ret, const char *in, IXslTransform* pTransform)
    {
        CEnvironmentDeploymentEngine* pEnvDepEngine = (CEnvironmentDeploymentEngine*) pTransform->getUserData();
        IDeploymentCallback* pCallback = pEnvDepEngine->m_pCallback;

        //input 'in' has format of the form [type]:[compType]:[compName]:[message]
        //type is either 'error' or 'warning' and any of the other parts may be empty strings
        //
        StringArray sArray;
        DelimToStringArray(in, sArray, ":");

        if (sArray.ordinality() != 4)
        {
            pCallback->printStatus(STATUS_ERROR, NULL, NULL, NULL, "%s", in);
            return;
        }
        
        const char* msgType  = sArray.item(0);
        const char* compType = sArray.item(1);
        const char* compName = sArray.item(2);
        const char* msg      = sArray.item(3);

        if (compType && !*compType)
            compType = NULL;
        
        if (compName && !*compName)
            compName = NULL;

        StatusType statusType;
        if (!stricmp(msgType, "error"))
        {
            statusType = STATUS_ERROR;
            pEnvDepEngine->m_nValidationErrors++;

            if (!compType || !compName)//if this error is not being reported under a particular component in tree
                pEnvDepEngine->m_sValidationErrors.append(msg);
        }
        else if (!strnicmp(msgType, "warn", 4))
            statusType = STATUS_WARN;
        else if (!stricmp(msgType, "OK"))
            statusType = STATUS_OK;
        else if (!strnicmp(msgType, "inc", 3))
            statusType = STATUS_INCOMPLETE;
        else statusType = !stricmp(msgType, "normal") ? STATUS_NORMAL : STATUS_ERROR;

        try
        {
            if (pCallback)
                pCallback->printStatus( statusType, 
                compType, 
                compName, 
                NULL, 
                "%s", msg);
        }
        catch (IException* e)
        {
            StringBuffer buf;
            e->errorMessage(buf);
            e->Release();

            pCallback->printStatus(STATUS_ERROR, NULL, NULL, NULL, "%s", buf.str());
        }
        catch(...)
        {
            pCallback->printStatus(STATUS_ERROR, NULL, NULL, NULL, "Unknown exception!");
        }
    }
    
    //---------------------------------------------------------------------------
    //  getCallback
    //---------------------------------------------------------------------------
    virtual IDeploymentCallback& getCallback() const
    {
        return *m_pCallback;
    }
    
    //---------------------------------------------------------------------------
    //  setInteractiveMode
    //---------------------------------------------------------------------------
    virtual void setInteractiveMode(bool bSet)
    {
        m_bInteractiveMode = bSet;
    }
    
    //---------------------------------------------------------------------------
    //  getEnvironment
    //---------------------------------------------------------------------------
    IConstEnvironment& getEnvironment() const 
    { 
        return m_environment; 
    }

    //---------------------------------------------------------------------------
    //  getInteractiveMode
    //---------------------------------------------------------------------------
    virtual bool getInteractiveMode() const
    {
        return m_bInteractiveMode;
    }

    //---------------------------------------------------------------------------
    //  getDeployLog
    //---------------------------------------------------------------------------
    virtual IDeployLog* getDeployLog()
    {
        return m_pDeployLog ? m_pDeployLog.getLink() : NULL;
    }

    //---------------------------------------------------------------------------
    //  addTempFile
    //---------------------------------------------------------------------------
    virtual void addTempFile(const char* filePath)
    {
        if (filePath)
            m_tempFiles.append(filePath);
    }

    //---------------------------------------------------------------------------
    //  addTempDirectory
    //---------------------------------------------------------------------------
    virtual void addTempDirectory(const char* dirPath)
    {
        if (dirPath)
            m_tempDirs.append(dirPath);
    }
    //---------------------------------------------------------------------------
    //  setDeployToFolder
    //---------------------------------------------------------------------------
    virtual void setDeployToFolder(const char* path)
    {
        StringBuffer machineName;
        StringBuffer localPath;
        StringBuffer tail;
        StringBuffer ext;

        if (splitUNCFilename(path, &machineName, &localPath, &tail, &ext))
        {

            const char* hostName = machineName.str() + 2;
            Owned<IConstMachineInfo> machine = m_environment.getMachine(hostName);
            if (!machine)
                throw MakeStringException(-1, "The computer '%s' used for deployment folder is undefined!", hostName);

            StringAttr netAddress;
            StringAttrAdaptor adaptor(netAddress);
            machine->getNetAddress(adaptor);
            if (!netAddress.get() || !*netAddress.get())
                throw MakeStringException(-1, 
                "The computer '%s' used for deployment folder does not have any network address defined!", hostName);

            StringBuffer uncPath(PATHSEPSTR PATHSEPSTR);
            uncPath.append( netAddress.get() );
            if (*localPath.str() != PATHSEPCHAR)
                uncPath.append( PATHSEPCHAR );
            uncPath.append( localPath );//note that the path ends with PATHSEPCHAR
            uncPath.append( tail );
            uncPath.append( ext );
            
            m_sDeployToFolder.set( uncPath.str() );

            getAccountInfo(hostName, m_sDeployToUser, m_sDeployToPswd);
        }
        else
            m_sDeployToFolder.set( path );

            /*
            Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Create Directory", NULL, NULL, NULL, NULL, m_sDeployToFolder.get());
            m_pCallback->printStatus(task);
            task->createDirectory();
            m_pCallback->printStatus(task);
        */
    }

    //---------------------------------------------------------------------------
    //  getDeployToFolder
    //---------------------------------------------------------------------------
    virtual const char* getDeployToFolder() const
    {
        return m_sDeployToFolder.get();
    }

    //---------------------------------------------------------------------------
    //  getDeployToAccountInfo
    //---------------------------------------------------------------------------
    virtual void getDeployToAccountInfo(const char*& user, const char*& pswd) const
    {
        user = m_sDeployToUser.get();
        pswd = m_sDeployToPswd.get();
    }

    //---------------------------------------------------------------------------
    //  lookupNetAddress
    //---------------------------------------------------------------------------
    StringAttr& lookupNetAddress(StringAttr& str, const char* computer) const
    {
        Owned<IConstMachineInfo> machine = m_environment.getMachine(computer);
        if (machine)
        {
            StringAttrAdaptor adaptor(str);
            machine->getNetAddress(adaptor);
        }
        return str;
    }

    //---------------------------------------------------------------------------
    //  lookupMachineOS
    //---------------------------------------------------------------------------
    EnvMachineOS lookupMachineOS(IPropertyTree& node) const
    {
        Owned<IConstMachineInfo> machine = m_environment.getMachine(node.queryProp("@computer"));
        return machine ? machine->getOS() : MachineOsUnknown;
    }

    //---------------------------------------------------------------------------
    //  getAccountInfo
    //---------------------------------------------------------------------------
    void getAccountInfo(const char* computer, StringAttr& user, StringAttr& pwd) const
    {
        Owned<IConstMachineInfo> machine = m_environment.getMachine(computer);
        if (machine)
        {
            Owned<IConstDomainInfo> domain = machine->getDomain();
            if (!domain)
                throw MakeStringException(-1, "The computer '%s' does not have any domain information!", computer);

            StringBuffer x;
            if (machine->getOS() == MachineOsW2K)
            {
                domain->getName(StringBufferAdaptor(x));
                if (x.length()) 
                    x.append(PATHSEPCHAR);
            }

            domain->getAccountInfo(StringBufferAdaptor(x), StringAttrAdaptor(pwd));
            user.set(x.str());
        }
        else
            throw MakeStringException(-1, "The computer '%s' is undefined!", computer);
    }

    //---------------------------------------------------------------------------
    //  getSSHAccountInfo
    //---------------------------------------------------------------------------
    void getSSHAccountInfo(const char* computer, StringAttr& user, StringAttr& sshKeyFile, StringAttr& sshKeyPassphrase) const
    {
        Owned<IConstMachineInfo> machine = m_environment.getMachine(computer);
        if (machine)
        {
            Owned<IConstDomainInfo> domain = machine->getDomain();
            if (!domain)
                throw MakeStringException(-1, "The computer '%s' does not have any domain information!", computer);

            StringBuffer x;
            if (machine->getOS() == MachineOsW2K)
            {
                domain->getName(StringBufferAdaptor(x));
                if (x.length()) 
                    x.append(PATHSEPCHAR);
            }

            domain->getSSHAccountInfo(StringBufferAdaptor(x), StringAttrAdaptor(sshKeyFile), StringAttrAdaptor(sshKeyPassphrase));
            user.set(x.str());
        }
        else
            throw MakeStringException(-1, "The computer '%s' is undefined!", computer);
    }

    virtual void setSourceDaliAddress( const char* addr )
    {
        m_sSrcDaliAddress.set( addr );
    }

    virtual const char* getSourceDaliAddress()
    {
        return m_sSrcDaliAddress.get();
    }

    virtual IArrayOf<IDeploymentEngine>& queryProcesses() { return m_processes; }

#ifdef _WINDOWS
    void NetErrorHandler(DWORD dwResult)
    {
        StringBuffer out;
        formatSystemError(out, dwResult);

        out.insert(0, "Failed to enumerate existing network connections:\n");
        while ( !m_pCallback->processException(NULL, NULL, NULL, NULL, out, "Network Error", NULL) )
            ;
    }

    bool EnumerateNetworkConnections()
    {
        DWORD dwResult;
        HANDLE hEnum;
        DWORD cbBuffer = 16384;         // 16K is a good size
        DWORD cEntries = -1;                // enumerate all possible entries
        LPNETRESOURCE lpnr;     // pointer to enumerated structures
        //
        // Call the WNetOpenEnum function to begin the enumeration.
        //
        dwResult = WNetOpenEnum(RESOURCE_CONNECTED, // connected network resources
                                        RESOURCETYPE_ANY,// all resources
                                        0,          // enumerate all resources
                                        NULL,       // NULL first time the function is called
                                        &hEnum);    // handle to the resource

        if (dwResult != NO_ERROR)
        {
            NetErrorHandler(dwResult);
            return false;
        }
        //
        // Call the GlobalAlloc function to allocate resources.
        //
        lpnr = (LPNETRESOURCE) GlobalAlloc(GPTR, cbBuffer);

        do
        {
            ZeroMemory(lpnr, cbBuffer);

            // Call the WNetEnumResource function to continue
            //  the enumeration.
            //
            dwResult = WNetEnumResource( hEnum,     // resource handle
            &cEntries,  // defined locally as -1
            lpnr,   // LPNETRESOURCE
            &cbBuffer); // buffer size
            // If the call succeeds, loop through the structures.
            //
            if (dwResult == NO_ERROR)
            {
                StringBuffer networkPath;
                for (DWORD i = 0; i < cEntries; i++)
                    if (lpnr[i].lpRemoteName)
                    {
                        // make a valid UNC path to connect to and see if we are not already connected
                        if (CDeploymentEngine::stripTrailingDirsFromUNCPath(lpnr[i].lpRemoteName, networkPath.clear()) && 
                            m_persistentConnections.find( networkPath.str() ) == m_persistentConnections.end())
                        {
                            //::MessageBox(NULL, networkPath.str(), lpnr[i].lpRemoteName, MB_OK);
                            m_persistentConnections.insert( networkPath.str() );
                        }
                    }
            }
            else if (dwResult != ERROR_NO_MORE_ITEMS)
            {
                NetErrorHandler(dwResult);
                break;
            }
        }
        while (dwResult != ERROR_NO_MORE_ITEMS);

        GlobalFree((HGLOBAL)lpnr); // free the memory

        dwResult = WNetCloseEnum(hEnum); // end the enumeration
        if (dwResult != NO_ERROR)
        { 
            NetErrorHandler(dwResult);
            return false;
        }
        return true;
    }

#endif//WINDOWS

    virtual bool IsPersistentConnection(const char* networkPath) const
    {
        return m_persistentConnections.find( networkPath ) != m_persistentConnections.end();
    }

protected:
    IArrayOf<IDeploymentEngine> m_processes;
    IConstEnvironment& m_environment;
    Owned<IDeploymentCallback> m_pCallback;
    Owned<IXslProcessor> m_processor;
    Owned<IXslTransform> m_transform;
    Owned<IXslFunction>  m_externalFunction;
    Owned<IDeployLog>       m_pDeployLog;
    set<string>             m_persistentConnections;
    int  m_espModuleCount;
    int  m_tempFileCount;
    bool m_abort;
    bool m_bLinuxDeployment;
    bool m_bInteractiveMode;
    StringBuffer m_sSshUserid;
    StringBuffer m_sSshPassword;
    unsigned int m_nValidationErrors;
    StringBuffer m_sValidationErrors;
    StringArray  m_tempFiles;
    StringArray  m_tempDirs;
    StringAttr   m_sDeployToFolder;
    StringAttr   m_sDeployToUser;
    StringAttr   m_sDeployToPswd;
    StringAttr   m_sSrcDaliAddress;
};


class CConfigGenMgr : public CEnvironmentDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    //---------------------------------------------------------------------------
    //  CConfigGenMgr
    //---------------------------------------------------------------------------
  CConfigGenMgr(IConstEnvironment& environment, IDeploymentCallback& callback, 
    IPropertyTree* pSelectedComponents, const char* inputDir, const char* outputDir, const char* compName, const char* compType, const char* ipAddr)
    : CEnvironmentDeploymentEngine(environment, callback, pSelectedComponents),
    m_inDir(inputDir), 
    m_outDir(outputDir),
    m_compName(compName),
    m_compType(compType),
    m_hostIpAddr(ipAddr)
  {
    Owned<IPropertyTree> pSelComps;
    if (!pSelectedComponents)
    {
      Owned<IPropertyTree> pEnvTree = &m_environment.getPTree();
      pSelComps.setown(getInstances(pEnvTree, compName, compType, ipAddr));
      pSelectedComponents = pSelComps;
    }

    initXML(pSelectedComponents);

    {
      Owned<IPropertyTreeIterator> it = pSelectedComponents->getElements("*");
      ForEach(*it)
      {
        IPropertyTree* pComponent = &it->query();
        IDeploymentEngine* pEngine = addProcess(pComponent->queryName(), pComponent->queryProp("@name"));

        Owned<IPropertyTreeIterator> iter = pComponent->getElements("*");
        if (iter->first())
        {
          pEngine->resetInstances();
          ForEach(*iter)
          {
            IPropertyTree* pChild = &iter->query();
            const char* tagName  = pChild->queryName();
            const char* instName = pChild->queryProp("@name");
            pEngine->addInstance(tagName, instName);

            //determine if this is linux deployment
            if (!m_bLinuxDeployment)
            {
              const char* computer = pChild->queryProp("@computer");
              Owned<IConstMachineInfo> pMachine = environment.getMachine(computer);
              if (!pMachine)
                throw MakeStringException(0, "Invalid Environment file. Instance '%s' of '%s' references a computer '%s' that has not been defined!", pChild->queryProp("@name"), pComponent->queryProp("@name"), computer);
              else if (pMachine->getOS() == MachineOsLinux)
                m_bLinuxDeployment = true;
            }
          }
        }
        else if (!m_bLinuxDeployment)//another previously added engine already does not have linux instance
        {
          //some components like thor and hole clusters don't show their instances in the 
          //deployment wizard so detect if they have any linux instance.
          const IArrayOf<IPropertyTree>& instances = pEngine->getInstances();
          if (instances.ordinality() > 0)
          {
            Owned<IConstMachineInfo> machine;// = m_environment.getMachine(instances.item(0).queryProp("@computer"));
            if (machine && machine->getOS() == MachineOsLinux)
              m_bLinuxDeployment = true;
          }
        }
      }
    }
  }

  //---------------------------------------------------------------------------
  //  addProcess
  //---------------------------------------------------------------------------
  IDeploymentEngine* addProcess(const char* processType, const char* processName)
  {
    assertex(processType);
    assertex(processName);
    StringBuffer xpath;
    xpath.appendf("Software/%s[@name='%s']", processType, processName);

    Owned<IPropertyTree> tree = &m_environment.getPTree();
    IPropertyTree* pComponent = tree->queryPropTree(xpath.str());
    if (!pComponent)
      throw MakeStringException(0, "%s with name %s was not found!", processType, processName);

    IDeploymentEngine* deployEngine;

    if (strcmp(processType, "RoxieCluster")==0)
      deployEngine = new CConfigGenEngine(*this, *m_pCallback, *pComponent, m_inDir, m_outDir, "*");
    else if (strcmp(processType, "ThorCluster")==0)
      deployEngine = new CThorConfigGenEngine(*this, *m_pCallback, *pComponent, m_inDir, m_outDir);
    else if (strcmp(processType, "EspProcess")==0)
      deployEngine = new CEspConfigGenEngine(*this, *m_pCallback, *pComponent, m_inDir, m_outDir);
    else
      deployEngine = new CConfigGenEngine(*this, *m_pCallback, *pComponent, m_inDir, m_outDir, "Instance", true);

    assertex(deployEngine);
    deployEngine->setXsl(m_processor, m_transform);
    m_processes.append(*deployEngine); // array releases members when destroyed
    return deployEngine;
  }

private:
  StringBuffer m_inDir;
  StringBuffer m_outDir;
  StringBuffer m_compName;
  StringBuffer m_compType;
  StringBuffer m_hostIpAddr;
};


//---------------------------------------------------------------------------
// Factory functions
//---------------------------------------------------------------------------
IEnvDeploymentEngine* createEnvDeploymentEngine(IConstEnvironment& environment, 
                                                                IDeploymentCallback& callback,
                                                                IPropertyTree* pSelectedComponents)
{
    try
    {
        return new CEnvironmentDeploymentEngine(environment, callback, pSelectedComponents);
    }
    catch (IException* e)
    {
        throw e;
    }
    catch(...)
    {
        throw MakeStringException(-1, "Unknown exception!");
    }
}

IEnvDeploymentEngine* createConfigGenMgr(IConstEnvironment& env, 
                                         IDeploymentCallback& callback,
                                         IPropertyTree* pSelectedComponents,
                                         const char* inputDir,
                                         const char* outputDir,
                                         const char* compName, 
                                         const char* compType,
                                         const char* ipAddr)
{
  try
  {
    StringBuffer inDir(inputDir);
    if (inDir.length() && inDir.charAt(inDir.length() - 1) != PATHSEPCHAR)
      inDir.append(PATHSEPCHAR);

    StringBuffer outDir(outputDir);
    if (outDir.length() && outDir.charAt(outDir.length() - 1) != PATHSEPCHAR)
      outDir.append(PATHSEPCHAR);

    return new CConfigGenMgr(env, callback, pSelectedComponents, inDir.str(), outDir.str(), compName, compType, ipAddr);
  }
  catch (IException* e)
  {
    throw e;
  }
  catch(...)
  {
    throw MakeStringException(-1, "Unknown exception!");
  }
}

bool matchDeployAddress(const char *searchIP, const char *envIP)
{
    if (searchIP && envIP && *searchIP && *envIP)
    {
        IpAddress ip(envIP);
        if (strcmp(searchIP, ".")==0)
            return ip.isLocal();
        else
        {
            IpAddress ip2(searchIP);
            return ip.ipequals(ip2);
        }
    }
    return false;
}

IPropertyTree* getInstances(const IPropertyTree* pEnvRoot, const char* compName, 
                            const char* compType, const char* ipAddr, bool listall)
{
  Owned<IPropertyTree> pSelComps(createPTree("SelectedComponents"));
  Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements("Software/*");
  const char* instanceNodeNames[] = { "Instance", "RoxieServerProcess", "RoxieSlaveProcess" };
  const char* logDirNames[] = { "@logDir", "@LogDir", "@dfuLogDir", "@eclLogDir" };

  ForEach(*iter)
  {
    IPropertyTree* pComponent = &iter->query();
    const char* type = pComponent->queryName();
    if (stricmp(type, "Topology")!=0 && stricmp(type, "Directories")!=0 && 
        ((!compName && !compType) || (compName && !strcmp(pComponent->queryProp("@name"), compName)) ||
        (!compName && compType && !strcmp(pComponent->queryProp("@buildSet"), compType))))
    {
      const char* name    = pComponent->queryProp("@name");
      const char* build   = pComponent->queryProp("@build");
      const char* buildSet= pComponent->queryProp("@buildSet");
      const char* logDir = NULL;

      if (listall)
        for (int i = 0; i < sizeof(logDirNames)/sizeof(char*); i++)
        {
          logDir = pComponent->queryProp(logDirNames[i]);
          if (logDir)
            break;
        }

      StringBuffer sXPath;
      sXPath.appendf("Programs/Build[@name='%s']/BuildSet[@name='%s']/@deployable", build, buildSet);
      const char* deployable = pEnvRoot->queryProp(sXPath.str());

      //either the @deployable does not exist or it is not one of 'no', 'false' or '0'
      if (!deployable || 
        (strcmp(deployable, "no") != 0 && strcmp(deployable, "false") != 0 && strcmp(deployable, "0") != 0))
      {
        IPropertyTree* pSelComp = NULL;
        Owned<IPropertyTreeIterator> iterInst = pComponent->getElements("*", iptiter_sort);
        bool bAdded = false;

        ForEach(*iterInst)
        {
          IPropertyTree* pInst = &iterInst->query();
          const char* computer = pInst->queryProp("@computer");
          const char* netAddr = pInst->queryProp("@netAddress");

          if (!computer || !*computer || !strcmp("Notes", pInst->queryName()))
            continue;

          if (!strcmp(buildSet, "thor"))
          {
            sXPath.clear().appendf("Hardware/Computer[@name=\"%s\"]", computer);
            IPropertyTree* pComputer = pEnvRoot->queryPropTree(sXPath.str());
            netAddr = pComputer->queryProp("@netAddress");
            if (matchDeployAddress(ipAddr, netAddr) || 
                (!ipAddr && netAddr && *netAddr))
            {
              if (!bAdded)
              {
                pSelComp = pSelComps->addPropTree(pComponent->queryName(), createPTree());
                pSelComp->addProp("@name", name);
                pSelComp->addProp("@buildSet", buildSet);
                pSelComp->addProp("@logDir", logDir);
                bAdded = true;
              }

              if (listall)
              {
                IPropertyTree* pInstance = pSelComp->addPropTree(pInst->queryName(), createPTree());
                pInstance->addProp("@name", pInst->queryProp("@name"));
                pInstance->addProp("@computer", computer);
                pInstance->addProp("@netAddress", netAddr);
              }
            }
          }
          else if (matchDeployAddress(ipAddr, netAddr) || 
                   (!ipAddr && netAddr && *netAddr))
          {
            if (!bAdded)
            {
              pSelComp = pSelComps->addPropTree(pComponent->queryName(), createPTree());
              pSelComp->addProp("@name", name);
              pSelComp->addProp("@buildSet", buildSet);
              pSelComp->addProp("@logDir", logDir);
              bAdded = true;
            }

            StringBuffer sb(pInst->queryName());

            for (UINT i=0; i<sizeof(instanceNodeNames) / sizeof(instanceNodeNames[0]); i++)
              if (!strcmp(sb.str(), instanceNodeNames[i]))
              {
                //allow multiple instances but do not allow either roxie servers or slaves more than once per computer
                if (listall || sb.str()[0] != 'R' || !pSelComp->queryPropTree(StringBuffer().appendf("*[@computer=\"%s\"]", computer)))
                {
                  IPropertyTree* pInstance = pSelComp->addPropTree(sb.str(), createPTree());
                  pInstance->addProp("@name", pInst->queryProp("@name"));
                  pInstance->addProp("@computer", pInst->queryProp("@computer"));
                  pInstance->addProp("@port", pInst->queryProp("@port"));
                  pInstance->addProp("@netAddress", pInst->queryProp("@netAddress"));
                  const char* directory = pInst->queryProp(sb.str()[0]=='R' ? "@dataDirectory" : "@directory");
                  if (directory && *directory)
                    pInstance->addProp("@directory", directory);
                }

                break;
              }
          }
        }
      }
    }
  }

  return pSelComps.getLink();
}


//---------------------------------------------------------------------------
// Module Globals
//---------------------------------------------------------------------------
const char* findFileExtension(const char* pszPath)
{
    const char* lastSlash = pathTail(pszPath);
    return strrchr(lastSlash ? lastSlash : pszPath, '.');
}

void removeTrailingPathSepChar(char* pszPath)
{
    if (pszPath)
    {
        char* lastChar = pszPath + strlen(pszPath) - 1;
        if (isPathSepChar(*lastChar))
            *lastChar = '\0';
    }
}

void stripNetAddr(const char* dir, StringBuffer& destpath, StringBuffer& destip, bool makeLinux)
{
    destpath.clear().append(dir);
    if (dir[0] == '\\' && dir[1] == '\\' && strlen(dir) > 2)
    {
        destip.clear().append(strchr(dir + 2, '\\') - (dir + 2), dir + 2);
        destpath.clear().append(strchr(dir + 2, '\\'));
    }
    
    if (makeLinux)
        destpath.replace('\\', '/');
}

//returns temp path that ends with path sep
//
#ifdef _WIN32
extern DWORD getLastError() { return ::GetLastError(); }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
    ::GetTempPath(bufsize, tempPath);
    ::GetLongPathName(tempPath, tempPath, bufsize);
    if (subdir && *subdir)
    {
        const int len = strlen(tempPath);
        char* p = tempPath + len;
        strcpy(p, subdir);
        p += strlen(subdir);
        *p++ = '\\';
        *p = '\0';
    }
}
#else//Linux specifics follow
extern DWORD getLastError() { return errno; }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
    assert(bufsize > 5);
    strcpy(tempPath, "/tmp/");
    if (subdir && *subdir)
    {
        strcat(tempPath, subdir);
        strcat(tempPath, "/");
    }
}
#endif
