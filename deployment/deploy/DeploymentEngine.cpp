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
#pragma warning(disable : 4786)
#include <functional>
#include <algorithm>
#include "deploy.hpp"
#include "environment.hpp"
#include "DeploymentEngine.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jmutex.hpp"
#include "xslprocessor.hpp"
#include "securesocket.hpp"
#ifndef _WIN32
#include <unistd.h>
#endif


/*static*/ CInstallFileList CDeploymentEngine::s_dynamicFileList;
/*static*/ CDeploymentEngine* CDeploymentEngine::s_xsltDepEngine = NULL;//deployment engine context for XSLT
/*static*/ bool CDeploymentEngine::s_bCacheableDynFile = false;
//---------------------------------------------------------------------------
//  CDeploymentEngine
//---------------------------------------------------------------------------
CDeploymentEngine::CDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
                                     IDeploymentCallback& callback,
                                     IPropertyTree &process, 
                                     const char *instanceType, 
                                     bool createIni)
                                     : m_envDepEngine(envDepEngine),
                                     m_environment(envDepEngine.getEnvironment()), 
                                     m_process(process), 
                                     m_instanceType(instanceType), 
                                     m_abort(false),
                                     m_startable(unknown),
                                     m_stoppable(unknown),
                                     m_createIni(createIni),
                                     m_curInstance(NULL),
                                     m_instanceCheck(true)
{
    m_pCallback.set(&callback);
    m_installFiles.setDeploymentEngine(*this);
    m_name.set(m_process.queryProp("@name"));
    m_rootNode.setown(&m_environment.getPTree());
    m_useSSHIfDefined = true;
    assertex(m_rootNode);
    
    // Get instances
    if (m_instanceType.length()==0)
        m_instances.append(OLINK(m_process));
    else
    {
        Owned<IPropertyTreeIterator> iter = m_process.getElements(m_instanceType);
        if (!iter->first())
            throw MakeStringException(0, "Process %s has no instances defined", m_name.get());
        
        for (iter->first(); iter->isValid(); iter->next())
            m_instances.append(iter->get());
    }
    
    // Get name to use for INI file - use buildset name
    if (m_createIni)
        m_iniFile.set(StringBuffer(m_process.queryProp("@buildSet")).append(".ini").str());
}

//---------------------------------------------------------------------------
//  ~CDeploymentEngine
//---------------------------------------------------------------------------
CDeploymentEngine::~CDeploymentEngine() 
{
    // Do disconnects
    set<string>::const_iterator iEnd = m_connections.end();
    set<string>::const_iterator i;
    for (i=m_connections.begin(); i!=iEnd; i++)
   {
      const char* path = (*i).c_str();
      if (!m_envDepEngine.IsPersistentConnection(path))
           disconnectHost( path );
   }
    
    if (m_externalFunction)
        m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction.get(), false);
    
    if (m_externalFunction2)
        m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction2.get(), false);
}


//---------------------------------------------------------------------------
//  addInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::addInstance(const char* tagName, const char* name)
{
    if (m_instanceType.length() == 0)
        throw MakeStringException(-1, "%s: Specification of individual instances is not allowed!", m_name.get());
    
    StringBuffer xpath;
    xpath.appendf("%s[@name='%s']", tagName, name);
    
    Owned<IPropertyTree> pInstance = m_process.getPropTree(xpath.str());
    
    if (!pInstance)
        throw MakeStringException(-1, "%s: Instance '%s' cannot be found!", m_name.get(), name);
    
    m_instances.append(*pInstance.getLink());
}

//---------------------------------------------------------------------------
//  getInstallFileCount
//---------------------------------------------------------------------------

// whether a method is trackable for progress stats purpose
static bool isMethodTrackable(const char* method)
{
    return strieq(method,"copy") || startsWith(method,"xsl"); //|| strieq(method,"esp_service_module");
}

int CDeploymentEngine::getInstallFileCount()
{
    const CInstallFileList& files = getInstallFiles().getInstallFileList();

    // debug untrackable files
    if (0)
    {
        StringBuffer s;
        for (CInstallFileList::const_iterator it=files.begin(); it!=files.end(); ++it)
        {
            const Linked<CInstallFile>& f = *it;
            if (!isMethodTrackable(f->getMethod().c_str()) || startsWith(f->getMethod().c_str(),"xsl"))
                s.append(f->getMethod().c_str()).append(": ").append(f->getSrcPath().c_str())
                .append(" --> ").append(f->getDestPath().c_str()).newline();
        }
        Owned<IFile> f = createIFile("c:\\temp\\files.txt");
        Owned<IFileIO> fio = f->open(IFOwrite);
        fio->write(0,s.length(),s.str());
    }

    // This includes all files, such as, esp_service_module, esp_plugins and custom 
    //return getInstallFiles().getInstallFileList().size(); 

    // Only count these we can handle properly
    int count = 0, xslcount = 0, total = 0;
    for (CInstallFileList::const_iterator it=files.begin(); it!=files.end(); ++it)
    {
      const Linked<CInstallFile>& f = *it;
      const char* method = f->getMethod().c_str();
      if (strieq(method,"copy"))
        count++;
      else if (startsWith(method,"xsl"))
        xslcount++;
    }

    bool isCached = m_instances.length() > 1;
    total = isCached ? count : 0;
    const char* depToFolder = m_envDepEngine.getDeployToFolder();

    ForEachItemIn(idx, m_instances)
    {
      IPropertyTree& instance = m_instances.item(idx);
      StringAttr curSSHUser, curSSHKeyFile, curSSHKeyPassphrase;
      m_envDepEngine.getSSHAccountInfo(instance.queryProp("@computer"),
        curSSHUser, curSSHKeyFile, curSSHKeyPassphrase);
      total += xslcount;

      if (m_useSSHIfDefined && !curSSHKeyFile.isEmpty() &&
        !curSSHUser.isEmpty() && !(depToFolder && *depToFolder))
      {
          total += 1;

          if (!isCached)
          {
            isCached = true;
            total += count;
          }
      }
      else
        total += count;
    }

  return total;
}

//---------------------------------------------------------------------------
//  getInstallFileSize
//---------------------------------------------------------------------------

offset_t CDeploymentEngine::getInstallFileSize()
{
    const CInstallFileList& files = getInstallFiles().getInstallFileList();
    offset_t fileSize = 0, xslSize = 0, total = 0;

    for (CInstallFileList::const_iterator it=files.begin(); it!=files.end(); ++it)
    {
      const Linked<CInstallFile>& f = *it;
      const char* method = f->getMethod().c_str();
      if (strieq(method,"copy"))
        fileSize += f->getSrcSize();
      else if (startsWith(method,"xsl"))
        xslSize += f->getSrcSize();
        
        /* debug
        if (f->getSrcSize()==24331)
        {
            VStringBuffer s("%s : %s -> %s", f->getMethod().c_str(), f->getSrcPath().c_str(), f->getDestPath().c_str());
            ::MessageBox((HWND)m_pCallback->getWindowHandle(), s.str(), "UNCOPIED",MB_OK);
        }*/
    }

    bool isCached = m_instances.length() > 1;
    total = isCached ? fileSize : 0;
    const char* depToFolder = m_envDepEngine.getDeployToFolder();

    ForEachItemIn(idx, m_instances)
    {
      total += fileSize + xslSize;

      if (!isCached)
      {
        IPropertyTree& instance = m_instances.item(idx);
        StringAttr curSSHUser, curSSHKeyFile, curSSHKeyPassphrase;
        m_envDepEngine.getSSHAccountInfo(instance.queryProp("@computer"), curSSHUser,
          curSSHKeyFile, curSSHKeyPassphrase);

        if (!curSSHKeyFile.isEmpty() && !m_curSSHUser.isEmpty() &&
          !(depToFolder && *depToFolder))
        {
          isCached = true;
          total += fileSize;
        }
      }
    }

  return total;
}

//---------------------------------------------------------------------------
//  start
//---------------------------------------------------------------------------
void CDeploymentEngine::start()
{
    //some components may not have defined startup.bat or stop.bat scripts
    //in their installset since those actions may not be relevant (for e.g.
    //dfu) so ignore startup/stop commands in that case
    checkBuild();
    
    if (m_startable == unknown)
        m_startable = searchDeployMap("startup", ".bat") ? yes : no;
    
    if (m_startable == yes)
    {
        ForEachItemIn(idx, m_instances)
        {
            checkAbort();
            IPropertyTree& instance = m_instances.item(idx);
            m_curInstance = instance.queryProp("@name");
            setSSHVars(instance);

            try
            {
                char tempPath[_MAX_PATH];
                getTempPath(tempPath, sizeof(tempPath), m_name);

                ensurePath(tempPath);
                m_envDepEngine.addTempDirectory( tempPath );

                startInstance(instance);
            }
            catch (IException* e)
            {
                if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e))//retry ?
                    idx--;
            }
            catch (...)
            {
                if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL))//retry ?
                    idx--;
            }
        }//for
        
        m_curInstance = NULL;
        clearSSHVars();
    }
}

//---------------------------------------------------------------------------
//  startInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::startInstance(IPropertyTree& node, const char* fileName/*="startup"*/)
{
    EnvMachineOS os = m_envDepEngine.lookupMachineOS(node);

    StringAttr hostDir(getHostDir(node).str());
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
        "Starting %s process on %s", m_name.get(), hostDir.get());
    
    Owned<IDeployTask> task;
    if (m_useSSHIfDefined)
    {
        const char* computer = node.queryProp("@computer");
        if (!computer || !*computer)
            return;

        const char* dir = hostDir.sget();
        StringBuffer destpath, destip;
        stripNetAddr(dir, destpath, destip);
        
        StringBuffer cmd, output, err, destdir;
        destdir.append(destpath.length() - 1, destpath.str());
        cmd.clear().appendf("%s%s %s", destpath.str(), fileName, destdir.str());
        task.set(createDeployTask(*m_pCallback, "Start Instance", m_process.queryName(), m_name.get(), m_curInstance, fileName, dir, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os));
        m_pCallback->printStatus(task);
        bool flag = task->execSSHCmd(destip.str(), cmd, output, err);
    }
    else
    {
        StringBuffer startCmd;
        startCmd.append(hostDir).append(fileName);
        if (os == MachineOsW2K)
            startCmd.append(".bat");

        StringAttr user, pwd;
        m_envDepEngine.getAccountInfo(node.queryProp("@computer"), user, pwd);
    
        // Spawn start process
        connectToHost(node);
        task.set(createDeployTask(*m_pCallback, "Start Instance", m_process.queryName(), 
                                             m_name.get(), m_curInstance, NULL, startCmd.str(), 
                                             m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os));
        m_pCallback->printStatus(task);
        task->createProcess(true, user, pwd);
    }

    m_pCallback->printStatus(task);
    checkAbort(task);
    
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
}

//---------------------------------------------------------------------------
//  stop
//---------------------------------------------------------------------------
void CDeploymentEngine::stop()
{
    //some components may not have defined startup.bat or stop.bat scripts
    //in their installset since those actions may not be relevant (for e.g.
    //dfu) so ignore startup/stop commands in that case
    checkBuild();
    
    if (m_stoppable == unknown)
        m_stoppable = searchDeployMap("stop", ".bat") ? yes : no;

    if (m_stoppable == yes)
    {
        ForEachItemIn(idx, m_instances)
        {
            checkAbort();
            
            IPropertyTree& instance = m_instances.item(idx);
            m_curInstance = instance.queryProp("@name");
            setSSHVars(instance);

            
            try
            {
                char tempPath[_MAX_PATH];
                getTempPath(tempPath, sizeof(tempPath), m_name);

                ensurePath(tempPath);
                m_envDepEngine.addTempDirectory( tempPath );

                stopInstance(instance);
            }
            catch (IException* e)
            {
                if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e))//retry ?
                    idx--;
            }
            catch (...)
            {
                if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL))//retry ?
                    idx--;
            }
        }
        m_curInstance = NULL;
        clearSSHVars();
    }
}


//---------------------------------------------------------------------------
//  stopInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::stopInstance(IPropertyTree& node, const char* fileName/*="stop"*/)
{
  EnvMachineOS os = m_envDepEngine.lookupMachineOS(node);
    StringAttr hostDir(getHostDir(node).str());
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, 
        "Stopping %s process on %s", m_name.get(), hostDir.get());
    
    Owned<IDeployTask> task;
    if (m_useSSHIfDefined)
    {
        const char* computer = node.queryProp("@computer");
        if (!computer || !*computer)
            return;

        const char* dir = hostDir.sget();
        StringBuffer destpath, destip;
        stripNetAddr(dir, destpath, destip);
        StringBuffer cmd, output, err, destdir;
        destdir.append(destpath.length() - 1, destpath.str());
        cmd.clear().appendf("%s%s %s", destpath.str(), fileName, destdir.str());
        task.set(createDeployTask(*m_pCallback, "Stop Instance", m_process.queryName(), m_name.get(), m_curInstance, fileName, dir, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os));
        m_pCallback->printStatus(task);
        bool flag = task->execSSHCmd(destip.str(), cmd, output, err);
    }
    else 
    {
        StringBuffer stopCmd;
        StringAttr user, pwd;
        stopCmd.append(hostDir).append(fileName);
        m_envDepEngine.getAccountInfo(node.queryProp("@computer"), user, pwd);
        
        EnvMachineOS os = m_envDepEngine.lookupMachineOS(node);
        if (os == MachineOsW2K)
            stopCmd.append(".bat");
        
        // Spawn stop process
        connectToHost(node);
        task.set(createDeployTask(*m_pCallback, "Stop Instance", m_process.queryName(), m_name.get(), 
                         m_curInstance, NULL, stopCmd.str(), m_curSSHUser.sget(), 
                         m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os));
        m_pCallback->printStatus(task);
        task->createProcess(true, user, pwd);
    }

    m_pCallback->printStatus(task);
    checkAbort(task);
    
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
}

//---------------------------------------------------------------------------
//  check
//---------------------------------------------------------------------------
void CDeploymentEngine::check()
{
    checkBuild();
    
    if (m_instances.empty())
        throw MakeStringException(0, "Process %s has no instances defined.  Nothing to do!", m_name.get());
    
    if (m_instanceCheck)
    {
      ForEachItemIn(idx, m_instances)
      {
          checkAbort();

          IPropertyTree& instance = m_instances.item(idx);
          m_curInstance = instance.queryProp("@name");

          checkInstance(instance);
      }
    }
    m_curInstance = NULL;
    clearSSHVars();
}

//---------------------------------------------------------------------------
//  queryDirectory
//---------------------------------------------------------------------------
const char *CDeploymentEngine::queryDirectory(IPropertyTree& node, StringBuffer& sDir) const
{
    const char *pszDir = node.queryProp("@directory");
    if (!pszDir)    
        pszDir = m_process.queryProp("@directory");
    
    sDir.clear();
    if (pszDir)
        sDir.append(pszDir).replace('/', '\\').replace(':', '$'); //make UNC path
    
    return pszDir ? sDir.str() : NULL;
}

//---------------------------------------------------------------------------
//  checkInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::checkInstance(IPropertyTree& node) const
{
    // Check for valid net address
    StringAttr sAttr;
    if (m_envDepEngine.lookupNetAddress(sAttr, node.queryProp("@computer")).length()==0)
        throw MakeStringException(0, "Process %s has invalid computer net address", m_name.get());
    
    // Check for valid directory
    StringBuffer directory;
    queryDirectory(node, directory);
    if (directory.length()==0)
        throw MakeStringException(0, "Process %s has invalid directory", m_name.get());
}

//---------------------------------------------------------------------------
//  checkBuild
//---------------------------------------------------------------------------
void CDeploymentEngine::checkBuild() const
{
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
        "Checking builds for process %s", m_name.get());
    
    // Make sure build and buildset are defined
    StringAttr build(m_process.queryProp("@build"));
    StringAttr buildset(m_process.queryProp("@buildSet"));
    if (build.length()==0 || buildset.length()==0)
        throw MakeStringException(0, "Process %s has no build or buildSet defined", m_name.get());
    
    // Make sure build is valid
    StringBuffer path;
    path.appendf("./Programs/Build[@name=\"%s\"]", build.get());
    IPropertyTree* buildNode = m_rootNode->queryPropTree(path.str());
    if (!buildNode)
        throw MakeStringException(0, "Process %s has invalid build", m_name.get());
    
    // Make sure buildset is valid
    path.clear().appendf("./BuildSet[@name=\"%s\"]", buildset.get());
    IPropertyTree* buildsetNode = buildNode->queryPropTree(path.str());
    if (!buildsetNode)
        throw MakeStringException(0, "Process %s has invalid buildSet", m_name.get());
    
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
}

//---------------------------------------------------------------------------
//  compare
//---------------------------------------------------------------------------
void CDeploymentEngine::compare(unsigned flags)
{
    m_compare = true;
    m_deployFlags = flags;
    _deploy(false);
}

//---------------------------------------------------------------------------
//  deploy
//---------------------------------------------------------------------------
void CDeploymentEngine::deploy(unsigned flags, bool useTempDir)
{
    m_compare = false;
    m_deployFlags = flags;
    _deploy(useTempDir);
}

//---------------------------------------------------------------------------
//  beforeDeploy
//---------------------------------------------------------------------------
void CDeploymentEngine::beforeDeploy()
{
    m_installFiles.clear();
    determineInstallFiles(m_process, m_installFiles);
    getCallback().installFileListChanged();

    char tempPath[_MAX_PATH];
    getTempPath(tempPath, sizeof(tempPath), m_name);

    ensurePath(tempPath);
    bool cacheFiles = false;
    const char* depToFolder = m_envDepEngine.getDeployToFolder();

    if (!m_compare && m_instances.ordinality() == 1 && !(depToFolder && *depToFolder))
    {
      IPropertyTree& instanceNode = m_instances.item(0);
      const char* curInstance = instanceNode.queryProp("@name");
      StringAttr sbSSHUser, sbSSHKeyFile, sbKeyPassphrase;
      m_envDepEngine.getSSHAccountInfo(instanceNode.queryProp("@computer"),
        sbSSHUser, sbSSHKeyFile, sbKeyPassphrase);
      cacheFiles = !sbSSHKeyFile.isEmpty();
    }

    if (m_instances.ordinality() > 1 || cacheFiles)
    {
        strcat(tempPath, "Cache");
        char* pszEnd = tempPath + strlen(tempPath);
        
        Owned<IFile> pFile = createIFile(tempPath);
        int i = 1;
        
        while (pFile->exists()) { //dir/file exists
            itoa(++i, pszEnd, 10);
            pFile.setown( createIFile(tempPath) );
        }
        
        m_envDepEngine.addTempDirectory( tempPath );
        
        strcat(tempPath, PATHSEPSTR);
        m_cachePath.set( tempPath );

        EnvMachineOS os = m_envDepEngine.lookupMachineOS( m_instances.item(0) );
        m_curInstance = "Cache";
        clearSSHVars();
        copyInstallFiles("Cache", -1, tempPath, os);
    }
    else
        m_cachePath.set( tempPath );
}

//---------------------------------------------------------------------------
//  _deploy
//---------------------------------------------------------------------------
void CDeploymentEngine::_deploy(bool useTempDir)
{
    m_renameDirList.kill();
    beforeDeploy();
    m_curSSHUser.clear();
    m_curSSHKeyFile.clear();
    m_curSSHKeyPassphrase.clear();
    ForEachItemIn(idx, m_instances)
    {
        checkAbort();
        
        IPropertyTree& instanceNode = m_instances.item(idx);
        m_curInstance = instanceNode.queryProp("@name");
        setSSHVars(instanceNode);
        
        try
        {
            deployInstance(instanceNode, useTempDir);
        }
        catch (IException* e)
        {
            if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e))//retry ?
                idx--;
        }
        catch (...)
        {
            if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL))//retry ?
                idx--;
        }
    }
    m_curInstance = NULL;
    clearSSHVars();
    afterDeploy();
}

//---------------------------------------------------------------------------
//  deployInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::deployInstance(IPropertyTree& instanceNode, bool useTempDir)
{
    StringAttr hostDir(getHostDir(instanceNode).str());
    StringAttr destDir(useTempDir ? getDeployDir(instanceNode).str() : hostDir.get());
    ensurePath(destDir);
    
    const char* pszHostDir = hostDir.get();
    if (pszHostDir && *pszHostDir==PATHSEPCHAR && *(pszHostDir+1)==PATHSEPCHAR && m_envDepEngine.lookupMachineOS(instanceNode) != MachineOsLinux)
        connectToHost(instanceNode);

    beforeDeployInstance(instanceNode, destDir);
    copyInstallFiles(instanceNode, destDir);
    afterDeployInstance(instanceNode, destDir);
    
    if (!m_compare && useTempDir)
    {
        checkAbort();
        EnvMachineOS os= m_envDepEngine.lookupMachineOS(instanceNode);
        renameDir(hostDir, NULL, os);
        renameDir(destDir, hostDir, os);
    }
}

//---------------------------------------------------------------------------
//  renameDirs
//---------------------------------------------------------------------------
void CDeploymentEngine::renameDirs()
{
    StringBuffer xpath, err;
    bool flag = false;

    while (m_renameDirList.length())
    {
        IDeployTask* task = &m_renameDirList.item(0);
        m_pCallback->printStatus(task);
        
        if (task->getMachineOS() == MachineOsLinux && strlen(task->getSSHKeyFile()) && strlen(task->getSSHUser()))
        {
            StringBuffer fromPath, toPath, err, destip;
            const char* source = task->getFileSpec(DT_SOURCE);
            stripNetAddr(source, fromPath, destip);
            stripNetAddr(task->getFileSpec(DT_TARGET), toPath, destip);
            StringBuffer cmd, output;
            cmd.clear().appendf("mv %s %s", fromPath.str(), toPath.str());
            flag = task->execSSHCmd(destip.str(), cmd, output, err);
        }
        else        
            task->renameFile();

        m_pCallback->printStatus(task);
        checkAbort(task);
        m_renameDirList.remove(0);
    }
}

//---------------------------------------------------------------------------
//  deleteFile
//---------------------------------------------------------------------------
void CDeploymentEngine::deleteFile(const char* target, const char* instanceName, EnvMachineOS os)
{
    checkAbort();
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Delete File", m_process.queryName(), 
                                             m_name.get(), instanceName, NULL, target, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), 
                                             m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);
    task->deleteFile();
    
    //only display status info for successful attempts since some temp files may be attempted to be
    //deleted more than one time, for instance, due to multiple esp bindings
    if (task->getErrorCode() == 0)
        m_pCallback->printStatus(task);
}

//---------------------------------------------------------------------------
//  backupDirs
//---------------------------------------------------------------------------
void CDeploymentEngine::backupDirs()
{
    ForEachItemIn(idx, m_instances)
    {
        checkAbort();
        
        IPropertyTree& instance = m_instances.item(idx);
        m_curInstance = instance.queryProp("@name");
        setSSHVars(instance);
        
        try
        {
            EnvMachineOS os = m_envDepEngine.lookupMachineOS(instance);

            if (os == MachineOsLinux && !m_curSSHUser.isEmpty() && !m_curSSHKeyFile.isEmpty())
            {
                StringAttr hostDir(getHostDir(instance).str());
                
                m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                    "Backing up directory %s", hostDir.get());

                StringBuffer bkPath;
                getBackupDirName(hostDir.sget(), bkPath);
                if (bkPath.length() == 0)
                    return;

                StringBuffer fromPath, toPath, err, fromip, toip;
                stripNetAddr(hostDir.sget(), fromPath, fromip);
                stripNetAddr(bkPath.str(), toPath, toip);

                StringBuffer cmd, output;
                StringBuffer tmp;
                tmp.appendf("%d", msTick());
                cmd.clear().appendf("cp -r %s %s", fromPath.str(), toPath.str());
                Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Backup Directory", m_process.queryName(), m_name.get(), 
                    m_curInstance, fromPath.str(), toPath.str(), m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), 
                    m_useSSHIfDefined, os);
                m_pCallback->printStatus(task);
                bool flag = task->execSSHCmd(fromip.str(), cmd, output, err);
                m_pCallback->printStatus(task);
                checkAbort(task);
            }
            else
            {
                connectToHost(instance);
                StringAttr hostDir(getHostDir(instance).str());
                
                m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                    "Backing up directory %s", hostDir.get());

                backupDir(hostDir);
            }
        }
        catch (IException* e)
        {
            if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e))//retry ?
                idx--;
        }
        catch (...)
        {
            if (!m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL))//retry ?
                idx--;
        }
    }
    m_curInstance = NULL;
    clearSSHVars();
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
}

//---------------------------------------------------------------------------
//  abort
//---------------------------------------------------------------------------
void CDeploymentEngine::abort()
{
    m_pCallback->printStatus(STATUS_NORMAL, m_process.queryName(), m_name.get(), m_curInstance, "Aborted!");
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Aborted!");
    m_abort = true;
}

//---------------------------------------------------------------------------
//  checkAbort
//---------------------------------------------------------------------------
void CDeploymentEngine::checkAbort(IDeployTask* task) const
{
    if (m_abort || m_pCallback->getAbortStatus() || (task && task->getAbort()))
        throw MakeStringException(0, "User abort");
}

//---------------------------------------------------------------------------
//  xslTransform
//---------------------------------------------------------------------------
void CDeploymentEngine::xslTransform(const char *xslFilePath, const char *outputFilePath, 
                                     const char* instanceName,
                                     EnvMachineOS os/*=MachineOsUnknown*/,
                                     const char* processName/*=NULL*/,
                                     bool isEspModuleOrPlugin)
{
    m_createIni = false;
    // Skip if not processing config files
    if (!(m_deployFlags & DEFLAGS_CONFIGFILES)) return;
    
    checkAbort();
    bool useSSH = true;
    
    if (m_compare) 
    {
        useSSH = false;
        outputFilePath = setCompare(outputFilePath);
    }
    else
            ensurePath(outputFilePath);

    m_transform->setParameter("processType", StringBuffer("'").append(m_process.queryName()).append("'").str());

    s_xsltDepEngine = this; //this is used in external function to get back to deployment engine instance

    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "XSL Transform", m_process.queryName(), 
        m_name.get(), instanceName, xslFilePath, outputFilePath, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), 
        m_curSSHKeyPassphrase.sget(), useSSH ? m_useSSHIfDefined : useSSH, os, processName);
    m_pCallback->printStatus(task);

    if (os == MachineOsLinux)
    {
        char tempPath[_MAX_PATH];
        getTempPath(tempPath, sizeof(tempPath), m_name.get());
        ensurePath(tempPath);
        m_envDepEngine.addTempDirectory( tempPath );
    }

    task->transformFile(*m_processor, *m_transform, m_cachePath.get());
    m_pCallback->printStatus(task);
    checkAbort(task);
    if (!isEspModuleOrPlugin)
    {
        try {
            Owned<IFile> file = createIFile(xslFilePath);
            m_pCallback->fileSizeCopied(file->size(),true);
        } catch (...) {
            m_pCallback->fileSizeCopied(0,true);
        }
    }

    if (m_compare) 
        compareFiles(os);
}

//---------------------------------------------------------------------------
//  setCompare
//---------------------------------------------------------------------------
const char* CDeploymentEngine::setCompare(const char *filename)
{
    assertex(m_compareOld.length()==0 && m_compareNew.length()==0);
    m_compareOld.set(filename);

   char tempfile[_MAX_PATH];
   getTempPath(tempfile, sizeof(tempfile), m_name);
   ensurePath(tempfile);

   strcat(tempfile, "compare");

   // Make sure file name is unique - at least during this session
   sprintf(&tempfile[strlen(tempfile)], "%d", m_envDepEngine.incrementTempFileCount());
   
   // Add same extension as filename for use with shell functions
   const char* ext = findFileExtension(filename);
   if (ext)
      strcat(tempfile, ext);

   m_envDepEngine.addTempFile(tempfile);
   m_compareNew.set(tempfile);
   return m_compareNew;
}

//---------------------------------------------------------------------------
//  compareFiles
//---------------------------------------------------------------------------
void CDeploymentEngine::compareFiles(EnvMachineOS os)
{
    compareFiles(m_compareNew, m_compareOld, os);
    //remove(m_compareNew); // let's keep the files around for later compares
    m_compareOld.clear();
    m_compareNew.clear();
}

//---------------------------------------------------------------------------
//  compareFiles
//---------------------------------------------------------------------------
void CDeploymentEngine::compareFiles(const char *newFile, const char *oldFile, EnvMachineOS os)
{
   Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Compare File", 
                                              m_process.queryName(), m_name.get(), 
                                              m_curInstance, newFile, oldFile, m_curSSHUser.sget(), 
                                              m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);
   m_pCallback->printStatus(task);
   task->compareFile(DTC_CRC | DTC_SIZE);
   task->setFileSpec(DT_SOURCE, newFile);
   m_pCallback->printStatus(task);
   checkAbort(task);
}

//---------------------------------------------------------------------------
//  writeFile
//---------------------------------------------------------------------------
void CDeploymentEngine::writeFile(const char* filename, const char* str, EnvMachineOS os)
{
    // Skip if not processing config files
    if (!(m_deployFlags & DEFLAGS_CONFIGFILES)) return;
    
    checkAbort();
    bool useSSH = true;
    if (m_compare) 
    {
        useSSH = false;
        filename = setCompare(filename);
    }
    else
        ensurePath(filename);
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Create File", m_process.queryName(), m_name.get(), 
        m_curInstance, NULL, filename, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), 
        useSSH?m_useSSHIfDefined:useSSH, os);
    m_pCallback->printStatus(task);
    if (useSSH?m_useSSHIfDefined:useSSH)
    {
        StringBuffer cmd, output, err, destpath, ip;
        stripNetAddr(filename, destpath, ip);
        cmd.clear().appendf("echo '%s' > %s; chmod 644 %s", str, destpath.str(), destpath.str());
        bool flag = task->execSSHCmd(ip.str(), cmd, output, err);
        if (!flag)
        {
            String errmsg(err.str());
            int index = errmsg.indexOf('\n');
            String* perr = errmsg.substring(0, index > 0? index : errmsg.length());
            output.clear().appendf("%s", perr->toCharArray());
            delete perr;
            throw MakeStringException(-1, "%s", output.str());
        }
    }
    else
        task->createFile(str);
    m_pCallback->printStatus(task);
    checkAbort(task);
    
    if (m_compare) compareFiles(os);
}

//---------------------------------------------------------------------------
//  lookupProcess
//---------------------------------------------------------------------------
IPropertyTree *CDeploymentEngine::lookupProcess(const char* type, const char* name) const
{
    if (name && *name && type && *type)
    {
        StringBuffer path;
        path.appendf("Software/%s[@name=\"%s\"]", type, name);
        Owned<IPropertyTreeIterator> iter = m_rootNode->getElements(path.str());
        if (iter->first() && iter->isValid())
        {
            return &iter->query();
        }
    }
    return NULL;
}

//---------------------------------------------------------------------------
//  lookupTable
//---------------------------------------------------------------------------
IPropertyTree* CDeploymentEngine::lookupTable(IPropertyTree* modelTree, const char *table) const
{
    StringBuffer xpath;
    xpath.appendf("./*[@name='%s']", table);
    
    IPropertyTree *ret = modelTree->queryPropTree(xpath.str());
    if (ret)
        return ret;
    else
        throw MakeStringException(0, "Table %s could not be found", table);
}

//---------------------------------------------------------------------------
//  getEndPoints
//---------------------------------------------------------------------------
StringBuffer& CDeploymentEngine::getEndPoints(const char* path, const char* delimiter, StringBuffer& endPoints) const
{
    Owned<IPropertyTreeIterator> iter = m_rootNode->getElements(path);
    for (iter->first(); iter->isValid(); iter->next())
    {
        Owned<IConstMachineInfo> machine = m_environment.getMachine(iter->query().queryProp("@computer"));
        if (machine)
        {
            if (endPoints.length())
                endPoints.append(delimiter);
            
            
            SCMStringBuffer scmSBuf;
            endPoints.append(machine->getNetAddress(scmSBuf).str());
            const char* port = iter->query().queryProp("@port");
            if (port)
                endPoints.append(":").append(port);
        }
    }
    return endPoints;
}

//---------------------------------------------------------------------------
//  getDaliServers
//---------------------------------------------------------------------------
StringBuffer& CDeploymentEngine::getDaliServers(StringBuffer& daliServers) const
{
    daliServers.clear();
    const char* name = m_process.queryProp("@daliServer");
    if (!name)
        name = m_process.queryProp("@daliServers");
    if (name)
    {
        StringBuffer path;
        path.appendf("./Software/DaliServerProcess[@name=\"%s\"]/Instance", name);
        getEndPoints(path.str(), ", ", daliServers);
    }
    return daliServers;
}

//---------------------------------------------------------------------------
//  getHostRoot
//---------------------------------------------------------------------------
StringBuffer CDeploymentEngine::getHostRoot(const char* computer, const char* dir, bool bIgnoreDepToFolder/*=false*/) const
{
    StringBuffer hostRoot;
    StringAttr netAddress;
    if (m_envDepEngine.lookupNetAddress(netAddress, computer).length() > 0)
    {
        const char* depToFolder = m_envDepEngine.getDeployToFolder();
        if (depToFolder && !bIgnoreDepToFolder)
            hostRoot.append(depToFolder);
        else
            hostRoot.append(PATHSEPCHAR).append(PATHSEPCHAR);

        hostRoot.append(netAddress).append(PATHSEPCHAR);

        //for Linux support, allow directories starting with '\' character
        if (dir)
        {
            if (isPathSepChar(*dir))
                dir++;
            while (*dir && !isPathSepChar(*dir))
                hostRoot.append(*dir++);
        }
    }
    return hostRoot;
}

//---------------------------------------------------------------------------
//  getHostDir
//---------------------------------------------------------------------------
StringBuffer CDeploymentEngine::getHostDir(IPropertyTree& node, bool bIgnoreDepToFolder/*=false*/)
{
    StringBuffer hostDir;
    StringAttr netAddress;
    if (m_envDepEngine.lookupNetAddress(netAddress, node.queryProp("@computer")).length() > 0)
    {
        const char* depToFolder = m_envDepEngine.getDeployToFolder();
        if (depToFolder && !bIgnoreDepToFolder)
        {
            hostDir.append(depToFolder);
            m_useSSHIfDefined = false;
        }
        else
            hostDir.append(PATHSEPCHAR).append(PATHSEPCHAR);

        hostDir.append(netAddress).append(PATHSEPCHAR);
        StringBuffer directory;
        const char* dir = queryDirectory(node, directory);
        //for Linux support, allow directories starting with '\' character
        if (dir)
        {
            if (isPathSepChar(*dir))
                dir++;
            hostDir.append(dir).append(PATHSEPCHAR);
        }
     }
    
    return hostDir;
}

//---------------------------------------------------------------------------
//  getDeployDir
//---------------------------------------------------------------------------
StringBuffer CDeploymentEngine::getDeployDir(IPropertyTree& node)
{
    char deployDir[_MAX_PATH];
    strcpy(deployDir, getHostDir(node).str());
    removeTrailingPathSepChar(deployDir);
    strcat(deployDir, "_deploy" PATHSEPSTR);
    return deployDir;
}

//---------------------------------------------------------------------------
// getLocalDir - returns @directory with '$' replaced by ':'
//---------------------------------------------------------------------------
StringBuffer CDeploymentEngine::getLocalDir(IPropertyTree& node) const
{
    StringBuffer localDir;
    queryDirectory(node, localDir);
    
    if (m_envDepEngine.lookupMachineOS(node) == MachineOsLinux)
    {
        localDir.replace(':', '$');
        localDir.replace('\\', '/');
    }
    else
    {
        localDir.replace('$', ':');
        localDir.replace('/', '\\');
    }
    
    return localDir;
}

//---------------------------------------------------------------------------
//  connectToHost
//---------------------------------------------------------------------------
void CDeploymentEngine::connectToHost(IPropertyTree& node, const char* dir/*=NULL*/)
{
    const char* computer = node.queryProp("@computer");
    StringBuffer directory;
    if (!dir)
        dir = queryDirectory(node, directory);

    StringAttr   user;
    StringAttr   pswd;
    m_envDepEngine.getAccountInfo(computer, user, pswd);
    connectToNetworkPath( getHostRoot(computer, dir).str(), user, pswd );
}

//---------------------------------------------------------------------------
//  static splitUNCPath
//---------------------------------------------------------------------------
bool CDeploymentEngine::stripTrailingDirsFromUNCPath(const char* uncPath, StringBuffer& netPath)
{
    const char* p = uncPath;
    if (p && *p && isPathSepChar(*p++) && *p && isPathSepChar(*p++))//is it really a network path starting with \\ or //
    {
        netPath.append(PATHSEPSTR PATHSEPSTR);

        //remove trailing directories like dir2, dir3 etc. from paths like \\ip\dir1\dir2\dir3
        while (*p && !isPathSepChar(*p))
            netPath.append(*p++);

        if (*p && isPathSepChar(*p++))
        {
            netPath.append( PATHSEPCHAR );
            if (*p && !isPathSepChar(*p))
            {
                while (*p && !isPathSepChar(*p))
                    netPath.append(*p++);
                netPath.append( PATHSEPCHAR );
            }
            return true;
        }
    }
    return false;
}

//---------------------------------------------------------------------------
//  connectToHost
//---------------------------------------------------------------------------
void CDeploymentEngine::connectToNetworkPath(const char* uncPath, const char* user, const char* pswd)
{
    StringBuffer path;
    // make a valid UNC path to connect to and see if we are not already connected
    if (!stripTrailingDirsFromUNCPath(uncPath, path) || m_connections.find( path.str() ) != m_connections.end())
        return;

    if (m_envDepEngine.getDeployToFolder())
        m_envDepEngine.getDeployToAccountInfo(user, pswd);

    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Connect", m_process.queryName(), m_name.get(), 
        m_curInstance, NULL, path.str(), "", "", "", false);
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Connecting to %s...", path.str());
    m_pCallback->printStatus(task);
    task->connectTarget(user, pswd, m_envDepEngine.getInteractiveMode());
    m_pCallback->printStatus(task);
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);

    // No sense in continuing if connection falied
    if (task->getErrorCode() != 0)
        throw MakeStringException(0, "%s", ""); //message already displayed!

    // Save connections for disconnecting during destructor
    m_connections.insert( path.str() );
}

//---------------------------------------------------------------------------
//  disconnectHost
//---------------------------------------------------------------------------
void CDeploymentEngine::disconnectHost(const char* uncPath)
{
    // Create log of directory
    IDeployLog* pDeployLog = m_envDepEngine.getDeployLog();
    if (pDeployLog)
        pDeployLog->addDirList(m_name, uncPath);
    
    // Disconnect
    bool disc = m_pCallback->onDisconnect(uncPath);
    if (disc)
    {
        Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Disconnect", m_process.queryName(), m_name.get(), 
            m_curInstance, NULL, uncPath, "", "", "", false);
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Disconnecting from %s...", uncPath);
        m_pCallback->printStatus(task);
        task->disconnectTarget();
        m_pCallback->printStatus(task);
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
    }
}

struct string_compare : public std::binary_function<const char*, const char*, bool> 
{
    bool operator()(const char* x, const char* y) const { return stricmp(x, y)==0;  }
};

//---------------------------------------------------------------------------
//  copyAttributes
//---------------------------------------------------------------------------
void CDeploymentEngine::copyAttributes(IPropertyTree *dst, IPropertyTree *src, const char** begin, const char** end)
{
    Owned<IAttributeIterator> attrs = src->getAttributes();
    for(attrs->first(); attrs->isValid(); attrs->next())
    {
        if(std::find_if(begin, end, std::bind1st(string_compare(), attrs->queryName()+1)) != end)
            dst->addProp(attrs->queryName(), attrs->queryValue());
    }
}

//---------------------------------------------------------------------------
//  copyUnknownAttributes
//---------------------------------------------------------------------------
void CDeploymentEngine::copyUnknownAttributes(IPropertyTree *dst, IPropertyTree *src, const char** begin, const char** end)
{
    Owned<IAttributeIterator> attrs = src->getAttributes();
    for(attrs->first(); attrs->isValid(); attrs->next())
    {
        if(std::find_if(begin, end, std::bind1st(string_compare(), attrs->queryName()+1)) == end)
            dst->addProp(attrs->queryName(), attrs->queryValue());
    }
}

//---------------------------------------------------------------------------
//  ensurePath
//---------------------------------------------------------------------------
void CDeploymentEngine::ensurePath(const char* filespec) const
{
    // Check if directory already exists
    StringBuffer dir;
    splitDirTail(filespec, dir);
    bool flag = true;
    EnvMachineOS os = MachineOsW2K;

    if (m_curInstance && m_curSSHUser.length() && m_curSSHKeyFile.length())
    {
        StringBuffer xpath, err;
        xpath.appendf("./Instance[@name='%s']", m_curInstance);
        IPropertyTree* pInstanceNode = m_process.queryPropTree(xpath.str());
        if (!pInstanceNode)
        {
            StringBuffer destpath, ip;
            stripNetAddr(dir, destpath, ip);
            if (!ip.length())
                flag = true;
            else
            {
                IConstMachineInfo* pInfo = m_envDepEngine.getEnvironment().getMachineByAddress(ip.str());
                os = pInfo->getOS();
                flag = !checkSSHFileExists(dir);
            }
        }
        else
        {
            os = m_envDepEngine.lookupMachineOS(*pInstanceNode);
            flag = !checkSSHFileExists(dir);
        }
    }
    
    if (flag)
    {
        Owned<IFile> pIFile = createIFile(dir.str());
        if ((m_curInstance && m_curSSHUser.length() && m_curSSHKeyFile.length()) || !pIFile->exists() || !pIFile->isDirectory())
        {
            Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Create Directory", m_process.queryName(), m_name.get(), 
                m_curInstance, NULL, dir.str(), m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(),
                m_useSSHIfDefined, os);
            m_pCallback->printStatus(task);
            task->createDirectory();
            m_pCallback->printStatus(task);
            checkAbort(task);
        }
    }
}

//---------------------------------------------------------------------------
//  renameDir
//---------------------------------------------------------------------------
void CDeploymentEngine::renameDir(const char* from, const char* to, EnvMachineOS os)
{
    assertex(!m_compare);
    assertex(from && *from);
  // If old path doesn't exist, then nothing to rename
  char oldPath[_MAX_PATH];
  strcpy(oldPath, from);
  removeTrailingPathSepChar(oldPath);
    
  if (!checkFileExists(oldPath)) 
      return;
    
  char newPath[_MAX_PATH];
  if (to && *to)
  {
      // Use destination provided
      assertex(strcmp(from, to) != 0);
      strcpy(newPath, to);
      removeTrailingPathSepChar(newPath);
  }
  else
  {
      // Create new path name with date suffix
      time_t t = time(NULL);
      struct tm* now = localtime(&t);
      sprintf(newPath, "%s_%02d_%02d", oldPath, now->tm_mon + 1, now->tm_mday);
      while (checkFileExists(newPath))
      {
          size32_t end = strlen(newPath) - 1;
          char ch = newPath[end];
          if (ch >= 'a' && ch < 'z')
              newPath[end] = ch + 1;
          else
              strcat(newPath, "a");
      }
  }
    
    // Save rename task
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Rename", m_process.queryName(), m_name.get(), 
        m_curInstance, oldPath, newPath, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);

  m_renameDirList.append(*task.getLink());
}

//---------------------------------------------------------------------------
//  backupDir
//---------------------------------------------------------------------------
void CDeploymentEngine::backupDir(const char* from)
{
    assertex(from && *from);
    // If from path doesn't exist, then nothing to backup
    char fromPath[_MAX_PATH];
    strcpy(fromPath, from);
    removeTrailingPathSepChar(fromPath);
    if (!checkFileExists(fromPath)) return;
 
    StringBuffer toPath;
    getBackupDirName(from, toPath); 

    // Copy directory
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Backup Directory", m_process.queryName(), m_name.get(), 
        m_curInstance, fromPath, toPath.str(),  m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined);
    m_pCallback->printStatus(task);
    task->copyDirectory();
    m_pCallback->printStatus(task);
    checkAbort(task);
}

void CDeploymentEngine::getBackupDirName(const char* from, StringBuffer& to)
{
  // If from path doesn't exist, then nothing to backup
    char fromPath[_MAX_PATH];
    strcpy(fromPath, from);
    removeTrailingPathSepChar(fromPath);
    if (!checkFileExists(fromPath)) return;
    
    // Create to path name with date suffix
    char toPath[_MAX_PATH];
    time_t t = time(NULL);
    struct tm* now = localtime(&t);
    sprintf(toPath, "%s_%02d_%02d", fromPath, now->tm_mon + 1, now->tm_mday);
    while (checkFileExists(toPath))
    {
        size32_t end = strlen(toPath) - 1;
        char ch = toPath[end];
        if (ch >= 'a' && ch < 'z')
            toPath[end] = ch + 1;
        else
            strcat(toPath, "a");
    }

  to.clear().append(toPath);
}

//---------------------------------------------------------------------------
//  copyInstallFiles
//---------------------------------------------------------------------------
void CDeploymentEngine::copyInstallFiles(IPropertyTree& instanceNode, const char* destPath)
{
    EnvMachineOS os = m_envDepEngine.lookupMachineOS(instanceNode);
    int instanceIndex = m_instances.find(instanceNode);
    const char* instanceName = instanceNode.queryProp("@name");
    copyInstallFiles(instanceName, instanceIndex, destPath, os);
}

void CDeploymentEngine::copyInstallFiles(const char* instanceName, int instanceIndex, const char* destPath, EnvMachineOS os)
{
    bool bCacheFiles = instanceIndex == -1 && !strcmp(instanceName, "Cache");
    s_dynamicFileList.clear();
    
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
        m_compare ? "Comparing install files for %s with %s..." : "Copying install files for %s to %s...", 
        m_name.get(), destPath);
    
    if (m_threadPool == NULL)
    {
        IThreadFactory* pThreadFactory = createDeployTaskThreadFactory();
        m_threadPool.setown(createThreadPool("Deploy Task Thread Pool", pThreadFactory, this, DEPLOY_THREAD_POOL_SIZE));
        pThreadFactory->Release();
    }
    else
    {
        int nThreads = m_threadPool->runningCount();
        if (nThreads > 0)
            throw MakeOsException(-1, "Unfinished threads detected!");
    }
    
    bool bCompare = m_compare;//save
    
    try 
    {
        m_pCallback->setAbortStatus(false);
        initializeMultiThreadedCopying();
        
        const CInstallFileList& fileList = m_installFiles.getInstallFileList();
        int nItems = fileList.size();
        int n;
        bool recCopyDone = false;

        for (n=0; n<nItems; n++)
        {
            CInstallFile& installFile = *fileList[n];
            
            const bool bCacheable = installFile.isCacheable();
            if (!bCacheFiles || bCacheable)
            {
                const char* method = installFile.getMethod().c_str();
                const char* source = installFile.getSrcPath().c_str();
                const char* params = installFile.getParams().c_str();
                string dest   = installFile.getDestPath().c_str();
        
                std::string::size_type pos;
                if ((pos = dest.find("@temp" PATHSEPSTR)) != std::string::npos)
                {
                    dest.replace(pos, strlen("@temp" PATHSEPSTR), m_cachePath.get());
                    //a temp file should not be copied over itself so ignore
                    if (!bCacheFiles && !stricmp(source, dest.c_str()))
                        continue;
                }
                else
                    dest.insert(0, destPath);//note that destPath is always terminated by PATHSEPCHAR

                if (params && !*params)
                    params = NULL;

                if (m_useSSHIfDefined && !bCacheFiles && !m_compare &&
                    !strcmp(method, "copy") && strcmp(instanceName, "Cache"))
                {
                  if (!recCopyDone)
                  {
                    StringBuffer sbsrc(source);

                    if (strrchr(source, PATHSEPCHAR))
                      sbsrc.setLength(strrchr(source, PATHSEPCHAR) - source + 1);

                    sbsrc.append("*");
                    StringBuffer sbdst(dest.c_str());

                    if (strrchr(sbdst.str(), PATHSEPCHAR))
                      sbdst.setLength(strrchr(sbdst.str(), PATHSEPCHAR) - sbdst.str() + 1);

                    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Copy Directory", m_process.queryName(), m_name.get(),
                      m_curInstance, sbsrc.str(), sbdst.str(), m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);
                    task->setFlags(m_deployFlags & DCFLAGS_ALL);
                    m_threadPool->start(task);
                    recCopyDone = true;
                  }

                  continue;
                }

                if (!processInstallFile(m_process, instanceName, method, source, dest.c_str(), os, bCacheable, params))
                    break;

                if (bCacheFiles && bCacheable)
                {
                    if (0 != stricmp(method, "copy"))
                        installFile.setMethod("copy");
                    
                    installFile.setSrcPath(dest.c_str());
                    m_envDepEngine.addTempFile(dest.c_str());
                }
            }
        }//for
        
        //now process any dynamically added files (via xslt's external function)
        nItems = s_dynamicFileList.size();
        for (n=0; n<nItems; n++)
        {
            const CInstallFile& installFile = *s_dynamicFileList[n];
            //const bool bCacheable = installFile.isCacheable();
            //if (bCacheable && instanceIndex>0)
            //  continue;

            const char* method = installFile.getMethod().c_str();
         //if we are not deploying build files and method is copy then ignore this file
         if (!(m_deployFlags & DEFLAGS_BUILDFILES) && (!method || !stricmp(method, "copy")))
                continue;

         const char* source = installFile.getSrcPath().c_str();
         const char* params = installFile.getParams().c_str();//only supported for dynamically added XSLT files
         bool bCacheable    = installFile.isCacheable();
         string dest = installFile.getDestPath();
         StringBuffer src(source);

         if (params && !*params)
            params = NULL;

         if (dest.empty())
            dest += pathTail( installFile.getSrcPath().c_str() );
         
         //any dynamically generated paths are generated with '\\'
         //In configenv, remote copying takes care of the paths
         //if this is configgen, and we are on linux, replace
         //paths with right separator
         if (PATHSEPCHAR == '/' && os == MachineOsLinux)
           src.replace('\\', '/');

         //resolve conflicts if method is not schema or del
         //find all occurrences of this destination file in the map and resove any conflicts
         //like size mismatch etc.
         bool bAddToFileMap = m_installFiles.resolveConflicts(m_process, method, src.str(), dest.c_str(),
                                 m_name, m_curInstance, params);

         if (bAddToFileMap)
         {
            CInstallFile* pInstallFileAdded = m_installFiles.addInstallFile(method, src.str(), dest.c_str(), bCacheable, params);

               std::string::size_type pos;
            if ((pos = dest.find("@temp" PATHSEPSTR)) != std::string::npos)
               dest.replace(pos, strlen("@temp" PATHSEPSTR), m_cachePath.get());
               else
                   dest.insert(0, destPath);//note that destPath is always terminated by PATHSEPCHAR

            if (!processInstallFile(m_process, instanceName, method, src.str(), dest.c_str(), os, bCacheable, params))
               break;

            if (bCacheFiles && bCacheable)
            {
                if (0 != stricmp(method, "copy"))
                    pInstallFileAdded->setMethod("copy");

                pInstallFileAdded->setSrcPath(dest.c_str());
                m_envDepEngine.addTempFile(dest.c_str());
            }
         }
      }//for

        m_threadPool->joinAll();
        m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
        //    if (m_createIni)
        //        createIniFile(destPath, os);
    }
    catch (IException* e)
    {
        m_compare = bCompare;
        m_threadPool->joinAll();
        throw e;
    }
    catch (...)
    {
        m_compare = bCompare;
        m_threadPool->joinAll();
        throw MakeErrnoException("Error deploying %s", m_name.get());
    }
}


bool CDeploymentEngine::processInstallFile(IPropertyTree& processNode, const char* instanceName, 
                                           const char* method, const char* source, const char* dest, 
                                           EnvMachineOS os, bool bCacheable, const char* params/*=NULL*/)
{
    while (true)
    {
        try
        {
            checkAbort();
            if (m_pCallback->getAbortStatus())
                return false;

            bool bCompare = m_compare;//save

            //if we are creating a temporary file then disable comparing - deploy
            //since the previously generated temporary file would have been deleted by now
            if (m_compare)
            {
                char tempfile[_MAX_PATH];
                getTempPath(tempfile, sizeof(tempfile), m_name);

                if (!strncmp(dest, tempfile, strlen(tempfile)))
                    m_compare = false;
            }

            // Skip if method is copy and we are not copying files
            if (!strnicmp(method, "copy", 4))
            {
                if (m_compare)
                {
                    compareFiles(source, dest, os );
                    Owned<IFile> f = createIFile(source);
                    getCallback().fileSizeCopied(f->size(),true);
                }
                else
                {
                    // Copy the file
                    ensurePath(dest);

                    if (!stricmp(method+4, "_block_until_done")) //copy_block_until_done
                    {
                        Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Copy File", m_process.queryName(), m_name.get(), 
                            m_curInstance, source, dest, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);
                        task->setFlags(m_deployFlags & DCFLAGS_ALL);

                        task->copyFile( m_deployFlags & (DCFLAGS_ALL | DTC_DEL_WRONG_CASE));
                        m_pCallback->printStatus(task);

                        if (task && task->getAbort())
                            throw MakeStringException(0, "User abort");
                    }
                    else
                    {
                        Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Copy File", m_process.queryName(), m_name.get(), 
                            m_curInstance, source, dest, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os);
                        task->setFlags(m_deployFlags & DCFLAGS_ALL);

                        m_threadPool->start(task);//start a thread for this task
                    }
                }
            }
            else if (!strnicmp(method, "xsl", 3)) //allow xsl, xslt or any other string starting with xsl
            {
                unsigned deployFlags = m_deployFlags;
                if (!strcmp(method, "xslt_deployment"))
                    m_deployFlags = deployFlags | DEFLAGS_CONFIGFILES;

                s_bCacheableDynFile = bCacheable;
                xslTransform(source, dest, instanceName, os, params);//params only supported for dynamically added xslt files
                s_bCacheableDynFile = false;

                m_deployFlags = deployFlags;
            }
            else if (!stricmp(method, "esp_service_module")) //processed from CEspDeploymentEngine::xslTransform
                ;
            else if (!stricmp(method, "esp_plugin"))         //processed from CEspDeploymentEngine::xslTransform
                ;
            else if (!stricmp(method, "model")) 
            {
                //extract name of model from dest file path
                StringBuffer dir;            
                const char* pszFileName = splitDirTail(dest, dir);
                const char* pszExtension= strchr(pszFileName, '.');
                if (pszExtension == NULL)
                    pszExtension = pszFileName + strlen(pszFileName);

                StringBuffer modelName;
                modelName.append('\'').append(pszExtension - pszFileName, pszFileName).append('\'');

                m_transform->setParameter("modelName", modelName.str());
                xslTransform(source, dest, instanceName, os);
            }
            /* -- unsupported now since this was deemed security hole -- 
            else if (!stricmp(method, "exec"))
            {
            m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Executing %s", dest);
            StringBuffer xpath;
            xpath.appendf("Instance[@name='%s']", instanceName);
            IPropertyTree* pInstanceNode = processNode.queryPropTree(xpath.str());

            StringAttr user, pwd;
            m_envDepEngine.getAccountInfo(pInstanceNode->queryProp("@computer"), user, pwd);

            // Spawn start process
            Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Remote Execution", m_name.get(), m_curInstance, NULL, dest, os);
            task->createProcess(true, user, pwd);
            m_pCallback->printStatus(task);
            checkAbort(task);
            }
            else if (!strnicmp(method, "dxsl", 4)) //allow dxsl, dxslt or any other string starting with dxsl
            {
            StringBuffer t(source);
            t.append(".xsl");
            xslTransform(source, t.str(), instanceName, os);
            xslTransform(t.str(), dest, instanceName, os);
            remove(t.str());
            }
            */
            else
                processCustomMethod(method, source, dest, instanceName, os);

            m_compare = bCompare;
            break;
        }
        catch (IException* e)
        {
            if (m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e))//ignore ?
                break;
        }
        catch (...)
        {
            if (m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL))//ignore ?
                break;
        }
    }
    return true;
}


//---------------------------------------------------------------------------
//  beforeDeployInstance
//---------------------------------------------------------------------------
void CDeploymentEngine::beforeDeployInstance(IPropertyTree& instanceNode, const char* destPath)
{
}

//---------------------------------------------------------------------------
//  checkFileExists
//---------------------------------------------------------------------------
bool CDeploymentEngine::checkFileExists(const char* filename) const
{
  StringBuffer xpath, err;
  bool flag = false;
  xpath.appendf("./Instance[@name='%s']", m_curInstance);
  IPropertyTree* pInstanceNode = m_process.queryPropTree(xpath.str());
  EnvMachineOS os = MachineOsW2K;

  if (!pInstanceNode)
  {
    StringBuffer destpath, ip;
    stripNetAddr(filename, destpath, ip);
    if (ip.length())
    {
      IConstMachineInfo* pInfo = m_envDepEngine.getEnvironment().getMachineByAddress(ip.str());
      os = pInfo->getOS();
    }
  }
  else
    os = m_envDepEngine.lookupMachineOS(*pInstanceNode);
  
  if (os == MachineOsLinux && m_curSSHUser.length() && m_curSSHKeyFile.length())
    return checkSSHFileExists(filename);
  else
  {
    Owned<IFile> pIFile = createIFile(filename);
    return pIFile->exists();
  }
}

//---------------------------------------------------------------------------
//  setXsl
//---------------------------------------------------------------------------
void CDeploymentEngine::setXsl(IXslProcessor* processor, IXslTransform* transform)
{
   m_processor = processor;
   m_transform = transform;

   m_externalFunction.setown(m_transform->createExternalFunction("addDeploymentFile", addDeploymentFile));
   m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction.get(), true);

   m_externalFunction2.setown(m_transform->createExternalFunction("siteCertificate", siteCertificateFunction));
   m_transform->setExternalFunction(SEISINT_NAMESPACE, m_externalFunction2.get(), true);
}

//---------------------------------------------------------------------------
//  createIniFile
//---------------------------------------------------------------------------
void CDeploymentEngine::createIniFile(const char* destPath, EnvMachineOS os)
{
    // Check if INI file needs to be created
    if (m_iniFile.length() == 0) return;
    
    // Output all attributes - except certain ones
    const char* ignore[] = {"name", "description", "build", "buildSet" };
    const char** begin = &ignore[0];
    const char** end   = &ignore[sizeof(ignore)/sizeof(*ignore)];
    
    StringBuffer str;
    str.append("# INI file generated by CDeploymentEngine\n\n");
    
    Owned<IAttributeIterator> aiter = m_process.getAttributes();
    for (aiter->first(); aiter->isValid(); aiter->next())
    {
        // Only output attribute if its value is non-blank
        const char* name = &aiter->queryName()[1];
        const char* val  = aiter->queryValue();
        if (val && *val)
        {
            if (std::find_if(begin, end, std::bind1st(string_compare(), name)) == end)
            {
                str.appendf("%s=%s\n", name, val);
            }
        }
    }
    writeFile(StringBuffer(destPath).append(m_iniFile).str(), str.str(), os);
}

//---------------------------------------------------------------------------
//  getBuildSetNode
//---------------------------------------------------------------------------
IPropertyTree* CDeploymentEngine::queryBuildSetNode(IPropertyTree& processNode, 
                                                    IPropertyTree*& buildNode) const
{
    // Get build node for process
    StringBuffer xpath("Programs/Build[@name='");
    xpath.appendf("%s']", processNode.queryProp("@build"));
    buildNode = m_rootNode->queryPropTree(xpath.str());
    assertex(buildNode);
    
    // Get buildSet node for process
    xpath.clear();
    xpath.appendf("BuildSet[@name=\"%s\"]", processNode.queryProp("@buildSet"));
    
    IPropertyTree* buildSetNode = buildNode->queryPropTree(xpath.str());
    assertex(buildSetNode);
    return buildSetNode;
}

IPropertyTree* CDeploymentEngine::getDeployMapNode(IPropertyTree* buildNode, IPropertyTree* buildSetNode) const
{
    // Get some useful attributes
    const char* url = buildNode->queryProp("@url");
    const char* path = buildSetNode->queryProp("@path");
    const char* installSet = buildSetNode->queryProp("@installSet");
    
    // Workout name for deploy map file
    StringBuffer deployFile(url);
    if (path && *path)
        deployFile.append(PATHSEPCHAR).append(path);
    if (installSet && *installSet)
        deployFile.append(PATHSEPCHAR).append(installSet);
    else
        deployFile.append("\\deploy_map.xml");
    
    // Read in deploy map file and process file elements
    
    IPropertyTree* deployNode = createPTreeFromXMLFile(deployFile.str(), ipt_caseInsensitive);
    assertex(deployNode);
    return deployNode;
}

bool CDeploymentEngine::searchDeployMap(const char* fileName, const char* optionalFileExt) const
{
    IPropertyTree* buildNode;
    IPropertyTree* buildSetNode = queryBuildSetNode(m_process, buildNode);
    Owned<IPropertyTree> deployNode = getDeployMapNode(buildNode, buildSetNode);
    
    StringBuffer xpath;
    xpath.appendf("File[@name='%s']", fileName);
    
    bool bFound = false;
    Owned<IPropertyTreeIterator> iter = deployNode->getElements(xpath.str());
    if (iter->first() && iter->isValid())
        bFound = true;
    else
    {
        xpath.clear().appendf("File[@name='%s%s']", fileName, optionalFileExt);
        iter.setown( deployNode->getElements(xpath.str()) );
        if (iter->first() && iter->isValid())
            bFound = true;
    }
    return bFound;
}

//---------------------------------------------------------------------------
//  determineInstallFiles
//---------------------------------------------------------------------------
int CDeploymentEngine::determineInstallFiles(IPropertyTree& processNode, CInstallFiles& installFiles) const
{
    try
    {
      m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, 
                               "Determining files to install for %s", processNode.queryProp("@name"));
        IPropertyTree* buildNode;
      IPropertyTree* buildSetNode = queryBuildSetNode(processNode, buildNode);
        Owned<IPropertyTree> deployNode = getDeployMapNode(buildNode, buildSetNode);
        StringBuffer srcFilePath;
        srcFilePath.ensureCapacity(_MAX_PATH);
        const char* url = buildNode->queryProp("@url");
        const char* path = buildSetNode->queryProp("@path");
        const bool bFindStartable = &m_process == &processNode && m_startable == unknown;
        const bool bFindStoppable = &m_process == &processNode && m_stoppable == unknown;
        
        Owned<IPropertyTreeIterator> iter = deployNode->getElements("File");
        ForEach(*iter)
        {
            IPropertyTree* pFile = &iter->query();
            // Get some useful attributes
            const char* name = pFile->queryProp("@name");
            
            //if this file is an installset (deploy_map.xml) then ignore it (don't deploy)
            //
            if (!stricmp(name, "deploy_map.xml"))
                continue;
            
            if (bFindStartable && !strnicmp(name, "startup", sizeof("startup")-1))
                m_startable = yes;

            if (bFindStoppable && !strnicmp(name, "stop", sizeof("stop")-1))
                m_stoppable = yes;

            const char* method  = pFile->queryProp("@method");
            if (method && !stricmp(method, "schema"))
                continue;// ignore - could validate against it if we felt brave!

            //if we are not deploying build files and method is copy then ignore this file
            if (!(m_deployFlags & DEFLAGS_BUILDFILES) && (!method || !stricmp(method, "copy")))
                continue;
            
            const char* srcPath = pFile->queryProp("@srcPath");
            const char* destPath= pFile->queryProp("@destPath");
            const char* destName= pFile->queryProp("@destName");
            bool bCacheable     = pFile->getPropBool("@cache", false);
            
            // Get source filespec
            if (srcPath && !strcmp(srcPath, "@temp"))
            {
                char tempfile[_MAX_PATH];
                getTempPath(tempfile, sizeof(tempfile), m_name);
                srcFilePath.clear().append(tempfile).append(name);
            }
            else
            {
                srcFilePath.clear().append(url).append(PATHSEPCHAR);
                if (path && *path)
                    srcFilePath.append(path).append(PATHSEPCHAR);
                if (srcPath && 0!=strcmp(srcPath, "."))
                {
                    if (!strncmp(srcPath, "..", 2) && (*(srcPath+2)=='/' || *(srcPath+2)=='\\'))
                    {
                        StringBuffer reldir(srcPath);
                        reldir.replace('/', '\\');

                        while (!strncmp(reldir.str(), "..\\", 3))
                        {
                            srcFilePath.setLength( srcFilePath.length() - 1 ); //remove last char PATHSEPCHAR
                            const char* tail = pathTail(srcFilePath.str());
                            srcFilePath.setLength( tail - srcFilePath.str() );
                            reldir.remove(0, 3);
                        }
                        srcFilePath.append(reldir).append(PATHSEPCHAR);
                    }
                    else
                        srcFilePath.append(srcPath).append(PATHSEPCHAR);
                }
                srcFilePath.append(name);
            }
            
            std::string sDestName;
            if (method && (!stricmp(method, "esp_service_module") || !stricmp(method, "esp_plugin")))
            {
                //if this is xsl transformation and we are not generating config files then ignore
                //
                if (!(m_deployFlags & DEFLAGS_CONFIGFILES) && !stricmp(method, "esp_service_module"))
                    continue;
                
                //if this file is an esp service module, encode name of service in the dest file name
                //so the esp deployment can figure out which service this file belongs to
                //
                const char* serviceName = processNode.queryProp("@name");
                
                //if destination name is specified then use it otherwise use <service-name>[index of module].xml
                sDestName = serviceName;
                if (destName)
                {
                    sDestName += '_';
                    sDestName += destName;
                }
                else
                {
                    int espServiceModules = m_envDepEngine.incrementEspModuleCount();
                    if (espServiceModules > 1)
                    {
                        char achNum[16];
                        itoa(espServiceModules, achNum, 10);
                        
                        sDestName += achNum;
                    }
                    sDestName += ".xml";
                }
                //encode name of service herein - this is needed by and removed by CEspDeploymentEngine::processServiceModules()
                sDestName += '+';
                sDestName += processNode.queryProp("@name");//encode the name of service
            }
            else if (method && (!stricmp(method, "xsl") || !stricmp(method, "xslt")) && !(m_deployFlags & DEFLAGS_CONFIGFILES))
                continue;//ignore xsl transformations if we are not generating config files
            else
            {
                if (!method || !*method)
                    method = "copy";
                
                // Get destination filespec
                if (destName && *destName)
                {
                    //we now support attribute names within the destination file names like delimted by @ and + (optional)
                    //for e.g. segment_@attrib1+_file_@attrib2 would produce segment_attribval1_file_attrib2value
                    //+ not necessary if the attribute name ends with the word, for e.g. file_@attrib1
                    //for instnace, suite_@eclServer+.bat would expand to suite_myeclserver.bat
                    //if this process has an @eclServer with value "myeclserver"
                    //
                    if (strchr(destName, '@') || strchr(destName, '+'))
                    {
                        char* pszParts = strdup(destName);
                        
                        char *saveptr;
                        const char* pszPart = strtok_r(pszParts, "+", &saveptr);
                        while (pszPart)
                        {
                            const char* p = pszPart;
                            if (*p)
                            {
                                if (strchr(p, '@'))//xpath for an attribute?
                                {
                                    // find name of attribute and replace it with its value
                                    const char* value = m_process.queryProp( p );
                                    if (value)
                                        sDestName.append(value);
                                }
                                else
                                    sDestName.append(p); //no attribute so copy verbatim
                            }
                            pszPart = strtok_r(NULL, "+", &saveptr);
                        }
                        free(pszParts);
                    }
                    else
                        sDestName = destName;
                    
                    
                    if (sDestName.empty())
                        throw MakeStringException(-1, "The destination file name '%s' for source file '%s' "
                        "translates to an empty string!", destName, name);
                }
            }
            
            StringBuffer destFilePath;
            destFilePath.ensureCapacity(_MAX_PATH);
            
            bool bTempFile = (destPath && !stricmp(destPath, "@temp")) ||
                !strnicmp(name, "@temp", 5); //@name starts with @temp or @tmp
            if (bTempFile)
            {
                if (sDestName.empty())//dest name not specified
                {
                    if (!strcmp(method, "copy"))
                        sDestName = name;
                    else
                    {
                        StringBuffer dir;
                        const char* pszFileName = splitDirTail(name, dir);
                        const char* pExt = findFileExtension(pszFileName);
                        
                        if (pExt)
                            sDestName.append(pszFileName, pExt-pszFileName);
                        else
                            sDestName.append(pszFileName);
                        
                        char index[16];
                        itoa(m_envDepEngine.incrementTempFileCount(), index, 10);
                        sDestName.append(index);
                        
                        if (pExt)
                            sDestName.append(pExt);
                    }
                }
                destFilePath.append("@temp" PATHSEPSTR);
            }
            else
            {
                if (destPath && *destPath)
                {
                    destFilePath.append(destPath);
                    if (destPath[strlen(destPath)-1] != PATHSEPCHAR)
                        destFilePath.append(PATHSEPCHAR);
                }
                
                if (sDestName.empty())
                    sDestName = name;
            }
            
            destFilePath.append(sDestName.c_str());
            
            //find all occurrences of this destination file in the map and resove any conflicts
            //like size mismatch etc.
            bool bAddToFileMap = installFiles.resolveConflicts(processNode, method, srcFilePath.str(), destFilePath.str(),
                m_name, m_curInstance, NULL);
            //resolve conflicts if method is not schema or exec
            if (0 != stricmp(method, "schema") && 0 != stricmp(method, "exec") && 0 != strnicmp(method, "del", 3)) 
            {
            }
            else if (!strnicmp(method, "del", 3))//treat files to be deleted as temp files - to be deleted AFTER we are done!
            {
                bTempFile = true;
                bAddToFileMap = false;
                m_envDepEngine.addTempFile(destFilePath.str());
            }
            
            
            if (bAddToFileMap)
            {
                if (bTempFile)
                    m_envDepEngine.addTempFile(destFilePath.str());
                
                //enable caching for files to be copied unless expressly asked not to do so
                //
                if (!bCacheable && !strcmp(method, "copy"))
                    bCacheable = pFile->getPropBool("@cache", true);
                
                installFiles.addInstallFile(method, srcFilePath.str(), destFilePath.str(), bCacheable, NULL);
            }
        }
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(0, "Error creating file list for process %s: %s", m_name.get(), msg.str());
    }
    catch (...)
    {
        throw MakeErrnoException("Error creating file list for process %s", m_name.get());
    }
    
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, NULL);
    return installFiles.getInstallFileList().size();
}


//---------------------------------------------------------------------------
//  resolveConflicts
//---------------------------------------------------------------------------
bool CInstallFileMap::resolveConflicts(IPropertyTree& processNode, const char* method, const char* srcPath, 
                                       const char* destPath, const char* compName, 
                                       const char* instanceName, const char* params)
{
    bool rc = true;//no unresolved conflicts so add to file map

    //DBGLOG("Resolving conflicts for %s from %s\n", srcPath, destPath);
    //resolve conflicts if method is not schema or del
    if (0 != stricmp(method, "schema") && 0 != stricmp(method, "del"))
    {
        //find all occurrences of this destination file in the map and resove any conflicts
        //like size mismatch etc.
        const_iterator iLower = lower_bound(destPath);
        const_iterator iUpper = upper_bound(destPath);
        offset_t srcSz = 0;
        unsigned srcCRC;

        for (const_iterator it=iLower; it != iUpper; it++)
        {
            rc = false;//don't add to file map

            CInstallFile* pInstallFile = (*it).second;
            const char* method2 = pInstallFile->getMethod().c_str();
            const char* srcPath2= pInstallFile->getSrcPath().c_str();
            const char* params2 = pInstallFile->getParams().c_str();

            //if file at destPath is being generated using same method and src file
            //anyway then don't warn - just don't add to file map
            //
            if ((!stricmp(method, method2) && !stricmp(srcPath, srcPath2)) || pInstallFile->isDuplicateSrcFile(srcPath))
            {
                //if method is xslt and params are different, then add to file map
                if (!stricmp(method, "xslt") && ((params == NULL && params2!= NULL) || 
                                            (params != NULL && params2 == NULL) || 
                                            (0 != stricmp(params, params2))))
                    rc = true;
            
                break;
            }

            bool rc2=true;
            if (srcSz == 0)
            {
                try
                {
                    srcCRC = getFileCRC(srcPath);
                    srcSz = filesize(srcPath);
                }
                catch(IException* e)
                {
                    rc2 = false;
                    StringBuffer msg;
                    e->errorMessage(msg);

                    m_pDepEngine->getCallback().printStatus(STATUS_WARN, processNode.queryName(), 
                                                                         processNode.queryProp("@name"), instanceName, "%s", msg.str());
                }
            }

            offset_t srcSz2; 
            unsigned srcCRC2;
            
            if (rc2) 
            {
                try {
                    srcSz2 = pInstallFile->getSrcSize();
                    srcCRC2 = pInstallFile->getSrcCRC();;
                } catch(IException* e) {
                    rc2 = false;
                    StringBuffer msg;
                    e->errorMessage(msg);
                    
                    m_pDepEngine->getCallback().printStatus(STATUS_WARN, processNode.queryName(), 
                        processNode.queryProp("@name"), instanceName, "%s", msg.str());
                }
            }

            if (rc2)
            {
                if (srcSz == srcSz2 && srcCRC == srcCRC2)
                    pInstallFile->addDuplicateSrcFile( srcPath );
                else
                {
                    //for starters, just display an error on every conflict even if this is a
                    //redeployment of the same file
                    //
                    const char* fileName = pathTail(destPath);
                    if (!fileName)
                        fileName = destPath;

                    StringBuffer path1;
                    const char* srcFileName1 = splitDirTail(srcPath, path1);
                    if (0 != strcmp(srcFileName1, fileName))//src file name is not same as dest file name
                        path1.clear().append(srcPath);
                    else
                        path1.remove( path1.length()-1, 1);

                    StringBuffer path2;
                    const char* srcFileName2 = splitDirTail(srcPath2, path2);
                    if (0 != strcmp(srcFileName2, fileName))//src file name is not same as dest file name
                        path2.clear().append(srcPath2);
                    else
                        path2.remove( path2.length()-1, 1);

                    bool bDiffMethods = 0 != strcmp(method, method2);
                    StringBuffer msg;
                    msg.appendf("File skipped: The file %s to be deployed from %s ", fileName, path1.str());

                    if (bDiffMethods)
                        msg.appendf("by method '%s' ", method);

                    msg.appendf("conflicts with another file from %s", path2.str());

                    if (bDiffMethods)
                        msg.appendf(" using method '%s'", method2);

                    msg.append('.');      

                    m_pDepEngine->getCallback().printStatus(STATUS_WARN, processNode.queryName(), 
                                                                         processNode.queryProp("@name"), instanceName, 
                                                                         "%s", msg.str());
                }
                break;
            }
      }
   }
   return rc;
}


//---------------------------------------------------------------------------
//  addDeploymentFile
//---------------------------------------------------------------------------
/*static*/
void CDeploymentEngine::addDeploymentFile(StringBuffer &ret, const char *in, IXslTransform*)
{
    //input is of the format <method>+<file name>+<source dir>+<dest filename>[+<dest subdir>]
    StringArray tokens;
    DelimToStringArray(in, tokens, "+");
    
    int len = tokens.length();
    if (len < 4)
        throw MakeStringException(0, "Invalid format for external function parameter!");
    
    const char* method  = tokens.item(0);
    const char* name    = tokens.item(1);
    const char* srcPath = tokens.item(2);
    const char* destName= tokens.item(3);
    
    StringBuffer srcPath2(srcPath);
    srcPath2.append(name);
    
    StringBuffer destPath;
    if (len > 4)
        destPath.append(tokens.item(4));
    destPath.append(destName);
    
    Owned<CInstallFile> pInstallFile = new CInstallFile(method, srcPath2.str(), destPath.str(), s_bCacheableDynFile);
    
    if (len > 5)
        pInstallFile->setParams(tokens.item(5));
    
    s_dynamicFileList.push_back(LinkedFilePtr(pInstallFile.get()));
}


//---------------------------------------------------------------------------
//  siteCertificate
//---------------------------------------------------------------------------
/*static*/
void CDeploymentEngine::siteCertificateFunction(StringBuffer &ret, const char *in, IXslTransform*)
{
    //input is of the format <processType>+<process name>+<instance name>+<output path>
    StringArray tokens;
    DelimToStringArray(in, tokens, "+");
    
    int len = tokens.length();
    if (len < 4)
        throw MakeStringException(0, "Invalid format for external function parameter!");
    
    const char* processType = tokens.item(0);
    const char* processName = tokens.item(1);
    const char* instanceName= tokens.item(2);
    const char* outputFile  = tokens.item(3);
    
    if (!processType || !*processType || !processName || !*processName ||
        !instanceName || !*instanceName || !outputFile || !*outputFile)
    {
        throw MakeStringException(0, "Invalid parameters for siteCertificate method call!");
    }
    
    IPropertyTree* pProcess = s_xsltDepEngine->lookupProcess(processType, processName);
    if (!pProcess)
        throw MakeStringException(0, "%s with name %s is not defined!", processType, processName);
    
    s_xsltDepEngine->siteCertificate( *pProcess, instanceName, outputFile );
}


//---------------------------------------------------------------------------
//  processCustomMethod
//---------------------------------------------------------------------------
void CDeploymentEngine::processCustomMethod(const char* method, const char *source, const char *outputFile, 
                                            const char *instanceName, EnvMachineOS os)
{
    //we only recognize ssl_certificate as the custom method so if any other method is sought 
    //then throw exception
    StringBuffer dir;
    const char* fileName = splitDirTail(source, dir);
    
    if (0 != stricmp(method, "ssl_certificate"))
        throw MakeStringException(0, "Process '%s': invalid method '%s' specified for file '%s'", 
        m_name.get(), method, fileName);
    
    siteCertificate(m_process, instanceName, outputFile);
}

//---------------------------------------------------------------------------
//  processCustomMethod
//---------------------------------------------------------------------------
void CDeploymentEngine::siteCertificate(IPropertyTree& process, const char *instanceName, const char *outputFile)
{
    const char* pszCertFile;
    const char* pszPrivFile;
    StringBuffer sPrivKey;
    StringBuffer sCertificate;
    bool rc;
    
    Owned<IDeployTask> task = createDeployTask( *m_pCallback, "Site Certificate", process.queryName(), 
        m_name.get(), m_curInstance, NULL, NULL, "", "", "", false);
    m_pCallback->printStatus(task);
    task->setProcessed();
    
    while (true)
    {
        try
        {
            //generate SSL certificate and private key, if they have not already been generated
            //and save them in the environment under the instance nodes.  Note that if the 
            //environment is read-only then these are not written out and are lost after close.
            //
            //todo: mark env modified so it gets saved!
            //
            StringBuffer xpath;
            xpath.appendf("Instance[@name='%s']", instanceName);
            
            IPropertyTree* pInstanceNode = process.queryPropTree(xpath.str());
            
            IPropertyTree* pHttps = process.queryPropTree("HTTPS");
            
            if (!pHttps)
            {
                if (!strcmp(process.queryName(), "EspProcess"))
                    throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nNo HTTPS information was specified.");
                else
                    pHttps = &process;
            }
            
            pszCertFile  = pHttps->queryProp("@certificateFileName");
            pszPrivFile  = pHttps->queryProp("@privateKeyFileName");
            
            if (!pszCertFile || !*pszCertFile || !pszPrivFile || !*pszPrivFile)
                throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nName for certificate or private key file was not specified.");
            
            IPropertyTree* pCertNode    = pInstanceNode->queryPropTree("Certificate");
            
            if (!pCertNode)
                pCertNode = pInstanceNode->addPropTree("Certificate", createPTree());
            else
                sCertificate.append( pCertNode->queryProp(NULL) );
            
            
            IPropertyTree* pPrivKeyNode = pInstanceNode->queryPropTree("PrivateKey" );
            
            if (!pPrivKeyNode)
                pPrivKeyNode = pInstanceNode->addPropTree("PrivateKey", createPTree());
            else
                sPrivKey.append( pPrivKeyNode->queryProp(NULL) );
            
            IPropertyTree* pCsrNode = pInstanceNode->queryPropTree("CSR" );
            StringBuffer sCSR;
            
            if (!pCsrNode)
                pCsrNode = pInstanceNode->addPropTree("CSR", createPTree());
            else
                sCSR.append( pCsrNode->queryProp(NULL) );
            
            bool bRegenerateCSR = pHttps->getPropBool("@regenerateCredentials", false);
            
            if (sCertificate.length()==0 || sPrivKey.length()==0 || sCSR.length()==0 || bRegenerateCSR)
            {
                m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Generating SSL certificate and private key files ...");
                
                const char* pszOrgUnit   = pHttps->queryProp("@organizationalUnit");
                const char* pszOrg       = pHttps->queryProp("@organization");
                const char* pszCity      = pHttps->queryProp("@city");
                const char* pszState     = pHttps->queryProp("@state");
                const char* pszCountry   = pHttps->queryProp("@country");
                const char* pszPassPhrase= pHttps->queryProp("@passphrase");
                const char* pszFQDN      = pInstanceNode->queryProp("@FQDN");
                const char* pszIpAddress = pInstanceNode->queryProp("@netAddress");
                int         daysValid    = pHttps->getPropInt("@daysValid", -1);
                
                if (!pszOrgUnit || !*pszOrgUnit || !pszOrg || !*pszOrg)
                    throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nOrganizational unit or organization was not specified.");
                
                if (!pszCity || !*pszCity || !pszState || !*pszState || !pszCountry || !*pszCountry)
                    throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nCity, state or country was not specified.");
                
                if (!pszPassPhrase || !*pszPassPhrase)
                    throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nPass phrase was not specified.");
                
                if (daysValid < -1)
                    throw MakeStringExceptionDirect(-1, "Cannot generate SSL certificate.\nNumber of days the certificate needs to be valid was not specified.");
                
                //call secure socket method to generate the certificate and private key into string buffers
                //
                Owned<ICertificate> cc = createCertificate();
                cc->setCountry(pszCountry);
                cc->setState  (pszState);
                cc->setCity   (pszCity);
                cc->setOrganization(pszOrg);
                cc->setOrganizationalUnit(pszOrgUnit);
                cc->setDestAddr( pszFQDN && *pszFQDN ? pszFQDN : pszIpAddress); //use FQDN if available, ip address otherwise
                cc->setDays    (daysValid);
                
                StringBuffer pwbuf;
                decrypt(pwbuf, pszPassPhrase);
                cc->setPassphrase(pwbuf.str());
                
                cc->generate(sCertificate.clear(), sPrivKey.clear());//throws exception!
                
                m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Generating Certificate Signing Request (CSR) ...");
                cc->generateCSR(sPrivKey.str(), sCSR.clear());
                
                if (bRegenerateCSR)
                    pHttps->setProp("@regenerateCredentials", "false");
                
                //set these generated values in the environment so we don't have to regenerate them later
                //
                if (!m_environment.isConstEnvironment())
                {
                    pCertNode->setProp(NULL, sCertificate.str());
                    pPrivKeyNode->setProp(NULL, sPrivKey.str());
                    pCsrNode->setProp(NULL, sCSR.str());
                    
                    m_pCallback->setEnvironmentUpdated();
                }
            }
            rc = true;
            break;
      }
      catch (IException* e)
      {
          StringBuffer msg;
          e->errorMessage(msg);
          
          task->setErrorString(msg.str());
          task->setErrorCode((DWORD)-1);
          
          if (m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e, NULL, NULL, task))//ignore ?
          {
              rc = false;
              break;
          }
          task->setErrorCode(0);
          task->setErrorString("");
      }
      catch (...)
      {
          task->setErrorString("Unknown exception!");
          task->setErrorCode((DWORD)-1);
          
          if (m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, NULL, NULL, NULL, task))//ignore ?
          {
              rc = false;
              break;
          }
          task->setErrorCode(0);
          task->setErrorString("");
      }
    }//while
    
    m_pCallback->printStatus(task);
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL);
    
    //write the string buffers out to temp files
    //
    if (rc && (m_deployFlags & DEFLAGS_BUILDFILES))
    {
        //Handle certificate first ------------------------------
        //
        //create temp file path to save certificate
        char tempfile[_MAX_PATH];
        getTempPath(tempfile, sizeof(tempfile), m_name);
        char* pTempFile = tempfile + strlen(tempfile);

        strcpy(pTempFile, pszCertFile);
        DeleteFile(tempfile);
        
        //write certificate in this temp file
        Owned<IFile> pFile = createIFile(tempfile);
        IFileIO* pFileIO = pFile->open(IFOcreate);
        pFileIO->write( 0, sCertificate.length(), sCertificate.str());
        pFileIO->Release();
        m_envDepEngine.addTempFile(tempfile);
        
        //add this file copy operation to our todo list
        Owned<CInstallFile> pInstallFile = new CInstallFile("copy", tempfile, pszCertFile);
        s_dynamicFileList.push_back(LinkedFilePtr(pInstallFile.get()));
        
        //Now handle private key ------------------------------
        
        //create temp file path to save private key
        strcpy(pTempFile, pszPrivFile);
        DeleteFile(tempfile);

        //write private key in this temp file
        pFile.set( createIFile(tempfile) );
        pFileIO = pFile->open(IFOcreate);
        pFileIO->write( 0, sPrivKey.length(), sPrivKey.str());
        pFileIO->Release();
        m_envDepEngine.addTempFile(tempfile);
        
        //add this file copy operation to our todo list
        Owned<CInstallFile> pInstallFile2 = new CInstallFile("copy", tempfile, pszPrivFile);
        s_dynamicFileList.push_back(LinkedFilePtr(pInstallFile2.get()));
    }
}


bool CDeploymentEngine::fireException(IException *e)
{
    StringBuffer msg;
    e->errorMessage(msg);
    // don't release e since that is done by our caller
    
    //don't process abort exception since processException will throw another exception
    if (strcmp(msg.str(), "Abort") != 0)
    {
        try
        {
            //BUG#48891: m_pCallback->processException() releases the exception, so bump the link count
            //and let the JThread handleException release the exception
            //Also note, if control comes here and we display the 
            //the abort, retry and ignore message box, there is no route to go back to the 
            //DeployTask and retry the operation. Therefore, any exception that come here 
            //need to be fixed to be caught and handled in the DeployTask
            e->Link();
            m_pCallback->processException(m_process.queryName(), m_name, m_curInstance, e);
        }
        catch (IException *e1) 
        {
            // smeda: 29299
            // do not rethrow exceptions from processException (as we are already in the 
            // middle of handling an exception). If rethrown, the jthread's parent.notifyStopped()
            // is not called which in turn will keep the ThreadPool's
            // joinwait() hanging forever for this thread to stop.
            EXCLOG(e1,"CDeploymentEngine::fireException");
            return false; // didn't handle the exception
        }
    }
    
    return true; //handled the exception
}

bool CDeploymentEngine::checkSSHFileExists(const char* dir) const
{
  bool flag = false;
  StringBuffer destpath, destip, cmd, output, tmp, err;
  stripNetAddr(dir, destpath, destip);

  if (destip.length() && m_curSSHUser.length() && m_curSSHKeyFile.length())
  {
    tmp.appendf("%d", msTick());
    cmd.clear().appendf("[ -e %s ] && echo %s", destpath.str(), tmp.str());
    Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Ensure Path", m_process.queryName(), m_name.get(), 
      m_curInstance, NULL, NULL, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined);
    task->execSSHCmd(destip.str(), cmd, output, err);
    flag = strstr(output.str(), tmp.str()) == output.str();
    checkAbort(task);
  }

  return flag;
}

void CDeploymentEngine::setSSHVars(IPropertyTree& instance)
{
  EnvMachineOS os = m_envDepEngine.lookupMachineOS(instance);
  m_curSSHUser.clear();
  m_curSSHKeyFile.clear();
  m_curSSHKeyPassphrase.clear();
  m_envDepEngine.getSSHAccountInfo(instance.queryProp("@computer"), m_curSSHUser, m_curSSHKeyFile, m_curSSHKeyPassphrase);
  m_useSSHIfDefined = !m_curSSHKeyFile.isEmpty() && !m_curSSHUser.isEmpty() && os == MachineOsLinux;
  const char* depToFolder = m_envDepEngine.getDeployToFolder();

  if (depToFolder && *depToFolder)
    m_useSSHIfDefined = false;
}

void CDeploymentEngine::clearSSHVars()
{
  m_curSSHUser.clear();
  m_curSSHKeyFile.clear();
  m_curSSHKeyPassphrase.clear();
}
