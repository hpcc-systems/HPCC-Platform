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

#include "WsDeployService.hpp"
#include "WsDeployEngine.hpp"

class CWsGenerateJSFromXsdThread : public CInterface, 
  implements IPooledThread
{
public:
  IMPLEMENT_IINTERFACE;

  CWsGenerateJSFromXsdThread()
  {
  }
  virtual ~CWsGenerateJSFromXsdThread()
  {
  }

  void init(void *startInfo) 
  {
    m_pTask.set((IDeployTask*)startInfo);
  }
  void main()
  {
    m_pTask->copyFile( m_pTask->getFlags() );

    static Mutex m;
    m.lock();
    try
    {
      m_pTask->getCallback().printStatus(m_pTask);
    }
    catch(IException* e)
    {
      e->Release();
    }
    m.unlock();

    if (m_pTask && m_pTask->getAbort())
    {
      m_pTask->getCallback().printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Aborting, please wait...");
      throw MakeStringException(0, "Abort");
    }
  }

  bool canReuse()
  {
    return true;
  }
  bool stop()
  {
    return true;
  }

  virtual IDeployTask* getTask () const     { return m_pTask;     }
  virtual void setTask (IDeployTask* pTask) { m_pTask.set(pTask); }

  virtual bool getAbort() const      { return s_abort;   }
  virtual void setAbort(bool bAbort) { s_abort = bAbort; }

private:
  Owned<IDeployTask> m_pTask;
  static bool s_abort;
};

CWsDeployEngine::CWsDeployEngine( CWsDeployExCE& service, IEspContext* ctx, IConstDeployInfo& deployInfo, const char* selComps, short version)
: m_service(service),
m_bAbort(false),
m_bEnvUpdated(false),
m_errorCount(0),
m_version(version),
m_depInfo(deployInfo),
m_pDeploy(createPTreeFromXMLString(selComps))
{
  m_pResponseXml.setown( createPTreeFromXMLString(selComps) );

  if (!m_pResponseXml)
    return;

  Owned<IPropertyTreeIterator> it = m_pResponseXml->getElements("SelectedComponents/*");
  ForEach(*it)
  {
    IPropertyTree* pComp = &it->query();
    IPropertyTree* pTasks = pComp->queryPropTree("Tasks");
    if (pTasks)
      pComp->removeTree(pTasks);

    pTasks =    pComp->addPropTree("Tasks", createPTree());

    Owned<IPropertyTreeIterator> itInstances = pComp->getElements("Instance");

    ForEach(*itInstances)
    {
      string key;
      key += pComp->queryProp("@name");
      key += ':';
      IPropertyTree* pInstance = &itInstances->query();
      key += pInstance->queryProp("@name");
      m_comp2TasksMap.insert( pair<string, IPropertyTree*>(key, pTasks) );
    }
  }
}


CWsDeployEngine::CWsDeployEngine( CWsDeployExCE& service, IConstDeployInfo& deployInfo, IEspContext* ctx)
: m_service(service),
m_depInfo(deployInfo),
m_bAbort(false),
m_bEnvUpdated(false),
m_errorCount(0),
m_version(1)
{
  //init our cache
  //
  IArrayOf<IConstComponent>& components = deployInfo.getComponents();
  unsigned int nComps = components.ordinality();
  if (nComps == 0)
    throw MakeStringException(-1, "No components were selected for deployment!");


  StringBuffer xml;
  CDeployInfo& depInfo = dynamic_cast<CDeployInfo&>( deployInfo );
  depInfo.serializeStruct(ctx, xml);

  m_pResponseXml.setown( createPTreeFromXMLString(xml.str()) );
  m_pSelComps = m_pResponseXml->queryPropTree("Components");

  //save a copy of component in request into our internal tree that we maintain and 
  //eventually plan to return as part of the response
  //
  Owned<IPropertyTreeIterator> it = m_pSelComps->getElements("Deploy/Components/Component");
  ForEach(*it)
  {
    IPropertyTree* pComp = &it->query();
    IPropertyTree* pTasks = pComp->queryPropTree("Tasks");
    if (pTasks)
      pComp->removeTree(pTasks);

    pTasks =    pComp->addPropTree("Tasks", createPTree());

    string key;
    //key += comp.getType();
    //key += ':';
    key += pComp->queryProp("Name");
    key += ':';
    key += pComp->queryProp("Instance");
    m_comp2TasksMap.insert( pair<string, IPropertyTree*>(key, pTasks) );
  }

  //prepare xml needed for actual deployment
  //
  initComponents(components);
}


void CWsDeployEngine::initComponents(IArrayOf<IConstComponent>& components)
{
  unsigned int nComps = components.ordinality();

  m_pDeploy.set(createPTree("Deploy"));
  IPropertyTree* pComps = m_pDeploy->addPropTree("SelectedComponents", createPTree());

  for (unsigned int i=0; i<nComps; i++)
  {
    IConstComponent& comp = components.item(i);

    const char* type = comp.getType();
    const char* name = comp.getName();

    if (!(type && *type && name && *name))
      throw MakeStringException(-1, "Invalid component specified!");

    StringBuffer xpath;
    xpath.appendf("%s[@name='%s']", type, name);

    IPropertyTree* pComp = pComps->queryPropTree(xpath.str());
    if (!pComp)
    {
      pComp = pComps->addPropTree(type, createPTree());
      pComp->addProp("@name", name);
    }
    const char* instType = comp.getInstanceType();
    IPropertyTree* pInst = pComp->addPropTree(instType, createPTree());
    pInst->addProp("@name", comp.getInstance());
    pInst->addProp("@computer", comp.getComputer());
  }
}


void CWsDeployEngine::deploy(CDeployOptions& pOptions)
{
  try
  {
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    factory->validateCache();   
    Owned<IConstEnvironment> constEnv(factory->openEnvironment());

    m_pEnvDepEngine.setown( createEnvDeploymentEngine(*constEnv, *this, m_pDeploy->queryPropTree("SelectedComponents")) );
    m_pEnvDepEngine->setInteractiveMode(false);

    bool rc = true;

    CDeployOptions& options = pOptions;

    //if (pOptions)
    //  options = *pOptions;
    //else
    //  options = dynamic_cast<CDeployOptions&>(m_depInfo.getOptions());

    //if (m_deployEngine->isLinuxDeployment() && (options.getStop() || options.getStart()))
    //   rc = askForSshAccountInfo();

    // Get timestamp
    time_t t = time(NULL);
    struct tm* now = localtime(&t);
    char timestamp[30];
    strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", now);

    // Validate data
    printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Performing preliminary validation based on XML schema ...");
    m_pEnvDepEngine->check();

    // Delete all existing connections
    //doDisconnects();   

    unsigned deployMode = DEFLAGS_NONE;
    if (options.getConfigFiles())
      deployMode |= DEFLAGS_CONFIGFILES;

    if (options.getBuildFiles())
    {
      deployMode |= DEFLAGS_BUILDFILES;
      if (options.getUpgradeBuildFiles())
        deployMode |= DCFLAGS_TIME | DCFLAGS_SIZE;
    }

    StringBuffer msg;
    if (options.getCompare())
    {
      m_pEnvDepEngine->compare(deployMode);
      msg.append("Compare");
    }
    else
    {
      // Get archive path and environment name
      StringBuffer envName;
      if (envName.length()==0) 
        envName = "deploy"; // make sure we have some name

      // Create unique name for archive file based on timestamp
      const char* archiveLogPath = options.getArchivePath();
      if (options.getArchiveEnv() || options.getLog())
      {
        if (!archiveLogPath || !*archiveLogPath)
          throw MakeStringExceptionDirect(-1, "Cannot archive or log without a path!");

        Owned<IFile> pIFile = createIFile(archiveLogPath);
        if (!pIFile->exists())
        {
          Owned<IDeployTask> task = createDeployTask(*this, "Create Directory", NULL, NULL, NULL, NULL, archiveLogPath, "", "", "", false);
          task->createDirectory();
        }
        else
          if (!pIFile->isDirectory())
            throw MakeStringException(-1, "The specified log/archive path '%s' is invalid!", archiveLogPath);
      }

      StringBuffer archiveFile;
      if (options.getArchiveEnv())
      {
        int n = 1;
        archiveFile.appendf("%s\\%s_%s.xml", archiveLogPath, envName.str(), timestamp);
        while (true)
        {
          Owned<IFile> pIFile = createIFile(archiveFile.str());
          archiveFile.clear().appendf("%s\\%s_%s_%d.xml", archiveLogPath, envName.str(), timestamp, n++);
          if (pIFile->exists())
            break;
        }

        m_pEnvDepEngine->archive(archiveFile);
      }

      // Create unique name for log file
      StringBuffer logFile;
      if (options.getLog())
      {
        int n = 1;
        logFile.appendf("%s\\%s_%s_log.xml", archiveLogPath, envName.str(), timestamp);
        while (true)
        {
          Owned<IFile> pIFile = createIFile(logFile.str());
          logFile.clear().appendf("%s\\%s_%s_log_%d.xml", archiveLogPath, envName.str(), timestamp, n++);
          if (pIFile->exists())
            break;
        }

        m_pEnvDepEngine->setLog(logFile.str(), archiveFile.str());
      }

      enum BackupMode backupMode;
      if (options.getBackupCopy())
        backupMode = DEBACKUP_COPY;
      else
        if (options.getBackupRename())
          backupMode = DEBACKUP_RENAME;
        else
          backupMode = DEBACKUP_NONE;

      m_pEnvDepEngine->deploy(deployMode, backupMode, options.getStop(), options.getStart());
      msg.append("Deployment");
    }

    // Check for errors
    if (getAbortStatus())
      printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Deployment aborted!");
    else
    {
      if (m_errorCount == 0)
        msg.append(" completed successfully");
      else
      {
        msg.append(" failed with ").append(m_errorCount).append(" error");
        if (m_errorCount > 1)
          msg.append('s');
      }
      printStatus(STATUS_NORMAL, NULL, NULL, NULL, "%s", msg.str());
    }
  }
  catch (IException* e)
  {
    m_deployStatus.clear();
    e->errorMessage(m_deployStatus);
    e->Release();
    m_errorCount++;
  }
  catch (...)
  {
    m_deployStatus.clear().append("Unknown exception!");
    m_errorCount++;
  }
}

void CWsDeployEngine::deploy()
{
  try
  {
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    factory->validateCache();   
    Owned<IConstEnvironment> constEnv(factory->openEnvironment());

    m_pEnvDepEngine.setown( createEnvDeploymentEngine(*constEnv, *this, m_pDeploy->queryPropTree("SelectedComponents")) );
    m_pEnvDepEngine->setInteractiveMode(false);

    bool rc = true;

    CDeployOptions& options = dynamic_cast<CDeployOptions&>(m_depInfo.getOptions());

    //if (m_deployEngine->isLinuxDeployment() && (options.getStop() || options.getStart()))
    //   rc = askForSshAccountInfo();

    // Get timestamp
    time_t t = time(NULL);
    struct tm* now = localtime(&t);
    char timestamp[30];
    strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", now);

    // Validate data
    printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Performing preliminary validation based on XML schema ...");
    m_pEnvDepEngine->check();

    // Delete all existing connections
    //doDisconnects();   

    unsigned deployMode = DEFLAGS_NONE;
    if (options.getConfigFiles())
      deployMode |= DEFLAGS_CONFIGFILES;

    if (options.getBuildFiles())
    {
      deployMode |= DEFLAGS_BUILDFILES;
      if (options.getUpgradeBuildFiles())
        deployMode |= DCFLAGS_TIME | DCFLAGS_SIZE;
    }

    StringBuffer msg;
    if (options.getCompare())
    {
      m_pEnvDepEngine->compare(deployMode);
      msg.append("Compare");
    }
    else
    {
      // Get archive path and environment name
      StringBuffer envName;
      if (envName.length()==0) 
        envName = "deploy"; // make sure we have some name

      // Create unique name for archive file based on timestamp
      const char* archiveLogPath = options.getArchivePath();
      if (options.getArchiveEnv() || options.getLog())
      {
        if (!archiveLogPath || !*archiveLogPath)
          throw MakeStringExceptionDirect(-1, "Cannot archive or log without a path!");

        Owned<IFile> pIFile = createIFile(archiveLogPath);
        if (!pIFile->exists())
        {
          Owned<IDeployTask> task = createDeployTask(*this, "Create Directory", NULL, NULL, NULL, NULL, archiveLogPath, "", "", "", false);
          task->createDirectory();
        }
        else
          if (!pIFile->isDirectory())
            throw MakeStringException(-1, "The specified log/archive path '%s' is invalid!", archiveLogPath);
      }

      StringBuffer archiveFile;
      if (options.getArchiveEnv())
      {
        int n = 1;
        archiveFile.appendf("%s\\%s_%s.xml", archiveLogPath, envName.str(), timestamp);
        while (true)
        {
          Owned<IFile> pIFile = createIFile(archiveFile.str());
          archiveFile.clear().appendf("%s\\%s_%s_%d.xml", archiveLogPath, envName.str(), timestamp, n++);
          if (pIFile->exists())
            break;
        }

        m_pEnvDepEngine->archive(archiveFile);
      }

      // Create unique name for log file
      StringBuffer logFile;
      if (options.getLog())
      {
        int n = 1;
        logFile.appendf("%s\\%s_%s_log.xml", archiveLogPath, envName.str(), timestamp);
        while (true)
        {
          Owned<IFile> pIFile = createIFile(logFile.str());
          logFile.clear().appendf("%s\\%s_%s_log_%d.xml", archiveLogPath, envName.str(), timestamp, n++);
          if (pIFile->exists())
            break;
        }

        m_pEnvDepEngine->setLog(logFile.str(), archiveFile.str());
      }

      enum BackupMode backupMode;
      if (options.getBackupCopy())
        backupMode = DEBACKUP_COPY;
      else
        if (options.getBackupRename())
          backupMode = DEBACKUP_RENAME;
        else
          backupMode = DEBACKUP_NONE;

      m_pEnvDepEngine->deploy(deployMode, backupMode, options.getStop(), options.getStart());
      msg.append("Deployment");
    }

    // Check for errors
    if (getAbortStatus())
      printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Deployment aborted!");
    else
    {
      if (m_errorCount == 0)
        msg.append(" completed successfully");
      else
      {
        msg.append(" failed with ").append(m_errorCount).append(" error");
        if (m_errorCount > 1)
          msg.append('s');
      }
      printStatus(STATUS_NORMAL, NULL, NULL, NULL, "%s", msg.str());
    }
  }
  catch (IException* e)
  {
    m_deployStatus.clear();
    e->errorMessage(m_deployStatus);
    e->Release();
    m_errorCount++;
  }
  catch (...)
  {
    m_deployStatus.clear().append("Unknown exception!");
    m_errorCount++;
  }
}


IPropertyTree* CWsDeployEngine::findTasksForComponent(const char* comp, const char* inst) const
{
  string key;
  key += comp;

  if (inst)
  {
    key += ':';
    key += inst;
  }

  if (m_comp2TasksMap.size() <= 0)
    return NULL;

  CStringToIptMap::const_iterator it = m_comp2TasksMap.find( key.c_str() );
  if (it == m_comp2TasksMap.end())
  {
    //not all tasks are component related
    if (!inst)
      return NULL;
    else
      throw MakeStringException(-1, "Internal error in cache management!");
  }

  return (*it).second;
}

bool CWsDeployEngine::onDisconnect(const char* target)
{
  return true;
}

void CWsDeployEngine::getSshAccountInfo(StringBuffer& userid, StringBuffer& password) const
{
}

void CWsDeployEngine::printStatus(IDeployTask* task)
{
  IPropertyTree* pTasks = findTasksForComponent(task->getCompName(), task->getInstanceName());

  if (!pTasks)
    return;

  StringBuffer xpath;
  xpath.appendf("Task[@id='%" I64F "d']", (__int64)task);
  IPropertyTree* pTask = pTasks->queryPropTree(xpath.str());

  if (!pTask)
  {
    pTask = pTasks->addPropTree("Task", createPTree());
    pTask->setPropInt64("@id", (__int64)task);

    const char* sourceFile = task->getFileName(DT_SOURCE);
    const char* targetFile = task->getFileName(DT_TARGET);

    if (strcmp(sourceFile, targetFile)==0 || strlen(sourceFile)==0)
      pTask->setProp( "FileName",   targetFile );
    else if (targetFile && strlen(targetFile)==0)
      pTask->setProp( "FileName",   sourceFile );
    else
    {
      StringBuffer fileName;
      fileName.append(sourceFile).append(" -> ").append(targetFile);
      pTask->setProp( "FileName",   fileName.str() );
    }

    pTask->setProp( "Caption",  task->getCaption() );
    pTask->setProp( "TargetPath",task->getFileSpec(DT_TARGET) );
    pTask->setProp( "SourcePath",task->getFileSpec(DT_SOURCE) );
    pTask->setProp( "Message",  task->getErrorString() );

    StatusType status;
    if (!task->isProcessed())
      status = STATUS_INCOMPLETE;
    else
      if (task->getErrorCode())
        status = STATUS_ERROR;
      else
        if (task->getWarnings())
          status = STATUS_WARN;
        else
          status = STATUS_NORMAL;

    pTask->setPropInt("Status", status);
  }
}

void CWsDeployEngine::printStatus(StatusType type, const char* processType, const char* process, 
                                  const char* instance, const char* format/*=NULL*/, ...)
{
  char buf[1024];
  if (format)
  {
    va_list args;
    va_start(args, format);
    if (_vsnprintf(buf, sizeof(buf), format, args) < 0)
      buf[sizeof(buf) - 1] = '\0';
    va_end(args);
  }
  else
    *buf = '\0';

  if (processType && process && instance)
  {
    IPropertyTree* pTasks = findTasksForComponent(process, instance);

    IPropertyTree* pTask = pTasks->addPropTree("Task", createPTree());
    //pTask->setProp( "Caption",    buf );
    pTask->setProp( "Message",  buf );
    pTask->setPropInt("Status",type);
  }
  else
    m_deployStatus.append( buf );
}


//the following throws exception on abort, returns true for ignore
bool CWsDeployEngine::processException(const char* processType, const char* process, const char* instance, 
                                       IException* e, const char* szMessage/*=NULL*/, const char* szCaption/*=NULL*/,
                                       IDeployTask* pTask /*=NULL*/ )
{
  if (getAbortStatus() || (pTask && pTask->getAbort()))
    throw MakeStringException(0, "User abort");

  StringBuffer msg;

  if (e)
  {
    e->errorMessage(msg);
    e->Release();
  }
  else
    if (szMessage && *szMessage)
      msg.append( szMessage );
    else
      msg.append("Unknown exception occurred!");

  int rc = true;//ignore
  if (msg.length() > 0)//the exception message hasn't already been shown?
  {
    //printStatus(STATUS_ERROR, processType, process, instance, szMessage);
    if (szCaption && *szCaption)
      DBGLOG("%s: %s", szCaption, msg.str());
    else
      DBGLOG("%s", msg.str());

  }

  m_errorCount++;

  return true;
}

