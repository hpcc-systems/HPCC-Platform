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
#include "jptree.hpp"
#include "dalideploymentengine.hpp"

//---------------------------------------------------------------------------
// CDaliDeploymentEngine
//---------------------------------------------------------------------------
CDaliDeploymentEngine::CDaliDeploymentEngine(IEnvDeploymentEngine& envDepEngine,
                                             IDeploymentCallback& callback, 
                                             IPropertyTree& process)
 : CDeploymentEngine(envDepEngine, callback, process, "Instance")
{
}

//---------------------------------------------------------------------------
// startInstance
//---------------------------------------------------------------------------
void CDaliDeploymentEngine::startInstance(IPropertyTree& instanceNode, const char* fileName/*="startup"*/)
{
   while (!m_pCallback->processException( m_process.queryName(), m_name, m_curInstance, NULL, 
                                          "You must start a Dali server manually!", 
                                          "Start Process"));//while retry
}

//---------------------------------------------------------------------------
// stopInstance
//---------------------------------------------------------------------------
void CDaliDeploymentEngine::stopInstance(IPropertyTree& instanceNode, const char* fileName/*="stop"*/)
{
   while (!m_pCallback->processException( m_process.queryName(), m_name, m_curInstance, 
                                          NULL, "You must stop a Dali server manually!", 
                                          "Stop Process"));//while retry
}

//---------------------------------------------------------------------------
// deploy
//---------------------------------------------------------------------------
void CDaliDeploymentEngine::_deploy(bool useTempDir)
{
   // Since we may be accessing the dali server that we're trying to deploy to,
   // we need to copy the files to a temporary directory, then use a start_dali
   // batch file to copy the files from the temporary directory to the real
   // directory defore starting the dali process.

   // Do deploy
   CDeploymentEngine::_deploy(false);
}

//---------------------------------------------------------------------------
// copyInstallFiles
//---------------------------------------------------------------------------
void CDaliDeploymentEngine::copyInstallFiles(IPropertyTree& instanceNode, const char* destPath)
{
    const char* computer = instanceNode.queryProp("@computer");
   if ((m_deployFlags & DEFLAGS_CONFIGFILES) && computer && *computer)
   {
      // Create dalisds.xml is not already exists
      StringBuffer hostRoot(getHostRoot(computer, NULL));

      const char* dir = m_process.queryProp("@dataPath");
      if (!dir || !*dir)
         dir = instanceNode.queryProp("@directory");

      if (dir)
      {
         if (isPathSepChar(*dir))
            dir++;
         StringBuffer sDir( dir );
         sDir.replace(':', '$');
         sDir.replace('/', '\\');
         sDir.insert(0, hostRoot.str());

         if (!checkFileExists( sDir.str() ))
         {
            Owned<IPropertyTree> tree = &m_environment.getPTree();
            StringBuffer xml("<SDS>");
            toXML(tree, xml);
            xml.append("</SDS>");

            sDir.append( "\\dalisds.xml" );
            writeFile(sDir.str(), xml.str());
         }
      }
   }
   // Copy install files to deploy subdir
   CDeploymentEngine::copyInstallFiles(instanceNode, destPath);
}

//---------------------------------------------------------------------------
//  getInstallPath
//---------------------------------------------------------------------------
bool CDaliDeploymentEngine::getInstallPath(const char *filename, StringBuffer& installPath)
{
    //BUG: 9254 - Deploy Wizard's "compare" for Dali looks in wrong folder
    //If filename is in deploy folder then compare file in folder above it.
    //
    StringBuffer machine;
    StringBuffer path;
    StringBuffer tail;
    StringBuffer ext;
    bool             rc;

    splitUNCFilename(filename, &machine, &path, &tail, &ext);
    const char* pszPath = path.str();
    const char* pattern = "deploy\\";
    const unsigned int patternLen = sizeof("deploy\\") - 1;
    const char* match = strstr(pszPath, pattern);

    if (match && strlen(match)==patternLen)//path ends with "deploy\\"
    {
        path.remove(match-pszPath, patternLen);
        installPath.append(machine).append(path).append(tail).append(ext);
        rc = true;
    }
    else
    {
        installPath.append(filename);
        rc = false;
    }
    return rc;
}

//---------------------------------------------------------------------------
//  setCompare
//---------------------------------------------------------------------------
const char* CDaliDeploymentEngine::setCompare(const char *filename)
{
    //BUG: 9254 - Deploy Wizard's "compare" for Dali looks in wrong folder
    //If filename is in deploy folder then compare file in folder above it.
    //
    StringBuffer installPath;
    getInstallPath(filename, installPath);

    return CDeploymentEngine::setCompare(installPath.str());
}

//---------------------------------------------------------------------------
//  compareFiles
//---------------------------------------------------------------------------
void CDaliDeploymentEngine::compareFiles(const char *newFile, const char *oldFile)
{
    //BUG: 9254 - Deploy Wizard's "compare" for Dali looks in wrong folder
    //If filename is in deploy folder then compare file in folder above it.
    //
    StringBuffer installPath;
    getInstallPath(oldFile, installPath);

   Owned<IDeployTask> task = createDeployTask(*m_pCallback, "Compare File", m_process.queryName(), m_name.get(),
     m_curInstance, newFile, installPath.str(), m_curSSHUser.str(), m_curSSHKeyFile.str(),
     m_curSSHKeyPassphrase.str(), m_useSSHIfDefined);
   m_pCallback->printStatus(task);
   task->compareFile(DTC_CRC | DTC_SIZE);
   m_pCallback->printStatus(task);
   checkAbort(task);
}
