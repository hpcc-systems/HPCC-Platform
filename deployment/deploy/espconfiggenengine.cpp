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
#include "jptree.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "environment.hpp"
#include "xslprocessor.hpp"
#include "espconfiggenengine.hpp"
#include <vector>
#include <set>

using std::set;
using std::string;

//---------------------------------------------------------------------------
// CEspConfigGenEngine
//---------------------------------------------------------------------------
CEspConfigGenEngine::CEspConfigGenEngine(IEnvDeploymentEngine& envDepEngine,
                                           IDeploymentCallback& callback, 
                                           IPropertyTree& process, const char* inputDir, 
                                           const char* outputDir)
 : CConfigGenEngine(envDepEngine, callback, process, inputDir, outputDir, "./Instance")
{
}

//---------------------------------------------------------------------------
// check
//---------------------------------------------------------------------------
void CEspConfigGenEngine::check()
{
   CConfigGenEngine::check();

   // Make sure all protocol and service referenced by bindings are valid
   Owned<IPropertyTreeIterator> iter = m_process.getElements("EspBinding");
   for (iter->first(); iter->isValid(); iter->next())
   {
      IPropertyTree* pBinding = &iter->query();
      const char* service = pBinding->queryProp("@service");

      if (service)
      {
         if (!lookupProcess("EspService", service))
            throw MakeStringException(0, "Process %s references unknown service %s", m_name.get(), service);
      }
      else
         throw MakeStringException(-1, "The ESP binding %s for ESP %s has missing service information!", 
                                   pBinding->queryProp("@name"), m_name.get());
   }

   // Make sure DaliServers are valid
   iter.setown(m_process.getElements(".//*[@daliServers]"));
   ForEach(*iter)
   {
      const char* name = iter->query().queryProp("@daliServers");
      if (name && *name && !lookupProcess("DaliServerProcess", name))
            throw MakeStringException(0, "Process %s references unknown DaliServers %s", m_name.get(), name);
   }

   // Make sure EclServer is valid
   iter.setown(m_process.getElements(".//*[@eclServer]"));
   ForEach(*iter)
   {
      const char* name = iter->query().queryProp("@eclServer");
      if (name && *name && !lookupProcess("EclServerProcess", name))
         throw MakeStringException(0, "Process %s references unknown EclServer %s", m_name.get(), name);
   }

   // Make sure AttributeServer is valid
   iter.setown(m_process.getElements(".//*[@attributeServer]"));
   ForEach(*iter)
   {
      const char* name = iter->query().queryProp("@attributeServer");
      if (name && *name && !lookupProcess("AttrServerProcess", name))
         throw MakeStringException(0, "Process %s references unknown AttributeServer %s", m_name.get(), name);
   }
}

//---------------------------------------------------------------------------
// createInstallFileMap
//---------------------------------------------------------------------------
int CEspConfigGenEngine::determineInstallFiles(IPropertyTree& node, CInstallFiles& installFiles) const
{
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "Merging file lists for ESP server and its bound service(s) ...");

   const char* myBuild = m_process.queryProp("@build");
   //process bindings for this esp process and add files for each service used by 
   //each binding before adding files for this esp process
   Owned<IPropertyTreeIterator> iBinding = m_process.getElements("EspBinding");
   ForEach(*iBinding)
   {
      IPropertyTree* pBinding = &iBinding->query();

      const char* szService = pBinding->queryProp("@service");

      // Lookup plugin process
      IPropertyTree* pService = lookupProcess("EspService", szService);
      if (!pService)
         throw MakeStringException(0, "Process %s references unknown esp service '%s'", m_name.get(), szService);

      const char* pszBuild = pService->queryProp("@build");
      if (!pszBuild || 0 != strcmp(pszBuild, myBuild))
         throw MakeStringException(0, "ESP service '%s' used by ESP process '%s'\n has a different build (%s) to its ESP process!", 
                                   szService, m_name.get(), pszBuild);

         // Get plugin file list from the plugin process
      CConfigGenEngine::determineInstallFiles(*pService, installFiles);
   }
    int rc = CConfigGenEngine::determineInstallFiles(node, installFiles);
    m_pCallback->printStatus(STATUS_NORMAL, NULL, NULL, NULL, "determineInstallFiles complete");
   return rc;
}

//---------------------------------------------------------------------------
//  processCustomMethod
//---------------------------------------------------------------------------
void CEspConfigGenEngine::processCustomMethod(const char *method, const char *source, const char *outputFile, 
                                               const char *instanceName, EnvMachineOS os)
{
   if (!stricmp(method, "ssl_certificate"))
   {
       //we only need to worry about SSL certificates and private keys if https is being used
       //by any of our bindings
       //
       Owned<IPropertyTreeIterator> it = m_process.getElements("EspBinding[@protocol='https']");
       if (!it->first())
          return;

   }

   CConfigGenEngine::processCustomMethod(method, source, outputFile, instanceName, os);
}


//---------------------------------------------------------------------------
// xslTransform
//---------------------------------------------------------------------------
void CEspConfigGenEngine::xslTransform(
        const char *xslFilePath,  const char *outputFilePath, 
        const char* instanceName, EnvMachineOS os/*=MachineOsUnknown*/,
        const char* processName/*=NULL*/,
        bool isEspModuleOrPlugin/*=false*/)
{
   m_createIni = false;
   
   // Skip if not processing config files

   checkAbort();
   if (!m_compare)
      ensurePath(outputFilePath);

   if (m_compare) 
      outputFilePath = setCompare(outputFilePath);

   if (instanceName)
   {
       m_transform->setParameter("instance", StringBuffer("'").append(instanceName).append("'").str());

      const char* szXslFileName = pathTail(xslFilePath);

      if (!stricmp(szXslFileName, "esp.xsl"))
      {
         //disable compare since these transforms are needed by us in any case for esp.xml
         const bool bCompare = m_compare;
         m_compare = false;

         //we need to pass in a list of file names created as a result of individual esp service modules
         //running xslt using method xslt_esp_service on their own service specific XSL files as specified
         //in install set (created by release_<service>.bat
         //
         //esp.xsl merges these in the output under /Environment/Software/EspProcess
         //
         StringBuffer serviceXsltOutputFiles;

         processServiceModules("esp_plugin",         serviceXsltOutputFiles, instanceName, os);
         processServiceModules("esp_service_module", serviceXsltOutputFiles, instanceName, os);

         m_compare = bCompare;

            IEnvDeploymentEngine& envDepEngine = getEnvDepEngine();
            const char* srcDaliAddress = envDepEngine.getSourceDaliAddress();

          m_transform->setParameter("espServiceName", "''");
          m_transform->setParameter("serviceFilesList", StringBuffer("'").append(serviceXsltOutputFiles.str()).append("'").str());         
          m_transform->setParameter("deployedFromDali", StringBuffer("'").append(srcDaliAddress).append("'").str());         
      }
   }

   if (m_deployFlags & DEFLAGS_CONFIGFILES) //are we processing config files?
   {
      Owned<IDeployTask> task = 
         createDeployTask(*m_pCallback, "XSL Transform", m_process.queryName(), m_name.get(), 
                           instanceName, xslFilePath, outputFilePath, m_curSSHUser.sget(), m_curSSHKeyFile.sget(), m_curSSHKeyPassphrase.sget(), m_useSSHIfDefined, os, processName);
      m_pCallback->printStatus(task);
      task->transformFile(*m_processor, *m_transform, m_cachePath.get());
      m_pCallback->printStatus(task);
      checkAbort(task);
   }

   if (m_compare) 
      compareFiles(os);
}


void CEspConfigGenEngine::processServiceModules(const char* moduleType, 
                                                 StringBuffer& serviceXsltOutputFiles,
                                                 const char* instanceName, 
                                                 EnvMachineOS os)
{
   set<string> serviceNamesProcessed;
   bool        bEspServiceModule = !stricmp(moduleType, "esp_service_module");

    char tempPath[_MAX_PATH];
    getTempPath(tempPath, sizeof(tempPath), m_name);

   const CInstallFileList& fileList = m_installFiles.getInstallFileList();
   CInstallFileList::const_iterator i    = fileList.begin();
   CInstallFileList::const_iterator iEnd = fileList.end();

   for (; i != iEnd; i++)
   {
      const CInstallFile& installFile = *(*i);
      const char* method = installFile.getMethod().c_str();

      if (!stricmp(method, moduleType))
      {
         const char* source = installFile.getSrcPath().c_str();
            std::string dest   = installFile.getDestPath().c_str();

         //our base class method CConfigGenEngine::determineInstallFiles() encoded 
         //dest is of the format <destpath>+<service name> so extract both of the parts
         //
            std::string::size_type pos = dest.find_last_of('+');
            std::string serviceName = dest.substr(pos+1);
            dest.erase(pos);

         if ((pos = dest.find("atmark_temp" PATHSEPSTR)) != std::string::npos)
            dest.replace(pos, strlen("atmark_temp" PATHSEPSTR), tempPath);

         //if method is esp_service_module then check to see if not processing config files
         //
         bool bProcess = true;
         if (bEspServiceModule)
         {
            if (m_deployFlags & DEFLAGS_CONFIGFILES)
            {
               serviceNamesProcessed.insert(serviceName);

               serviceXsltOutputFiles.append(dest.c_str());
               serviceXsltOutputFiles.append(';');
            }
            else
               bProcess = false;
         }

         if (bProcess)
         {
                serviceName.insert(0, "\'");
                serviceName += '\'';
             m_transform->setParameter("espServiceName", serviceName.c_str());

                CConfigGenEngine::xslTransform(source, dest.c_str(), instanceName, os, NULL, true);
         }

      }
   }
}
