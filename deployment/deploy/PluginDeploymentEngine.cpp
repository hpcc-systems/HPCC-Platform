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
#include "PluginDeploymentEngine.hpp"
#include <Shlwapi.h>    // for path functions

//#####################################################################################################
//#  NOTE: Plugins can no longer be shared and deployed as a component.  To avoid version conflicts,  #
//#        any component using a plugin (currently only ecl/attr servers) must deploy any necessary   #
//#        plugins as part of its deployment.                                                         # 
//#        This (pre-existing) class merely serves as a base class for components that need plugins.  #
//#####################################################################################################

//---------------------------------------------------------------------------
// CPluginDeploymentEngine
//---------------------------------------------------------------------------
CPluginDeploymentEngine::CPluginDeploymentEngine(IEnvDeploymentEngine& envDepEngine,
                                                 IConstEnvironment& environment, 
                                                 IPropertyTree& process,
                                                 const char* instanceType/*=NULL*/)
 : CDeploymentEngine(envDepEngine, environment, process, instanceType)
{
}


//---------------------------------------------------------------------------
// getPluginDirectory
//
// returns absolute path where plugins are to be deployed
//---------------------------------------------------------------------------
void CPluginDeploymentEngine::getPluginDirectory(const char* destPath, StringBuffer& sPluginDest) const
{
   sPluginDest.clear().append(destPath); 
   sPluginDest.replace('\\', '/');

   StringBuffer sPluginsDir; //relative path (from ECL server installation directory) for plugins
   m_process.getProp("@pluginsPath", sPluginsDir);

   if (sPluginsDir.length())
   {
      sPluginsDir.replace('\\', '/');
      sPluginsDir.replace('$', ':');

      if (! ::PathIsRelative(sPluginsDir.str()))
         throw MakeStringExceptionDirect(-1, "Plugins path for ECL server must be relative to its installation directory!");

      if (!strncmp(sPluginsDir.str(), "./", 2))
         sPluginsDir.remove(0, 2);

      sPluginDest.append(sPluginsDir);
   }

   const char* pchLast = sPluginDest.str() + sPluginDest.length() - 1;
   if (*pchLast != '/')
      sPluginDest.append('/');
}

//---------------------------------------------------------------------------
// getDefaultPlugins
//---------------------------------------------------------------------------
void CPluginDeploymentEngine::getDefaultPlugins(StringArray& plugins, 
                                                   StringBuffer& sPluginsPath, 
                                                   const char* destDir) const
{
    mmapStr2PairStrStr fileMap;
    CDeploymentEngine::createInstallFileMap(m_process, ".", fileMap);
    
    // Get install file list and sort out plugins
    mmapStr2PairStrStr::const_iterator i;
    mmapStr2PairStrStr::const_iterator iEnd = fileMap.end();
    
    for (i=fileMap.begin(); i != iEnd; i++)
    {
        const char* dest = (*i).first.c_str();
        const char* ext = ::PathFindExtension(dest);
        if (ext && *ext)
        {
            if (stricmp(ext, ".ecl")==0 || stricmp(ext, ".eclmod")==0)
            {
                if (plugins.find(dest) == NotFound)
            {
                    plugins.append(dest);
               sPluginsPath.append(dest).append(';');
            }
            }
         else
               // Check that file matches plugin path .\plugins or ./plugins as specified in the release_eclserver file
               if (strlen(dest) > 2 && *dest == '.' && (*(dest+1)=='\\' || *(dest+1)=='/') && 
                strnicmp(dest+2, "plugins", sizeof("plugins")-1) == 0)
               {
                   // Append plugin to files list - only care about dlls
                   if (stricmp(ext, ".dll")==0 || stricmp(ext, ".so")==0)
                   {
                  StringBuffer path(destDir);
                  path.append(::PathFindFileName( dest ));

                       if (plugins.find(path.str()) == NotFound)
                  {
                           plugins.append(path.str());
                     sPluginsPath.append(path.str()).append(';');
                  }
                   }
               }
        }
    }
}

//---------------------------------------------------------------------------
// getPlugins
//---------------------------------------------------------------------------
void CPluginDeploymentEngine::getPlugins(StringArray& plugins,
                                            StringBuffer& sPluginsPath, 
                                            const char* pluginDest) const
{
   // Iterate through plugin references
   Owned<IPropertyTreeIterator> iter = m_process.getElements("PluginRef");
   ForEach(*iter)
   {
      // Lookup plugin process
      const char* pluginName = iter->query().queryProp("@process");
      IPropertyTree* pluginProcess = lookupProcess("PluginProcess", pluginName);
      if (!pluginProcess)
         throw MakeStringException(0, "Process %s references unknown plugin %s", m_name, pluginName);

      // Get plugin file list from the plugin process
      mmapStr2PairStrStr fileMap;
      CDeploymentEngine::createInstallFileMap(*pluginProcess, pluginDest, fileMap);

      mmapStr2PairStrStr::const_iterator i;
      mmapStr2PairStrStr::const_iterator iEnd = fileMap.end();

      for (i=fileMap.begin(); i != iEnd; i++)
      {
         const char* dest = (*i).first.c_str();
         const char* ext = ::PathFindExtension(dest);

         // Append plugin to files list - only care about dll/so
         if (ext && (stricmp(ext, ".dll")==0 || stricmp(ext, ".so")==0) && 
             plugins.find(dest) == NotFound)
         {
            plugins.append(dest);
            sPluginsPath.append(dest).append(';');
         }
       }
   }
}

//---------------------------------------------------------------------------
// createInstallFileMap
//---------------------------------------------------------------------------
int CPluginDeploymentEngine::createInstallFileMap(IPropertyTree& node, 
                                                     const char* destPath, 
                                                     mmapStr2PairStrStr& fileMap) const
{
   CDeploymentEngine::createInstallFileMap(node, destPath, fileMap);

   //get files for plugin references...
   //
   StringBuffer sPluginDest;
   getPluginDirectory(destPath, sPluginDest);//get absolute path where plugins are to be deployed

   //process plugin references for this eclserver process and add files for each service used by 
   //each binding before adding files for this esp process
   Owned<IPropertyTreeIterator> iter = m_process.getElements("PluginRef");
   ForEach(*iter)
   {
      // Lookup plugin process
      const char* pluginName = iter->query().queryProp("@process");
      IPropertyTree* pluginProcess = lookupProcess("PluginProcess", pluginName);
      if (!pluginProcess)
         throw MakeStringException(0, "Process %s references unknown plugin %s", m_name, pluginName);

      // Get plugin file list from the plugin process
      CDeploymentEngine::createInstallFileMap(*pluginProcess, sPluginDest.str(), fileMap);
   }
   return fileMap.size();
}

