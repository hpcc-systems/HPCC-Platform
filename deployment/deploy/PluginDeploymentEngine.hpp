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
#ifndef PLUGINDEPLOYMENTENGINE_HPP_INCL
#define PLUGINDEPLOYMENTENGINE_HPP_INCL

#pragma warning(disable : 4786)
#include "deploymentengine.hpp"

//---------------------------------------------------------------------------
//  CPluginDeploymentEngine
//---------------------------------------------------------------------------
class CPluginDeploymentEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
   CPluginDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
                           IConstEnvironment& environment, 
                           IPropertyTree& process, 
                           const char* instanceType=NULL);

protected:
   virtual int  createInstallFileMap(IPropertyTree& node, const char* destPath, 
                                     mmapStr2PairStrStr& fileMap) const;

   void getPluginDirectory(const char* destPath, StringBuffer& sPluginDest) const;
   void getDefaultPlugins(StringArray& plugins, StringBuffer& sPluginsPath, 
                          const char* destDir) const;
   void getPlugins(StringArray& plugins, StringBuffer& sPluginsPath, 
                   const char* pluginDest) const;
};
//---------------------------------------------------------------------------
#endif // PLUGINDEPLOYMENTENGINE_HPP_INCL
