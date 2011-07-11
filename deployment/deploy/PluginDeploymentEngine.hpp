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
