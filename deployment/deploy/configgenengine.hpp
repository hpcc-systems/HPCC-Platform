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
#ifndef CONFIGGENENGINE_HPP_INCL
#define CONFIGGENENGINE_HPP_INCL

#include "DeploymentEngine.hpp"

//---------------------------------------------------------------------------
//  CConfigGenEngine
//---------------------------------------------------------------------------
class CConfigGenEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CConfigGenEngine(IEnvDeploymentEngine& envDepEngine, 
                     IDeploymentCallback& callback, IPropertyTree& process, 
                     const char* inputDir="", const char* outputDir="", 
                     const char* instanceType=NULL, bool createIni=false);

protected:
    virtual int  determineInstallFiles(IPropertyTree& node, CInstallFiles& installFiles) const;
  virtual void deployInstance(IPropertyTree& node, bool useTempDir);
  virtual void beforeDeploy();
  void createFakePlugins(StringBuffer& destFilePath) const;

private:
  StringBuffer m_inDir;
  StringBuffer m_outDir;
};
//---------------------------------------------------------------------------
#endif // CONFIGGENENGINE_HPP_INCL

