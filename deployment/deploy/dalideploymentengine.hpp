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
#ifndef DALIDEPLOYMENTENGINE_HPP_INCL
#define DALIDEPLOYMENTENGINE_HPP_INCL

#include "DeploymentEngine.hpp"

//---------------------------------------------------------------------------
//  CDaliDeploymentEngine
//---------------------------------------------------------------------------
class CDaliDeploymentEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
   CDaliDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
                         IDeploymentCallback& callback, 
                         IPropertyTree& process);

protected:
   virtual void startInstance(IPropertyTree& node, const char* fileName="startup");
   virtual void stopInstance (IPropertyTree& node, const char* fileName="stop"   );

    void _deploy(bool useTempDir);   
   void copyInstallFiles(IPropertyTree& instanceNode, const char* destPath);
    virtual const char* setCompare(const char *filename);
    virtual void compareFiles(const char *newFile, const char *oldFile);

private:
    virtual bool getInstallPath(const char *filename, StringBuffer& installPath);
//   void createDaliConf();

private:
   StringBuffer m_daliConf;
};
//---------------------------------------------------------------------------
#endif // DALIDEPLOYMENTENGINE_HPP_INCL
