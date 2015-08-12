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
