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
#ifndef ESPDEPLOYMENTENGINE_HPP_INCL
#define ESPDEPLOYMENTENGINE_HPP_INCL

#include "DeploymentEngine.hpp"

//---------------------------------------------------------------------------
//  CEspDeploymentEngine
//---------------------------------------------------------------------------
class CEspDeploymentEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CEspDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
        IDeploymentCallback& callback,
        IPropertyTree& process);
    
    virtual void xslTransform(const char *xsl, const char *outputFile, const char *instanceName,
        EnvMachineOS os=MachineOsUnknown, const char* processName=NULL, bool isEspModuleOrPlugin=false);
    
    virtual void processCustomMethod(const char *method, const char *source, const char *outputFile, 
        const char *instanceName, EnvMachineOS os);
    
protected:
    void check();
    int  determineInstallFiles(IPropertyTree& node, CInstallFiles& installFiles) const;
    void processServiceModules(const char* moduleType, StringBuffer& serviceXsltOutputFiles,
        const char* instanceName, EnvMachineOS os);
private:
};
//---------------------------------------------------------------------------
#endif // ESPDEPLOYMENTENGINE_HPP_INCL
