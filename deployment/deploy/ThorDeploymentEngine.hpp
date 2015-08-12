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
#ifndef THORDEPLOYMENTENGINE_HPP_INCL
#define THORDEPLOYMENTENGINE_HPP_INCL

#include "DeploymentEngine.hpp"

//---------------------------------------------------------------------------
// CThorDeploymentEngine
//---------------------------------------------------------------------------
class CThorDeploymentEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CThorDeploymentEngine(IEnvDeploymentEngine& envDepEngine,
                         IDeploymentCallback& callback,
                         IPropertyTree& process);

protected:
   void check();
};
//---------------------------------------------------------------------------
#endif // THORDEPLOYMENTENGINE_HPP_INCL
