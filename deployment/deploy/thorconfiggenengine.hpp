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
#ifndef THORCONFIGGENENGINE_HPP_INCL
#define THORCONFIGGENENGINE_HPP_INCL

#include "configgenengine.hpp"

//---------------------------------------------------------------------------
// CThorDeploymentEngine
//---------------------------------------------------------------------------
class CThorConfigGenEngine : public CConfigGenEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CThorConfigGenEngine(IEnvDeploymentEngine& envDepEngine,
                         IDeploymentCallback& callback,
                         IPropertyTree& process, 
                         const char* inputDir="", 
                         const char* outputDir="");

protected:
   void check();
};
//---------------------------------------------------------------------------
#endif // THORCONFIGGENENGINE_HPP_INCL
