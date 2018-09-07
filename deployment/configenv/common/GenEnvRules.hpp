/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#ifndef _GENENVRULES_HPP_
#define _GENENVRULES_HPP_

//#include "jiface.hpp"
//#include "jliball.hpp"
//#include "XMLTags.h"

namespace ech
{

//Reference WizardInputs::setWizardRules
//Reference WizardInputs::setTopologyParam

class GenEnvRules
{
public:
   GenEnvRules(const char* filename) { loadFile(filename); }
   void loadFile(const char* filename);
   int getPropInt(const char* propname, int dft=0) const;
   const char* getProp(const char* propname) const;
   bool foundInProp(const char* propname, const char* item, const char* sep=",") const;
   bool doNotGenTheCompOptional(const char* compname) const;
   bool isValidServerCombo(const char* server1, const char* server2) const;
   const char* getRoxieAgentRedType() const { return roxieAgentRedType; }
   unsigned getRoxieAgentRedChannels() const { return roxieAgentRedChannels; }
   unsigned getRoxieAgentRedOffset() const { return roxieAgentRedOffset; };

private:
   Owned<IProperties> rules;
   unsigned roxieAgentRedChannels;
   unsigned roxieAgentRedOffset;
   StringBuffer roxieAgentRedType;
   bool roxieOnDemand;
};

}
#endif
