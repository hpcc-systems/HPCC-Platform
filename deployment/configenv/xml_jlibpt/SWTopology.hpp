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
#ifndef _SWTOPOLOGY_HPP_
#define _SWTOPOLOGY_HPP_

#include "EnvHelper.hpp"
#include "SWComponentBase.hpp"

namespace ech
{

class SWTopology : public SWComponentBase
{
public:
   SWTopology(EnvHelper * envHelper);

   virtual void create(IPropertyTree *params);
   virtual unsigned add(IPropertyTree *params);

   virtual void processNameChanged(const char* process,  const char* newName, const char* oldName);
   void checkTopologyCluster(IPropertyTree *cluster);
   void checkTopology(IPropertyTree *topology);

private:

};

}
#endif
