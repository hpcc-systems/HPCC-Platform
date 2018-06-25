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
#ifndef _ENVSETTINGS_HPP_
#define _ENVSETTINGS_HPP_

#include "EnvHelper.hpp"
#include "ComponentBase.hpp"

namespace ech
{

class EnvSettings : public ComponentBase
{
public:
   EnvSettings(EnvHelper * envHelper);


   //IMPLEMENT_IINTERFACE;

   virtual void create(IPropertyTree *params);
   virtual unsigned add(IPropertyTree *params);
   //virtual int addNode(IPropertyTree *node, const char* xpath, bool merge) { return; }
   virtual void modify(IPropertyTree *params);
   virtual void remove(IPropertyTree *params);

private:
   StringArray m_notifyList;

};

}

#endif
