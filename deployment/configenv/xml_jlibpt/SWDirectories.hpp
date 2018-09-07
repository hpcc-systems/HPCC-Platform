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
#ifndef _SWDIRECTORIES_HPP_
#define _SWDIRECTORIES_HPP_

#include "SWComponentBase.hpp"
#include "EnvHelper.hpp"

namespace ech
{

class SWDirectories : public ComponentBase
{
public:
   SWDirectories(const char* name, EnvHelper * envHelper);

   IMPLEMENT_IINTERFACE;

   virtual void create(IPropertyTree *params);
   virtual unsigned add(IPropertyTree *params);
   virtual void modify(IPropertyTree *params);
   virtual void remove(IPropertyTree *params);

   void setCategoryAttributes(IPropertyTree *envTree, const char* dirName, const char* dirPath);

};

}

#endif
