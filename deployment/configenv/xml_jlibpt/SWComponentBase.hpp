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
#ifndef _SWCOMPONENTBASE_HPP_
#define _SWCOMPONENTBASE_HPP_

#include "EnvHelper.hpp"
#include "ComponentBase.hpp"

namespace ech
{

class SWComponentBase : public ComponentBase
{
public:
   SWComponentBase(const char* name, EnvHelper * envHelper);
   virtual ~SWComponentBase();

   virtual void create(IPropertyTree *params);
   virtual unsigned add(IPropertyTree *params);
   virtual void modify(IPropertyTree *params);
   //virtual void remove(IPropertyTree *params);
   virtual IPropertyTree * addComponent(IPropertyTree *params);

protected:
   Owned<IPropertyTree> m_pSchema;
   IPropertyTree*  m_pBuildSet;
   StringArray m_notifyList;

   StringBuffer m_buildSetName;
   StringBuffer m_buildName;
   StringBuffer m_xsdFileName;
   StringBuffer m_processName;

};

}
#endif
