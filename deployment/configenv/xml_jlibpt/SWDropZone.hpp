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
#ifndef _SWDROPZONE_HPP_
#define _SWDROPZONE_HPP_

#include "EnvHelper.hpp"
#include "SWProcess.hpp"

namespace ech
{

class SWDropZone : public SWProcess
{
public:
    SWDropZone(const char* name, EnvHelper * envHelper);
    void checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent);
    virtual IPropertyTree * findInstance(IPropertyTree *comp, IPropertyTree* computerNode);
    virtual void addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName);

//protected:

};

}
#endif
