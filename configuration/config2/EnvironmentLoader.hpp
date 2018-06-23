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


#ifndef _CONFIG2_ENVIRONMENTLOADER_HPP_
#define _CONFIG2_ENVIRONMENTLOADER_HPP_

#include <string>
#include <fstream>
#include <vector>
#include "SchemaItem.hpp"
#include "EnvironmentNode.hpp"
#include "Status.hpp"
#include "NameValue.hpp"
#include "platform.h"


class DECL_EXPORT EnvironmentLoader
{
    public:

        EnvironmentLoader() { }
        virtual ~EnvironmentLoader() { }
        virtual std::vector<std::shared_ptr<EnvironmentNode>> load(std::istream &in, const std::shared_ptr<SchemaItem> &pSchemaItem) const = 0;


    protected:

        std::shared_ptr<SchemaItem> m_pSchemaItem;
};

#endif
