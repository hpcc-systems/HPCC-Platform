/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _CONFIG_FILE_COMPONENT_UTILS_HPP_
#define _CONFIG_FILE_COMPONENT_UTILS_HPP_


#include "jiface.hpp"
#include "jutil.hpp"
#include "jstring.hpp"

//namespace CONFIGURATOR
//{

class CConfigFileComponentUtils : public CInterface
{
public:

    IMPLEMENT_IINTERFACE

    CConfigFileComponentUtils();
    virtual ~CConfigFileComponentUtils();

    void getAvailableComponets(::StringArray& compArray) const;
    void getAvailableESPServices(::StringArray& compArray) const;
    void getDefinedDirectories(::StringArray& definedDirectoriesArray) const;
    void getDirectoryPath(const char *pkey, ::StringBuffer& path) const;
    void setDirectoryPath(const char* pkey, const char* pval);

protected:
private:
};

//}
#endif // _CONFIG_FILE_COMPONENT_UTILS_HPP_
