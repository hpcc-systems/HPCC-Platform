/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#ifndef _ESPWIZ_ws_wudetails_HPP__
#define _ESPWIZ_ws_wudetails_HPP__

#include "workunit.hpp"
#include "jstatcodes.h"


class WUDetails
{
public:
    WUDetails(IConstWorkUnit *_workunit, const char *_wuid);
    void processRequest(IEspWUDetailsRequest &req, IEspWUDetailsResponse &resp);

private:
    Linked<IConstWorkUnit> workunit;
    StringBuffer wuid;

    WuScopeFilter wuScopeFilter;
    void buildWuScopeFilter(IConstWUScopeFilter & requestScopeFilter, IConstWUNestedFilter & nestedFilter,
                            IConstWUPropertiesToReturn & propertiesToReturn, const char * filter,
                            IConstWUScopeOptions & scopeOptions);
    void buildPropertyFilter(IArrayOf<IConstWUPropertyFilter> & reqPropertyFilter);
};


#endif
