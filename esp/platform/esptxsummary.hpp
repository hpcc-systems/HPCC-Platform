/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef ESPTXSUMMARY_HPP
#define ESPTXSUMMARY_HPP

#include "esp.hpp"
#include "esphttp.hpp"

// Creates a standard instance of the IEspTxSummary interface. The
// 'creationTime' parameter allows the summary's time-stamp to be synchronized
// with that of the owning object. If zero or omitted, time-stamps will be
// based on the instance's time of construction.
ESPHTTP_API IEspTxSummary* createEspTxSummary(unsigned creationTime = 0);

#endif // ESPTXSUMMARY_HPP
