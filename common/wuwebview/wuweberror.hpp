/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef WUERROR_HPP
#define WUERROR_HPP

#include "jexcept.hpp"

/* Errors can occupy range 5100..5199 */

#define WUWEBERR_WorkUnitNotFound               5500
#define WUWEBERR_ManifestNotFound               5501
#define WUWEBERR_ViewResourceNotFound           5502
#define WUWEBERR_UnknownViewType                5503
#define WUWEBERR_TargetNotFound                 5504
#define WUWEBERR_QueryNotFound                  5505

#endif
