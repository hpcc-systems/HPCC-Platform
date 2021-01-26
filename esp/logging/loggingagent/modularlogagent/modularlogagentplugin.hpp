/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef _MODULARLOGAGENTPLUGIN_HPP_
#define _MODULARLOGAGENTPLUGIN_HPP_

#include "modularlogagent.hpp"

#ifdef MODULARLOGAGENT_EXPORTS
    #define MODULARLOGAGENTPLUGIN_API DECL_EXPORT
#else
    #define MODULARLOGAGENTPLUGIN_API DECL_IMPORT
#endif

#endif // _MODULARLOGAGENTPLUGIN_HPP_