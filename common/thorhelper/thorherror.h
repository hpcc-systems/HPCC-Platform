/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef THORHERROR_H
#define THORHERROR_H

#include "jexcept.hpp"
#include "errorlist.h"

#define THORHELPER_DEBUG_ERROR              (THORHELPER_ERROR_START + 0)
#define THORHELPER_INTERNAL_ERROR           (THORHELPER_ERROR_START + 1)
#define THORHELPER_DATA_ERROR               (THORHELPER_ERROR_START + 2)
#define THORHELPER_UNSUPPORTED_ENCODING     (THORHELPER_ERROR_START + 3)

//Errors with associated text
#define THORCERR_InvalidXmlFromXml          (THORHELPER_ERROR_START + 50)
#define THORCERR_InvalidXmlFromXml_Text     "Invalid xml passed to FROMXML"

#define THORCERR_InvalidJsonFromJson          (THORHELPER_ERROR_START + 55)
#define THORCERR_InvalidJsonFromJson_Text     "Invalid json passed to FROMJSON"

#endif
