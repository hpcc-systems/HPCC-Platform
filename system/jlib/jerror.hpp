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


#ifndef JERROR_HPP
#define JERROR_HPP

#include "jexcept.hpp"
#include "jerrorrange.hpp"

/* Errors generated in jlib */

#define JLIBERR_BadlyFormedDateTime             1000
#define JLIBERR_BadUtf8InArguments              1001
#define JLIBERR_InternalError                   1002

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define JLIBERR_BadlyFormedDateTime_Text        "Badly formatted date/time '%s'"
#define JLIBERR_BadUtf8InArguments_Text         "The utf separators/terminators aren't valid utf-8"

#endif
