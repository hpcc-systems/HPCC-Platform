/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef PARAMS2XML_HPP
#define PARAMS2XML_HPP

#include "jliball.hpp"
#include "esdl_def.hpp"

#define PARAMS2XML_OUTPUT_XML_TAG_NAME 0x0001
#define PARAMS2XML_INPUT_XML_TAG_NAME 0x0002

esdl_decl void params2xml(IEsdlDefinition *def, const char *structname, IProperties *params, StringBuffer &xmlstr, unsigned flags, double ver);
esdl_decl void params2xml(IEsdlDefinition *def, const char *service, const char *method, EsdlDefTypeId esdltype, IProperties *params, StringBuffer &xmlstr, unsigned flags, double ver);

#endif //PARAMS2XML_HPP
