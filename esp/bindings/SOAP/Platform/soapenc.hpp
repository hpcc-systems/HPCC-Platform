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

#ifndef _SOAPENC_HPP__
#define _SOAPENC_HPP__

#include "jliball.hpp"

class SoapEnc
{
public:
    static void deserialize(const char* str, StringAttr& val);
    static void deserialize(const char* str, unsigned long& val);
    static void deserialize(const char* str, StringArray& val);
    
    static StringBuffer& serialize(StringBuffer& str, long val);
    static StringBuffer& serialize(StringBuffer& str, StringArray& val);
};

#endif
