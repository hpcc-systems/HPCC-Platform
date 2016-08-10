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

#ifndef _SOAPMACRO_HPP__
#define _SOAPMACRO_HPP__
#include "soapenc.hpp"

#define BEGIN_SOAP_REQ(MyService) \
class CSoap##MyService##Request : implements IEsp##MyService##Request, public CInterface \
{\
public:\
        IMPLEMENT_IINTERFACE;

#define END_SOAP_REQ() };

#define BEGIN_CONSTRUCTOR(MyService) \
    CSoap##MyService##Request(IRpcMessage* rpc_call)\
   {

#define END_CONSTRUCTOR() }

#define REQ_PARAMETER(MyTag) \
    rpc_call->get_value(#MyTag, m_##MyTag); 

#define REQ_METHOD_STRINGATTR(MyFunction, MyTag) \
    StringAttr m_##MyTag; \
    const char * MyFunction() const \
    { \
        return m_##MyTag.get(); \
    };

#define REQ_METHOD_LONG(MyFunction, MyTag) \
    unsigned long m_##MyTag; \
    unsigned long MyFunction() const \
    { \
        return m_##MyTag; \
    };

#define REQ_METHOD_BOOL(MyFunction, MyTag) \
    bool m_##MyTag; \
    bool MyFunction() const \
    { \
        return m_##MyTag; \
    };

#endif
