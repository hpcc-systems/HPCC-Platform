/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _SOAPMACRO_HPP__
#define _SOAPMACRO_HPP__
#include "soapenc.hpp"

#define BEGIN_SOAP_REQ(MyService) \
class CSoap##MyService##Request : public CInterface, \
    implements IEsp##MyService##Request\
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
