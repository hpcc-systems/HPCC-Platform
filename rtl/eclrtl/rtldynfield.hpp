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

#ifndef rtldynfield_hpp
#define rtldynfield_hpp

#include "rtlfield.hpp"

//These classes support the dynamic creation of type and field information

struct ECLRTL_API RtlDynFieldInfo : public RtlFieldInfo
{
public:
    RtlDynFieldInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type)
    : RtlFieldInfo(_name, _xpath, _type, nullptr)
    {
    }
    ~RtlDynFieldInfo()
    {
        free(const_cast<char *>(name));
        free(const_cast<char *>(xpath));
    }
};


//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlDynRecordTypeInfo : public RtlRecordTypeInfo
{
    inline RtlDynRecordTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlRecordTypeInfo(_fieldType, _length, _fields) {}
    ~RtlDynRecordTypeInfo() { delete[] fields; }
};

#endif
