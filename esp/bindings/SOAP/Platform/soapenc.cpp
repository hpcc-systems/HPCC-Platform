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


//ESP Bindings
#include "SOAP/Platform/soapenc.hpp"

void SoapEnc::deserialize(const char* str, StringAttr& val)
{
    val.set(str);
}

void SoapEnc::deserialize(const char* str, unsigned long& val) 
{
    if(str == NULL)
        val = 0;
    else
        val = atol(str);
}

// Temparary solution, elementss are separated by semicollons.
void SoapEnc::deserialize(const char* str, StringArray& val)
{
    if(str == NULL)
        return;
    const char* pb;
    const char* pe;

    pb = str;
    
    for(;;) 
    {
        if(*pb == '\0')
            return;

        pe = pb;

        while(*pe && *pe != ';')
            pe++;
        
        char* oneelem = new char[pe - pb + 1];
        memcpy(oneelem, pb, pe - pb);
        oneelem[pe - pb] = '\0';
        val.append(oneelem);
        delete oneelem;

        pb = pe + 1;
    }

}


    
StringBuffer& SoapEnc::serialize(StringBuffer& str, long val)
{
    return str.append(val);
}

// Temparary solution, elementss are separated by semicollons.
StringBuffer& SoapEnc::serialize(StringBuffer& str, StringArray& val)
{
    ForEachItemIn(x, val)
    {
        const char* oneelem = (const char*)val.item(x);
        if(x > 0)
            str.append(";");
        str.append(oneelem);
    }

    return str;
}
