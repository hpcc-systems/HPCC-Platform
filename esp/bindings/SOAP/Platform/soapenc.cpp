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
