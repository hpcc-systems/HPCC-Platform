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

// wshelpers.hpp: 
//
//////////////////////////////////////////////////////////////////////
#pragma warning( disable : 4786)

#ifndef _ESPWIZ_WSHELPERS_HPP__
#define _ESPWIZ_WSHELPERS_HPP__

static StringBuffer &buildComma(StringBuffer &s, unsigned __int64 val)
{
    if (val)
    {
        unsigned chars = 0;
        unsigned pos = s.length();
        while (val)
        {
            if (chars==3)
            {
                s.insert(pos, ',');
                chars = 0;
            }
            s.insert(pos, (char) ('0'+val%10));
            chars++;
            val /= 10;
        }
    }
    else
        s.append('0');
    return s;
}

struct comma
{
    comma(__int64 _val): val(_val) {}
    __int64 val;
};

struct XML
{
    XML(const char* _s): s(_s) {}
    const char* s;
};

inline StringBuffer& operator<<(StringBuffer& buf, const XML& f) 
{ 
    encodeUtf8XML(f.s,buf);
    return buf; 
}

inline StringBuffer& operator<<(StringBuffer& buf, const comma& c) 
{ 
    return c.val>=0 ? buildComma(buf, c.val) : buf;
}

struct JScript
{
    JScript(const char* _s): s(_s) {}
    const char* s;
};

inline StringBuffer& operator<<(StringBuffer& buf, const JScript& j) 
{ 
    StringBuffer script;
    appendStringAsCPP(script,strlen(j.s),j.s,false);
    buf<<XML(script.str());
    return buf;
}

#endif // _ESPWIZ_WSHELPERS_HPP__
