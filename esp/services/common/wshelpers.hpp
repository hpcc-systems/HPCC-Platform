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

// wshelpers.hpp: 
//
//////////////////////////////////////////////////////////////////////
#pragma warning( disable : 4786)

#ifndef _ESPWIZ_WSHELPERS_HPP__
#define _ESPWIZ_WSHELPERS_HPP__

template<typename T> inline StringBuffer& operator<<(StringBuffer& buf, const T& t) { return buf.append(t); }

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
