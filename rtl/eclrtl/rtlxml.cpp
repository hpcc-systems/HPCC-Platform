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

#include "limits.h"
#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "eclrtl.hpp"
#include "bcd.hpp"
#include "unicode/uchar.h"
#include "unicode/ucol.h"
#include "unicode/ustring.h"
#include "unicode/ucnv.h"
#include "unicode/schriter.h"
#include "unicode/regex.h"
#include "unicode/normlzr.h"
#include "unicode/locid.h"
#include "jlog.hpp"
#include "jmd5.hpp"
#include "rtlqstr.ipp"

#ifndef _WIN32
//typedef long long __int64;
#define _fastcall
#define __fastcall
#define _stdcall
#define __stdcall
#endif

//---------------------------------------------------------------------------

void outputXmlString(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(field, out, 0, len);
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlBool(bool field, const char *fieldname, StringBuffer &out)
{
    const char * text = field ? "true" : "false";
    if (fieldname)
        out.append('<').append(fieldname).append('>').append(text).append("</").append(fieldname).append('>');
    else
        out.append(text);
}

//ECLRTL_API void outputXmlDecimal(unsigned len, const char *field, const char *fieldname, StrignBuffer &out);
static char hexchar[] = "0123456789ABCDEF";
void outputXmlData(unsigned len, const void *_field, const char *fieldname, StringBuffer &out)
{
    const unsigned char *field = (const unsigned char *) _field;
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    for (unsigned int i = 0; i < len; i++)
    {
        out.append(hexchar[field[i] >> 4]).append(hexchar[field[i] & 0x0f]);
    }
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}
void outputXmlInt(__int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlReal(double field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    DecLock();
    if (DecValid(true, size*2-1, field))
    {
        DecPushDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        out.append(finger);
    }
    else
        out.append("####");
    DecUnlock();
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    DecLock();
    if (DecValid(false, size*2, field))
    {
        DecPushUDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        out.append(finger);
    }
    else
        out.append("####");
    DecUnlock();
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out)
{
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(buff, out, 0, bufflen, true); // output as UTF-8
    if (fieldname)
        out.append("</").append(fieldname).append('>');
    rtlFree(buff);
}

void outputXmlUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(field, out, 0, rtlUtf8Size(len, field), true); // output as UTF-8
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlBeginNested(const char *fieldname, StringBuffer &out)
{
    out.append('<').append(fieldname).append('>');
}

void outputXmlEndNested(const char *fieldname, StringBuffer &out)
{
    out.append("</").append(fieldname).append('>');
}

void outputXmlSetAll(StringBuffer &out)
{
    out.append("<All/>");
}

//---------------------------------------------------------------------------

void outputXmlAttrString(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    encodeXML(field, out, ENCODE_NEWLINES, len);
    out.append('"');
}

void outputXmlAttrData(unsigned len, const void *field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlData(len, field, NULL, out);
    out.append('"');
}

void outputXmlAttrBool(bool field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlBool(field, NULL, out);
    out.append('"');
}

void outputXmlAttrInt(__int64 field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"").append(field).append('"');
}

void outputXmlAttrUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"").append(field).append('"');
}

void outputXmlAttrReal(double field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"").append(field).append('"');
}

void outputXmlAttrDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlDecimal(field, size, precision, NULL, out);
    out.append('"');
}

void outputXmlAttrUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlUDecimal(field, size, precision, NULL, out);
    out.append('"');
}

void outputXmlAttrUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlUnicode(len, field, NULL, out);
    out.append('"');
}

void outputXmlAttrUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    out.append(' ').append(fieldname).append("=\"");
    outputXmlUtf8(len, field, NULL, out);
    out.append('"');
}

//---------------------------------------------------------------------------
