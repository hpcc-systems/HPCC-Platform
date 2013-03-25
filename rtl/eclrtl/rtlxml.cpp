/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "rtlbcd.hpp"
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

//---------------------------------------------------------------------------

void outputXmlString(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(field, out, 0, len);
    if (fieldname && *fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlBool(bool field, const char *fieldname, StringBuffer &out)
{
    const char * text = field ? "true" : "false";
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>').append(text).append("</").append(fieldname).append('>');
    else
        out.append(text);
}

//ECLRTL_API void outputXmlDecimal(unsigned len, const char *field, const char *fieldname, StrignBuffer &out);
static char hexchar[] = "0123456789ABCDEF";
void outputXmlData(unsigned len, const void *_field, const char *fieldname, StringBuffer &out)
{
    const unsigned char *field = (const unsigned char *) _field;
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>');
    for (unsigned int i = 0; i < len; i++)
    {
        out.append(hexchar[field[i] >> 4]).append(hexchar[field[i] & 0x0f]);
    }
    if (fieldname && *fieldname)
        out.append("</").append(fieldname).append('>');
}
void outputXmlInt(__int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlReal(double field, const char *fieldname, StringBuffer &out)
{
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>').append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}
void outputXmlDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname && *fieldname)
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
    if (fieldname && *fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname && *fieldname)
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
    if (fieldname && *fieldname)
        out.append("</").append(fieldname).append('>');
}

void outputXmlUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out)
{
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(buff, out, 0, bufflen, true); // output as UTF-8
    if (fieldname && *fieldname)
        out.append("</").append(fieldname).append('>');
    rtlFree(buff);
}

void outputXmlUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname && *fieldname)
        out.append('<').append(fieldname).append('>');
    encodeXML(field, out, 0, rtlUtf8Size(len, field), true); // output as UTF-8
    if (fieldname && *fieldname)
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

void outputJsonUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out)
{
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
    appendJSONValue(out, fieldname, bufflen, buff); // output as UTF-8
    rtlFree(buff);
}

void outputJsonDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    appendJSONNameOrDelimit(out, fieldname);
    DecLock();
    if (DecValid(true, size*2-1, field))
    {
        DecPushDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        out.append(finger);
    }
    DecUnlock();
}

void outputJsonUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    appendJSONNameOrDelimit(out, fieldname);
    DecLock();
    if (DecValid(false, size*2, field))
    {
        DecPushUDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        out.append(finger);
    }
    DecUnlock();
}
