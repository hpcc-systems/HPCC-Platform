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

#include "platform.h"
#include "jlib.hpp"
#include "thorxmlwrite.hpp"
#include "eclrtl.hpp"
#include "rtlkey.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "rtlbcd.hpp"

CommonXmlWriter::CommonXmlWriter(unsigned _flags, unsigned initialIndent, IXmlStreamFlusher *_flusher) 
{
    flusher = _flusher;
    flags = _flags;
    indent = initialIndent;
    nestLimit = flags & XWFnoindent ? (unsigned) -1 : 0;
    tagClosed = true;
}

CommonXmlWriter::~CommonXmlWriter()
{
    flush(true);
}

CommonXmlWriter & CommonXmlWriter::clear()
{
    out.clear();
    indent = 0;
    nestLimit = flags & XWFnoindent ? (unsigned) -1 : 0;
    tagClosed = true;
    return *this;
}

bool CommonXmlWriter::checkForAttribute(const char * fieldname)
{
    if (!tagClosed)
    {
        if (fieldname && (fieldname[0] == '@'))
            return true;
        closeTag();
    }
    return false;
}

void CommonXmlWriter::closeTag()
{
    if (!tagClosed)
    {
        out.append(">");
        if (!nestLimit)
            out.newline();
        tagClosed = true;
    }
    flush(false);
}

void CommonXmlWriter::outputQuoted(const char *text)
{
    out.append(text);
}

void CommonXmlWriter::outputString(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimStrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrString(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlString(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}


void CommonXmlWriter::outputQString(unsigned len, const char *field, const char *fieldname)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
    outputString(len, temp, fieldname);
}


void CommonXmlWriter::outputBool(bool field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrBool(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlBool(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputData(unsigned len, const void *field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrData(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlData(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputInt(__int64 field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrInt(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlInt(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputUInt(unsigned __int64 field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrUInt(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlUInt(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputReal(double field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrReal(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlReal(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrDecimal(field, size, precision, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlDecimal(field, size, precision, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrUDecimal(field, size, precision, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlUDecimal(field, size, precision, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputUnicode(unsigned len, const UChar *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUnicodeStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUnicodeStrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrUnicode(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlUnicode(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputUtf8(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUtf8StrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUtf8StrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrUtf8(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputXmlUtf8(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonXmlWriter::outputXmlns(const char *name, const char *uri)
{
    StringBuffer fieldname("xmlns:");
    outputXmlAttrString(strlen(uri), uri, fieldname.append(name), out);
}

void CommonXmlWriter::outputBeginDataset(const char *dsname, bool nestChildren)
{
    outputBeginNested("Dataset", nestChildren, false); //indent row, not dataset for backward compatibility
    if (nestChildren && indent==0)
        indent++;
    if (!dsname || !*dsname)
        return;
    out.append(" name='"); //single quote for backward compatibility
    outputXmlUtf8(strlen(dsname), dsname, NULL, out);
    out.append("'");
}

void CommonXmlWriter::outputEndDataset(const char *dsname)
{
    outputEndNested("Dataset", false);
}

void CommonXmlWriter::outputBeginNested(const char *fieldname, bool nestChildren, bool doIndent)
{
    if (!fieldname || !*fieldname)
        return;

    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputBeginNested(leading, nestChildren, doIndent);
        outputBeginNested(sep+1, nestChildren, doIndent);
        return;
    }

    closeTag();
    if (!nestLimit && doIndent)
        out.pad(indent);
    out.append('<').append(fieldname);
    if (doIndent)
        indent += 1;
    if (!nestChildren && !nestLimit)
        nestLimit = indent;
    tagClosed = false;
}

void CommonXmlWriter::outputBeginNested(const char *fieldname, bool nestChildren)
{
    outputBeginNested(fieldname, nestChildren, true);
}

void CommonXmlWriter::outputEndNested(const char *fieldname, bool doIndent)
{
    if (!fieldname || !*fieldname)
        return;

    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputEndNested(sep+1, doIndent);
        outputEndNested(leading, doIndent);
        return;
    }

    if (flags & XWFexpandempty)
       closeTag();
    if (!tagClosed)
    {
        out.append("/>");
        tagClosed = true;
    }
    else
    {
        if (!nestLimit && doIndent)
            out.pad(indent-1);
        out.append("</").append(fieldname).append('>');
    }
    if (indent==nestLimit)
        nestLimit = 0;
    if (doIndent)
        indent -= 1;
    if (!nestLimit)
        out.newline();
}

void CommonXmlWriter::outputEndNested(const char *fieldname)
{
    outputEndNested(fieldname, true);
}
void CommonXmlWriter::outputSetAll()
{
    closeTag();
    if (!nestLimit)
        out.pad(indent);
    outputXmlSetAll(out);
    if (!nestLimit)
        out.newline();
}

//=====================================================================================

CommonJsonWriter::CommonJsonWriter(unsigned _flags, unsigned initialIndent, IXmlStreamFlusher *_flusher)
{
    flusher = _flusher;
    flags = _flags;
    indent = initialIndent;
    nestLimit = flags & XWFnoindent ? (unsigned) -1 : 0;
    needDelimiter = false;
}

CommonJsonWriter::~CommonJsonWriter()
{
    flush(true);
}

CommonJsonWriter & CommonJsonWriter::clear()
{
    out.clear();
    indent = 0;
    nestLimit = flags & XWFnoindent ? (unsigned) -1 : 0;
    return *this;
}

void CommonJsonWriter::checkFormat(bool doDelimit, bool delimitNext, int inc)
{
    if (doDelimit)
    {
        if (needDelimiter)
        {
            if (!out.length()) //new block
               out.append(',');
            else
                delimitJSON(out);
        }
        if (!nestLimit)
            out.append('\n').pad(indent);
    }
    indent+=inc;
    needDelimiter = delimitNext;
}

void CommonJsonWriter::checkDelimit(int inc)
{
    checkFormat(true, true, inc);
}


const char *CommonJsonWriter::checkItemName(CJsonWriterItem *item, const char *name, bool simpleType)
{
    if (simpleType && (!name || !*name))
        name = "#value"; //xml mixed content
    if (item && item->depth==0 && strieq(item->name, name))
        return NULL;
    return name;
}

const char *CommonJsonWriter::checkItemName(const char *name, bool simpleType)
{
    CJsonWriterItem *item = (arrays.length()) ? &arrays.tos() : NULL;
    return checkItemName(item, name, simpleType);
}

const char *CommonJsonWriter::checkItemNameBeginNested(const char *name)
{
    CJsonWriterItem *item = (arrays.length()) ? &arrays.tos() : NULL;
    name = checkItemName(item, name, false);
    if (item)
        item->depth++;
    return name;
}

const char *CommonJsonWriter::checkItemNameEndNested(const char *name)
{
    CJsonWriterItem *item = (arrays.length()) ? &arrays.tos() : NULL;
    if (item)
        item->depth--;
    return checkItemName(item, name, false);
}

void CommonJsonWriter::outputQuoted(const char *text)
{
    checkDelimit();
    appendJSONValue(out, NULL, text);
}

void CommonJsonWriter::outputString(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimStrLen(len, field) == 0))
        return;
    checkDelimit();
    appendJSONStringValue(out, checkItemName(fieldname), len, field, true);
}

void CommonJsonWriter::outputQString(unsigned len, const char *field, const char *fieldname)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
    outputString(len, temp, fieldname);
}

void CommonJsonWriter::outputBool(bool field, const char *fieldname)
{
    checkDelimit();
    appendJSONValue(out, checkItemName(fieldname), field);
}

void CommonJsonWriter::outputData(unsigned len, const void *field, const char *fieldname)
{
    checkDelimit();
    appendJSONDataValue(out, checkItemName(fieldname), len, field);
}

void CommonJsonWriter::outputInt(__int64 field, const char *fieldname)
{
    checkDelimit();
    appendJSONValue(out, checkItemName(fieldname), field);
}

void CommonJsonWriter::outputUInt(unsigned __int64 field, const char *fieldname)
{
    checkDelimit();
    appendJSONValue(out, checkItemName(fieldname), field);
}

void CommonJsonWriter::outputReal(double field, const char *fieldname)
{
    checkDelimit();
    appendJSONValue(out, checkItemName(fieldname), field);
}

void CommonJsonWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    checkDelimit();
    outputJsonDecimal(field, size, precision, checkItemName(fieldname), out);
}

void CommonJsonWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    checkDelimit();
    outputJsonUDecimal(field, size, precision, checkItemName(fieldname), out);
}

void CommonJsonWriter::outputUnicode(unsigned len, const UChar *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUnicodeStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUnicodeStrLen(len, field) == 0))
        return;
    checkDelimit();
    outputJsonUnicode(len, field, checkItemName(fieldname), out);
}

void CommonJsonWriter::outputUtf8(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUtf8StrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUtf8StrLen(len, field) == 0))
        return;
    checkDelimit();
    appendJSONStringValue(out, checkItemName(fieldname), len, field, true);
}

void CommonJsonWriter::outputBeginArray(const char *fieldname)
{
    arrays.append(*new CJsonWriterItem(fieldname));
    const char * sep = strchr(fieldname, '/');
    while (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        appendJSONName(out, leading).append(" {");
        fieldname = sep+1;
        sep = strchr(fieldname, '/');
    }
    checkFormat(false, false, 1);
    appendJSONName(out, fieldname).append('[');
}

void CommonJsonWriter::outputEndArray(const char *fieldname)
{
    arrays.pop();
    checkFormat(false, true, -1);
    out.append(']');
    const char * sep = (fieldname) ? strchr(fieldname, '/') : NULL;
    while (sep)
    {
        out.append('}');
        sep = strchr(sep+1, '/');
    }
}

void CommonJsonWriter::outputBeginDataset(const char *dsname, bool nestChildren)
{
    if (dsname && *dsname)
        outputBeginNested(dsname, nestChildren);
}

void CommonJsonWriter::outputEndDataset(const char *dsname)
{
    if (dsname && *dsname)
        outputEndNested(dsname);
}

void CommonJsonWriter::outputBeginNested(const char *fieldname, bool nestChildren)
{
    if (!fieldname || !*fieldname)
        return;

    flush(false);
    checkFormat(true, false, 1);
    fieldname = checkItemNameBeginNested(fieldname);
    if (fieldname)
    {
        const char * sep = (fieldname) ? strchr(fieldname, '/') : NULL;
        while (sep)
        {
            StringAttr leading(fieldname, sep-fieldname);
            appendJSONName(out, leading).append("{");
            fieldname = sep+1;
            sep = strchr(fieldname, '/');
        }
        appendJSONName(out, fieldname);
    }
    out.append("{");
    if (!nestChildren && !nestLimit)
        nestLimit = indent;
}

void CommonJsonWriter::outputEndNested(const char *fieldname)
{
    if (!fieldname || !*fieldname)
        return;

    flush(false);
    checkFormat(false, true, -1);
    fieldname = checkItemNameEndNested(fieldname);
    if (fieldname)
    {
        const char * sep = (fieldname) ? strchr(fieldname, '/') : NULL;
        while (sep)
        {
            out.append('}');
            sep = strchr(sep+1, '/');
        }
    }
    out.append("}");
    if (indent==nestLimit)
        nestLimit = 0;
}

void CommonJsonWriter::outputSetAll()
{
    flush(false);
    checkDelimit();
    appendJSONValue(out, "All", true);
}

//=====================================================================================

inline void outputEncodedXmlString(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:string\">");
    encodeXML(field, out, 0, len);
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

inline void outputEncodedXmlBool(bool field, const char *fieldname, StringBuffer &out)
{
    const char * text = field ? "true" : "false";
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:boolean\">").append(text).append("</").append(fieldname).append('>');
    else
        out.append(text);
}

static char thorHelperhexchar[] = "0123456789ABCDEF";
inline void outputEncodedXmlData(unsigned len, const void *_field, const char *fieldname, StringBuffer &out)
{
    const unsigned char *field = (const unsigned char *) _field;
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:hexBinary\">");
    for (unsigned int i = 0; i < len; i++)
    {
        out.append(thorHelperhexchar[field[i] >> 4]).append(thorHelperhexchar[field[i] & 0x0f]);
    }
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

inline void outputEncoded64XmlData(unsigned len, const void *_field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:base64Binary\">");
    JBASE64_Encode(_field, len, out, false);
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

inline void outputEncodedXmlInt(__int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:integer\">").append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}

inline void outputEncodedXmlUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:nonNegativeInteger\">").append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}

inline void outputEncodedXmlReal(double field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:double\">").append(field).append("</").append(fieldname).append('>');
    else
        out.append(field);
}

inline void outputEncodedXmlDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:decimal\">");
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

inline void outputEncodedXmlUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:decimal\">");
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

inline void outputEncodedXmlUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out)
{
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:string\">");
    encodeXML(buff, out, 0, bufflen, true); // output as UTF-8
    if (fieldname)
        out.append("</").append(fieldname).append('>');
    rtlFree(buff);
}

inline void outputEncodedXmlUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out)
{
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:string\">");
    encodeXML(field, out, 0, rtlUtf8Size(len, field), true); // output as UTF-8
    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

//=====================================================================================

CommonEncodedXmlWriter::CommonEncodedXmlWriter(unsigned _flags, unsigned initialIndent, IXmlStreamFlusher *_flusher) 
    : CommonXmlWriter(_flags, initialIndent, _flusher) 
{
}

void CommonEncodedXmlWriter::outputString(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimStrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrString(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlString(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputBool(bool field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrBool(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlBool(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputData(unsigned len, const void *field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrData(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlData(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputInt(__int64 field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrInt(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlInt(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputUInt(unsigned __int64 field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrUInt(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlUInt(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputReal(double field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrReal(field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlReal(field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrDecimal(field, size, precision, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlDecimal(field, size, precision, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrUDecimal(field, size, precision, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlUDecimal(field, size, precision, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputUnicode(unsigned len, const UChar *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUnicodeStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUnicodeStrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrUnicode(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlUnicode(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

void CommonEncodedXmlWriter::outputUtf8(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUtf8StrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUtf8StrLen(len, field) == 0))
        return;
    if (checkForAttribute(fieldname))
        outputXmlAttrUtf8(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncodedXmlUtf8(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

//=====================================================================================
CommonEncoded64XmlWriter::CommonEncoded64XmlWriter(unsigned _flags, unsigned initialIndent, IXmlStreamFlusher *_flusher) 
    : CommonEncodedXmlWriter(_flags, initialIndent, _flusher) 
{
}

void CommonEncoded64XmlWriter::outputData(unsigned len, const void *field, const char *fieldname)
{
    if (checkForAttribute(fieldname))
        outputXmlAttrData(len, field, fieldname+1, out);
    else
    {
        if (!nestLimit)
            out.pad(indent);
        outputEncoded64XmlData(len, field, fieldname, out);
        if (!nestLimit)
            out.newline();
    }
}

//=====================================================================================

CommonXmlWriter * CreateCommonXmlWriter(unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher, XMLWriterType xmlType)
{
    switch (xmlType)
    {
    case WTStandard:
        return new CommonXmlWriter(_flags, _initialIndent, _flusher);//standard XML writer
    case WTEncodingData64:
        return new CommonEncoded64XmlWriter(_flags, _initialIndent, _flusher);//writes xsd type attributes, and all data as base64binary
    case WTEncoding:
        return new CommonEncodedXmlWriter(_flags, _initialIndent, _flusher);//writes xsd type attributes, and all data as hexBinary
    default:
        assertex(false);
        return NULL;
    }
}

//=====================================================================================

IXmlWriter * createIXmlWriter(unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher, XMLWriterType xmlType)
{
    if (xmlType==WTJSON)
        return new CommonJsonWriter(_flags, _initialIndent, _flusher);
    return CreateCommonXmlWriter(_flags, _initialIndent, _flusher, xmlType);
}

//=====================================================================================

SimpleOutputWriter::SimpleOutputWriter()
{
    separatorNeeded = false;
}

void SimpleOutputWriter::outputFieldSeparator()
{
    if (separatorNeeded)
        out.append(',');
    separatorNeeded = true;
}

SimpleOutputWriter & SimpleOutputWriter::clear()
{
    out.clear();
    separatorNeeded = false;
    return *this;
}

void SimpleOutputWriter::outputQuoted(const char *text)
{
    out.append(text);
}

void SimpleOutputWriter::outputString(unsigned len, const char *field, const char *)
{
    outputFieldSeparator();
    out.append(len, field);
}

void SimpleOutputWriter::outputQString(unsigned len, const char *field, const char *fieldname)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
    outputString(len, temp, fieldname);
}

void SimpleOutputWriter::outputBool(bool field, const char *)
{
    outputFieldSeparator();
    outputXmlBool(field, NULL, out);
}

void SimpleOutputWriter::outputData(unsigned len, const void *field, const char *)
{
    outputFieldSeparator();
    outputXmlData(len, field, NULL, out);
}

void SimpleOutputWriter::outputInt(__int64 field, const char *)
{
    outputFieldSeparator();
    outputXmlInt(field, NULL, out);
}

void SimpleOutputWriter::outputUInt(unsigned __int64 field, const char *)
{
    outputFieldSeparator();
    outputXmlUInt(field, NULL, out);
}

void SimpleOutputWriter::outputReal(double field, const char *)
{
    outputFieldSeparator();
    outputXmlReal(field, NULL, out);
}

void SimpleOutputWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *)
{
    outputFieldSeparator();
    outputXmlDecimal(field, size, precision, NULL, out);
}

void SimpleOutputWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *)
{
    outputFieldSeparator();
    outputXmlUDecimal(field, size, precision, NULL, out);
}

void SimpleOutputWriter::outputUnicode(unsigned len, const UChar *field, const char *)
{
    outputFieldSeparator();
    outputXmlUnicode(len, field, NULL, out);
}

void SimpleOutputWriter::outputUtf8(unsigned len, const char *field, const char *)
{
    outputFieldSeparator();
    outputXmlUtf8(len, field, NULL, out);
}

void SimpleOutputWriter::outputBeginNested(const char *s, bool)
{
    if (!s || !*s)
        return;
    outputFieldSeparator();
    out.append('[');
    separatorNeeded = false;
}

void SimpleOutputWriter::outputEndNested(const char *s)
{
    if (!s || !*s)
        return;
    out.append(']');
    separatorNeeded = true;
}

void SimpleOutputWriter::outputSetAll()
{
    out.append('*');
}

void SimpleOutputWriter::newline()
{
    out.append('\n');
}

//=====================================================================================

CommonFieldProcessor::CommonFieldProcessor(StringBuffer &_result, bool _trim) : result(_result), trim(_trim)
{
}
void CommonFieldProcessor::processString(unsigned len, const char *value, const RtlFieldInfo * field)
{
    if (trim)
        len = rtlTrimStrLen(len, value);
    result.append("'");
    outputXmlString(len, value, NULL, result);
    result.append("'");
}
void CommonFieldProcessor::processBool(bool value, const RtlFieldInfo * field)
{
    outputXmlBool(value, NULL, result);
}
void CommonFieldProcessor::processData(unsigned len, const void *value, const RtlFieldInfo * field)
{
    outputXmlData(len, value, NULL, result);
}
void CommonFieldProcessor::processInt(__int64 value, const RtlFieldInfo * field)
{
    outputXmlInt(value, NULL, result);
}
void CommonFieldProcessor::processUInt(unsigned __int64 value, const RtlFieldInfo * field)
{
    outputXmlUInt(value, NULL, result);
}
void CommonFieldProcessor::processReal(double value, const RtlFieldInfo * field)
{
    outputXmlReal(value, NULL, result);
}
void CommonFieldProcessor::processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
{
    outputXmlDecimal(value, digits, precision, NULL, result);
}
void CommonFieldProcessor::processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
{
    outputXmlUDecimal(value, digits, precision, NULL, result);
}
void CommonFieldProcessor::processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
{
    if (trim)
        len = rtlTrimUnicodeStrLen(len, value);
    outputXmlUnicode(len, value, NULL, result);
}
void CommonFieldProcessor::processQString(unsigned len, const char *value, const RtlFieldInfo * field)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, value);
    processString(len, temp, field);
}
void CommonFieldProcessor::processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
{   
    if (trim)
        len = rtlTrimUtf8StrLen(len, value);
    outputXmlUtf8(len, value, NULL, result);
}

bool CommonFieldProcessor::processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
{
    result.append('[');
    if (isAll)
        result.append("ALL");
    return true;
}
bool CommonFieldProcessor::processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
{
    result.append('[');
    return true;
}
bool CommonFieldProcessor::processBeginRow(const RtlFieldInfo * field)
{
    result.append('{');
    return true;
}
void CommonFieldProcessor::processEndSet(const RtlFieldInfo * field) 
{
    result.append(']');
}
void CommonFieldProcessor::processEndDataset(const RtlFieldInfo * field)
{
    result.append(']');
}
void CommonFieldProcessor::processEndRow(const RtlFieldInfo * field)
{
    result.append('}');
}


//=============================================================================================

void printKeyedValues(StringBuffer &out, IIndexReadContext *segs, IOutputMetaData *rowMeta)
{
    unsigned totalKeyedSize = 0;
    unsigned numSegs = segs->ordinality();
    while (numSegs)
    {
        IKeySegmentMonitor &seg = *segs->item(numSegs-1);
        if (!seg.isWild())
        {
            totalKeyedSize = seg.getOffset() + seg.getSize();
            break;
        }
        numSegs--;
    }
    if (numSegs)
    {
        byte *tempRow = (byte *) alloca(totalKeyedSize);
        byte *savedRow = (byte *) alloca(totalKeyedSize);
        const RtlFieldInfo * const *fields = rowMeta->queryTypeInfo()->queryFields();
        unsigned fieldOffset = 0;
        bool inKeyed = false;
        bool inWild = false;
        for (unsigned segNo = 0; segNo < numSegs; segNo++)
        {
            IKeySegmentMonitor &seg = *segs->item(segNo);
            unsigned segOffset = seg.getOffset();
            unsigned segSize = seg.getSize();
            while (fieldOffset < segOffset + segSize) // This is trying to cope with the combined case but not sure it completely does
            {
                assertex(fields[0]->type->isFixedSize());
                unsigned curFieldSize = fields[0]->type->size(NULL, NULL);
                if (seg.isWild())
                {
                    if (!inWild)
                    {
                        if (inKeyed)
                        {
                            out.append("),");
                            inKeyed = false;
                        }
                        out.append("WILD(");
                        inWild = true;
                    }
                    else
                        out.append(',');
                    out.append(fields[0]->name);
                }
                else
                {
                    StringBuffer setValues;
                    CommonFieldProcessor setProcessor(setValues, true);
                    unsigned numValues = 0;
                    unsigned subStringLength = 0;
                    if (!seg.isEmpty())
                    {
                        seg.setLow(tempRow);
                        loop
                        {
                            if (numValues)
                                setValues.append(",");
                            memcpy(savedRow+segOffset, tempRow+segOffset, segSize);
                            seg.endRange(tempRow);
                            if (memcmp(savedRow+segOffset, tempRow+segOffset, segSize) != 0)
                            {
                                // Special case - if they differ only in trailing values that are 0 vs 0xff, then it's a substring match...
                                if (numValues==0 && (fields[0]->type->fieldType & (RFTMkind | RFTMebcdic)) == type_string)
                                {
                                    unsigned pos;
                                    for (pos = 0; pos < segSize; pos++)
                                    {
                                        if (savedRow[segOffset+pos] != tempRow[segOffset+pos])
                                            break;
                                    }
                                    subStringLength = pos;
                                    for (; pos < segSize; pos++)
                                    {
                                        if (savedRow[segOffset+pos] != 0 || tempRow[segOffset+pos] != 0xff)
                                        {
                                            subStringLength = 0;
                                            break;
                                        }
                                    }
                                }
                                fields[0]->process(savedRow + fieldOffset, tempRow, setProcessor);
                                setValues.append("..");
                                fields[0]->process(tempRow + fieldOffset, tempRow, setProcessor);
                                numValues+=2;
                            }
                            else
                            {
                                fields[0]->process(tempRow + fieldOffset, tempRow, setProcessor);
                                numValues++;
                            }
                            if (!seg.increment(tempRow))
                                break;
                        }
                    }
                    if (!inKeyed)
                    {
                        if (inWild)
                        {
                            out.append("),");
                            inWild = false;
                        }
                        out.append("KEYED(");
                        inKeyed = true;
                    }
                    else
                        out.append(',');
                    out.append(fields[0]->name);
                    if (numValues==1)
                        out.append("=").append(setValues);
                    else if (subStringLength)
                        out.appendf("[1..%d]='", subStringLength).append(subStringLength, (char *) savedRow+fieldOffset).append("'");
                    else
                        out.append(" IN [").append(setValues).append("]");
                }
                fieldOffset += curFieldSize;
                fields++;
                if (!fields[0])
                    break;
            }
        }
        if (inKeyed || inWild)
            out.append(")");
    }
    else
        out.append("UNKEYED");
}


extern thorhelper_decl void convertRowToXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    const byte * self = (const byte *)row;
    if (flags == (unsigned)-1)
        flags = XWFtrim|XWFopt|XWFnoindent;

    CommonXmlWriter writer(flags);
    info.toXML(self, writer);
    //could use detach...
    unsigned sizeResult;
    rtlStrToStrX(sizeResult, result, writer.length(), writer.str());
    lenResult = rtlUtf8Length(sizeResult, result);
}
