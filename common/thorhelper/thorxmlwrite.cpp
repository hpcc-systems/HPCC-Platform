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

#include "platform.h"
#include "jlib.hpp"
#include "thorxmlwrite.hpp"
#include "eclrtl.hpp"
#include "rtlkey.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "bcd.hpp"

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


void CommonXmlWriter::outputBeginNested(const char *fieldname, bool nestChildren)
{
    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputBeginNested(leading, nestChildren);
        outputBeginNested(sep+1, nestChildren);
        return;
    }

    closeTag();
    if (!nestLimit)
        out.pad(indent);
    out.append('<').append(fieldname);
    indent += 1;
    if (!nestChildren && !nestLimit)
        nestLimit = indent;
    tagClosed = false;
}

void CommonXmlWriter::outputEndNested(const char *fieldname)
{
    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputEndNested(sep+1);
        outputEndNested(leading);
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
        if (!nestLimit)
            out.pad(indent-1);
        out.append("</").append(fieldname).append('>');
    }
    if (indent==nestLimit)
        nestLimit = 0;
    indent -= 1;
    if (!nestLimit)
        out.newline();
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
    const unsigned char *field = (const unsigned char *) _field;
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
        return new CommonXmlWriter(_flags, _initialIndent, _flusher);//standart XML writer
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

void SimpleOutputWriter::outputBeginNested(const char *, bool)
{
    outputFieldSeparator();
    out.append('[');
    separatorNeeded = false;
}

void SimpleOutputWriter::outputEndNested(const char *)
{
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
void CommonFieldProcessor::processSetAll(const RtlFieldInfo * field)
{
    result.append("ALL");
}
void CommonFieldProcessor::processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
{   
    if (trim)
        len = rtlTrimUtf8StrLen(len, value);
    outputXmlUtf8(len, value, NULL, result);
}

bool CommonFieldProcessor::processBeginSet(const RtlFieldInfo * field)
{
    result.append('[');
    return true;
}
bool CommonFieldProcessor::processBeginDataset(const RtlFieldInfo * field) 
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
