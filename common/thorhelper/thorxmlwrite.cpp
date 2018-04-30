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

#include "platform.h"
#include "jlib.hpp"
#include "thorxmlwrite.hpp"
#include "eclrtl.hpp"
#include "rtlkey.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "rtlformat.hpp"
#include "rtlbcd.hpp"


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

extern thorhelper_decl void convertRowToJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    const byte * self = (const byte *)row;
    if (flags == (unsigned)-1)
        flags = XWFtrim|XWFopt|XWFnoindent;

    CommonJsonWriter writer(flags);
    info.toXML(self, writer);
    //could use detach...
    unsigned sizeResult;
    rtlStrToStrX(sizeResult, result, writer.length(), writer.str());
    lenResult = rtlUtf8Length(sizeResult, result);
}
