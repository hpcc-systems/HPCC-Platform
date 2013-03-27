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

#ifndef THORXMLWRITE_HPP
#define THORXMLWRITE_HPP

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define thorhelper_decl __declspec(dllexport)
 #else
  #define thorhelper_decl __declspec(dllimport)
 #endif
#else
 #define thorhelper_decl
#endif

#include "eclhelper.hpp"
#include "jptree.hpp"
#include "thorhelper.hpp"

interface IXmlStreamFlusher
{
    virtual void flushXML(StringBuffer &current, bool isClose) = 0;
};

class thorhelper_decl CommonXmlWriter : public CInterface, implements IXmlWriter
{
public:
    CommonXmlWriter(unsigned _flags, unsigned initialIndent=0,  IXmlStreamFlusher *_flusher=NULL);
    ~CommonXmlWriter();
    IMPLEMENT_IINTERFACE;

    CommonXmlWriter & clear();
    unsigned length() const                                 { return out.length(); }
    const char * str() const                                { return out.str(); }

    virtual void outputQuoted(const char *text);
    virtual void outputQString(unsigned len, const char *field, const char *fieldname);
    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
    virtual void outputBeginNested(const char *fieldname, bool nestChildren);
    virtual void outputEndNested(const char *fieldname);
    virtual void outputBeginArray(const char *fieldname){}; //repeated elements are inline for xml
    virtual void outputEndArray(const char *fieldname){};
    virtual void outputSetAll();

protected:
    bool checkForAttribute(const char * fieldname);
    void closeTag();
    inline void flush(bool isClose)
    {
        if (flusher)
            flusher->flushXML(out, isClose);
    }

protected:
    IXmlStreamFlusher *flusher;
    StringBuffer out;
    unsigned flags;
    unsigned indent;
    unsigned nestLimit;
    bool tagClosed;
};

class thorhelper_decl CommonJsonWriter : public CInterface, implements IXmlWriter
{
public:
    CommonJsonWriter(unsigned _flags, unsigned initialIndent=0,  IXmlStreamFlusher *_flusher=NULL);
    ~CommonJsonWriter();
    IMPLEMENT_IINTERFACE;

    CommonJsonWriter & clear();
    unsigned length() const                                 { return out.length(); }
    const char * str() const                                { return out.str(); }
    void checkDelimit(int inc=0);
    void checkFormat(bool doDelimit, bool needDelimiter=true, int inc=0);

    virtual void outputQuoted(const char *text);
    virtual void outputQString(unsigned len, const char *field, const char *fieldname);
    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
    virtual void outputBeginNested(const char *fieldname, bool nestChildren);
    virtual void outputEndNested(const char *fieldname);
    virtual void outputBeginArray(const char *fieldname);
    virtual void outputEndArray(const char *fieldname);
    virtual void outputSetAll();

protected:
    inline void flush(bool isClose)
    {
        if (flusher)
            flusher->flushXML(out, isClose);
    }

protected:
    IXmlStreamFlusher *flusher;
    StringArray arrays;
    StringBuffer out;
    unsigned flags;
    unsigned indent;
    unsigned nestLimit;
    bool needDelimiter;
};

//Writes type encoded XML strings  (xsi:type="xsd:string", xsi:type="xsd:boolean" etc)
class thorhelper_decl CommonEncodedXmlWriter : public CommonXmlWriter
{
public:
    CommonEncodedXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL);

    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
};

//Writes all encoded DATA fields as base64Binary
class thorhelper_decl CommonEncoded64XmlWriter : public CommonEncodedXmlWriter
{
public:
    CommonEncoded64XmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL);

    virtual void outputData(unsigned len, const void *field, const char *fieldname);
};

enum XMLWriterType{WTStandard, WTEncoding, WTEncodingData64, WTJSON} ;
CommonXmlWriter * CreateCommonXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL, XMLWriterType xmlType=WTStandard);
thorhelper_decl IXmlWriter * createIXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL, XMLWriterType xmlType=WTStandard);

class thorhelper_decl SimpleOutputWriter : public CInterface, implements IXmlWriter
{
    void outputFieldSeparator();
    bool separatorNeeded;
public:
    SimpleOutputWriter();
    IMPLEMENT_IINTERFACE;

    SimpleOutputWriter & clear();
    unsigned length() const                                 { return out.length(); }
    const char * str() const                                { return out.str(); }

    virtual void outputQuoted(const char *text);
    virtual void outputQString(unsigned len, const char *field, const char *fieldname);
    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
    virtual void outputBeginNested(const char *fieldname, bool nestChildren);
    virtual void outputEndNested(const char *fieldname);
    virtual void outputBeginArray(const char *fieldname){}
    virtual void outputEndArray(const char *fieldname){}
    virtual void outputSetAll();

    void newline();
protected:
    StringBuffer out;
};

class thorhelper_decl CommonFieldProcessor : public CInterface, implements IFieldProcessor
{
    bool trim;
    StringBuffer &result;
public:
    IMPLEMENT_IINTERFACE;
    CommonFieldProcessor(StringBuffer &_result, bool _trim=false);
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field);
    virtual void processBool(bool value, const RtlFieldInfo * field);
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field);
    virtual void processInt(__int64 value, const RtlFieldInfo * field);
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field);
    virtual void processReal(double value, const RtlFieldInfo * field);
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field);
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field);
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field);
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field);
    virtual void processSetAll(const RtlFieldInfo * field);
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field);

    virtual bool processBeginSet(const RtlFieldInfo * field);
    virtual bool processBeginDataset(const RtlFieldInfo * field); 
    virtual bool processBeginRow(const RtlFieldInfo * field);
    virtual void processEndSet(const RtlFieldInfo * field);
    virtual void processEndDataset(const RtlFieldInfo * field);
    virtual void processEndRow(const RtlFieldInfo * field);

};

extern thorhelper_decl void printKeyedValues(StringBuffer &out, IIndexReadContext *segs, IOutputMetaData *rowMeta);

extern thorhelper_decl void convertRowToXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags = (unsigned)-1);

#endif // THORXMLWRITE_HPP
