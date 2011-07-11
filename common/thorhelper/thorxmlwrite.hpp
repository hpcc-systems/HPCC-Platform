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

//Writed all encoded DATA fields as base64Binary
class thorhelper_decl CommonEncoded64XmlWriter : public CommonEncodedXmlWriter
{
public:
    CommonEncoded64XmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL);

    virtual void outputData(unsigned len, const void *field, const char *fieldname);
};

enum XMLWriterType{WTStandard, WTEncoding, WTEncodingData64} ;
CommonXmlWriter * CreateCommonXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL, XMLWriterType xmlType=WTStandard);

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
    virtual void outputSetAll();

    void newline();
protected:
    StringBuffer out;
    bool csv;
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
