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

#ifndef THORXMLWRITE_HPP
#define THORXMLWRITE_HPP

#ifdef THORHELPER_EXPORTS
 #define thorhelper_decl DECL_EXPORT
#else
 #define thorhelper_decl DECL_IMPORT
#endif

#include "eclhelper.hpp"
#include "jptree.hpp"
#include "thorhelper.hpp"


class thorhelper_decl CommonFieldProcessor : implements IFieldProcessor, public CInterface
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
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field);

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data);
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows);
    virtual bool processBeginRow(const RtlFieldInfo * field);
    virtual void processEndSet(const RtlFieldInfo * field);
    virtual void processEndDataset(const RtlFieldInfo * field);
    virtual void processEndRow(const RtlFieldInfo * field);

};

class thorhelper_decl PropertyTreeXmlWriter : implements CInterfaceOf<IXmlWriter>
{
public:
    PropertyTreeXmlWriter(IPropertyTree * _root) : root(_root) {}

    virtual void outputInlineXml(const char *text) override;
    virtual void outputQuoted(const char *text) override;
    virtual void outputQString(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputString(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputBool(bool field, const char *fieldname) override;
    virtual void outputData(unsigned len, const void *field, const char *fieldname) override;
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname) override;
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname) override;
    virtual void outputReal(double field, const char *fieldname) override;
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) override;
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) override;
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname) override;
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputBeginDataset(const char *dsname, bool nestChildren) override;
    virtual void outputEndDataset(const char *dsname) override;
    virtual void outputBeginNested(const char *fieldname, bool nestChildren) override;
    virtual void outputEndNested(const char *fieldname) override;
    virtual void outputBeginArray(const char *fieldname) override; //repeated elements are inline for xml
    virtual void outputEndArray(const char *fieldname) override;
    virtual void outputSetAll() override;
    virtual void outputXmlns(const char *name, const char *uri) override;

protected:
    void outputLiteralString(size32_t size, const char *value, const char *fieldname);

protected:
    IPropertyTree * root;
    ICopyArrayOf<IPropertyTree> stack;
};



extern thorhelper_decl void printKeyedValues(StringBuffer &out, IIndexReadContext *segs, IOutputMetaData *rowMeta);

extern thorhelper_decl void convertRowToXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags = (unsigned)-1);
extern thorhelper_decl void convertRowToJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags = (unsigned)-1);


#endif // THORXMLWRITE_HPP
