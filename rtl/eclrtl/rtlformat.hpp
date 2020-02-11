#ifndef ECLFORMAT_HPP
#define ECLFORMAT_HPP


#include "jiface.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"

interface IXmlWriterExt : extends IXmlWriter
{
    virtual IXmlWriterExt & clear() = 0;
    virtual size32_t length() const = 0;
    virtual const char *str() const = 0;

    virtual IInterface *saveLocation() const = 0;
    virtual void rewindTo(IInterface *location) = 0;
    virtual void cutFrom(IInterface *location, StringBuffer& databuf) = 0;
    virtual void outputNumericString(const char *field, const char *fieldname) = 0;
    virtual void outputInline(const char* text) = 0;
    virtual void finalize() = 0;
    virtual void checkDelimiter() = 0;
};

class ECLRTL_API SimpleOutputWriter : implements IXmlWriterExt, public CInterface
{
    void outputFieldSeparator();
    bool separatorNeeded;
public:
    SimpleOutputWriter();
    IMPLEMENT_IINTERFACE;

    virtual SimpleOutputWriter & clear() override;
    virtual size32_t length() const override                { return out.length(); }
    virtual const char * str() const override               { return out.str(); }
    virtual void finalize() override                        {}
    virtual void checkDelimiter() override                  {}


    virtual void outputQuoted(const char *text) override;
    virtual void outputQString(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputString(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputBool(bool field, const char *fieldname) override;
    virtual void outputData(unsigned len, const void *field, const char *fieldname) override;
    virtual void outputReal(double field, const char *fieldname) override;
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) override;
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) override;
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname) override;
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname) override;
    virtual void outputBeginNested(const char *fieldname, bool nestChildren) override;
    virtual void outputEndNested(const char *fieldname) override;
    virtual void outputBeginDataset(const char *dsname, bool nestChildren) override {}
    virtual void outputEndDataset(const char *dsname) override {}
    virtual void outputBeginArray(const char *fieldname) override {}
    virtual void outputEndArray(const char *fieldname) override {}
    virtual void outputSetAll() override;
    virtual void outputInlineXml(const char *text) override {} //for appending raw xml content
    virtual void outputXmlns(const char *name, const char *uri) override {}

    virtual void outputInt(__int64 field, unsigned size, const char *fieldname) override;
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname) override;

    void newline();

    virtual IInterface *saveLocation() const override { UNIMPLEMENTED; }
    virtual void rewindTo(IInterface *location) override { UNIMPLEMENTED; }
    virtual void cutFrom(IInterface *location, StringBuffer& databuf) override { UNIMPLEMENTED; }
    virtual void outputNumericString(const char *field, const char *fieldname) override { UNIMPLEMENTED; }
    virtual void outputInline(const char* text) override { UNIMPLEMENTED; }

protected:
    StringBuffer out;
};


interface IXmlStreamFlusher
{
    virtual void flushXML(StringBuffer &current, bool isClose) = 0;
};

class ECLRTL_API CommonXmlPosition : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CommonXmlPosition(size32_t _pos, unsigned _indent, unsigned _nestLimit, bool _tagClosed, bool _needDelimiter) :
        pos(_pos), indent(_indent), nestLimit(_nestLimit), tagClosed(_tagClosed), needDelimiter(_needDelimiter)
    {}

public:
    size32_t pos = 0;
    unsigned indent = 0;
    unsigned nestLimit = 0;
    bool tagClosed = false;
    bool needDelimiter = false;
};

class ECLRTL_API CommonXmlWriter : implements IXmlWriterExt, public CInterface
{
public:
    CommonXmlWriter(unsigned _flags, unsigned initialIndent=0,  IXmlStreamFlusher *_flusher=NULL);
    ~CommonXmlWriter();
    IMPLEMENT_IINTERFACE;

    void outputBeginNested(const char *fieldname, bool nestChildren, bool doIndent);
    void outputEndNested(const char *fieldname, bool doIndent);

    virtual void outputInlineXml(const char *text){closeTag(); out.append(text); flush(false);} //for appending raw xml content
    virtual void outputInline(const char* text) { outputInlineXml(text); }
    virtual void outputQuoted(const char *text);
    virtual void outputQString(unsigned len, const char *field, const char *fieldname);
    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
    virtual void outputBeginDataset(const char *dsname, bool nestChildren);
    virtual void outputEndDataset(const char *dsname);
    virtual void outputBeginNested(const char *fieldname, bool nestChildren);
    virtual void outputEndNested(const char *fieldname);
    virtual void outputBeginArray(const char *fieldname){}; //repeated elements are inline for xml
    virtual void outputEndArray(const char *fieldname){};
    virtual void outputSetAll();
    virtual void outputXmlns(const char *name, const char *uri);

    //IXmlWriterExt
    virtual IXmlWriterExt & clear();
    virtual unsigned length() const                                 { return out.length(); }
    virtual const char * str() const                                { return out.str(); }
    virtual void finalize() override                                {}
    virtual void checkDelimiter() override                          {}

    virtual IInterface *saveLocation() const
    {
        if (flusher)
            throwUnexpected();

        return new CommonXmlPosition(length(), indent, nestLimit, tagClosed, false);
    }
    virtual void rewindTo(IInterface *saved)
    {
        if (flusher)
            throwUnexpected();

        CommonXmlPosition *position = dynamic_cast<CommonXmlPosition *>(saved);
        if (!position)
            return;
        if (position->pos < out.length())
        {
            out.setLength(position->pos);
            tagClosed = position->tagClosed;
            indent = position->indent;
            nestLimit = position->nestLimit;
        }
    }
    virtual void cutFrom(IInterface *location, StringBuffer& databuf);

    virtual void outputNumericString(const char *field, const char *fieldname)
    {
        outputCString(field, fieldname);
    }

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

class ECLRTL_API CommonJsonWriter : implements IXmlWriterExt, public CInterface
{
public:
    CommonJsonWriter(unsigned _flags, unsigned initialIndent=0,  IXmlStreamFlusher *_flusher=NULL);
    ~CommonJsonWriter();
    IMPLEMENT_IINTERFACE;

    void checkDelimit(int inc=0);
    void checkFormat(bool doDelimit, bool needDelimiter=true, int inc=0);
    void prepareBeginArray(const char *fieldname);

    virtual void outputInlineXml(const char *text) //for appending raw xml content
    {
        if (text && *text)
            outputUtf8(strlen(text), text, "xml");
    }
    virtual void outputInline(const char* text) { out.append(text); }
    virtual void outputQuoted(const char *text);
    virtual void outputQString(unsigned len, const char *field, const char *fieldname);
    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
    virtual void outputBeginDataset(const char *dsname, bool nestChildren);
    virtual void outputEndDataset(const char *dsname);
    virtual void outputBeginNested(const char *fieldname, bool nestChildren);
    virtual void outputEndNested(const char *fieldname);
    virtual void outputBeginArray(const char *fieldname);
    virtual void outputEndArray(const char *fieldname);
    virtual void outputSetAll();
    virtual void outputXmlns(const char *name, const char *uri){}
    virtual void outputNumericString(const char *field, const char *fieldname);

    //IXmlWriterExt
    virtual IXmlWriterExt & clear();
    virtual unsigned length() const                                 { return out.length(); }
    virtual const char * str() const                                { return out.str(); }
    virtual void finalize() override                                {}
    virtual void checkDelimiter() override                          { checkDelimit(); }
    virtual void rewindTo(unsigned int prevlen)                     { if (prevlen < out.length()) out.setLength(prevlen); }
    virtual IInterface *saveLocation() const
    {
        if (flusher)
            throwUnexpected();

        return new CommonXmlPosition(length(), indent, nestLimit, false, needDelimiter);
    }
    virtual void rewindTo(IInterface *saved)
    {
        if (flusher)
            throwUnexpected();

        CommonXmlPosition *position = dynamic_cast<CommonXmlPosition *>(saved);
        if (!position)
            return;
        if (position->pos < out.length())
        {
            out.setLength(position->pos);
            needDelimiter = position->needDelimiter;
            indent = position->indent;
            nestLimit = position->nestLimit;
        }
    }
    virtual void cutFrom(IInterface *location, StringBuffer& databuf);

    void outputBeginRoot(){out.append('{');}
    void outputEndRoot(){out.append('}');}

protected:
    inline void flush(bool isClose)
    {
        if (flusher)
            flusher->flushXML(out, isClose);
    }

    class CJsonWriterItem : public CInterface
    {
    public:
        CJsonWriterItem(const char *_name) : name(_name), depth(0){}

        StringAttr name;
        unsigned depth;
    };

    const char *checkItemName(CJsonWriterItem *item, const char *name, bool simpleType=true);
    const char *checkItemName(const char *name, bool simpleType=true);
    const char *checkItemNameBeginNested(const char *name);
    const char *checkItemNameEndNested(const char *name);
    bool checkUnamedArrayItem(bool begin);


    IXmlStreamFlusher *flusher;
    CIArrayOf<CJsonWriterItem> arrays;
    StringBuffer out;
    unsigned flags;
    unsigned indent;
    unsigned nestLimit;
    bool needDelimiter;
};

class ECLRTL_API CommonJsonObjectWriter : public CommonJsonWriter
{
public:
    CommonJsonObjectWriter(unsigned _flags, unsigned initialIndent,  IXmlStreamFlusher *_flusher) : CommonJsonWriter(_flags, initialIndent, _flusher)
    {
        outputBeginRoot();
    }
    ~CommonJsonObjectWriter()
    {
        if (!final)
            outputEndRoot();
    }
    virtual void finalize() override
    {
        if (!final)
        {
            final=true;
            outputEndRoot();
        }
    }

private:
    bool final=false;
};

class ECLRTL_API CPropertyTreeWriter : public CSimpleInterfaceOf<IXmlWriter>
{
public:
    CPropertyTreeWriter(IPropertyTree *_root=nullptr, unsigned _flags=0);

    void setRoot(IPropertyTree &_root);

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
    virtual void outputBeginArray(const char *fieldname)  override {}; //repeated elements are inline for xml
    virtual void outputEndArray(const char *fieldname) override {};
    virtual void outputSetAll();
    virtual void outputXmlns(const char *name, const char *uri);

protected:
    bool checkForAttribute(const char * fieldname);

protected:
    Linked<IPropertyTree> root;
    IPropertyTree *target = nullptr;
    ICopyArrayOf<IPropertyTree> nestedLevels;
    unsigned flags;
};


ECLRTL_API StringBuffer &buildJsonHeader(StringBuffer  &header, const char *suppliedHeader, const char *rowTag);
ECLRTL_API StringBuffer &buildJsonFooter(StringBuffer  &footer, const char *suppliedFooter, const char *rowTag);

//Writes type encoded XML strings  (xsi:type="xsd:string", xsi:type="xsd:boolean" etc)
class ECLRTL_API CommonEncodedXmlWriter : public CommonXmlWriter
{
public:
    CommonEncodedXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL);

    virtual void outputString(unsigned len, const char *field, const char *fieldname);
    virtual void outputBool(bool field, const char *fieldname);
    virtual void outputData(unsigned len, const void *field, const char *fieldname);
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname);
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname);
    virtual void outputReal(double field, const char *fieldname);
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname);
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname);
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname);
};

//Writes all encoded DATA fields as base64Binary
class ECLRTL_API CommonEncoded64XmlWriter : public CommonEncodedXmlWriter
{
public:
    CommonEncoded64XmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL);

    virtual void outputData(unsigned len, const void *field, const char *fieldname);
};

enum XMLWriterType{WTStandard, WTEncoding, WTEncodingData64, WTJSONObject, WTJSONRootless} ;
ECLRTL_API CommonXmlWriter * CreateCommonXmlWriter(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL, XMLWriterType xmlType=WTStandard);
ECLRTL_API IXmlWriterExt * createIXmlWriterExt(unsigned _flags, unsigned initialIndent=0, IXmlStreamFlusher *_flusher=NULL, XMLWriterType xmlType=WTStandard);

struct CSVOptions
{
    StringAttr delimiter, terminator;
    bool includeHeader;
};

class CCSVItem : public CInterface, implements IInterface
{
    unsigned columnID, nextRowID, rowCount, nestedLayer;
    StringAttr name, type, value, parentXPath;
    StringArray childNames;
    MapStringTo<bool> childNameMap;
    bool isNestedItem, simpleNested, currentRowEmpty, outputHeader = false;
public:
    CCSVItem() : columnID(0), nextRowID(0), rowCount(0), nestedLayer(0), isNestedItem(false),
        simpleNested(false), currentRowEmpty(true) { };

    IMPLEMENT_IINTERFACE;
    inline const char* getName() const { return name.get(); };
    inline void setName(const char* _name) { name.set(_name); };
    inline const char* getValue() const { return value.get(); };
    inline void setValue(const char* _value) { value.set(_value); };
    inline unsigned getColumnID() const { return columnID; };
    inline void setColumnID(unsigned _columnID) { columnID = _columnID; };

    inline unsigned getNextRowID() const { return nextRowID; };
    inline void setNextRowID(unsigned _rowID) { nextRowID = _rowID; };
    inline void incrementNextRowID() { nextRowID++; };
    inline unsigned getRowCount() const { return rowCount; };
    inline void setRowCount(unsigned _rowCount) { rowCount = _rowCount; };
    inline void incrementRowCount() { rowCount++; };
    inline bool getCurrentRowEmpty() const { return currentRowEmpty; };
    inline void setCurrentRowEmpty(bool _currentRowEmpty) { currentRowEmpty = _currentRowEmpty; };

    inline unsigned getNestedLayer() const { return nestedLayer; };
    inline void setNestedLayer(unsigned _nestedLayer) { nestedLayer = _nestedLayer; };
    inline bool checkIsNestedItem() const { return isNestedItem; };
    inline void setIsNestedItem(bool _isNestedItem) { isNestedItem = _isNestedItem; };
    inline bool checkSimpleNested() const { return simpleNested; };
    inline void setSimpleNested(bool _simpleNested) { simpleNested = _simpleNested; };
    inline bool checkOutputHeader() const { return outputHeader; };
    inline void setOutputHeader(bool _outputHeader) { outputHeader = _outputHeader; };
    inline const char* getParentXPath() const { return parentXPath.str(); };
    inline void setParentXPath(const char* _parentXPath) { parentXPath.set(_parentXPath); };
    inline StringArray& getChildrenNames() { return childNames; };
    inline void addChildName(const char* name)
    {
        if (hasChildName(name))
            return;
        childNameMap.setValue(name, true);
        childNames.append(name);
    };
    inline bool hasChildName(const char* name)
    {
        bool* found = childNameMap.getValue(name);
        return (found && *found);
    };
    inline void clearContentVariables()
    {
        nextRowID = rowCount = 0;
        currentRowEmpty = true;
    };
};

class CCSVRow : public CInterface, implements IInterface
{
    unsigned rowID;
    CIArrayOf<CCSVItem> columns;
public:
    CCSVRow(unsigned _rowID) : rowID(_rowID) {};
    IMPLEMENT_IINTERFACE;

    inline unsigned getRowID() const { return rowID; };
    inline void setRowID(unsigned _rowID) { rowID = _rowID; };
    inline unsigned getColumnCount() const { return columns.length(); };

    const char* getColumnValue(unsigned columnID) const;
    void setColumn(unsigned columnID, const char* columnName, const char* columnValue);
};

//CommonCSVWriter is used to output a WU result in CSV format.
//Read CSV header information;
//If needed, output CSV headers into the 'out' buffer;
//Read each row (a record) of the WU result and output into the 'out' buffer;
//The 'out' buffer can be accessed through the str() method.
class ECLRTL_API CommonCSVWriter: public CInterface, implements IXmlWriterExt
{
    class CXPathItem : public CInterface, implements IInterface
    {
        bool isArray;
        StringAttr path;
    public:
        CXPathItem(const char* _path, bool _isArray) : isArray(_isArray), path(_path) { };

        IMPLEMENT_IINTERFACE;
        inline const char* getPath() const { return path.get(); };
        inline bool getIsArray() const { return isArray; };
    };
    CSVOptions options;
    bool readingCSVHeader, addingSimpleNestedContent;
    unsigned recordCount, headerColumnID, nestedHeaderLayerID;
    StringBuffer currentParentXPath, auditOut;
    StringArray headerXPathList;
    MapStringTo<bool> topHeaderNameMap;
    MapStringToMyClass<CCSVItem> csvItems;
    CIArrayOf<CCSVRow> contentRowsBuffer;
    CIArrayOf<CXPathItem> dataXPath;//xpath in caller

    void escapeQuoted(unsigned len, char const* in, StringBuffer& out);
    bool checkHeaderName(const char* name);
    CCSVItem* getParentCSVItem();
    CCSVItem* getCSVItemByFieldName(const char* name);
    void addColumnToRow(CIArrayOf<CCSVRow>& rows, unsigned rowID, unsigned colID, const char* columnValue, const char* columnName);
    void addCSVHeader(const char* name, const char* type, bool isNested, bool simpleNested, bool outputHeader);
    void addContentField(const char* field, const char* fieldName);
    void addStringField(unsigned len, const char* field, const char* fieldName);
    void setChildrenNextRowID(const char* path, unsigned rowID);
    unsigned getChildrenMaxNextRowID(const char* path);
    unsigned getChildrenMaxColumnID(CCSVItem* item, unsigned& maxColumnID);
    void addChildNameToParentCSVItem(const char* name);
    void setParentItemRowEmpty(CCSVItem* item, bool empty);
    void addFieldToParentXPath(const char* fieldName);
    void removeFieldFromCurrentParentXPath(const char* fieldName);
    void appendDataXPathItem(const char* fieldName, bool isArray);
    bool isDataRow(const char* fieldName);
    void outputCSVRows(CIArrayOf<CCSVRow>& rows, bool isHeader);
    void outputHeadersToBuffer();
    void finishContentResultRow();

    void auditHeaderInfo()
    {
        ForEachItemIn(i, headerXPathList)
        {
            const char* path = headerXPathList.item(i);
            CCSVItem* item = csvItems.getValue(path);
            if (!item)
                continue;
            if (!item->checkIsNestedItem())
            {
                auditOut.appendf("dumpHeaderInfo path<%s> next row<%d> col<%d>: name<%s> - value<%s>\n", path, item->getNextRowID(),
                    item->getColumnID(), item->getName() ? item->getName() : "", item->getValue() ? item->getValue() : "");
            }
            else
            {
                auditOut.appendf("dumpHeaderInfo path<%s> next row<%d> col<%d>: name<%s> - value<%s>\n", path, item->getNextRowID(),
                    item->getColumnID(), item->getName() ? item->getName() : "", item->getValue() ? item->getValue() : "");
            }
        }
    }

public:
    CommonCSVWriter(unsigned _flags, CSVOptions& _options, IXmlStreamFlusher* _flusher = NULL);
    ~CommonCSVWriter();

    IMPLEMENT_IINTERFACE;

    inline void flush(bool isClose)
    {
        if (flusher)
            flusher->flushXML(out, isClose);
    }

    virtual unsigned length() const { return out.length(); }
    virtual const char* str() const { return out.str(); }
    virtual void finalize() override {}
    virtual void checkDelimiter() override {}

    virtual void rewindTo(IInterface* location) { };
    virtual void cutFrom(IInterface *location, StringBuffer& databuf) { };
    virtual IInterface* saveLocation() const
    {
        if (flusher)
            throwUnexpected();
        return NULL;
    };

    //IXmlWriter
    virtual void outputString(unsigned len, const char* field, const char* fieldName);
    virtual void outputBool(bool field, const char* fieldName);
    virtual void outputData(unsigned len, const void* field, const char* fieldName);
    virtual void outputInt(__int64 field, unsigned size, const char* fieldName);
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char* fieldName);
    virtual void outputReal(double field, const char *fieldName);
    virtual void outputDecimal(const void* field, unsigned size, unsigned precision, const char* fieldName);
    virtual void outputUDecimal(const void* field, unsigned size, unsigned precision, const char* fieldName);
    virtual void outputUnicode(unsigned len, const UChar* field, const char* fieldName);
    virtual void outputQString(unsigned len, const char* field, const char* fieldName);
    virtual void outputUtf8(unsigned len, const char* field, const char* fieldName);
    virtual void outputBeginNested(const char* fieldName, bool simpleNested);
    virtual void outputEndNested(const char* fieldName);
    virtual void outputBeginDataset(const char* dsname, bool nestChildren);
    virtual void outputEndDataset(const char* dsname);
    virtual void outputBeginArray(const char* fieldName);
    virtual void outputEndArray(const char* fieldName);
    virtual void outputSetAll() { };
    virtual void outputXmlns(const char* name, const char* uri) { };
    virtual void outputQuoted(const char* text)
    {
        //No fieldName. Is it valid for CSV?
    };
    virtual void outputInlineXml(const char* text)//for appending raw xml content
    {
        //Dynamically add a new header 'xml' and insert the header.
        //But, not sure we want to do that for a big WU result.
        //if (text && *text)
          //outputUtf8(strlen(text), text, "xml");
    };
    virtual void outputInline(const char* text) { out.append(text); }

    //IXmlWriterExt
    virtual void outputNumericString(const char* field, const char* fieldName);
    virtual IXmlWriterExt& clear();

    void outputBeginNested(const char* fieldName, bool simpleNested, bool outputHeader);
    void outputEndNested(const char* fieldName, bool outputHeader);
    void outputCSVHeader(const char* name, const char* type);
    void finishCSVHeaders();
    const char* auditStr() const { return auditOut.str(); }

protected:
    IXmlStreamFlusher* flusher;
    StringBuffer out;
    unsigned flags;
};

#endif
