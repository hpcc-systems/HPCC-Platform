#include "jiface.hpp"
#include "jbuff.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "junicode.hpp"
#include "rtlbcd.hpp"
#include "rtlformat.hpp"


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

void SimpleOutputWriter::outputInt(__int64 field, unsigned size, const char *)
{
    outputFieldSeparator();
    outputXmlInt(field, NULL, out);
}

void SimpleOutputWriter::outputUInt(unsigned __int64 field, unsigned size, const char *)
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

IXmlWriterExt & CommonXmlWriter::clear()
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

void CommonXmlWriter::cutFrom(IInterface *location, StringBuffer& databuf)
{
    if (flusher)
        throwUnexpected();

    CommonXmlPosition *position = dynamic_cast<CommonXmlPosition *>(location);
    if (!position)
        return;
    if (position->pos < out.length())
    {
        size32_t startInd = position->pos;
        if (!position->tagClosed && out.charAt(startInd) == '>')
            startInd += 1;
        if (!position->nestLimit && startInd < out.length() && out.charAt(startInd) == '\n')
            startInd += 1;
        if (startInd < out.length())
            databuf.append(out.length() - startInd, out.str() + startInd);
        out.setLength(position->pos);
        tagClosed = position->tagClosed;
        indent = position->indent;
        nestLimit = position->nestLimit;
    }
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
//    outputString(len, temp, fieldname, isnumeric);
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

void CommonXmlWriter::outputInt(__int64 field, unsigned size, const char *fieldname)
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

void CommonXmlWriter::outputUInt(unsigned __int64 field, unsigned size, const char *fieldname)
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
    StringBuffer fieldname;
    if (!streq(name, "xmlns"))
        fieldname.append("xmlns:");
    outputXmlAttrUtf8(rtlUtf8Length(strlen(uri), uri), uri, fieldname.append(name), out);
}

void CommonXmlWriter::outputBeginDataset(const char *dsname, bool nestChildren)
{
    outputBeginNested("Dataset", nestChildren, false); //indent row, not dataset for backward compatibility
    if (nestChildren && indent==0)
        indent++;
    if (!dsname || !*dsname)
        return;
    out.append(" name='"); //single quote for backward compatibility
    outputXmlUtf8(rtlUtf8Length(strlen(dsname), dsname), dsname, NULL, out);
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

IXmlWriterExt & CommonJsonWriter::clear()
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

bool CommonJsonWriter::checkUnamedArrayItem(bool begin)
{
    CJsonWriterItem *item = (arrays.length()) ? &arrays.tos() : NULL;
    if (item && item->depth==(begin ? 0 : 1) && item->name.isEmpty())
        return true;
    return false;
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

void CommonJsonWriter::outputNumericString(const char *field, const char *fieldname)
{
    unsigned len = (size32_t)strlen(field);

    if (flags & XWFtrim)
        len = rtlTrimStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimStrLen(len, field) == 0))
        return;
    checkDelimit();
    appendJSONStringValue(out, checkItemName(fieldname), len, field, true, false);
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

void CommonJsonWriter::outputInt(__int64 field, unsigned size, const char *fieldname)
{
    checkDelimit();
    if (size < 7) //JavaScript only supports 53 significant bits
        appendJSONValue(out, checkItemName(fieldname), field);
    else
    {
        appendJSONNameOrDelimit(out, checkItemName(fieldname));
        out.append('"').append(field).append('"');
    }
}
void CommonJsonWriter::outputUInt(unsigned __int64 field, unsigned size, const char *fieldname)
{
    checkDelimit();
    if (size < 7) //JavaScript doesn't support unsigned, and only supports 53 significant bits
        appendJSONValue(out, checkItemName(fieldname), field);
    else
    {
        appendJSONNameOrDelimit(out, checkItemName(fieldname));
        out.append('"').append(field).append('"');
    }
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
    appendJSONStringValue(out, checkItemName(fieldname), rtlUtf8Size(len, field), field, true);
}

void CommonJsonWriter::prepareBeginArray(const char *fieldname)
{
    arrays.append(*new CJsonWriterItem(fieldname));
    const char * sep = (fieldname) ? strchr(fieldname, '/') : NULL;
    while (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        appendJSONName(out, leading).append(" {");
        fieldname = sep+1;
        sep = strchr(fieldname, '/');
    }
    checkFormat(false, false, 1);
}

void CommonJsonWriter::outputBeginArray(const char *fieldname)
{
    prepareBeginArray(fieldname);
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

void CommonJsonWriter::cutFrom(IInterface *location, StringBuffer& databuf)
{
    if (flusher)
        throwUnexpected();

    CommonXmlPosition *position = dynamic_cast<CommonXmlPosition *>(location);
    if (!position)
        return;
    if (position->pos < out.length())
    {
        databuf.append(out.length() - position->pos, out.str() + position->pos);
        out.setLength(position->pos);
        needDelimiter = position->needDelimiter;
        indent = position->indent;
        nestLimit = position->nestLimit;
    }
}

void CommonJsonWriter::outputBeginNested(const char *fieldname, bool nestChildren)
{
    if (!fieldname)
        return;
    if (!*fieldname && !checkUnamedArrayItem(true))
        return;

    checkFormat(true, false, 1);
    fieldname = checkItemNameBeginNested(fieldname);
    if (fieldname && *fieldname)
    {
        const char * sep = strchr(fieldname, '/');
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
    if (!fieldname)
        return;
    if (!*fieldname && !checkUnamedArrayItem(false))
        return;

    checkFormat(false, true, -1);
    fieldname = checkItemNameEndNested(fieldname);
    if (fieldname && *fieldname)
    {
        const char * sep = strchr(fieldname, '/');
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
    checkDelimit();
    appendJSONValue(out, "All", true);
}


//=====================================================================================


CPropertyTreeWriter::CPropertyTreeWriter(IPropertyTree *_root, unsigned _flags) : root(_root), flags(_flags)
{
    target = root;
}

void CPropertyTreeWriter::setRoot(IPropertyTree &_root)
{
    nestedLevels.clear();
    root.set(&_root);
    target = root;
}

void CPropertyTreeWriter::outputInlineXml(const char *text)
{
    Owned<IPropertyTree> xmlTree = createPTreeFromXMLString(text);
    target->addPropTree(xmlTree->queryName(), xmlTree.getLink());
}

void CPropertyTreeWriter::outputQuoted(const char *text)
{
    target->setProp(nullptr, text);
}

void CPropertyTreeWriter::outputString(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimStrLen(len, field) == 0))
        return;
    StringBuffer tmp(len, field);
    target->setProp(fieldname, tmp.str());
}


void CPropertyTreeWriter::outputQString(unsigned len, const char *field, const char *fieldname)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
//    outputString(len, temp, fieldname, isnumeric);
    outputString(len, temp, fieldname);
}


void CPropertyTreeWriter::outputBool(bool field, const char *fieldname)
{
    target->addPropBool(fieldname, field);
}

void CPropertyTreeWriter::outputData(unsigned len, const void *field, const char *fieldname)
{
    target->addPropBin(fieldname, len, field);
}

void CPropertyTreeWriter::outputInt(__int64 field, unsigned size, const char *fieldname)
{
    target->addPropInt64(fieldname, field);
}

void CPropertyTreeWriter::outputUInt(unsigned __int64 field, unsigned size, const char *fieldname)
{
    target->addPropInt64(fieldname, field);
}

void CPropertyTreeWriter::outputReal(double field, const char *fieldname)
{
    StringBuffer fieldStr;
    target->setProp(fieldname, fieldStr.append(field).str());
}

void CPropertyTreeWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    StringBuffer fieldStr;
    outputXmlDecimal(field, size, precision, nullptr, fieldStr);
    target->setProp(fieldname, fieldStr);
}

void CPropertyTreeWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
{
    StringBuffer fieldStr;
    outputXmlUDecimal(field, size, precision, nullptr, fieldStr);
    target->setProp(fieldname, fieldStr);
}

void CPropertyTreeWriter::outputUnicode(unsigned len, const UChar *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUnicodeStrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUnicodeStrLen(len, field) == 0))
        return;
    StringBuffer fieldStr;
    outputXmlUnicode(len, field, fieldname, fieldStr);
    target->setProp(fieldname, fieldStr);
}

void CPropertyTreeWriter::outputUtf8(unsigned len, const char *field, const char *fieldname)
{
    if (flags & XWFtrim)
        len = rtlTrimUtf8StrLen(len, field);
    if ((flags & XWFopt) && (rtlTrimUtf8StrLen(len, field) == 0))
        return;
    StringBuffer fieldStr;
    outputXmlUtf8(len, field, nullptr, fieldStr);
    target->setProp(fieldname, fieldStr);
}

void CPropertyTreeWriter::outputXmlns(const char *name, const char *uri)
{
    StringBuffer fieldname;
    if (!streq(name, "xmlns"))
        fieldname.append("xmlns:");
    StringBuffer fieldStr;
    outputXmlUtf8(rtlUtf8Length(strlen(uri), uri), uri, nullptr, fieldStr);
    target->setProp(fieldname, fieldStr);
}

void CPropertyTreeWriter::outputBeginDataset(const char *dsname, bool nestChildren)
{
    outputBeginNested("Dataset", nestChildren);
    if (!dsname || !*dsname)
        return;
    StringBuffer dsNameUtf8;
    outputXmlUtf8(rtlUtf8Length(strlen(dsname), dsname), dsname, nullptr, dsNameUtf8);
    target->addProp("@name", dsNameUtf8);
}

void CPropertyTreeWriter::outputEndDataset(const char *dsname)
{
    outputEndNested("Dataset");
}

void CPropertyTreeWriter::outputBeginNested(const char *fieldname, bool nestChildren)
{
    if (!fieldname || !*fieldname)
        return;

    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputBeginNested(leading, nestChildren);
        outputBeginNested(sep+1, nestChildren);
        return;
    }

    nestedLevels.append(*target);
    target = target->addPropTree(fieldname);
}

void CPropertyTreeWriter::outputEndNested(const char *fieldname)
{
    if (!fieldname || !*fieldname)
        return;

    const char * sep = strchr(fieldname, '/');
    if (sep)
    {
        StringAttr leading(fieldname, sep-fieldname);
        outputEndNested(sep+1);
        outputEndNested(leading);
        return;
    }
    target = &nestedLevels.popGet();
}

void CPropertyTreeWriter::outputSetAll()
{
    // JCS->GH what's this for?
    target->addPropTree("All");
}

//=====================================================================================


StringBuffer &buildJsonHeader(StringBuffer  &header, const char *suppliedHeader, const char *rowTag)
{
    if (suppliedHeader)
    {
        header.append(suppliedHeader);
        if (rowTag && *rowTag)
            appendJSONName(header, rowTag).append('[');
        return header;
    }

    if (rowTag && *rowTag)
    {
        header.append('{');
        appendJSONName(header, rowTag);
    }

    return header.append('[');
}

StringBuffer &buildJsonFooter(StringBuffer  &footer, const char *suppliedFooter, const char *rowTag)
{
    if (suppliedFooter)
    {
        if (rowTag && *rowTag)
            footer.append(']');
        footer.append(suppliedFooter);
        return footer;
    }
    return footer.append((rowTag && *rowTag) ? "]}" : "]");
}

static char thorHelperhexchar[] = "0123456789ABCDEF";
//=====================================================================================

static char csvQuote = '\"';

CommonCSVWriter::CommonCSVWriter(unsigned _flags, CSVOptions& _options, IXmlStreamFlusher* _flusher)
{
    flusher = _flusher;
    flags = _flags;

    options.terminator.set(_options.terminator.get());
    options.delimiter.set(_options.delimiter.get());
    options.includeHeader = _options.includeHeader;  //output CSV headers
    recordCount = headerColumnID = 0;
    nestedHeaderLayerID = 0;
    readingCSVHeader = true;
    addingSimpleNestedContent = false; //Set by CommonCSVWriter::checkHeaderName()
}

CommonCSVWriter::~CommonCSVWriter()
{
    flush(true);
}

void CommonCSVWriter::outputString(unsigned len, const char* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;
    addStringField(len, field, fieldName);
}

void CommonCSVWriter::outputBool(bool field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;
    addContentField((field) ? "true" : "false", fieldName);
}

void CommonCSVWriter::outputData(unsigned len, const void* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    const unsigned char *value = (const unsigned char *) field;
    for (unsigned int i = 0; i < len; i++)
        v.append(thorHelperhexchar[value[i] >> 4]).append(thorHelperhexchar[value[i] & 0x0f]);
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputInt(__int64 field, unsigned size, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    v.append(field);
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputUInt(unsigned __int64 field, unsigned size, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    v.append(field);
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputReal(double field, const char *fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    v.append(field);
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputDecimal(const void* field, unsigned size, unsigned precision, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    char dec[50];
    BcdCriticalBlock bcdBlock;
    if (DecValid(true, size*2-1, field))
    {
        DecPushDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        v.append(finger);
    }
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputUDecimal(const void* field, unsigned size, unsigned precision, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    char dec[50];
    BcdCriticalBlock bcdBlock;
    if (DecValid(false, size*2, field))
    {
        DecPushUDecimal(field, size, precision);
        DecPopCString(sizeof(dec), dec);
        const char *finger = dec;
        while(isspace(*finger)) finger++;
        v.append(finger);
    }
    addContentField(v.str(), fieldName);
}

void CommonCSVWriter::outputUnicode(unsigned len, const UChar* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    StringBuffer v;
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
    addStringField(bufflen, buff, fieldName);
    rtlFree(buff);
}

void CommonCSVWriter::outputQString(unsigned len, const char* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
    addStringField(len, temp, fieldName);
}

void CommonCSVWriter::outputUtf8(unsigned len, const char* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    addStringField(rtlUtf8Size(len, field), field, fieldName);
}

void CommonCSVWriter::outputNumericString(const char* field, const char* fieldName)
{
    if (!checkHeaderName(fieldName))
        return;

    addStringField((size32_t)strlen(field), field, fieldName);
}

void CommonCSVWriter::appendDataXPathItem(const char* fieldName, bool isArray)
{
    Owned<CXPathItem> item = new CXPathItem(fieldName, isArray);
    dataXPath.append(*item.getClear());
}

bool CommonCSVWriter::isDataRow(const char* fieldName)
{
    if (dataXPath.empty())
        return false;

    CXPathItem& xPathItem = dataXPath.item(dataXPath.length() - 1);
    return xPathItem.getIsArray() && strieq(fieldName, xPathItem.getPath());
}

void CommonCSVWriter::outputBeginNested(const char* fieldName, bool simpleNested, bool outputHeader)
{
    //This method is called when retrieving csv headers.
    if (!fieldName || !*fieldName || !readingCSVHeader)
        return;

    addCSVHeader(fieldName, NULL, true, simpleNested, outputHeader);
    if (simpleNested) //ECL SET has only one column (parent name should be used as column name).
        headerColumnID++;

    //nestedHeaderLayerID is used as row ID when output CSV headers.
    if (outputHeader)
        nestedHeaderLayerID++;
    addFieldToParentXPath(fieldName);
}

void CommonCSVWriter::outputEndNested(const char* fieldName, bool outputHeader)
{
    //This method is called when retrieving csv headers.
    if (!fieldName || !*fieldName || !readingCSVHeader)
        return;

    removeFieldFromCurrentParentXPath(fieldName);
    if (outputHeader)
        nestedHeaderLayerID--;
}

void CommonCSVWriter::outputBeginNested(const char* fieldName, bool simpleNested)
{
    if (!fieldName || !*fieldName || readingCSVHeader)
        return;

    if (!isDataRow(fieldName))
    {//A nested item begins.
        //Call appendDataXPathItem() after the isDataRpw()
        //because previous data xpath is used in isDataRpw().
        appendDataXPathItem(fieldName, false);
        addFieldToParentXPath(fieldName);
    }
    else
    {//A new row begins inside a nested item.
        appendDataXPathItem(fieldName, false);

        if (!currentParentXPath.isEmpty())
        {
            //Add row xpath if it is not the 1st xpath.
            addFieldToParentXPath(fieldName);

            CCSVItem* item = getParentCSVItem();
            if (!item)
                return;

            //Check row count for the ParentCSVItem.
            //If this is not the first row, all children of the ParentCSVItem should
            //start from the MaxNextRowID of the last row.
            unsigned rowCount = item->getRowCount();
            if (rowCount > 0)
            {//Starting from the second result row, the NextRowIDs of every children are reset based on the last result row.
                StringBuffer path(currentParentXPath);
                path.setLength(path.length() - 1);
                setChildrenNextRowID(path.str(), getChildrenMaxNextRowID(path.str()));
            }

            item->setCurrentRowEmpty(true);
        }
    }
}

void CommonCSVWriter::outputEndNested(const char* fieldName)
{
    if (!fieldName || !*fieldName || readingCSVHeader)
        return;

    dataXPath.pop();
    if (!isDataRow(fieldName))
    {//This is an end of a nested item.
        removeFieldFromCurrentParentXPath(fieldName);
    }
    else
    {//A row ends inside the nested item
        //Set row count for ParentCSVItem of this field.
        if (!currentParentXPath.isEmpty())
        {
            CCSVItem* item = getParentCSVItem();
            if (item && !item->getCurrentRowEmpty())
            {
                //Increase row count for this item
                item->incrementRowCount();
                item->setCurrentRowEmpty(true);
            }
        }

        removeFieldFromCurrentParentXPath(fieldName);
        //if dataXPath.length() back to 1, this should be the end of a content result row.
        if (dataXPath.length() == 1)
            finishContentResultRow();
    }
}

void CommonCSVWriter::outputBeginArray(const char* fieldName)
{
    appendDataXPathItem(fieldName, true);
};

void CommonCSVWriter::outputEndArray(const char* fieldName)
{
    dataXPath.pop();
};

void CommonCSVWriter::outputBeginDataset(const char* dsname, bool nestChildren)
{
    //This is called to add a <Dataset> tag outside of a wu result xml. No need for csv.
};

void CommonCSVWriter::outputEndDataset(const char* dsname)
{
};

IXmlWriterExt& CommonCSVWriter::clear()
{
    recordCount = /*rowCount =*/ headerColumnID = 0;
    nestedHeaderLayerID = 0;
    readingCSVHeader = true;

    addingSimpleNestedContent = false;
    currentParentXPath.clear();
    headerXPathList.kill();
    topHeaderNameMap.kill();
    contentRowsBuffer.clear();
    csvItems.kill();
    out.clear();
    auditOut.clear();
    return *this;
};

void CommonCSVWriter::outputCSVHeader(const char* name, const char* type)
{
    if (!name || !*name)
        return;

    addCSVHeader(name, type, false, false, true);
    headerColumnID++;
}

void CommonCSVWriter::finishCSVHeaders()
{
    if (options.includeHeader)
        outputHeadersToBuffer();
    readingCSVHeader = false;
    currentParentXPath.clear();

#ifdef _DEBUG
    auditHeaderInfo();
#endif
}

void CommonCSVWriter::outputHeadersToBuffer()
{
    CIArrayOf<CCSVRow> rows;
    ForEachItemIn(i, headerXPathList)
    {
        const char* path = headerXPathList.item(i);
        CCSVItem* item = csvItems.getValue(path);
        if (!item || !item->checkOutputHeader())
            continue;

        unsigned colID = item->getColumnID();
        if (item->checkIsNestedItem())
        {
            unsigned maxColumnID = colID;
            getChildrenMaxColumnID(item, maxColumnID);
            colID += (maxColumnID - colID)/2;
        }
        addColumnToRow(rows, item->getNestedLayer(), colID, item->getName(), NULL);
    }

    outputCSVRows(rows, true);
}

//Go through every children to find out MaxColumnID.
unsigned CommonCSVWriter::getChildrenMaxColumnID(CCSVItem* item, unsigned& maxColumnID)
{
    StringBuffer path(item->getParentXPath());
    path.append(item->getName());

    StringArray& names = item->getChildrenNames();
    ForEachItemIn(i, names)
    {
        StringBuffer childPath(path);
        childPath.append("/").append(names.item(i));
        CCSVItem* childItem = csvItems.getValue(childPath.str());
        if (!childItem)
            continue;

        if (childItem->checkIsNestedItem())
            maxColumnID = getChildrenMaxColumnID(childItem, maxColumnID);
        else
        {
            unsigned columnID = childItem->getColumnID();
            if (columnID > maxColumnID)
                maxColumnID = columnID;
        }
    }
    return maxColumnID;
}

void CommonCSVWriter::escapeQuoted(unsigned len, char const* in, StringBuffer& out)
{
    char const* finger = in;
    while (len--)
    {
        //RFC-4180, paragraph "If double-quotes are used to enclose fields, then a double-quote
        //appearing inside a field must be escaped by preceding it with another double quote."
        //unsigned newLen = 0;
        if (*finger == '"')
            out.append('"');
        out.append(*finger);
        finger++;
    }
}

CCSVItem* CommonCSVWriter::getParentCSVItem()
{
    if (currentParentXPath.isEmpty())
        return NULL;

    StringBuffer path(currentParentXPath);
    path.setLength(path.length() - 1);
    return csvItems.getValue(path.str());
}

CCSVItem* CommonCSVWriter::getCSVItemByFieldName(const char* name)
{
    StringBuffer path;
    if (currentParentXPath.isEmpty())
        path.append(name);
    else
        path.append(currentParentXPath.str()).append(name);
    return csvItems.getValue(path.str());
}

bool CommonCSVWriter::checkHeaderName(const char* name)
{
    if (!name || !*name)
        return false;

    addingSimpleNestedContent = false; //Initialize to false.
    if (currentParentXPath.isEmpty())
    {
        bool* found = topHeaderNameMap.getValue(name);
        return (found && *found);
    }

    CCSVItem* item = getParentCSVItem();
    if (!item)
        return false;

    addingSimpleNestedContent = item->checkSimpleNested();
    if (addingSimpleNestedContent) //ECL: SET OF string, int, etc
        return true;

    return item->hasChildName(name);
}

void CommonCSVWriter::addColumnToRow(CIArrayOf<CCSVRow>& rows, unsigned rowID, unsigned colID, const char* columnValue, const char* columnName)
{
    if (!columnValue)
        columnValue = "";
    if (rowID < rows.length())
    { //add the column to existing row
        CCSVRow& row = rows.item(rowID);
        row.setColumn(colID, NULL, columnValue);
    }
    else
    { //new row
        Owned<CCSVRow> newRow = new CCSVRow(rowID);
        newRow->setColumn(colID, NULL, columnValue);
        rows.append(*newRow.getClear());
    }

    if (currentParentXPath.isEmpty())
        return;

    if (!addingSimpleNestedContent && columnName && *columnName)
    {
        CCSVItem* item = getCSVItemByFieldName(columnName);
        if (item)
            item->incrementNextRowID();
    }

    CCSVItem* parentItem = getParentCSVItem();
    if (parentItem)
    {
        if (addingSimpleNestedContent) //ECL: SET OF string, int, etc. NextRowID should be stored in Parent item.
            parentItem->incrementNextRowID();
        setParentItemRowEmpty(parentItem, false);
    }
}

void CommonCSVWriter::setParentItemRowEmpty(CCSVItem* item, bool empty)
{
    item->setCurrentRowEmpty(empty);
    StringBuffer parentXPath(item->getParentXPath());
    if (parentXPath.isEmpty())
        return;
    //If this item is not empty, its parent is not empty.
    parentXPath.setLength(parentXPath.length() - 1);
    setParentItemRowEmpty(csvItems.getValue(parentXPath), empty);
}

void CommonCSVWriter::addCSVHeader(const char* name, const char* type, bool isNested, bool simpleNested, bool outputHeader)
{
    if (checkHeaderName(name))
        return;//Duplicated header. Should never happen.

    Owned<CCSVItem> headerItem = new CCSVItem();
    headerItem->setName(name);
    headerItem->setIsNestedItem(isNested);
    headerItem->setSimpleNested(simpleNested);
    headerItem->setOutputHeader(outputHeader);
    headerItem->setColumnID(headerColumnID);
    headerItem->setNestedLayer(nestedHeaderLayerID);
    headerItem->setParentXPath(currentParentXPath.str());
    StringBuffer xPath(currentParentXPath);
    xPath.append(name);
    csvItems.setValue(xPath.str(), headerItem);

    headerXPathList.append(xPath.str());
    addChildNameToParentCSVItem(name);
    if (currentParentXPath.isEmpty())
        topHeaderNameMap.setValue(name, true);
}

void CommonCSVWriter::addContentField(const char* field, const char* fieldName)
{
    CCSVItem* item = NULL;
    if (addingSimpleNestedContent) //ECL: SET OF string, int, etc. ColumnID should be stored in Parent item.
        item = getParentCSVItem();
    else
        item = getCSVItemByFieldName(fieldName);

    addColumnToRow(contentRowsBuffer, item ? item->getNextRowID() : 0, item ? item->getColumnID() : 0, field, fieldName);
}

void CommonCSVWriter::addStringField(unsigned len, const char* field, const char* fieldName)
{
    StringBuffer v;
    v.append(csvQuote);
    escapeQuoted(len, field, v);
    v.append(csvQuote);
    addContentField(v.str(), fieldName);
}

unsigned CommonCSVWriter::getChildrenMaxNextRowID(const char* path)
{
    CCSVItem* item = csvItems.getValue(path);
    if (!item)
        return 0; //Should never happen

    if (!item->checkIsNestedItem())
        return item->getNextRowID();

    unsigned maxRowID = item->getNextRowID();
    StringBuffer basePath(path);
    basePath.append("/");
    StringArray& names = item->getChildrenNames();
    ForEachItemIn(i, names)
    {
        StringBuffer childPath(basePath);
        childPath.append(names.item(i));
        unsigned rowID = getChildrenMaxNextRowID(childPath.str());
        if (rowID > maxRowID)
            maxRowID = rowID;
    }
    return maxRowID;
}

void CommonCSVWriter::setChildrenNextRowID(const char* path, unsigned rowID)
{
    CCSVItem* item = csvItems.getValue(path);
    if (!item)
        return;

    if (!item->checkIsNestedItem())
    {
        item->setNextRowID(rowID);
        return;
    }

    StringArray& names = item->getChildrenNames();
    ForEachItemIn(i, names)
    {
        StringBuffer childPath(path);
        childPath.append("/").append(names.item(i));
        CCSVItem* childItem = csvItems.getValue(childPath.str());
        if (!childItem)
            continue;

        childItem->setNextRowID(rowID);//for possible new row
        if (childItem->checkIsNestedItem())
        {
            childItem->setRowCount(0);
            setChildrenNextRowID(childPath.str(), rowID);
        }
    }
}

void CommonCSVWriter::addChildNameToParentCSVItem(const char* name)
{
    if (!name || !*name)
        return;

    if (currentParentXPath.isEmpty())
        return;

    CCSVItem* item = getParentCSVItem();
    if (item)
        item->addChildName(name);
}

void CommonCSVWriter::addFieldToParentXPath(const char* fieldName)
{
    currentParentXPath.append(fieldName).append("/");
}

void CommonCSVWriter::removeFieldFromCurrentParentXPath(const char* fieldName)
{
    unsigned len = strlen(fieldName);
    if (currentParentXPath.length() > len+1)
        currentParentXPath.setLength(currentParentXPath.length() - len - 1);
    else
        currentParentXPath.setLength(0);
}

void CommonCSVWriter::outputCSVRows(CIArrayOf<CCSVRow>& rows, bool isHeader)
{
    bool firstRow = true;
    ForEachItemIn(i, rows)
    {
        if (firstRow && !isHeader)
        {
            out.append(recordCount);
            firstRow = false;
        }

        CCSVRow& row = rows.item(i);
        unsigned len = row.getColumnCount();
        for (unsigned col = 0; col < len; col++)
            out.append(options.delimiter.get()).append(row.getColumnValue(col));
        out.append(options.terminator.get());
    }
}

void CommonCSVWriter::finishContentResultRow()
{
    recordCount++;
    outputCSVRows(contentRowsBuffer, false);

    //Prepare for possible next record
    currentParentXPath.setLength(0);
    contentRowsBuffer.kill();
    ForEachItemIn(i, headerXPathList)
    {
        const char* path = headerXPathList.item(i);
        CCSVItem* item = csvItems.getValue(path);
        if (item)
            item->clearContentVariables();
    }
};

void CCSVRow::setColumn(unsigned columnID, const char* columnName, const char* columnValue)
{
    unsigned len = columns.length();
    if (columnID < len)
    {
        CCSVItem& column = columns.item(columnID);
        if (columnName && *columnName)
            column.setName(columnName);
        column.setValue(columnValue);
    }
    else
    {
        for (unsigned i = len; i <= columnID; i++)
        {
            Owned<CCSVItem> column = new CCSVItem();
            if (i == columnID)
            {
                if (columnName && *columnName)
                    column->setName(columnName);
                column->setValue(columnValue);
            }
            columns.append(*column.getClear());
        }
    }
}

const char* CCSVRow::getColumnValue(unsigned columnID) const
{
    if (columnID >= columns.length())
        return ""; //This should never happens.
    CCSVItem& column = columns.item(columnID);
    return column.getValue();
};

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

    BcdCriticalBlock bcdBlock;
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

    if (fieldname)
        out.append("</").append(fieldname).append('>');
}

inline void outputEncodedXmlUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out)
{
    char dec[50];
    if (fieldname)
        out.append('<').append(fieldname).append(" xsi:type=\"xsd:decimal\">");

    BcdCriticalBlock bcdBlock;
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

void CommonEncodedXmlWriter::outputInt(__int64 field, unsigned size, const char *fieldname)
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

void CommonEncodedXmlWriter::outputUInt(unsigned __int64 field, unsigned size, const char *fieldname)
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

IXmlWriterExt * createIXmlWriterExt(unsigned _flags, unsigned _initialIndent, IXmlStreamFlusher *_flusher, XMLWriterType xmlType)
{
    if (xmlType==WTJSONRootless)
        return new CommonJsonWriter(_flags, _initialIndent, _flusher);
    if (xmlType==WTJSONObject)
        return new CommonJsonObjectWriter(_flags, _initialIndent, _flusher);
    return CreateCommonXmlWriter(_flags, _initialIndent, _flusher, xmlType);
}
