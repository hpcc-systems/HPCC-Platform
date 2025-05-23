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



#ifndef __JSTRING__
#define __JSTRING__

#include "jiface.hpp"
#include "jio.hpp"
#include "jstream.hpp"
#include "jbuff.hpp"
#include <string>

// A Java compatible String and StringBuffer class - useful for dynamic strings.
#ifdef _DEBUG
#define CATCH_USE_AFTER_FREE
#endif

class String;
interface IAtom;
interface IFile;

class jlib_decl StringBuffer
{
    enum { InternalBufferSize = 16 };
public:
    explicit StringBuffer();
    explicit StringBuffer(String & value);
    explicit StringBuffer(const char *value);
    explicit StringBuffer(StringBuffer && value);
    explicit StringBuffer(size_t len, const char *value);
    explicit StringBuffer(const StringBuffer & value);
    explicit StringBuffer(char value);
    ~StringBuffer();

    inline size32_t length() const                      { return (size32_t)curLen; }
    inline bool     isEmpty() const                     { return (curLen == 0); }
    void            setLength(size_t len);
    inline void     ensureCapacity(size_t max)        { if (maxLen <= curLen + max) _realloc(curLen + max); }
    char *          ensureCapacity(size_t max, size_t & got); // ensure there is space, but do not increase curLen
    size32_t        lengthUtf8() const;

    StringBuffer &  append(char value);
    StringBuffer &  append(unsigned char value);
    StringBuffer &  append(const char * value);
    StringBuffer &  append(const unsigned char * value);
    StringBuffer &  append(const IAtom * value);
    StringBuffer &  append(size_t len, const char * value);
    StringBuffer &  append(const char * value, size_t offset, size_t len);
    StringBuffer &  append(double value);
    StringBuffer &  append(float value);
    StringBuffer &  append(int value);
    StringBuffer &  append(unsigned value);
    StringBuffer &  append(__int64 value);
    StringBuffer &  append(unsigned __int64 value);
    StringBuffer &  append(const String & value);
    StringBuffer &  append(const IStringVal & value);
    StringBuffer &  append(const IStringVal * value);
    StringBuffer &  append(const std::string & value) { return append(value.size(), value.c_str()); }
    StringBuffer &  appendN(size_t count, char fill);
    StringBuffer &  appendf(const char *format, ...) __attribute__((format(printf, 2, 3)));
    StringBuffer &  appendLower(size_t len, const char * value);
    StringBuffer &  appendLower(const char * value) { return appendLower(strlen(value), value); }

    StringBuffer &  setf(const char* format, ...) __attribute__((format(printf,2,3)));
    StringBuffer &  limited_valist_appendf(size_t szLimit, const char *format, va_list args) __attribute__((format(printf,3,0)));
    inline StringBuffer &valist_appendf(const char *format, va_list args) __attribute__((format(printf,2,0))) { return limited_valist_appendf(0, format, args); }
    StringBuffer &  appendhex(unsigned char value, bool lower);
    inline char     charAt(size_t pos) { return buffer[pos]; }
    inline StringBuffer & clear() { curLen = 0; return *this; }
    void            kill();
    void            getChars(size_t srcBegin, size_t srcEnd, char * target) const;
    StringBuffer &  insert(size_t offset, char value);
    StringBuffer &  insert(size_t offset, unsigned char value);
    StringBuffer &  insert(size_t offset, const char * value);
    StringBuffer &  insert(size_t offset, const unsigned char * value);
    StringBuffer &  insert(size_t offset, double value);
    StringBuffer &  insert(size_t offset, float value);
    StringBuffer &  insert(size_t offset, int value);
    StringBuffer &  insert(size_t offset, unsigned value);
    StringBuffer &  insert(size_t offset, __int64 value);
    StringBuffer &  insert(size_t offset, const String & value);
    StringBuffer &  insert(size_t offset, const StringBuffer & value);
    StringBuffer &  insert(size_t offset, const IStringVal & value);
    StringBuffer &  insert(size_t offset, const IStringVal * value);
    StringBuffer &  reverse();
    void            setCharAt(size_t offset, char value);
    void            replace(size_t offset, size_t len, const void * value);

    //Non-standard functions:
    MemoryBuffer &  deserialize(MemoryBuffer & in);
    MemoryBuffer &  serialize(MemoryBuffer & out) const;
    StringBuffer &  loadFile(const char *fname, bool binaryMode=false);
    StringBuffer &  loadFile(IFile* f);

    StringBuffer &  append(const StringBuffer & value);
    StringBuffer &  newline();
    StringBuffer &  pad(size_t count);
    StringBuffer &  padTo(size_t count);
    char *          detach();
    StringBuffer &  clip();
    StringBuffer &  trim();
    StringBuffer &  trimLeft();
    inline StringBuffer &  trimRight() {  return clip(); }
    StringBuffer &  remove(size_t start, size_t len);
    const char *    str() const;
    StringBuffer &  toLowerCase();
    StringBuffer &  toUpperCase();
    StringBuffer &  replace(char oldChar, char newChar);
    StringBuffer &  replaceString(const char* oldStr, const char* newStr);
    StringBuffer &  replaceStringNoCase(const char* oldStr, const char* newStr);
    char *          reserve(size_t size);
    char *          reserveTruncate(size_t size);
    void            setown(StringBuffer &other);
    size_t          space() const { return maxLen - curLen - 1; }
    StringBuffer &  stripChar(char oldChar);
    void            swapWith(StringBuffer &other);
    void setBuffer(size_t buffLen, char * newBuff, size_t strLen);

    inline StringBuffer& set(const char* value) { return clear().append(value); }
    inline operator const char* () const { return str(); }
    inline StringBuffer& operator=(const char* value)
    {
        return clear().append(value);
    }
    inline StringBuffer& operator=(const StringBuffer& value)
    {
        return clear().append(value.str());
    }
    StringBuffer& operator=(StringBuffer&& value);
    explicit operator bool() const { return (length() != 0); }

    StringBuffer &  appendlong(long value);
    StringBuffer &  appendulong(unsigned long value);

private: // long depreciated
    StringBuffer &  append(long value);
    StringBuffer &  append(unsigned long value);
    StringBuffer &  insert(size_t offset, long value);

protected:
    inline bool useInternal() const { return buffer == internalBuffer; }
    void init()
    {
        buffer = internalBuffer;
        curLen = 0;
        maxLen = InternalBufferSize;
    }
    void freeBuffer();
    void _insert(size_t offset, size_t insertLen);
    void _realloc(size_t newLen);

private:
    char                internalBuffer[InternalBufferSize];
    char *              buffer;
    size_t              curLen;
    size_t              maxLen;
};

// add a variable-parameter constructor to StringBuffer.
class jlib_decl VStringBuffer : public StringBuffer
{
public:
    VStringBuffer(const char* format, ...) __attribute__((format(printf, 2, 3)));
};

class SCMStringBuffer : public CInterface, implements IStringVal
{

public:
    IMPLEMENT_IINTERFACE;
    StringBuffer s;

    virtual const char * str() const { return s.str(); };
    virtual void set(const char *val) { s.clear().append(val); };
    virtual void set(StringBuffer &&str) { s.swapWith(str); }
    virtual void clear() { s.clear(); };
    virtual void setLen(const char *val, unsigned length) { s.clear().append(length, val); };
    virtual unsigned length() const { return s.length(); };
};

class jlib_decl String : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    String();
    //    String(byte[]);
    //    String(byte[], int);
    //    String(byte[], int, int);
    //    String(byte[], int, int, int);
    //    String(byte[], int, int, String);
    //    String(byte[], String);
    String(const char * value);
    String(const char * value, int offset, int count);
    String(String & value);
    String(StringBuffer & value);
    ~String();

    char    charAt(size32_t index) const;
    int     compareTo(const String & value) const;
    int     compareTo(const char* value) const;
    String *  concat(const String & value) const;
    bool    endsWith(const String & value) const;
    bool    endsWith(const char* value) const;
    bool    equals(String & value) const;
    bool    equalsIgnoreCase(const String & value) const;
    void    getBytes(int srcBegin, int srcEnd, void * dest, int dstBegin) const;
    void    getChars(int srcBegin, int srcEnd, void * dest, int dstBegin) const;
    int     hashCode() const;
    int     indexOf(int ch) const;
    int     indexOf(int ch, int from) const;
    int     indexOf(const String & search) const;
    int     indexOf(const String & search, int from) const;
    int     lastIndexOf(int ch) const;
    int     lastIndexOf(int ch, int from) const;
    int     lastIndexOf(const String & search) const;
    int     lastIndexOf(const String & serach, int from) const;
    size32_t  length() const;
    bool    startsWith(String & value) const;
    bool    startsWith(String & value, int offset) const;
    bool    startsWith(const char* value) const;
    String *  substring(int beginIndex) const;
    String *  substring(int beginIndex, int endIndex) const;
    const char *str() const;
    String *  toLowerCase() const;
    String *  toString();               // Links this
    String *  toUpperCase() const;
    String *  trim() const;



protected:
    char * text;
};


//This simple class is useful for storing string member variables
class jlib_decl StringAttr
{
public:
    inline StringAttr(void)                     { text = NULL; }
    StringAttr(const char * _text, size_t _len);
    StringAttr(const char * _text);
    StringAttr(const StringAttr & src);
    StringAttr(StringAttr && src);
    StringAttr& operator = (StringAttr && from);
    StringAttr& operator = (const StringAttr & from);
    inline ~StringAttr(void)
    {
        free(text);
#ifdef CATCH_USE_AFTER_FREE
        text = const_cast<char *>("<use-after-free>");
#endif
    }

    inline operator const char * () const       { return text; }
    inline void clear()                         { setown(NULL); }
    inline char * detach()                      { char * ret = text; text = NULL; return ret; }
    inline const char * get() const             { return text; }
    inline size_t length() const                { return text ? strlen(text) : 0; }
    inline bool isEmpty() const                 { return !text||!*text; } // faster than (length==0)
    inline const char * str() const             { return text ? text : ""; } // safe form (doesn't return NULL)

    void         set(const char * _text);
    void         setown(const char * _text);
    void         set(const char * _text, size_t _len);
    void         set(const StringBuffer & source);
    void         setown(StringBuffer & source);
    void         toLowerCase();
    void         toUpperCase();
    void         swapWith(StringAttr & other);
private:
    char *       text;
};


class jlib_decl StringAttrBuilder : public StringBuffer
{
public:
    StringAttrBuilder(StringAttr & _target);
    ~StringAttrBuilder();

protected:
    StringAttr & target;
};

class jlib_decl StringAttrAdaptor : implements IStringVal
{
public:
    StringAttrAdaptor(StringAttr & _attr) : attr(_attr) {}

    virtual const char * str() const { return attr.get(); };
    virtual void set(const char *val) { attr.set(val); };
    virtual void clear() { attr.clear(); };
    virtual void setLen(const char *val, unsigned length) { attr.set(val, length); };
    virtual unsigned length() const { return (unsigned)attr.length(); };

private:
    StringAttr & attr;
};

class jlib_decl StringBufferAdaptor : implements IStringVal
{

public:
    StringBufferAdaptor(StringBuffer & _buffer) : buffer(_buffer) { initsize=buffer.length(); }

    virtual const char * str() const { return buffer.str(); };
    virtual void set(const char *val) { clear(); buffer.append(val); };
    virtual void clear() { buffer.setLength(initsize); }
    virtual void setLen(const char *val, unsigned length) { clear(); buffer.append(length, val); };
    virtual unsigned length() const { return buffer.length(); };

private:
    size_t initsize;
    StringBuffer & buffer;
};

#ifdef __GNUC__
class jlib_decl GccStringAttrAdaptor
{
public:
    GccStringAttrAdaptor(StringAttr & _attr) : adaptor(_attr) {}
    inline operator IStringVal & ()     { return adaptor; }
private:
    StringAttrAdaptor       adaptor;
};

class jlib_decl GccStringBufferAdaptor
{
public:
    GccStringBufferAdaptor(StringBuffer & _buffer) : adaptor(_buffer) {}
    inline operator IStringVal & ()     { return adaptor; }
private:
    StringBufferAdaptor     adaptor;
};
#define StringAttrAdaptor GccStringAttrAdaptor
#define StringBufferAdaptor GccStringBufferAdaptor
#endif

class jlib_decl StringBufferItem : public CInterface, public StringBuffer
{
public:
    StringBufferItem()                                  : StringBuffer() {}
    StringBufferItem(const char *value)                 : StringBuffer(value) {}
    StringBufferItem(unsigned len, const char *value)   : StringBuffer(len, value) {}
    StringBufferItem(const StringBuffer & value)        : StringBuffer(value) {}
};

class jlib_decl StringAttrItem : public CInterface
{
public:
    StringAttrItem(void) {}
    StringAttrItem(const char * _text) : text(_text) {}
    StringAttrItem(const char * _text, unsigned _len);

public:
    StringAttr      text;
};

// --$appendURL-----------------------------------------------------------------
// appends the URL encoded version of src to dest
// if len is unspecified, then src is assumed to be an NTS
// if lower is TRUE a-f is used for hex numbers, otherwise A-F is used
// -----------------------------------------------------------------------------
#define ENCODE_SPACES 1
#define ENCODE_NEWLINES 2
#define ENCODE_WHITESPACE 3
#define ENCODE_NONE 4

interface IEntityHelper
{
    virtual bool find(const char *entity, StringBuffer &value) = 0;
};

void jlib_decl appendURL(StringBuffer *dest, const char *src, size32_t len = -1, char lower=FALSE, bool keepUnderscore=false);
extern jlib_decl StringBuffer &appendDecodedURL(StringBuffer &out, const char *url);
extern jlib_decl StringBuffer &appendDecodedURL(StringBuffer &s, size_t len, const char *url);
extern jlib_decl StringBuffer & appendStringAsCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak);
extern jlib_decl StringBuffer & appendStringAsQuotedCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak);
extern jlib_decl StringBuffer & appendDataAsHex(StringBuffer &out, unsigned len, const void * data);
extern jlib_decl StringBuffer & appendStringAsSQL(StringBuffer & out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendStringAsECL(StringBuffer & out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendStringAsQuotedECL(StringBuffer &out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendUtf8AsECL(StringBuffer &out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendStringAsUtf8(StringBuffer &out, unsigned len, const char * src);


extern jlib_decl const char *decodeJSON(const char *x, StringBuffer &ret, unsigned len=(unsigned)-1, const char **errMark=NULL);
extern jlib_decl void extractItem(StringBuffer & res, const char * src, const char * sep, int whichItem, bool caps);
extern jlib_decl const char *encodeXML(const char *x, StringBuffer &ret, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl const char *decodeXML(const char *x, StringBuffer &ret, const char **errMark=NULL, IEntityHelper *entityHelper=NULL, bool strict = true);
extern jlib_decl void encodeXML(const char *x, IIOStream &out, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl void decodeXML(ISimpleReadStream &in, StringBuffer &out, unsigned len=(unsigned)-1);

extern jlib_decl int utf8CharLen(unsigned char ch);
extern jlib_decl int utf8CharLen(const unsigned char *ch, unsigned maxsize = (unsigned)-1);

extern jlib_decl bool replaceString(StringBuffer & result, size_t lenSource, const char *source, size_t lenOldStr, const char* oldStr, size_t lenNewStr, const char* newStr, bool avoidCopyIfUnmatched);

interface IVariableSubstitutionHelper
{
    virtual bool findVariable(const char *name, StringBuffer &value) = 0;
};

extern jlib_decl StringBuffer &replaceVariables(StringBuffer & result, const char *source, bool exceptions, IVariableSubstitutionHelper *helper, const char* delim = "${", const char* term = "}");
extern jlib_decl StringBuffer &replaceEnvVariables(StringBuffer & result, const char *source, bool exceptions, const char* delim = "${env.", const char* term = "}");

inline const char *encodeUtf8XML(const char *x, StringBuffer &ret, unsigned flags=0, unsigned len=(unsigned)-1)
{
    return encodeXML(x, ret, flags, len, true);
}

inline StringBuffer &appendXMLTagName(StringBuffer &xml, const char *tag, const char *prefix=NULL)
{
    if (prefix && *prefix)
        xml.append(prefix).append(':');
    xml.append(tag);
    return xml;
}

extern jlib_decl StringBuffer & appendXMLOpenTag(StringBuffer &xml, const char *tag, const char *prefix=NULL, bool complete=true, bool close=false, const char *uri=NULL);

inline StringBuffer &appendXMLAttr(StringBuffer &xml, const char *name, const char *value, const char *prefix=NULL, bool useDblQuote=false)
{
    if (!name || !*name || !value)
        return xml;
    xml.append(' ');
    appendXMLTagName(xml, name, prefix);
    xml.append("=").append(useDblQuote ? '"' : '\'');
    encodeXML(value, xml);
    xml.append(useDblQuote ? '"' : '\'');
    return xml;
}

inline StringBuffer & appendXMLCloseTag(StringBuffer &xml, const char *tag, const char *prefix=NULL)
{
    if (!tag || !*tag)
        return xml;

    xml.append("</");
    return appendXMLTagName(xml, tag, prefix).append('>');
}

inline StringBuffer &appendXMLTag(StringBuffer &xml, const char *tag, const char *value, const char *prefix=NULL, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=true)
{
    appendXMLOpenTag(xml, tag, prefix);
    if (value && *value)
    {
        if (flags != ENCODE_NONE)
            encodeXML(value, xml, flags, len, utf8);
        else
            xml.append(value);
    }
    return appendXMLCloseTag(xml, tag, prefix);
}

inline StringBuffer &delimitJSON(StringBuffer &s, bool addNewline=false, bool escapeNewline=false)
{
    if (s.length() && !strchr("{ [:,n\n", s.charAt(s.length()-1))) //'n' or '\n' indicates already formatted with optionally escaped newline
    {
        s.append(",");
        if (addNewline)
            s.append(escapeNewline ? "\\n" : "\n");
        else
            s.append(' ');
    }
    return s;
}

/*
* Encodes a CSV column, not an entire CSV record
*/
jlib_decl StringBuffer &encodeCSVColumn(StringBuffer &s, const char *value);

jlib_decl StringBuffer &encodeJSON(StringBuffer &s, const char *value);
jlib_decl StringBuffer &encodeJSON(StringBuffer &s, unsigned len, const char *value);

jlib_decl StringBuffer &appendJSONName(StringBuffer &s, const char *name);
jlib_decl StringBuffer &appendfJSONName(StringBuffer &s, const char *format, ...) __attribute__((format(printf, 2, 3)));
jlib_decl StringBuffer &appendJSONDataValue(StringBuffer& s, const char *name, unsigned len, const void *_value);
jlib_decl StringBuffer &appendJSONRealValue(StringBuffer& s, const char *name, double value);

inline StringBuffer &appendJSONNameOrDelimit(StringBuffer &s, const char *name)
{
    if (name && *name)
        return appendJSONName(s, name);
    return delimitJSON(s);
}

inline StringBuffer &appendJSONStringValue(StringBuffer& s, const char *name, unsigned len, const char *value, bool encode, bool quoted=true)
{
    appendJSONNameOrDelimit(s, name);
    if (!value)
        return s.append("null");
    if (quoted)
        s.append('"');
    if (encode)
        encodeJSON(s, len, value);
    else
        s.append(len, value);
    if (quoted)
        s.append('"');
    return s;
}

inline StringBuffer &appendJSONStringValue(StringBuffer& s, const char *name, const char *value, bool encode, bool quoted=true)
{
    return appendJSONStringValue(s, name, (size32_t)(value ? strlen(value) : 0), value, encode, quoted);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, bool value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append((value) ? "true" : "false");
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, const char *value)
{
    return appendJSONStringValue(s, name, value, true);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, long value)
{
    appendJSONNameOrDelimit(s, name);
    return s.appendlong(value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, __int64 value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append(value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, unsigned __int64 value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append(value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, double value)
{
    return ::appendJSONRealValue(s, name, value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, float value)
{
    return ::appendJSONRealValue(s, name,  value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, unsigned long value)
{
    appendJSONNameOrDelimit(s, name);
    return s.appendulong(value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, int value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append(value);
}

inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, unsigned value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append(value);
}

template <typename TValue>
inline StringBuffer& operator << (StringBuffer& s, const TValue& value)
{
    return s.append(value);
}

extern jlib_decl void toLower(std::string & value);

extern jlib_decl bool checkUnicodeLiteral(char const * str, unsigned length, unsigned & ep, StringBuffer & msg);
extern jlib_decl void decodeCppEscapeSequence(StringBuffer & out, const char * in, bool errorIfInvalid);
extern jlib_decl bool strToBool(const char * text);
inline const char *boolToStr(bool b) { return b ? "true" : "false"; }
extern jlib_decl bool strToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(const char * text);
extern jlib_decl StringBuffer & ncnameEscape(char const * in, StringBuffer & out);
extern jlib_decl StringBuffer & ncnameUnescape(char const * in, StringBuffer & out);
extern jlib_decl StringBuffer & elideString(StringBuffer & s, unsigned maxLength);

extern jlib_decl bool startsWith(const char* src, const char* prefix);
extern jlib_decl bool endsWith(const char* src, const char* suffix);

extern jlib_decl bool startsWithIgnoreCase(const char* src, const char* prefix);
extern jlib_decl bool endsWithIgnoreCase(const char* src, const char* suffix);

inline bool strieq(const char* s, const char* t) { return stricmp(s,t)==0; }
inline bool streq(const char* s, const char* t) { return strcmp(s,t)==0; }
inline bool strsame(const char* s, const char* t) { return (s == t) || (s && t && strcmp(s,t)==0); }  // also allow nulls
inline bool strisame(const char* s, const char* t) { return (s == t) || (s && t && stricmp(s,t)==0); }  // also allow nulls
inline bool isEmptyString(const char *text) { return !text||!*text; }
inline const char * nullIfEmptyString(const char * text) { return isEmptyString(text) ? nullptr : text; }
inline bool hasPrefix(const char * text, const char * prefix, bool caseSensitive)
{
    if (caseSensitive)
        return strncmp(text, prefix, strlen(prefix)) == 0;
    else
        return strnicmp(text, prefix, strlen(prefix)) == 0;
}

// Search for a string in a null terminated array of const char * strings
extern jlib_decl unsigned matchString(const char * search, const char * const * strings);

extern jlib_decl char *j_strtok_r(char *str, const char *delim, char **saveptr);
extern jlib_decl int j_memicmp (const void *s1, const void *s2, size32_t len);
extern jlib_decl size32_t memcount(size32_t len, const char * str, char search);

extern jlib_decl const char * nullText(const char * text);
extern jlib_decl bool loadBinaryFile(StringBuffer & contents, const char *filename, bool throwOnError);

template <typename LineProcessor>
void processLines(const StringBuffer & content, LineProcessor process)
{
    const char * cur = content;
    while (*cur)
    {
        const char * next = strchr(cur, '\n');
        if (next)
        {
            if (next != cur)
                process(next-cur, cur);
            cur = next+1;
        }
        else
        {
            process(strlen(cur), cur);
            break;
        }
    }
}

//Convert a character to the underlying value - out of range values are undefined
inline unsigned hex2digit(char c)
{
    if (c >= 'a')
        return (c - 'a' + 10);
    else if (c >= 'A')
        return (c - 'A' + 10);
    return (c - '0');
}

inline byte getHexPair(const char * s)
{
    return hex2digit(s[0]) << 4 | hex2digit(s[1]);
}


//General purpose function for processing option strings in the form option[=value],option[=value],...
using optionCallback = std::function<void(const char * name, const char * value)>;
extern jlib_decl void processOptionString(const char * options, optionCallback callback);

extern jlib_decl const char * stristr(const char *haystack, const char *needle);
extern jlib_decl void getSnakeCase(StringBuffer & out, const char * camelValue);
//If the string has any characters, ensure the last character matches the separator
extern jlib_decl void ensureSeparator(StringBuffer & out, char separator);

//Search for one block of bytes within another block of bytes - memmem is not standard, so we provide our own
extern jlib_decl const void * jmemmem(size_t lenHaystack, const void * haystack, size_t lenNeedle, const void *needle);

// For preventing command injection, sanitize the argument to be passed to the system command
extern jlib_decl StringBuffer& sanitizeCommandArg(const char* arg, StringBuffer& sanitized);

// For formatting integer values into comma separated string
extern jlib_decl StringBuffer& formatWithCommas(unsigned __int64 value, StringBuffer& formatted);

#endif
