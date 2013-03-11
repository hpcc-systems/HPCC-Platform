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



#ifndef __JSTRING__
#define __JSTRING__

#include "jexpdef.hpp"
#include "jiface.hpp"
#include "jio.hpp"
#include "jstream.hpp"
#include "jbuff.hpp"

// A Java compatible String and StringBuffer class - useful for dynamic strings.

class String;
interface IAtom;
interface IFile;

class jlib_decl StringBuffer
{
public:
    StringBuffer();
    StringBuffer(String & value);
    StringBuffer(const char *value);
    StringBuffer(unsigned len, const char *value);
    StringBuffer(const StringBuffer & value);
    inline ~StringBuffer() { free(buffer); }

    inline size32_t length() const                      { return curLen; }
    inline void     Release() const                     { delete this; }    // for consistency even though not link counted
    void            setLength(unsigned len);
    inline void     ensureCapacity(unsigned max)        { if (maxLen <= curLen + max) _realloc(curLen + max); }
    
    StringBuffer &  append(char value);
    StringBuffer &  append(unsigned char value);
    StringBuffer &  append(const char * value);
    StringBuffer &  append(const unsigned char * value);
    StringBuffer &  append(const IAtom * value);
    StringBuffer &  append(unsigned len, const char * value);
    StringBuffer &  append(const char * value, int offset, int len);
//  StringBuffer &  append(const unsigned char * value, int offset, int len);
    StringBuffer &  append(double value);
    StringBuffer &  append(float value);
    StringBuffer &  append(int value);
    StringBuffer &  append(unsigned value);
    StringBuffer &  append(__int64 value);
    StringBuffer &  append(unsigned __int64 value);
    StringBuffer &  append(const String & value);
    StringBuffer &  append(const IStringVal & value);
    StringBuffer &  append(const IStringVal * value);
    StringBuffer &  appendN(size32_t count, char fill);
    StringBuffer &  appendf(const char *format, ...) __attribute__((format(printf, 2, 3)));
    StringBuffer &  appendLower(unsigned len, const char * value);

    StringBuffer &  setf(const char* format, ...) __attribute__((format(printf,2,3)));
    StringBuffer &  limited_valist_appendf(unsigned szLimit, const char *format, va_list args);
    inline StringBuffer &valist_appendf(const char *format, va_list args) { return limited_valist_appendf(0, format, args); }
    StringBuffer &  appendhex(unsigned char value, char lower);
    inline char     charAt(size32_t pos) { return buffer[pos]; }
    inline StringBuffer & clear() { curLen = 0; return *this; }
    void            kill();
    void            getChars(int srcBegin, int srcEnd, char * target) const;
    StringBuffer &  insert(int offset, char value);
    StringBuffer &  insert(int offset, unsigned char value);
    StringBuffer &  insert(int offset, const char * value);
    StringBuffer &  insert(int offset, const unsigned char * value);
    StringBuffer &  insert(int offset, double value);
    StringBuffer &  insert(int offset, float value);
    StringBuffer &  insert(int offset, int value);
    StringBuffer &  insert(int offset, unsigned value);
    StringBuffer &  insert(int offset, __int64 value);
    StringBuffer &  insert(int offset, const String & value);
    StringBuffer &  insert(int offset, const StringBuffer & value);
    StringBuffer &  insert(int offset, const IStringVal & value);
    StringBuffer &  insert(int offset, const IStringVal * value);
    StringBuffer &  reverse();
    void            setCharAt(unsigned offset, char value);
    
    //Non-standard functions:
    MemoryBuffer &  deserialize(MemoryBuffer & in);
    MemoryBuffer &  serialize(MemoryBuffer & out) const;
    StringBuffer &  loadFile(const char *fname, bool binaryMode=false);
    StringBuffer &  loadFile(IFile* f);
    
    StringBuffer &  append(const StringBuffer & value);
    StringBuffer &  newline();
    StringBuffer &  pad(unsigned count);
    StringBuffer &  padTo(unsigned count);
    inline const char * str() const { return toCharArray(); }
    char *          detach();
    StringBuffer &  clip();
    StringBuffer &  trim();
    StringBuffer &  trimLeft();
    inline StringBuffer &  trimRight() {  return clip(); }
    StringBuffer &  remove(unsigned start, unsigned len);
    const char *    toCharArray() const;
    StringBuffer &  toLowerCase();
    StringBuffer &  toUpperCase();
    StringBuffer &  replace(char oldChar, char newChar);
    StringBuffer &  replaceString(const char* oldStr, const char* newStr);
    char *          reserve(size32_t size);
    char *          reserveTruncate(size32_t size);
    void            swapWith(StringBuffer &other);
    void setBuffer(size32_t buffLen, char * newBuff, size32_t strLen);

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


    StringBuffer &  appendlong(long value);
    StringBuffer &  appendulong(unsigned long value);
private: // long depreciated
    StringBuffer &  append(long value);
    StringBuffer &  append(unsigned long value);
    StringBuffer &  insert(int offset, long value);

protected:
    void init()
    {
        buffer = NULL;
        curLen = 0;
        maxLen = 0;
    }
    void _insert(unsigned offset, size32_t insertLen);
    void _realloc(size32_t newLen);

private:    
    mutable char *  buffer;
    size32_t            curLen;
    size32_t            maxLen;
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
    const char *toCharArray() const;
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
    StringAttr(const char * _text, unsigned _len);
    StringAttr(const char * _text);
    StringAttr(const StringAttr & src);
    inline ~StringAttr(void) { free(text); }
    
    inline operator const char * () const       { return text; }
    inline void clear()                         { setown(NULL); }
    inline char * detach()                      { char * ret = text; text = NULL; return ret; }
    inline const char * get(void) const         { return text; }
    inline size32_t     length() const          { return text ? (size32_t)strlen(text) : 0; }
    inline bool isEmpty() const                 { return !text||!*text; } // faster than (length==0)
    inline const char * sget(void) const        { return text ? text : ""; } // safe form of get (doesn't return NULL)

    void         set(const char * _text);
    void         setown(const char * _text);
    void         set(const char * _text, unsigned _len);
    void         toLowerCase();
    void         toUpperCase();
    
private:
    char *       text;
    
private:
    StringAttr &operator = (const StringAttr & from);
};

class jlib_decl StringAttrAdaptor : public CInterface, implements IStringVal
{
public:
    StringAttrAdaptor(StringAttr & _attr) : attr(_attr) {}
    IMPLEMENT_IINTERFACE;

    virtual const char * str() const { return attr.get(); };
    virtual void set(const char *val) { attr.set(val); };
    virtual void clear() { attr.clear(); };
    virtual void setLen(const char *val, unsigned length) { attr.set(val, length); };
    virtual unsigned length() const { return attr.length(); };

private:
    StringAttr & attr;
};

class jlib_decl StringBufferAdaptor : public CInterface, implements IStringVal
{

public:
    StringBufferAdaptor(StringBuffer & _buffer) : buffer(_buffer) { initsize=buffer.length(); }
    IMPLEMENT_IINTERFACE;

    virtual const char * str() const { return buffer.str(); };
    virtual void set(const char *val) { clear(); buffer.append(val); };
    virtual void clear() { buffer.setLength(initsize); } 
    virtual void setLen(const char *val, unsigned length) { clear(); buffer.append(length, val); };
    virtual unsigned length() const { return buffer.length(); };

private:
    size32_t initsize;
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

void jlib_decl appendURL(StringBuffer *dest, const char *src, size32_t len = -1, char lower=FALSE);
extern jlib_decl StringBuffer & appendStringAsCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak);
extern jlib_decl StringBuffer & appendStringAsQuotedCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak);
extern jlib_decl StringBuffer & appendDataAsHex(StringBuffer &out, unsigned len, const void * data);
extern jlib_decl StringBuffer & appendStringAsSQL(StringBuffer & out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendStringAsECL(StringBuffer & out, unsigned len, const char * src);
extern jlib_decl StringBuffer & appendStringAsQuotedECL(StringBuffer &out, unsigned len, const char * src);

extern jlib_decl const char *decodeJSON(const char *x, StringBuffer &ret, unsigned len=(unsigned)-1, const char **errMark=NULL);
extern jlib_decl void extractItem(StringBuffer & res, const char * src, const char * sep, int whichItem, bool caps);
extern jlib_decl const char *encodeXML(const char *x, StringBuffer &ret, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl const char *decodeXML(const char *x, StringBuffer &ret, unsigned len=(unsigned)-1, const char **errMark=NULL, IEntityHelper *entityHelper=NULL);
extern jlib_decl const char *encodeXML(const char *x, IIOStream &out, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl void decodeXML(ISimpleReadStream &in, StringBuffer &out, unsigned len=(unsigned)-1);

extern jlib_decl int utf8CharLen(const unsigned char *ch);

inline const char *encodeUtf8XML(const char *x, StringBuffer &ret, unsigned flags=false, unsigned len=(unsigned)-1)
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

inline StringBuffer &appendXMLAttr(StringBuffer &xml, const char *name, const char *value, const char *prefix=NULL)
{
    if (!name || !*name || !value)
        return xml;
    xml.append(' ');
    appendXMLTagName(xml, name, prefix);
    encodeXML(value, xml.append("='"));
    xml.append("'");
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

jlib_decl StringBuffer &encodeJSON(StringBuffer &s, const char *value);
jlib_decl StringBuffer &appendJSONName(StringBuffer &s, const char *name);

inline StringBuffer &appendJSONNameOrDelimit(StringBuffer &s, const char *name)
{
    if (name && *name)
        return appendJSONName(s, name);
    return delimitJSON(s);
}

template <typename type>
inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, type value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append(value);
}

//specialization
template <>
inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, bool value)
{
    appendJSONNameOrDelimit(s, name);
    return s.append((value) ? "true" : "false");
}

template <>
inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, const char *value)
{
    appendJSONNameOrDelimit(s, name);
    if (!value)
        return s.append("null");
    return encodeJSON(s.append('"'), value).append('"');
}

template <>
inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, long value)
{
    appendJSONNameOrDelimit(s, name);
    return s.appendlong(value);
}

template <>
inline StringBuffer &appendJSONValue(StringBuffer& s, const char *name, unsigned long value)
{
    appendJSONNameOrDelimit(s, name);
    return s.appendulong(value);
}

extern jlib_decl void decodeCppEscapeSequence(StringBuffer & out, const char * in, bool errorIfInvalid);
extern jlib_decl bool strToBool(const char * text);
extern jlib_decl bool strToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(const char * text);
extern jlib_decl StringBuffer & ncnameEscape(char const * in, StringBuffer & out);
extern jlib_decl StringBuffer & ncnameUnescape(char const * in, StringBuffer & out);
extern jlib_decl StringBuffer & elideString(StringBuffer & s, unsigned maxLength);

extern jlib_decl bool startsWith(const char* src, const char* dst);
extern jlib_decl bool endsWith(const char* src, const char* dst);

extern jlib_decl bool startsWithIgnoreCase(const char* src, const char* dst);
extern jlib_decl bool endsWithIgnoreCase(const char* src, const char* dst);

inline bool strieq(const char* s, const char* t) { return stricmp(s,t)==0; }
inline bool streq(const char* s, const char* t) { return strcmp(s,t)==0; }
inline bool strsame(const char* s, const char* t) { return (s == t) || (!s && !t && strcmp(s,t)==0); }  // also allow nulls

extern jlib_decl char *j_strtok_r(char *str, const char *delim, char **saveptr);
extern jlib_decl int j_memicmp (const void *s1, const void *s2, size32_t len); 
extern jlib_decl size32_t memcount(size32_t len, const char * str, char search);

#endif
