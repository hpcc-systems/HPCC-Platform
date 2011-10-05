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
    inline bool isEmpty()                       { return !text||!*text; } // faster than (length==0)
    inline const char * sget(void) const        { return text ? text : ""; } // safe form of get (doesn't return NULL)

    void         set(const char * _text);
    void         setown(const char * _text);
    void         set(const char * _text, unsigned _len);
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

extern jlib_decl void extractItem(StringBuffer & res, const char * src, const char * sep, int whichItem, bool caps);
extern jlib_decl const char *encodeXML(const char *x, StringBuffer &ret, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl const char *decodeXML(const char *x, StringBuffer &ret, unsigned len=(unsigned)-1, const char **errMark=NULL, IEntityHelper *entityHelper=NULL);
extern jlib_decl const char *encodeXML(const char *x, IIOStream &out, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
extern jlib_decl void decodeXML(ISimpleReadStream &in, StringBuffer &out, unsigned len=(unsigned)-1);

inline const char *encodeUtf8XML(const char *x, StringBuffer &ret, unsigned flags=false, unsigned len=(unsigned)-1)
{
    return encodeXML(x, ret, flags, len, true);
}

inline StringBuffer &appendXMLTag(StringBuffer &xml, const char *tag, const char *value, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=true)
{
    xml.append('<').append(tag).append('>');
    if (value && *value)
        encodeXML(value, xml, flags, len, utf8);
    return xml.append("</").append(tag).append('>');
}

extern jlib_decl void decodeCppEscapeSequence(StringBuffer & out, const char * in, bool errorIfInvalid);
extern jlib_decl bool strToBool(const char * text);
extern jlib_decl bool strToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(size_t len, const char * text);
extern jlib_decl bool clipStrToBool(const char * text);
extern jlib_decl StringBuffer & ncnameEscape(char const * in, StringBuffer & out);
extern jlib_decl StringBuffer & ncnameUnescape(char const * in, StringBuffer & out);

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
