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


#ifndef JUTIL_HPP
#define JUTIL_HPP

#include "jlib.hpp"
#include "jstring.hpp"
#include "jarray.tpp"
#include "jbuff.hpp"

//#define NAMEDCOUNTS

interface IPropertyTree;

void jlib_decl MilliSleep(unsigned milli);
long jlib_decl atolong_l(const char * s,int l);
int  jlib_decl atoi_l(const char * s,int l);
__int64 jlib_decl atoi64_l(const char * s,int l);
inline __int64 atoi64(const char* s) { return atoi64_l(s, (int)strlen(s)); }
#ifndef _WIN32
extern jlib_decl char * itoa(int n, char *str, int b);
extern jlib_decl char * ltoa(long n, char *str, int b);
extern jlib_decl char * ultoa(unsigned long n, char *str, int b);
#define Sleep(milli) MilliSleep(milli)
#endif
void jlib_decl packNumber(char * target, const char * source, unsigned slen);
void jlib_decl unpackNumber(char * target, const char * source, unsigned tlen);

int jlib_decl numtostr(char *dst, char _value);
int jlib_decl numtostr(char *dst, short _value);
int jlib_decl numtostr(char *dst, int _value);
int jlib_decl numtostr(char *dst, long _value);
int jlib_decl numtostr(char *dst, __int64 _value);
int jlib_decl numtostr(char *dst, unsigned char value);
int jlib_decl numtostr(char *dst, unsigned short value);
int jlib_decl numtostr(char *dst, unsigned int value);
int jlib_decl numtostr(char *dst, unsigned long value);
int jlib_decl numtostr(char *dst, unsigned __int64 _value);

extern jlib_decl HINSTANCE LoadSharedObject(const char *name, bool isGlobal, bool raiseOnError);
extern jlib_decl void FreeSharedObject(HINSTANCE h);

class jlib_decl SharedObject
{
public:
    SharedObject()      { h = 0; bRefCounted = false; }
    ~SharedObject()     { unload(); }
    
    bool load(const char * dllName, bool isGlobal, bool raiseOnError=false);
    bool loadCurrentExecutable();
    bool loaded()       { return h != 0; }
    void unload();
    HINSTANCE getInstanceHandle() const { return h; }
public:
    HINSTANCE       h;
    bool bRefCounted;
};

//---------------------------------------------------------------------------

//functions for generating unique identifiers consisting of 0..9,A..V
typedef unsigned __int64  unique_id_t;

extern jlib_decl StringBuffer & appendUniqueId(StringBuffer & target, unique_id_t value);
extern jlib_decl unique_id_t    getUniqueId();
extern jlib_decl StringBuffer & getUniqueId(StringBuffer & target);
extern jlib_decl void           resetUniqueId();

extern jlib_decl unsigned getRandom();              // global 
extern jlib_decl void seedRandom(unsigned seed);

interface IRandomNumberGenerator: public IInterface
{
    virtual void seed(unsigned seedval)=0;
    virtual unsigned next()=0;
};

extern jlib_decl IRandomNumberGenerator *createRandomNumberGenerator();

#ifdef WIN32

// Reentrant version of the rand() function for use with multithreaded applications.
// rand_r return value between 0 and RAND_R_MAX (exclusive). Not that RAND_MAX is
// implementation dependent: SHORT_MAX on Windows, INT_MAX on Linux.
jlib_decl int rand_r(unsigned int *seed);
#define RAND_R_MAX  INT_MAX

#else 

#define RAND_R_MAX  RAND_MAX

#endif

interface IShuffledIterator: extends IInterface
{
    virtual void seed(unsigned seedval)=0;  // ony required for repeatability
    virtual bool first()=0;
    virtual bool isValid() = 0;
    virtual bool next() = 0;
    virtual unsigned get() = 0;
    virtual unsigned lookup(unsigned idx) = 0;  // looks up idx'th entry
};

extern jlib_decl IShuffledIterator *createShuffledIterator(unsigned n); // returns iterator that returns 0..n-1 in shuffled order

/* misc */
extern jlib_decl bool isCIdentifier(const char* id);

/* base64 encoder/decoder */
extern jlib_decl void JBASE64_Encode(const void *data, long length, StringBuffer &out, bool addLineBreaks=true);
extern jlib_decl void JBASE64_Encode(const void *data, long length, IIOStream &out, bool addLineBreaks=true);
extern jlib_decl StringBuffer &JBASE64_Decode(const char *in, StringBuffer &out);
extern jlib_decl MemoryBuffer &JBASE64_Decode(const char *in, MemoryBuffer &out);
extern jlib_decl StringBuffer &JBASE64_Decode(ISimpleReadStream &in, StringBuffer &out);
extern jlib_decl MemoryBuffer &JBASE64_Decode(ISimpleReadStream &in, MemoryBuffer &out);

/**
 * Decode base 64 encoded string.
 * It handles forbidden printable and non-printable chars. Space(s) inserted among the valid chars,
 * missing pad chars and invalid length.
 *
 * @param length        Length of the input string.
 * @param in            Pointer to base64 encoded string
 * @param out           Decoded string if the input is valid
 * @return              True when success
 */
extern jlib_decl bool JBASE64_Decode(size32_t length, const char *in, StringBuffer &out);

extern jlib_decl void JBASE32_Encode(const char *in,StringBuffer &out);  // result all lower
extern jlib_decl void JBASE32_Decode(const char *in,StringBuffer &out);  

/* URL: http://user:passwd@host:port/path */
extern jlib_decl StringBuffer& encodeUrlUseridPassword(StringBuffer& out, const char* in);
extern jlib_decl StringBuffer& decodeUrlUseridPassword(StringBuffer& out, const char* in);

class jlib_decl StringArray : public ArrayOf<const char *, const char *>
{
public:
    // Appends a list in a string delimited by 'delim'
    void appendList(const char *list, const char *delim);
    // Appends a list in a string delimited by 'delim' without duplicates
    void appendListUniq(const char *list, const char *delim);
};
class CIStringArray : public StringArray, public CInterface
{
};

extern jlib_decl unsigned msTick();
extern jlib_decl unsigned usTick();
extern jlib_decl int make_daemon(bool printpid=false);  // outputs pid to stdout if printpid true
extern jlib_decl void doStackProbe();

#ifndef arraysize
#define arraysize(T) (sizeof(T)/sizeof(*T))
#endif

extern jlib_decl bool callExternalProgram(const char *progname, const StringBuffer &input, StringBuffer &output, StringArray *env=NULL);

extern jlib_decl unsigned __int64 greatestCommonDivisor(unsigned __int64 left, unsigned __int64 right);

inline unsigned hex2num(char next)
{
    if ((next >= '0') && (next <= '9'))
        return next - '0';
    if ((next >= 'a') && (next <= 'f'))
        return next - 'a' + 10;
    if ((next >= 'A') && (next <= 'F'))
        return next - 'A' + 10;
    return 0;
}

extern jlib_decl void initThreadLocal(int len, void* val);
extern jlib_decl void* getThreadLocalVal();
extern jlib_decl void clearThreadLocal();


extern jlib_decl bool matchesMask(const char *fn, const char *mask, unsigned p, unsigned n);
extern jlib_decl StringBuffer &expandMask(StringBuffer &buf, const char *mask, unsigned p, unsigned n);
extern jlib_decl bool constructMask(StringAttr &attr, const char *fn, unsigned p, unsigned n);
extern jlib_decl bool deduceMask(const char *fn, bool expandN, StringAttr &mask, unsigned &p, unsigned &n); // p is 0 based in these routines

class HashKeyElement;
class jlib_decl NamedCount
{
    HashKeyElement *ht;
public:
    NamedCount();
    ~NamedCount();
    void set(const char *name);
};

#ifdef NAMEDCOUNTS
#define DECL_NAMEDCOUNT NamedCount namedCount
#define INIT_NAMEDCOUNT namedCount.set(typeid(*this).name())
#else
#define DECL_NAMEDCOUNT
#define INIT_NAMEDCOUNT {}
#endif

extern jlib_decl StringBuffer &dumpNamedCounts(StringBuffer &str);


interface IAuthenticatedUser: extends IInterface
{
    virtual bool login(const char *user, const char *passwd) = 0;
    virtual void impersonate()=0;
    virtual void revert()=0;
    virtual const char *username()=0;
};

interface IAtom;
extern jlib_decl IAuthenticatedUser *createAuthenticatedUser();
extern jlib_decl void serializeAtom(MemoryBuffer & target, IAtom * name);
extern jlib_decl IAtom * deserializeAtom(MemoryBuffer & source);

template <class KEY, class VALUE, class COMPARE>
VALUE * binsearch(KEY key, VALUE * * values, unsigned num, COMPARE * cmp)
{
    unsigned l = 0;
    unsigned u = num;
    while(l<u)
    {
        unsigned i = (l+u)/2;
        int c = cmp->compare(key, values[i]);
        if(c == 0)
        {
            return values[i];
        }
        else if(c < 0)
        {
            u = i;
        }
        else
        {
            l = i+1;
        }
    }
    return NULL;
}

extern jlib_decl StringBuffer &genUUID(StringBuffer &in, bool nocase=false);

// Convert offset_t to string
// Note that offset_t can be 32, 64 bit integers or a structure
class jlib_decl OffsetToString
{
    StringBuffer m_buffer;
public:
    OffsetToString(offset_t offset);
    const char* str() { return m_buffer.str(); }
};

extern jlib_decl StringBuffer passwordInput(const char* prompt, StringBuffer& passwd);

extern jlib_decl IPropertyTree *getHPCCEnvironment(const char *configFileName=NULL);
extern jlib_decl bool getConfigurationDirectory(const IPropertyTree *dirtree, // NULL to use HPCC config
                                                const char *category, 
                                                const char *component,
                                                const char *instance, 
                                                StringBuffer &dirout);

extern jlib_decl const char * matchConfigurationDirectoryEntry(const char *path,const char *mask,StringBuffer &name, StringBuffer &component, StringBuffer &instance);
extern jlib_decl bool replaceConfigurationDirectoryEntry(const char *path,const char *frommask,const char *tomask,StringBuffer &out);

extern jlib_decl const char *queryCurrentProcessPath(); 

extern jlib_decl int parseCommandLine(const char * cmdline, MemoryBuffer &mb, const char** &argvout); // parses cmdline into argvout returning arg count (mb used as buffer)

extern jlib_decl bool safe_ecvt(size_t len, char * buffer, double value, int numDigits, int * decimal, int * sign);
extern jlib_decl bool safe_fcvt(size_t len, char * buffer, double value, int numPlaces, int * decimal, int * sign);
extern jlib_decl StringBuffer &getTempFilePath(StringBuffer & target, const char * component, IPropertyTree * pTree);

#endif

