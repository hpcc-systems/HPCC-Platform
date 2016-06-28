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

#ifdef _WIN32
#pragma warning(disable: 4996)
#include "winprocess.hpp"
#include <conio.h>
#endif
#include "platform.h"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jerror.hpp"
#ifdef _WIN32
#include <mmsystem.h> // for timeGetTime 
#include <float.h> //for _isnan and _fpclass
#else
#include <unistd.h> // read()
#include <sys/wait.h>
#include <pwd.h>
#ifdef __linux__
#include <crypt.h>
#include <shadow.h>
#endif
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <termios.h>
#include <signal.h>
#include <paths.h>
#include <cmath>
#include <random>
#include <map>
#include "build-config.h"
#endif

#include "portlist.h"

static NonReentrantSpinLock * cvtLock;

#ifdef _WIN32
static IRandomNumberGenerator * protectedGenerator;
static CriticalSection * protectedGeneratorCs;
#endif

#if defined (__APPLE__)
#include <mach-o/dyld.h>
#include <mach/mach_time.h> /* mach_absolute_time */
mach_timebase_info_data_t timebase_info  = { 1,1 };
#endif

MODULE_INIT(INIT_PRIORITY_SYSTEM)
{
    cvtLock = new NonReentrantSpinLock;
#ifdef _WIN32
    protectedGenerator = createRandomNumberGenerator();
    protectedGeneratorCs = new CriticalSection;
#endif
#if defined (__APPLE__)
    if (mach_timebase_info(&timebase_info) != KERN_SUCCESS)
        return false;
#endif
    return true;
}

MODULE_EXIT()
{
    delete cvtLock;
#ifdef _WIN32
    protectedGenerator->Release();
    delete protectedGeneratorCs;
#endif
}


//===========================================================================

bool safe_ecvt(size_t len, char * buffer, double value, int numDigits, int * decimal, int * sign)
{
#ifdef _WIN32
    return _ecvt_s(buffer, len, value, numDigits, decimal, sign) == 0;
#else
    NonReentrantSpinBlock block(*cvtLock);
    const char * result = ecvt(value, numDigits, decimal, sign);
    if (!result)
        return false;
    strncpy(buffer, result, len);
    return true;
#endif
}

bool safe_fcvt(size_t len, char * buffer, double value, int numPlaces, int * decimal, int * sign)
{
#ifdef _WIN32
    return _fcvt_s(buffer, len, value, numPlaces, decimal, sign) == 0;
#else
    NonReentrantSpinBlock block(*cvtLock);
    const char * result = fcvt(value, numPlaces, decimal, sign);
    if (!result)
        return false;
    strncpy(buffer, result, len);
    return true;
#endif
}

//===========================================================================

bool j_isnan(double x)
{
#ifdef _MSC_VER
    return _isnan(x)!=0;
#else
    return std::isnan(x);
#endif
}

bool j_isinf(double x)
{
#ifdef _MSC_VER
    int fpv = _fpclass(x);
    return (fpv==_FPCLASS_PINF || fpv==_FPCLASS_NINF);
#else
    return std::isinf(x);
#endif
}

#ifdef _WIN32
void MilliSleep(unsigned milli)
{
    Sleep(milli); 
}

#else

void MilliSleep(unsigned milli)
{
    if (milli) {
        unsigned target = msTick()+milli;
        loop {
            timespec sleepTime;
            
            if (milli>=1000)
            {
                sleepTime.tv_sec = milli/1000;
                milli %= 1000;
            }
            else
                sleepTime.tv_sec = 0;
            sleepTime.tv_nsec = milli * 1000000;
            if (nanosleep(&sleepTime, NULL)==0)
                break;
            if (errno!=EINTR) {
                PROGLOG("MilliSleep err %d",errno);
                break;
            }
            milli = target-msTick();
            if ((int)milli<=0)
                break;
        }
    }
    else
        ThreadYield();  // 0 means  yield 

}

#endif




long atolong_l(const char * s,int l)
{ 
    char t[32];
    memcpy(t,s,l);
    t[l]=0;
    return atol(t);
}

int  atoi_l(const char * s,int l)
{ 
    char t[32];
    memcpy(t,s,l);
    t[l]=0;
    return atoi(t);
}

__int64 atoi64_l(const char * s,int l)
{
    __int64 result = 0;
    char sign = '+';

    while (l>0 && isspace(*s))
    {
        l--;
        s++;
    }

    if (l>0 && (*s == '-' || *s == '+'))
    {
        sign = *s;
        l--;
        s++;
    }
    
    while (l>0 && isdigit(*s))
    {
        result = 10 * result + ((*s) - '0'); 
        l--;
        s++;
    }

    if (sign == '-')
        return -result;
    else
        return result;
}

#ifndef _WIN32
static char *_itoa(unsigned long n, char *str, int b, bool sign)
{
    char *s = str;
    
    if (sign)
        n = -n;
    
    do
    {
        byte d = n % b;
        *(s++) = d+((d<10)?'0':('a'-10));
    }
    while ((n /= b) > 0);
    if (sign)
        *(s++) = '-';
    *s = '\0';
    
    // reverse
    char *s2 = str;
    s--;
    while (s2<s)
    {
        char tc = *s2;
        *(s2++) = *s;
        *(s--) = tc;
    }
    
    return str;
}
char *itoa(int n, char *str, int b)
{
    return _itoa(n, str, b, (n<0));
}
char *ltoa(long n, char *str, int b)
{
    return _itoa(n, str, b, (n<0));
}
char *ultoa(unsigned long n, char *str, int b)
{
    return _itoa(n, str, b, false);
}
#endif

void packNumber(char * target, const char * source, unsigned slen)
{
    unsigned next = 0;
    while (slen)
    {
        unsigned c = *source++;
        if (c == ' ') c = '0';
        next = (next << 4) + (c - '0');
        slen--;
        
        if ((slen & 1) == 0)
        {
            *target++ = next;
            next = 0;
        }
    }
}


void unpackNumber(char * target, const char * source, unsigned tlen)
{
    if (tlen & 1)
    {
        *target = '0' + *source++;
        tlen--;
    }
    
    while (tlen)
    {
        unsigned char next = *source++;
        *target++ = (next >> 4) + '0';
        *target++ = (next & 15) + '0';
        tlen -= 2;
    }
}

//-----------------------------------------------------------------------

class jlib_thrown_decl CorruptDllException : public CInterfaceOf<ICorruptDllException>
{
public:
    CorruptDllException(int code, const char *_dllName, const char *_dlError)
    : errcode(code)
    {
        VStringBuffer s("Error loading %s: %s", _dllName, _dlError);
        msg.set(s.str());
    };
    int  errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append(msg.get());
    }
    MessageAudience errorAudience() const
    {
        return MSGAUD_operator;
    }
private:
    int errcode;
    StringAttr msg;
};

static bool isCorruptDll(const char *errorMessage)
{
    // yuk.
    // Add other error strings for corrupt .so files as/when we encounter them
    if (strstr(errorMessage, "file too short") ||
        strstr(errorMessage, "ELF load command past end of file"))
        return true;
    return false;

}
//-----------------------------------------------------------------------
HINSTANCE LoadSharedObject(const char *name, bool isGlobal, bool raiseOnError)
{
#if defined(_WIN32)
    UINT oldMode = SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOOPENFILEERRORBOX);
#else
    // don't think anything to do here.
#endif

    DynamicScopeCtx scope;


    StringBuffer tmp;
    if (name&&isPathSepChar(*name)&&isPathSepChar(name[1])) {
        RemoteFilename rfn;
        rfn.setRemotePath(name); 
        SocketEndpoint ep;
        if (!rfn.isLocal()) {
            // I guess could copy to a temporary location but currently just fail
            throw MakeStringException(-1,"LoadSharedObject: %s is not a local file",name);
        }
        name = rfn.getLocalPath(tmp).str();
    }


#if defined(_WIN32)
    HINSTANCE h = LoadLibrary(name);
    if (!LoadSucceeded(h))
    {
        int errcode = GetLastError();
        StringBuffer errmsg;
        formatSystemError(errmsg, errcode);
        //Strip trailing newlines - makes output/errors messages cleaner
        unsigned len = errmsg.length();
        while (len)
        {
            char c = errmsg.charAt(len-1);
            if ((c != '\r') && (c != '\n'))
                break;
            len--;
        }
        errmsg.setLength(len);
        DBGLOG("Error loading %s: %d - %s", name, errcode, errmsg.str());
        if (raiseOnError)
            throw MakeStringException(0, "Error loading %s: %d - %s", name, errcode, errmsg.str());
    }
#else
    HINSTANCE h = dlopen((char *)name, isGlobal ? RTLD_NOW|RTLD_GLOBAL : RTLD_NOW);
    if(h == NULL)
    {
        // Try again, with .so extension if necessary
        StringBuffer path, tail, ext;
        splitFilename(name, &path, &path, &tail, &ext, false);
        if (!streq(ext.str(), SharedObjectExtension))
        {
            // Assume if there's no .so, there may also be no lib at the beginning
            if (strncmp(tail.str(), SharedObjectPrefix, strlen(SharedObjectPrefix)) != 0)
                path.append(SharedObjectPrefix);
            path.append(tail).append(ext).append(SharedObjectExtension);
            name = path.str();
            h = dlopen((char *)name, isGlobal ? RTLD_NOW|RTLD_GLOBAL : RTLD_NOW);
        }
        if (h == NULL)
        {
            StringBuffer dlErrorMsg(dlerror());
            DBGLOG("Warning: Could not load %s: %s", name, dlErrorMsg.str());
            if (raiseOnError)
            {
                if (isCorruptDll(dlErrorMsg.str()))
                    throw new CorruptDllException(errno, name, dlErrorMsg.str());
                else
                    throw MakeStringException(0, "Error loading %s: %s", name, dlErrorMsg.str());
            }
        }
    }

#endif
    scope.setSoContext(h);

#if defined(_WIN32)
    SetErrorMode(oldMode);
#else
    // don't think anything to do here.
#endif

    return h;
}

void FreeSharedObject(HINSTANCE h)
{
    ExitModuleObjects(h);
#if defined(_WIN32)
    FreeLibrary(h);
#else
    dlclose(h);
#endif
}

bool SharedObject::load(const char * dllName, bool isGlobal, bool raiseOnError)
{
    unload();

#ifdef _WIN32
    UINT oldMode = SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOOPENFILEERRORBOX);
    if (dllName)
    {
        h=LoadSharedObject(dllName, isGlobal, raiseOnError);
        bRefCounted = true;
    }
    else
    {
        h=GetModuleHandle(NULL);
        bRefCounted = false;
    }
    SetErrorMode(oldMode);
#else
    h=LoadSharedObject(dllName, isGlobal, raiseOnError);
    bRefCounted = true;
#endif
    if (!LoadSucceeded(h))
    {
        h = 0;
        return false;
    }
    return true;
}

bool SharedObject::loadCurrentExecutable()
{
    unload();
#ifdef _WIN32
    h=GetModuleHandle(NULL);
    bRefCounted = false;
#else
    h=dlopen(NULL, RTLD_NOW);
    bRefCounted = true;
#endif
    return true;
}

void *SharedObject::getEntry(const char * name) const
{
    return GetSharedProcedure(getInstanceHandle(), name);
}

void SharedObject::unload()
{
    if (h && bRefCounted) FreeSharedObject(h);
    h = 0;
}

//-----------------------------------------------------------------------

IPluggableFactory *loadPlugin(const IPropertyTree *pluginInfo)
{
    const char *pluginName = pluginInfo->queryProp("@pluginName");
    const char *entrypoint = pluginInfo->queryProp("@entrypoint");
    if (!pluginName || !entrypoint)
        throw makeStringException(0, "Plugin information missing plugin name or entrypoint");
    Owned<SharedObject> pluginDll = new SharedObject;
    if (!pluginDll->load(pluginName, false, true))
        throw makeStringExceptionV(0, "Failed to load plugin %s", pluginName);
    IPluggableFactoryFactory pf = (IPluggableFactoryFactory) pluginDll->getEntry(entrypoint);
    if (!pf)
        throw makeStringExceptionV(0, "Function %s not found in plugin %s", entrypoint,  pluginName);
    IPluggableFactory *factory =  pf(pluginDll, pluginInfo);
    if (!factory)
        throw makeStringExceptionV(0, "Factory function %s returned NULL in plugin %s", entrypoint, pluginName);
    return factory;
}

//-----------------------------------------------------------------------

/*

  We use a 64 bit number for generating temporaries so that we are unlikely to get any
  clashes (being paranoid).  This should mean if a temporary ID is allocated 1,000,000,000
  times a second, then we won't repeat until 400 years later - assuming the machine stays
  alive for that long.
  
    Using a 32 bit number we loop after about an hour if we allocated 1,000,000 a second - 
    not an unreasonable estimate for the future.
*/

static unique_id_t nextTemporaryId;

StringBuffer & appendUniqueId(StringBuffer & target, unique_id_t value)
{
    //Just generate a temporary name from the __int64 - 
    //Don't care about the format, therfore use base 32 in reverse order.
    while (value)
    {
        unsigned next = ((unsigned)value) & 31;
        value = value >> 5;
        if (next < 10)
            target.append((char)(next+'0'));
        else
            target.append((char)(next+'A'-10));
    }
    return target;
}

unique_id_t getUniqueId()
{
    return ++nextTemporaryId;
}

StringBuffer & getUniqueId(StringBuffer & target)
{
    return appendUniqueId(target, ++nextTemporaryId);
}


void resetUniqueId()
{
    nextTemporaryId = 0;
}


//-----------------------------------------------------------------------

#define make_numtostr(VTYPE)                                    \
    int numtostr(char *dst, signed VTYPE _value)                    \
{                                                                \
    int c;                                                        \
    unsigned VTYPE value;                                        \
    if (_value<0)                                                \
{                                                            \
    *(dst++) = '-';                                            \
    value = (unsigned VTYPE) -_value;                        \
    c = 1;                                                    \
}                                                            \
    else                                                        \
{                                                            \
    c = 0;                                                    \
    value = _value;                                            \
}                                                            \
    char temp[11], *end = temp+10;                                \
    char *tmp = end;                                            \
    *tmp = '\0';                                                \
    \
    while (value>=10)                                            \
{                                                            \
    unsigned VTYPE next = value / 10;                        \
    *(--tmp) = ((char)(value-next*10))+'0';                    \
    value = next;                                            \
}                                                            \
    *(--tmp) = ((char)value)+'0';                                \
    \
    int diff = (int)(end-tmp);                                            \
    int i=diff+1;                                                \
    while (i--)    *(dst++) = *(tmp++);                            \
    \
    return c+diff;                                                \
}

#define make_unumtostr(VTYPE)                                    \
    int numtostr(char *dst, unsigned VTYPE value)            \
{                                                                \
    char temp[11], *end = temp+10;                                \
    char *tmp = end;                                            \
    *tmp = '\0';                                                \
    \
    while (value>=10)                                            \
{                                                            \
    unsigned VTYPE next = value / 10;                        \
    *(--tmp) = ((char)(value-next*10))+'0';                    \
    value = next;                                            \
}                                                            \
    *(--tmp) = ((char)value)+'0';                                \
    \
    int diff = (int)(end-tmp);                                            \
    int i=diff+1;                                                \
    while (i--)    *(dst++) = *(tmp++);                            \
    \
    return diff;                                                \
}

make_numtostr(char);
make_numtostr(short);
make_numtostr(int);
make_numtostr(long);

make_unumtostr(char);
make_unumtostr(short);
make_unumtostr(int);
make_unumtostr(long);

int numtostr(char *dst, __int64 _value)
{
    int c;
    unsigned __int64 value;
    if (_value<0)
    {
        *(dst++) = '-';
        value = (unsigned __int64) -_value;
        c = 1;
    }
    else
    {
        value = _value;
        c = 0;
    }
    char temp[24], *end = temp+23, *tmp = end;
    *tmp = '\0';
    unsigned __int32 v3 = (unsigned __int32)(value / LLC(10000000000));
    unsigned __int64 vv = value - ((unsigned __int64)v3*LLC(10000000000));
    unsigned __int32 v2 = (unsigned __int32)(vv / 100000);
    unsigned __int32 v1 = (unsigned __int32) (vv - (v2 * 100000));
    unsigned __int32 next;
    while (v1>=10)
    {
        next = v1/10;
        *(--tmp) = ((char)(v1-next*10))+'0';
        v1 = next;
    }
    *(--tmp) = ((char)v1)+'0';
    if (v2)
    {
        char *d = end-5;
        while (d != tmp)
            *(--tmp) = '0';
        while (v2>=10)
        {
            next = v2/10;
            *(--tmp) = ((char)(v2-next*10))+'0';
            v2 = next;
        }
        *(--tmp) = ((char)v2)+'0';
    }
    if (v3)
    {
        char *d = end-10;
        while (d != tmp)
            *(--tmp) = '0';
        while (v3>=10)
        {
            next = v3/10;
            *(--tmp) = ((char)(v3-next*10))+'0';
            v3 = next;
        }
        *(--tmp) = ((char)v3)+'0';
    }
    
    int diff = (int)(end-tmp);
#ifdef USEMEMCPY
    memcpy(dst, tmp, diff+1);
#else
    int i=diff+1;
    while (i--)
    {    *(dst++) = *(tmp++);
    }
#endif
    return c+diff;
}

int numtostr(char *dst, unsigned __int64 value)
{
    char temp[24], *end = temp+23, *tmp = end;
    *tmp = '\0';
    unsigned __int32 v3 = (unsigned __int32)(value / LLC(10000000000));
    unsigned __int64 vv = value - ((unsigned __int64)v3*LLC(10000000000));
    unsigned __int32 v2 = (unsigned __int32)(vv / 100000);
    unsigned __int32 v1 = (unsigned __int32) (vv - (v2 * 100000));
    unsigned __int32 next;
    while (v1>=10)
    {
        next = v1/10;
        *(--tmp) = ((char)(v1-next*10))+'0';
        v1 = next;
    }
    *(--tmp) = ((char)v1)+'0';
    if (v2)
    {
        char *d = end-5;
        while (d != tmp)
            *(--tmp) = '0';
        while (v2>=10)
        {
            next = v2/10;
            *(--tmp) = ((char)(v2-next*10))+'0';
            v2 = next;
        }
        *(--tmp) = ((char)v2)+'0';
    }
    if (v3)
    {
        char *d = end-10;
        while (d != tmp)
            *(--tmp) = '0';
        while (v3>=10)
        {
            next = v3/10;
            *(--tmp) = ((char)(v3-next*10))+'0';
            v3 = next;
        }
        *(--tmp) = ((char)v3)+'0';
    }
    
    int diff = (int)(end-tmp);
#ifdef USEMEMCPY
    memcpy(dst, tmp, diff+1);
#else
    int i=diff+1;
    while (i--)
    {    *(dst++) = *(tmp++);
    }
#endif
    return diff;
}


class CRandom: public CInterface, public IRandomNumberGenerator
{
    // from Knuth if I remember correctly
#define HISTORYSIZE 55
#define HISTORYMAX (HISTORYSIZE-1)

    unsigned history[HISTORYSIZE];
    unsigned ptr;
    unsigned lower;

public:
    IMPLEMENT_IINTERFACE;

    CRandom() 
    { 
        seed((unsigned)get_cycles_now()); 
    }
    
    void seed(unsigned su) 
    {
        ptr = HISTORYMAX;
        lower = 23;


        double s = 91648253+su;
        double a = 1389796;
        double m = 2147483647;  
        unsigned i;
        for (i=0;i<HISTORYSIZE;i++) { // just used for initialization
            s *= a;
            int q = (int)(s/m);
            s -= q*m;
            history[i] = (unsigned)s;
        }
    }
    

    unsigned next()
    {
        if (ptr==0) {
            ptr = HISTORYMAX;
            lower--;
        }
        else {
            ptr--;
            if (lower==0)
                lower = HISTORYMAX;
            else
                lower--;
        }
        unsigned ret = history[ptr]+history[lower];
        history[ptr] = ret;
        return ret;
    }

};

thread_local Owned<IRandomNumberGenerator> RandomMain = createRandomNumberGenerator();

unsigned getRandom()
{
    return RandomMain->next();
}

void seedRandom(unsigned seed)
{
    RandomMain->seed(seed);
}

IRandomNumberGenerator *createRandomNumberGenerator()
{
    return new CRandom();
}

class CPseudoRandomNumberGenerator: public CInterface, public IPseudoRandomNumberGenerator
{
public:
    IMPLEMENT_IINTERFACE

    CPseudoRandomNumberGenerator()
    {
        std::array<unsigned int, 5> sequence;
        std::random_device rd;
        std::seed_seq seeds{rd(), rd(), rd(), rd(), rd()};
        seeds.generate(sequence.begin(), sequence.end());
        auto seedIter = sequence.begin();
        pMinStd_Rand0     = new std::minstd_rand0(*seedIter);
        pMinStd_Rand      = new std::minstd_rand(*(++seedIter));
        pMT19937          = new std::mt19937(*(++seedIter));
        pRANLUX24_Base    = new std::ranlux24_base(*(++seedIter));
        pRANLUX48_Base    = new std::ranlux48_base(*(++seedIter));
    }

    virtual ~CPseudoRandomNumberGenerator()
    {
        delete pMinStd_Rand0;
        delete pMinStd_Rand;
        delete pMT19937;
        delete pRANLUX24_Base;
        delete pRANLUX48_Base;
    }

    template<class D>
    unsigned next(enum ePseudoRandomNumberEngine engine, D &d)
    {
        switch(engine)
        {
        case(MINSTD_RAND):
            return (d)(*pMinStd_Rand);
        case(MT19937):
            return (d)(*pMT19937);
        case(RANLUX24_BASE):
            return (d)(*pRANLUX24_Base);
        case(RANLUX48_BASE):
            return (d)(*pRANLUX48_Base);
        case(MINSTD_RAND0):
        default:
            return (d)(*pMinStd_Rand0);
        }
    }

    unsigned nextUniform(enum ePseudoRandomNumberEngine engine, unsigned int lower_bound, unsigned int upper_bound)
    {
        std::pair<unsigned, unsigned> _pair(lower_bound, upper_bound);
        auto iter = uniformDistributionMap.find(_pair);

        if (iter == uniformDistributionMap.end())
        {
            std::uniform_int_distribution<unsigned int> distribution(lower_bound, upper_bound);
            iter = uniformDistributionMap.insert(std::move(std::pair<decltype(_pair),decltype(distribution)>(_pair, distribution))).first;
        }
        return next(engine, iter->second);
    }

    unsigned nextBinomial(enum ePseudoRandomNumberEngine engine, double probability, unsigned int upper_bound)
    {
        std::pair<double, unsigned> _pair(probability, upper_bound);
        auto iter = binomialDistributionMap.find(_pair);

        if (iter == binomialDistributionMap.end())
        {
            std::binomial_distribution<unsigned int> distribution(upper_bound, probability);
            iter = binomialDistributionMap.insert(std::move(std::pair<decltype(_pair),decltype(distribution)>(_pair, distribution))).first;
        }
        return next(engine, iter->second);
    }

    unsigned nextNegativeBinomial(enum ePseudoRandomNumberEngine engine, double probability, unsigned int upper_bound)
    {
        std::pair<double, unsigned> _pair(probability, upper_bound);
        auto iter = negativeBinomialDistributionMap.find(_pair);

        if (iter == negativeBinomialDistributionMap.end())
        {
            std::negative_binomial_distribution<unsigned int> distribution(upper_bound, probability);
            iter = negativeBinomialDistributionMap.insert(std::move(std::pair<decltype(_pair),decltype(distribution)>(_pair, distribution))).first;
        }
        return next(engine, iter->second);
    }

    unsigned nextGeometric(enum ePseudoRandomNumberEngine engine, double probability)
    {
        auto iter = geometricDistributionMap.find(probability);

        if (iter == geometricDistributionMap.end())
        {
            std::geometric_distribution<unsigned> distribution(probability);
            iter = geometricDistributionMap.insert(std::move(std::pair<double,decltype(distribution)>(probability, distribution))).first;
        }
        return next(engine, iter->second);
    }

    unsigned nextPoisson(enum ePseudoRandomNumberEngine engine, double mean)
    {
        auto iter = poissonDistributionMap.find(mean);

        if (iter == poissonDistributionMap.end())
        {
            std::poisson_distribution<unsigned> distribution(mean);
            iter = poissonDistributionMap.insert(std::move(std::pair<double,decltype(distribution)>(mean, distribution))).first;
        }
        return next(engine, iter->second);
    }

private:

    // Pseudo-Random Number Engines
    std::minstd_rand0   *pMinStd_Rand0;
    std::minstd_rand    *pMinStd_Rand;
    std::mt19937        *pMT19937;
    std::ranlux24_base  *pRANLUX24_Base;
    std::ranlux48_base  *pRANLUX48_Base;

    // Containers for distributions maps
    std::map<std::pair<unsigned, unsigned>, std::uniform_int_distribution<unsigned>>        uniformDistributionMap;
    std::map<std::pair<unsigned, double>, std::binomial_distribution<unsigned>>             binomialDistributionMap;
    std::map<std::pair<unsigned, double>, std::negative_binomial_distribution<unsigned>>    negativeBinomialDistributionMap;
    std::map<double, std::geometric_distribution<unsigned>>                                 geometricDistributionMap;
    std::map<double, std::poisson_distribution<unsigned>>                                   poissonDistributionMap;
};

IPseudoRandomNumberGenerator *createPseudoRandomNumberGenerator()
{
    return new CPseudoRandomNumberGenerator();
}

#ifdef WIN32
// This function has the same prototype for rand_r, but seed is ignored.

jlib_decl int rand_r(unsigned int *seed)
{
    CriticalBlock procedure(*protectedGeneratorCs);
    return (protectedGenerator->next() & RAND_R_MAX);
}
#if ((RAND_R_MAX & (RAND_R_MAX+1)) != 0)
#error RAND_R_MAX expected to be 2^n-1
#endif
#endif

class CShuffledIterator: public CInterface, implements IShuffledIterator
{
    CRandom rand;
    unsigned *seq;
    unsigned idx;
    unsigned num;
public:
    IMPLEMENT_IINTERFACE;

    CShuffledIterator(unsigned _num)
    {
        num = _num;
        idx = 0;
        seq = NULL;
    }
    ~CShuffledIterator()
    {
        delete [] seq;
    }

    bool first()
    {
        if (!seq)
            seq = new unsigned[num];
        idx = 0;
        if (!num)
            return false;
        unsigned i;
        for (i=0;i<num;i++)
            seq[i] = i;
        while (i>1) {
            unsigned j = rand.next()%i;  // NB i is correct here
            i--;
            unsigned t = seq[j];
            seq[j] = seq[i];
            seq[i] = t;
        }
        return true;
    }

    bool isValid()
    {
        return idx<num;
    }

    bool next() 
    {
        if (idx<num) 
            idx++;
        return isValid();
    }

    unsigned get()
    {
        return lookup(idx);
    }

    unsigned lookup(unsigned i) 
    {
        if (!seq)
            first();
        return seq[i%num];
    }

    void seed(unsigned su) 
    {
        rand.seed(su);
    }


};

extern jlib_decl IShuffledIterator *createShuffledIterator(unsigned n)
{
    return new CShuffledIterator(n);
}


/* Check whether a string is a valid C identifier. */
bool isCIdentifier(const char* id)
{
    if (id==NULL || *id==0)
        return false;
    
    if (!isalpha(*id) && *id!='_')
        return false;

    for (++id; *id != 0; id++)
        if (!isalnum(*id) && *id!='_')
            return false;

    return true;
}

//-------------------------------------------------------------------
static const char BASE64_enc[65] =  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "abcdefghijklmnopqrstuvwxyz"
                                    "0123456789+/";

static const unsigned char BASE64_dec[256] =
{
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x00, 0x00, 0x00, 0x3f,
0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

static const char pad = '=';


//
// Encode the input in a base64 format
//
// const void * data -> data to be encoded
// long length       -> length in bytes of this data
// IIOStream &out    -> Write the result into this stream
//
void JBASE64_Encode(const void *data, long length, IIOStream &out, bool addLineBreaks/*=true*/)
{
    const unsigned char *in = static_cast<const unsigned char *>(data);

    unsigned char one;
    unsigned char two;
    unsigned char three;

    long i;
    for(i = 0; i < length && length - i >= 3;)
    {
        one = *(in + i++);
        two = *(in + i++);
        three = *(in + i++);

        // 0x30 -> 0011 0000 b
        // 0x3c -> 0011 1100 b
        // 0x3f -> 0011 1111 b
        //
        writeCharToStream(out, BASE64_enc[one >> 2]);
        writeCharToStream(out, BASE64_enc[((one << 4) & 0x30) | (two >> 4)]);
        writeCharToStream(out, BASE64_enc[((two << 2)  & 0x3c) | (three >> 6)]);
        writeCharToStream(out, BASE64_enc[three & 0x3f]);

        if(addLineBreaks && (i % 54 == 0))
        {
            writeCharToStream(out, '\n');
        }
    }

    switch(length - i)
    {
        case 2:
            one = *(in + i++);
            two = *(in + i++);

            writeCharToStream(out, BASE64_enc[one >> 2]);
            writeCharToStream(out, BASE64_enc[((one << 4) & 0x30) | (two >> 4)]);
            writeCharToStream(out, BASE64_enc[(two << 2)  & 0x3c]);
            writeCharToStream(out, pad);
        break;

        case 1:
            one = *(in + i++);

            writeCharToStream(out, BASE64_enc[one >> 2]);
            writeCharToStream(out, BASE64_enc[(one << 4) & 0x30]);
            writeCharToStream(out, pad);
            writeCharToStream(out, pad);
        break;
    }
}

// JCSMORE could have IIOStream StringBuffer adapter inplace of below.
void JBASE64_Encode(const void *data, long length, StringBuffer &out, bool addLineBreaks/*=true*/)
{
    const unsigned char *in = static_cast<const unsigned char *>(data);

    unsigned char one;
    unsigned char two;
    unsigned char three;

    long i;
    for(i = 0; i < length && length - i >= 3;)
    {
        one = *(in + i++);
        two = *(in + i++);
        three = *(in + i++);

        // 0x30 -> 0011 0000 b
        // 0x3c -> 0011 1100 b
        // 0x3f -> 0011 1111 b
        //
        out.append(BASE64_enc[one >> 2]);
        out.append(BASE64_enc[((one << 4) & 0x30) | (two >> 4)]);
        out.append(BASE64_enc[((two << 2)  & 0x3c) | (three >> 6)]);
        out.append(BASE64_enc[three & 0x3f]);

        if(addLineBreaks && (i % 54 == 0))
        {
            out.append('\n');
        }
    }

    switch(length - i)
    {
        case 2:
            one = *(in + i++);
            two = *(in + i++);

            out.append(BASE64_enc[one >> 2]);
            out.append(BASE64_enc[((one << 4) & 0x30) | (two >> 4)]);
            out.append(BASE64_enc[(two << 2)  & 0x3c]);
            out.append(pad);
        break;

        case 1:
            one = *(in + i++);

            out.append(BASE64_enc[one >> 2]);
            out.append(BASE64_enc[(one << 4) & 0x30]);
            out.append(pad);
            out.append(pad);
        break;
    }
}

//
// Decode the input in a base64 format
// 
// const char *in -> The string to be decoded
// StringBuffer & out       -> Decoded string here
//
StringBuffer &JBASE64_Decode(ISimpleReadStream &in, StringBuffer &out)
{
    unsigned char c1, cs[3];
    unsigned char &c2 = *cs;
    unsigned char &c3 = *(cs+1);
    unsigned char &c4 = *(cs+2);

    unsigned char d1, d2, d3, d4;

    for(;;)
    {
        if (in.read(1, &c1))
            break;
        if (!c1)
            break;
        else if (!isspace(c1))
        {
            in.read(3, cs);
            d1 = BASE64_dec[c1];
            d2 = BASE64_dec[c2];
            d3 = BASE64_dec[c3];
            d4 = BASE64_dec[c4];

            out.append((char)((d1 << 2) | (d2 >> 4)));

            if(c3 == pad)
                break;

            out.append((char)((d2 << 4) | (d3 >> 2)));

            if(c4 == pad)
                break;

            out.append((char)((d3 << 6) | d4));
        }
    }

    return out;
}

MemoryBuffer &JBASE64_Decode(ISimpleReadStream &in, MemoryBuffer &out)
{
    unsigned char c1, cs[3];
    unsigned char &c2 = *cs;
    unsigned char &c3 = *(cs+1);
    unsigned char &c4 = *(cs+2);

    unsigned char d1, d2, d3, d4;

    for(;;)
    {
        if (in.read(1, &c1) != 1)
            break;
        if (!c1)
            break;
        else if (!isspace(c1))
        {
            in.read(3, cs);
            d1 = BASE64_dec[c1];
            d2 = BASE64_dec[c2];
            d3 = BASE64_dec[c3];
            d4 = BASE64_dec[c4];

            out.append((char)((d1 << 2) | (d2 >> 4)));

            if(c3 == pad)
                break;

            out.append((char)((d2 << 4) | (d3 >> 2)));

            if(c4 == pad)
                break;

            out.append((char)((d3 << 6) | d4));
        }
    }

    return out;
}

StringBuffer &JBASE64_Decode(const char *incs, StringBuffer &out)
{
    unsigned char c1, c2, c3, c4;
    unsigned char d1, d2, d3, d4;

    for(;;)
    {
        c1 = *incs++;
        if (!c1)
            break;
        else if (!isspace(c1))
        {
            c2 = *incs++;
            c3 = *incs++;
            c4 = *incs++;
            d1 = BASE64_dec[c1];
            d2 = BASE64_dec[c2];
            d3 = BASE64_dec[c3];
            d4 = BASE64_dec[c4];

            out.append((char)((d1 << 2) | (d2 >> 4)));

            if(c3 == pad)
                break;

            out.append((char)((d2 << 4) | (d3 >> 2)));

            if(c4 == pad)
                break;

            out.append((char)((d3 << 6) | d4));
        }
    }

    return out;
}


MemoryBuffer &JBASE64_Decode(const char *incs, MemoryBuffer &out)
{
    unsigned char c1, c2, c3, c4;
    unsigned char d1, d2, d3, d4;

    for(;;)
    {
        c1 = *incs++;
        if (!c1)
            break;
        else if (!isspace(c1))
        {
            c2 = *incs++;
            c3 = *incs++;
            c4 = *incs++;
            d1 = BASE64_dec[c1];
            d2 = BASE64_dec[c2];
            d3 = BASE64_dec[c3];
            d4 = BASE64_dec[c4];

            out.append((char)((d1 << 2) | (d2 >> 4)));

            if(c3 == pad)
                break;

            out.append((char)((d2 << 4) | (d3 >> 2)));

            if(c4 == pad)
                break;

            out.append((char)((d3 << 6) | d4));
        }
    }

    return out;
}



bool JBASE64_Decode(size32_t length, const char *incs, StringBuffer &out)
{
    out.ensureCapacity(((length / 4) + 1) * 3);

    const char * end = incs + length;
    unsigned char c1;
    unsigned char c[4];
    unsigned cIndex = 0;
    unsigned char d1, d2, d3, d4;
    bool fullQuartetDecoded = false;

    while (incs < end)
    {
        c1 = *incs++;

        if (isspace(c1))
            continue;

        if (!BASE64_dec[c1] && ('A' != c1) && (pad != c1))
        {
            // Forbidden char
            fullQuartetDecoded = false;
            break;
        }

        c[cIndex++] = c1;
        fullQuartetDecoded = false;

        if (4 == cIndex)
        {
            d1 = BASE64_dec[c[0]];
            d2 = BASE64_dec[c[1]];
            d3 = BASE64_dec[c[2]];
            d4 = BASE64_dec[c[3]];

            out.append((char)((d1 << 2) | (d2 >> 4)));
            fullQuartetDecoded = true;

            if (pad == c[2])
                break;

            out.append((char)((d2 << 4) | (d3 >> 2)));

            if( pad == c[3])
                break;

            out.append((char)((d3 << 6) | d4));

            cIndex = 0;
        }
    }

    return fullQuartetDecoded;
}

static const char enc32[33] =
           "abcdefghijklmnopqrstuvwxyz"
           "234567";
static inline void encode5_32(const byte *in,StringBuffer &out)
{
    // 5 bytes in 8 out
    out.append(enc32[(in[0] >> 3)]);
    out.append(enc32[((in[0] & 0x07) << 2) | (in[1] >> 6)]);
    out.append(enc32[(in[1] >> 1) & 0x1f]);
    out.append(enc32[((in[1] & 0x01) << 4) | (in[2] >> 4)]);
    out.append(enc32[((in[2] & 0x0f) << 1) | (in[3] >> 7)]);
    out.append(enc32[(in[3] >> 2) & 0x1f]);
    out.append(enc32[((in[3] & 0x03) << 3) | (in[4] >> 5)]);
    out.append(enc32[in[4] & 0x1f]);
}

void JBASE32_Encode(const char *in,StringBuffer &out)
{
    size32_t len = (size32_t)strlen(in);
    while (len>=5) {
        encode5_32((const byte *)in,out);
        in += 5;
        len -= 5;
    }
    byte rest[5];
    memcpy(rest,in,len);
    memset(rest+len,0,5-len);
    encode5_32(rest,out);
}

static inline byte decode_32c(char c)
{
    byte b = (byte)c;
    if (b>=97) {
        if (b<=122)
            return b-97;
    }
    else if ((b>=50)&&(b<=55))
        return b-24;
    return 0;
}

void JBASE32_Decode(const char *bi,StringBuffer &out)
{
    loop {
        byte b[8];
        for (unsigned i=0;i<8;i++)
            b[i] =  decode_32c(*(bi++));
        byte o;
        o = ((b[0] & 0x1f) << 3) | ((b[1] & 0x1c) >> 2);
        if (!o) return;
        out.append(o);
        o = ((b[1] & 0x03) << 6) | ((b[2] & 0x1f) << 1) | ((b[3] & 0x10) >> 4);
        if (!o) return;
        out.append(o);
        o = ((b[3] & 0x0f) << 4) | ((b[4] & 0x1e) >> 1);
        if (!o) return;
        out.append(o);
        o = ((b[4] & 0x01) << 7) | ((b[5] & 0x1f) << 2) | ((b[6] & 0x18) >> 3);
        if (!o) return;
        out.append(o);
        o = ((b[6] & 0x07) << 5) | (b[7] & 0x1f);
        if (!o) return;
        out.append(o);
    }           
}


static void DelimToStringArray(const char *csl, StringArray &dst, const char *delim, bool deldup)
{
    if (!csl)
        return;
    const char *s = csl;
    char c;
    if (!delim)
        c = ',';
    else if (*delim&&!delim[1])
        c = *delim;
    else 
        c = 0;
    StringBuffer str;
    unsigned dstlen=dst.ordinality();
    loop {
        while (isspace(*s))
            s++;
        if (!*s&&(dst.ordinality()==dstlen)) // this check is to allow trailing separators (e.g. ",," is 3 (NULL) entries) but not generate an entry for ""
            break;
        const char *e = s;
        while (*e) {
            if (c) {
                if (*e==c)
                    break;
            }
            else if (strchr(delim,*e))
                break;
            e++;
        }
        str.clear().append((size32_t)(e-s),s).clip();
        if (deldup) {
            const char *s1 = str.str();
            unsigned i;
            for (i=0;i<dst.ordinality();i++)
                if (strcmp(s1,dst.item(i))==0)
                    break;
            if (i>=dst.ordinality())
                dst.append(s1);
        }
        else
            dst.append(str.str());
        if (!*e) 
            break;
        s = e+1;
    }
}

void StringArray::appendList(const char *list, const char *delim)
{
    DelimToStringArray(list, *this, delim, false);
}

void StringArray::appendListUniq(const char *list, const char *delim)
{
    DelimToStringArray(list, *this, delim, true);
}

void StringArray::sortAscii(bool nocase)
{
    PARENT::sort(nocase ? CCmp::compareNC : CCmp::compare);
}

void StringArray::sortAsciiReverse(bool nocase)
{
    PARENT::sort(nocase ? CCmp::revCompareNC : CCmp::revCompare);
}

void StringArray::sortCompare(int (*compare)(const char * const * l, const char * const * r))
{
    PARENT::sort(compare);
}


#ifdef _WIN32


unsigned msTick() { return timeGetTime(); }
unsigned usTick()
{
    static __int64 freq=0;
    LARGE_INTEGER v;
    if (!freq) {
        if (QueryPerformanceFrequency(&v))
            freq=v.QuadPart;
        if (!freq)
            return 0;
    }
    if (!QueryPerformanceCounter(&v))
        return 0;
    return (unsigned) ((v.QuadPart*1000000)/freq); // hope dividend doesn't overflow though it might
}

#else
#ifdef CLOCK_MONOTONIC
static bool use_gettimeofday=false;
unsigned msTick() 
{ 
    if (!use_gettimeofday) {
        timespec tm;
        if (clock_gettime(CLOCK_MONOTONIC, &tm)>=0)
            return tm.tv_sec*1000+(tm.tv_nsec/1000000); 
        use_gettimeofday = true;
        fprintf(stderr,"clock_gettime CLOCK_MONOTONIC returns %d",errno);   // don't use PROGLOG
    }
    struct timeval tm;
    gettimeofday(&tm,NULL);
    return tm.tv_sec*1000+(tm.tv_usec/1000); 
}
unsigned usTick() 
{ 
    if (!use_gettimeofday) {
        timespec tm;
        if (clock_gettime(CLOCK_MONOTONIC, &tm)>=0)
            return tm.tv_sec*1000000+(tm.tv_nsec/1000); 
        use_gettimeofday = true;
        fprintf(stderr,"clock_gettime CLOCK_MONOTONIC returns %d",errno);   // don't use PROGLOG
    }
    struct timeval tm;
    gettimeofday(&tm,NULL);
    return tm.tv_sec*1000000+tm.tv_usec; 
}
#elif __APPLE__

unsigned usTick()
{
    __uint64 nano = mach_absolute_time() * (uint64_t)timebase_info.numer / (uint64_t)timebase_info.denom;
    return nano / 1000;
}

unsigned msTick()
{
    __uint64 nano = mach_absolute_time() * (uint64_t)timebase_info.numer / (uint64_t)timebase_info.denom;
    return nano / 1000000;
}

#else
#warning "clock_gettime(CLOCK_MONOTONIC) not supported"
unsigned msTick() 
{ 
  struct timeval tm;
  gettimeofday(&tm,NULL);
  return tm.tv_sec*1000+(tm.tv_usec/1000); 
}
unsigned usTick() 
{ 
  struct timeval tm;
  gettimeofday(&tm,NULL);
  return tm.tv_sec*1000000+tm.tv_usec; 
}
#endif
#endif


int make_daemon(bool printpid) 
{
#ifndef _WIN32
    pid_t   pid, sid;
    pid = fork();
    if (pid < 0) {
        PrintLog("fork failed\n");
        return(EXIT_FAILURE);
    }
    if (pid > 0) {
        if (printpid) {
            int status;
            waitpid(pid, &status, 0);
            if (WEXITSTATUS(status)!=0) 
                return EXIT_FAILURE;
        }
        exit(EXIT_SUCCESS); 
    }

    if ((sid = setsid()) < 0) {
        PrintLog("error: set sid failed\n");
        return(EXIT_FAILURE);
    }

    umask(0);


    pid = fork();                           // To prevent zombies
    if (pid < 0) {                          
        PrintLog("fork failed (2)\n");
        return(EXIT_FAILURE);
    }
    if (pid > 0) {
        if (printpid)
            fprintf(stdout,"%d\n",pid);
        exit(EXIT_SUCCESS);
    }

    if (!freopen("/dev/null", "r", stdin) ||
        !freopen("/dev/null", "w", stdout) ||
        !freopen("/dev/null", "w", stderr)) {
        PrintLog("reopen std in/out/err failed\n");
        return(EXIT_FAILURE);
    }

    return(EXIT_SUCCESS);
#else
     return 0;
#endif

}

//Calculate the greatest common divisor using Euclid's method
unsigned __int64 greatestCommonDivisor(unsigned __int64 left, unsigned __int64 right)
{
    loop
    {
        if (left > right)
        {
            if (right == 0)
                return left;
            left = left % right;
        }
        else
        {
            if (left == 0)
                return right;
            right = right % left;
        }
    }
}

//The whole point of this function is to force memory to be accessed on the stack to avoid page faults.
//Therefore disable the gcc warning.
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif

//In a separate module to stop optimizer removing the surrounding catch.
void doStackProbe()
{
    byte local;
    const volatile byte * x = (const byte *)&local;
    byte forceload __attribute__((unused)) = x[-4096];
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef _WIN32

DWORD dwTlsIndex = -1;
CriticalSection tlscrit;

void initThreadLocal(int len, void* val)
{
    {
        CriticalBlock b(tlscrit);
        if(dwTlsIndex == -1)
        {
            if ((dwTlsIndex = TlsAlloc()) == TLS_OUT_OF_INDEXES) 
                throw MakeStringException(-1, "TlsAlloc failed"); 
        }
    }

    LPVOID lpvData; 
    
    lpvData = TlsGetValue(dwTlsIndex); 
    if (lpvData != 0) 
        LocalFree((HLOCAL) lpvData); 
    
    // Initialize the TLS index for this thread. 
    lpvData = (LPVOID) LocalAlloc(LPTR, len);
    memcpy((char*)lpvData, val, len);
    if (! TlsSetValue(dwTlsIndex, lpvData))
        throw MakeStringException(-1, "TlsSetValue error");
}

void* getThreadLocalVal()
{
    if(dwTlsIndex == -1)
        return NULL;

    return TlsGetValue(dwTlsIndex);
}

void clearThreadLocal()
{
    if(dwTlsIndex == -1)
        return;

    LPVOID lpvData = TlsGetValue(dwTlsIndex); 
    if (lpvData != 0) 
    {
        LocalFree((HLOCAL) lpvData);  
        if (! TlsSetValue(dwTlsIndex, NULL))
            throw MakeStringException(-1, "TlsSetValue error");
    }
}

#else
// Key for the thread-specific buffer
static pthread_key_t buffer_key;
// Once-only initialisation of the key
static pthread_once_t buffer_key_once = PTHREAD_ONCE_INIT;

// Free the thread-specific buffer
static void buffer_destroy(void * buf)
{
    if(buf)
        free(buf);
}

// Allocate the key 
static void buffer_key_alloc()
{
    pthread_key_create(&buffer_key, buffer_destroy);
}

// Allocate the thread-specific buffer
void initThreadLocal(int len, void* val)
{
    pthread_once(&buffer_key_once, buffer_key_alloc);
    void* valbuf = malloc(len);
    memcpy(valbuf, val, len);
    pthread_setspecific(buffer_key, valbuf);
}

// Return the thread-specific buffer
void* getThreadLocalVal()
{
    return (char *) pthread_getspecific(buffer_key);
}

void clearThreadLocal()
{
}

#endif

StringBuffer &expandMask(StringBuffer &buf, const char *mask, unsigned p, unsigned n)
{
    const char *s=mask;
    if (s)
        while (*s) {
            char next = *(s++);
            if (next=='$') {
                char pc = toupper(s[0]);
                if (pc&&(s[1]=='$')) {
                    if (pc=='P') {
                        buf.append(p+1);
                        next = 0;
                        s+=2;
                    }   
                    else if (pc=='N') {
                        buf.append(n);
                        next = 0;
                        s+=2;
                    }   
                }
            }
            if (next)
                buf.append(next);
        }
    return buf;
}

static const char *findExtension(const char *fn)
{
    if (!fn)
        return NULL;
    const char *ret = strchr(fn,'.');
    if (ret) {
        loop {
            ret++;
            const char *s = strchr(ret,'.');
            if (!s)
                break;
            ret = s;
        }
    }
    return ret;
}

unsigned runExternalCommand(StringBuffer &output, const char *cmd, const char *input)
{
    try
    {
        Owned<IPipeProcess> pipe = createPipeProcess();
        pipe->run(cmd, cmd, ".", input != NULL, true, true, 1024*1024);
        if (input)
        {
            pipe->write(strlen(input), input);
            pipe->closeInput();
        }
        char buf[1024];
        while (true)
        {
            size32_t read = pipe->read(sizeof(buf), buf);
            if (!read)
                break;
            output.append(read, buf);
        }
        int ret = pipe->wait();
        StringBuffer error;
        while (true)
        {
            size32_t read = pipe->readError(sizeof(buf), buf);
            if (!read)
                break;
            error.append(read, buf);
        }
        return ret;
    }
    catch (IException *E)
    {
        E->Release();
        output.clear();
        return 255;
    }
}

bool matchesMask(const char *fn, const char *mask, unsigned p, unsigned n)
{
    StringBuffer match;
    expandMask(match,mask,p,n);
    return (stricmp(fn,match.str())==0);
}

bool constructMask(StringAttr &attr, const char *fn, unsigned p, unsigned n)
{
    StringBuffer buf;
    const char *ext = findExtension(fn);
    if (!ext)
        return false;
    buf.append((size32_t)(ext-fn),fn).append("_$P$_of_$N$");
    if (matchesMask(fn,buf.str(),p,n)) {
        attr.set(buf.str());
        return true;
    }
    return false;
}

bool deduceMask(const char *fn, bool expandN, StringAttr &mask, unsigned &pret, unsigned &nret)
{
    const char *e = findExtension(fn);
    if (!e)
        return false;
    loop {                  
        const char *s=e;
        if (*s=='_') {
            s++;
            unsigned p = 0;
            while (isdigit(*s))
                p = p*10+*(s++)-'0';
            if (p&&(memcmp(s,"_of_",4)==0)) {
                s += 4;
                pret = p-1;
                p = 0;
                while (isdigit(*s))
                    p = p*10+*(s++)-'0';
                nret = p;
                if (((*s==0)||(*s=='.'))&&(p>pret)) {
                    StringBuffer buf;
                    if (expandN)
                        buf.append((size32_t)(e-fn),fn).append("_$P$_of_").append(p);
                    else
                        buf.append((size32_t)(e-fn),fn).append("_$P$_of_$N$");
                    if (*s=='.')
                        buf.append(s);
                    mask.set(buf);
                    return true;
                }
            }
        }
        e--;
        loop {
            if (e==fn)
                return false;
            if (*(e-1)=='.')
                break;
            e--;
        }
    }
    return false;
}

//==============================================================
#ifdef _WIN32


class CWindowsAuthenticatedUser: public CInterface, implements IAuthenticatedUser
{
    StringAttr name;
    HANDLE usertoken;
public:
    IMPLEMENT_IINTERFACE;
    CWindowsAuthenticatedUser()
    {
        usertoken = (HANDLE)-1;
    }
    ~CWindowsAuthenticatedUser()
    {
        if (usertoken != (HANDLE)-1)
            CloseHandle(usertoken);
    }
    bool login(const char *user, const char *passwd) 
    {
        name.clear();
        if (usertoken != (HANDLE)-1)
            CloseHandle(usertoken);
        StringBuffer domain("");
        const char *ut = strchr(user,'\\');
        if (ut) {
            domain.clear().append((size32_t)(ut-user),user);
            user = ut+1;
        }
        BOOL res = LogonUser((LPTSTR)user,(LPTSTR)(domain.length()==0?NULL:domain.str()),(LPTSTR)passwd,LOGON32_LOGON_NETWORK,LOGON32_PROVIDER_DEFAULT,&usertoken);
        if (res==0)
            return false;
        name.set(user);
        return true;
    }
    void impersonate()
    {
        if (!ImpersonateLoggedOnUser(usertoken))
            throw makeOsException(GetLastError());
    }

    void revert()
    {
        RevertToSelf();
    }

    const char *username()
    {
        return name.get();
    }
};

IAuthenticatedUser *createAuthenticatedUser() { return new CWindowsAuthenticatedUser; }

#elif defined(__linux__)

class CLinuxAuthenticatedUser: public CInterface, implements IAuthenticatedUser
{
    StringAttr name;
    uid_t uid;
    gid_t gid;
    uid_t saveuid;
    gid_t savegid;

public:
    IMPLEMENT_IINTERFACE;
    bool login(const char *user, const char *passwd) 
    {
        name.clear();
        const char *ut = strchr(user,'\\');
        if (ut) 
            user = ut+1; // remove windows domain
        struct passwd *pw;
        char *epasswd;
        if ((pw = getpwnam(user)) == NULL) 
            return false;
        struct spwd *spwd = getspnam(user);
        if (spwd) 
            epasswd = spwd->sp_pwdp;
        else
            epasswd = pw->pw_passwd;
        if (!epasswd||!*epasswd) 
            return false;
        if (strcmp(crypt(passwd,epasswd),epasswd)!=0) 
            return false;
        uid = pw->pw_uid;
        gid = pw->pw_gid;
        name.set(pw->pw_name);
        return true;
    }
    void impersonate()
    {
        saveuid = geteuid();
        savegid = getegid();
        if (setegid(gid) == -1)
            throw makeOsException(errno, "Failed to set effective group id");
        if (seteuid(uid) == -1)
            throw makeOsException(errno, "Failed to set effective user id");
    }

    void revert()
    {
        if (seteuid(saveuid) == -1)
            throw makeOsException(errno, "Failed to restore effective group id");
        if (setegid(savegid) == -1)
            throw makeOsException(errno, "Failed to restore effective user id");
    }

    const char *username()
    {
        return name.get();
    }

};



IAuthenticatedUser *createAuthenticatedUser() { return new CLinuxAuthenticatedUser; }
#elif defined(__FreeBSD__) || defined (__APPLE__)

IAuthenticatedUser *createAuthenticatedUser() { UNIMPLEMENTED; }

#endif


extern jlib_decl void serializeAtom(MemoryBuffer & target, IAtom * name)
{
    StringBuffer lower(str(name));
    lower.toLowerCase();
    serialize(target, lower.str());
}

extern jlib_decl IAtom * deserializeAtom(MemoryBuffer & source)
{
    StringAttr text;
    deserialize(source, text);
    if (text)
        return createAtom(text);
    return NULL;
}

//==============================================================

static const char enc64[65] =
           "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
           "abcdefghijklmnopqrstuvwxyz"
           "0123456789-_";

static inline void encode3_64(byte *in,StringBuffer &out)
{
    // 3 bytes in 4 out
    out.append(enc64[in[0]>>2]);
    out.append(enc64[((in[0] << 4) & 0x30) | (in[1] >> 4)]);
    out.append(enc64[((in[1] << 2)  & 0x3c) | (in[2] >> 6)]);
    out.append(enc64[in[2] & 0x3f]);
}




static NonReentrantSpinLock lock;
static unsigned uuidbin[5] = {0,0,0,0,0};
StringBuffer &genUUID(StringBuffer &out, bool nocase)
{ // returns a 24 char UUID for nocase=false or 32 char for nocase=true
    lock.enter();
    // could be quicker using statics
    if (uuidbin[0]==0) {
        queryHostIP().getNetAddress(sizeof(uuidbin[0]),uuidbin);
        uuidbin[1] = (unsigned)GetCurrentProcessId();
    }
    time_t t;
    time(&t);
    uuidbin[2] = (unsigned)t;
    uuidbin[3] = msTick();
    uuidbin[4]++;  
    byte *in = (byte *)uuidbin;
    if (nocase) {
        encode5_32(in,out);
        encode5_32(in+5,out);
        encode5_32(in+10,out);
        encode5_32(in+15,out);
    }
    else {
        encode3_64(in,out);
        encode3_64(in+3,out);
        encode3_64(in+6,out);
        encode3_64(in+9,out);
        byte tmp[3];            // drop two msb bytes from msec time
        tmp[0] = in[12];
        tmp[1] = in[13];
        tmp[2] = in[16];
        encode3_64(tmp,out);
        encode3_64(in+17,out);
    }       
    lock.leave();
    return out;
}

//==============================================================

class jlib_decl CNameCountTable : public AtomRefTable
{
public:
    CNameCountTable(bool _nocase=false) : AtomRefTable(_nocase) { }
    StringBuffer &dump(StringBuffer &str)
    {
        SuperHashIteratorOf<HashKeyElement> iter(*this);
        CriticalBlock b(crit);
        ForEach (iter)
        {
            HashKeyElement &elem = iter.query();
            str.append(elem.get()).append(", count = ").append(elem.queryReferences()).newline();
        }
        return str;
    }
};

static CNameCountTable *namedCountHT;


MODULE_INIT(INIT_PRIORITY_SYSTEM)
{
    namedCountHT = new CNameCountTable;
    return true;
}

MODULE_EXIT()
{
    delete namedCountHT;
}

NamedCount::NamedCount() 
{
    ht = NULL;
}
NamedCount::~NamedCount()
{
    if (ht) namedCountHT->releaseKey(ht);
}
void NamedCount::set(const char *name)
{
    ht = namedCountHT->queryCreate(name);
}

StringBuffer &dumpNamedCounts(StringBuffer &str)
{
    return namedCountHT->dump(str);
}

//==============================================================
// class OffsetToString

OffsetToString::OffsetToString(offset_t offset) 
{
#if defined(_MSC_VER) && !defined (_POSIX_) && (__STDC__ || _INTEGRAL_MAX_BITS < 64)
    // fpos_t is defined as struct (see <stdio.h> in VC)
    __int64 v = offset.lopart + (offset.hipart<<32);
    m_buffer.append(v);
#else
    m_buffer.append(offset);
#endif
}   
/* Gentoo libc version omits these symbols which are directly          */
/* referenced by some 3rd party libraries (sybase, Hasp).  Until these */
/* libs get updated, provide linkable symbols within jlib for these... */
#if defined(__linux__) && (__GNUC__ >= 3)
const jlib_decl unsigned short int *__ctype_b = *__ctype_b_loc ();
const jlib_decl __int32_t *__ctype_tolower = *__ctype_tolower_loc();
const jlib_decl __int32_t *__ctype_toupper = *__ctype_toupper_loc();

// There seems to be some debate about whether these are needed
//#elif (__GNUC__ >= 3)
//const unsigned short int *__ctype_b = *__ctype_b_loc ();
//const unsigned int *__ctype_tolower = *__ctype_tolower_loc();
//const unsigned int *__ctype_toupper = *__ctype_toupper_loc();
#endif
//==============================================================
// URL Password, username handling

/*
From ftp://ftp.isi.edu/in-notes/rfc1738.txt:

        http://<user>:<password>@<host>:<port>/<url-path>

   Some or all of the parts "<user>:<password>@", ":<password>",
   ":<port>", and "/<url-path>" may be excluded.  

   The user name (and password), if present, are followed by a
   commercial at-sign "@". Within the user and password field, any ":",
   "@", or "/" must be encoded.
*/

jlib_decl StringBuffer& encodeUrlUseridPassword(StringBuffer& out, const char* in)
{
    for (const char* p = in; *p; p++)
    {
        switch(*p)
        {
        // mentioned in rfc1738
        case ':': out.append("%3A"); break;
        case '@': out.append("%40"); break;
        case '/': out.append("%2F"); break;
        
        // these are not in the spec, but IE/Firefox has trouble if left un-encoded
        case '%': out.append("%25"); break;

        // these are not necessary: both IE and firefox handle them correctly
        /*
          case '&': out.append("%26"); break;
          case ' ': out.append("%20"); break;
        */
        default: out.append(*p); break;
        }
    }
    return out;
}

inline bool isHexChar(char c) 
{ 
    return (c>='0' && c<='9') 
        || (c>='A' && c<='F')
        || (c>='a' && c<='f');
}

int hexValue(char c)
{
    return (c>='0' && c<='9') ? c-'0' : 
        ((c>='A' && (c<='F') ? c-'A'+10 : c-'a'+10));
}

jlib_decl StringBuffer& decodeUrlUseridPassword(StringBuffer& out, const char* in)
{
    for (const char* p = in; *p; p++)
    {
        if (*p=='%' && isHexChar(*(p+1)) && isHexChar(*(p+2)) )
        {
            char c1 = *(p+1), c2 = *(p+2);
            int x = (hexValue(c1)<<4) + hexValue(c2);
            out.appendf("%c",x);
            p += 2;
        } 
        else
            out.appendf("%c",*p);
    }
    return out;
}


StringBuffer jlib_decl passwordInput(const char* prompt, StringBuffer& passwd)
{
#ifdef _WIN32
    printf("%s", prompt);
    size32_t entrylen = passwd.length();
    loop {
        char c = getch();
        if (c == '\r') 
            break;
        if (c == '\b') {
            if (passwd.length()>entrylen) {
                printf("\b \b");
                passwd.setLength(passwd.length()-1);
            }
        }
        else {
            passwd.append(c);
            printf("*"); 
        }
    }
    printf("\n");
#else
    // unfortuantely linux tty can't easily support using '*' hiding
    sigset_t    saved_signals;
    sigset_t    set_signals;
    struct termios saved_term;
    struct termios set_term;

    FILE *term = fopen(_PATH_TTY, "w+");
    if (!term)
        term = stdin;
    int termfd = fileno(term);
    fprintf(stdout, "%s", prompt);
    fflush(stdout);

    sigemptyset(&set_signals);
    sigaddset(&set_signals, SIGINT);
    sigaddset(&set_signals, SIGTSTP);
    sigprocmask(SIG_BLOCK, &set_signals, &saved_signals);
    tcgetattr(termfd, &saved_term);
    set_term = saved_term;
    set_term.c_lflag &= ~(ECHO|ECHOE|ECHOK|ECHONL);
    tcsetattr(termfd, TCSAFLUSH, &set_term);
    char c = EOF;
    int rd = ::read(termfd,&c,1);
    while ((rd==1)&&(c!='\r')&&(c!='\n')) {
        passwd.append(c);
        rd = ::read(termfd,&c,1);
    }
    int err = (rd<0)?errno:0;
    tcsetattr(termfd, TCSAFLUSH, &saved_term);
    sigprocmask(SIG_SETMASK, &saved_signals, 0);
    if (term!=stdin)
        fclose(term);
    if (err)
        throw makeOsException(err);
#endif
    return passwd;
}

StringBuffer & fillConfigurationDirectoryEntry(const char *dir,const char *name, const char *component, const char *instance, StringBuffer &dirout)
{
    while (*dir) {
        if (*dir=='[') {
            if (memicmp(dir+1,"NAME]",5)==0) {
                dirout.append(name);
                dir += 5;
            }
            else if (memicmp(dir+1,"COMPONENT]",10)==0) {
                dirout.append(component);
                dir += 10;
            }
            else if (memicmp(dir+1,"INST]",5)==0){
                dirout.append(instance);
                dir += 5;
            }
            else
                dirout.append('[');
        }
        else
            dirout.append(*dir);
        dir++;
    }
    return dirout;
}

IPropertyTree *getHPCCEnvironment(const char *configFileName)
{
    StringBuffer configFileSpec(configFileName);
    if (!configFileSpec.length())
#ifdef _WIN32 
        return NULL;
#else
        configFileSpec.set(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
#endif  
    Owned<IProperties> props = createProperties(configFileSpec.str());
    if (props)
    {
        StringBuffer envfile;
        if (props->getProp("environment",envfile)&&envfile.length())
        {
            if (!isAbsolutePath(envfile.str()))
            {
                StringBuffer tail(envfile);
                splitDirTail(configFileSpec.str(),envfile.clear());
                addPathSepChar(envfile).append(tail);
            }
            Owned<IFile> file = createIFile(envfile.str());
            if (file)
            {
                Owned<IFileIO> fileio = file->open(IFOread);
                if (fileio)
                    return createPTree(*fileio);
            }
        }
    }
    return NULL;
}

static CriticalSection securitySettingsCrit;
static bool useSSL = false;
static StringAttr certificate;
static StringAttr privateKey;
static bool retrieved = false;
jlib_decl bool querySecuritySettings(bool *          _useSSL,
                                     unsigned short *_port,
                                     const char * *  _certificate,
                                     const char * *  _privateKey)
{
    if (!retrieved)
    {
        CriticalBlock b(securitySettingsCrit);
        if (!retrieved)
        {
            try
            {
                StringBuffer configFileSpec;
#ifndef _WIN32
                configFileSpec.set(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
#endif
                Owned<IProperties> conf = createProperties(configFileSpec.str(), true);
                useSSL = conf->getPropBool("dfsUseSSL", false);
                if (useSSL)
                {
                    certificate.set(conf->queryProp("dfsSSLCertFile"));
                    privateKey.set(conf->queryProp("dfsSSLPrivateKeyFile"));
                }
                retrieved = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "Error processing environment.conf\n");
                throwUnexpected();
            }
        }
    }
    if (retrieved)
    {
        if (_useSSL)
            *_useSSL = useSSL;
        if (_port)
            *_port = useSSL ? SECURE_DAFILESRV_PORT : DAFILESRV_PORT;
        if (_certificate)
            *_certificate = certificate.get();
        if (_privateKey)
            *_privateKey = privateKey.get();
    }
    else
    {
        if (_useSSL)
            *_useSSL = false;
        if (_port)
            *_port = DAFILESRV_PORT;
    }
    return retrieved;
}

static IPropertyTree *getOSSdirTree()
{
    Owned<IPropertyTree> envtree = getHPCCEnvironment();
    if (envtree) {
        IPropertyTree *ret = envtree->queryPropTree("Software/Directories");
        if (ret) 
            return createPTreeFromIPT(ret);
    }
    return NULL;
}

bool getConfigurationDirectory(const IPropertyTree *useTree, const char *category, const char *component, const char *instance, StringBuffer &dirout)
{
    Linked<const IPropertyTree> dirtree = useTree;
    if (!dirtree) 
        dirtree.setown(getOSSdirTree());
    if (dirtree && category && *category)
    {
        const char *name = dirtree->queryProp("@name");
        if (name&&*name) {
            StringBuffer q("Category[@name=\"");
            q.append(category).append("\"]");
            IPropertyTree *cat = dirtree->queryPropTree(q.str()); // assume only 1
            if (cat) {
                IPropertyTree *over = NULL;
                if (instance&&*instance) {
                    q.clear().append("Override[@instance=\"").append(instance).append("\"]");
                    Owned<IPropertyTreeIterator> it1 = cat->getElements(q.str());
                    ForEach(*it1) {
                        IPropertyTree &o1 = it1->query();
                        if ((!component||!*component)) {
                            if (!over)
                                over = &o1;
                        }
                        else {
                            const char *comp = o1.queryProp("@component");
                            if (!comp||!*comp) {
                                if (!over)
                                    over = &o1;
                            }
                            else if (strcmp(comp,component)==0) {
                                over = &o1;
                                break;
                            }
                        }
                    }
                }
                if (!over&&component&&*component) {
                    q.clear().append("Override[@component=\"").append(component).append("\"]");
                    Owned<IPropertyTreeIterator> it2 = cat->getElements(q.str());
                    ForEach(*it2) {
                        IPropertyTree &o2 = it2->query();
                        if ((!instance||!*instance)) {
                            over = &o2;
                            break;
                        }
                        else {
                            const char *inst = o2.queryProp("@instance");
                            if (!inst||!*inst) {
                                over = &o2;
                                break;
                            }
                        }
                    }
                }
                const char *dir = over?over->queryProp("@dir"):cat->queryProp("@dir");
                if (dir&&*dir) {
                    fillConfigurationDirectoryEntry(dir,name,component,instance,dirout);
                    return true;
                }
            }
        }
    }
    return false;
}


const char * matchConfigurationDirectoryEntry(const char *path,const char *mask,StringBuffer &name, StringBuffer &component, StringBuffer &instance)
{
    // first check matches from (and set any values)
    // only handles simple masks currently
    StringBuffer var;
    PointerArray val;
    const char *m = mask;
    const char *p = path;
    loop {
        char c = *m;
        if (!c)
            break;
        m++;
        StringBuffer *out=NULL;
        if (c=='[') {
            if (memicmp(m,"NAME]",5)==0) {
                out = &name;
                m += 5;
            }
            else if (memicmp(m,"COMPONENT]",10)==0) {
                out = &component;
                m += 10;
            }
            else if (memicmp(m,"INST]",5)==0) {
                out = &instance;
                m += 5;
            }
        }
        if (out) {
            StringBuffer mtail;
            while (*m&&!isPathSepChar(*m)&&(*m!='['))
                mtail.append(*(m++));
            StringBuffer ptail;
            while (*p&&!isPathSepChar(*p))
                ptail.append(*(p++));
            if (ptail.length()<mtail.length())
                return NULL;
            size32_t l = ptail.length()-mtail.length();
            if (l&&(memcmp(ptail.str()+l,mtail.str(),mtail.length())!=0))
                return NULL;
            out->clear().append(l,ptail.str());
        }
        else if (c!=*(p++))
            return NULL;
    }
    if (!*p)
        return p;
    if (isPathSepChar(*p))
        return p+1;
    return NULL;
}

bool replaceConfigurationDirectoryEntry(const char *path,const char *frommask,const char *tomask,StringBuffer &out)
{
    StringBuffer name;
    StringBuffer comp;
    StringBuffer inst;
    const char *tail = matchConfigurationDirectoryEntry(path,frommask,name,comp,inst);
    if (!tail)
        return false;
    fillConfigurationDirectoryEntry(tomask,name,comp,inst,out);
    if (*tail)
        addPathSepChar(out).append(tail);
    return true;
}

static CriticalSection sect;
static StringAttr processPath;
const char * queryCurrentProcessPath()
{
    CriticalBlock block(sect);
    if (processPath.isEmpty()) 
    {
#if _WIN32
        HMODULE hModule = GetModuleHandle(NULL);
        char path[MAX_PATH];
        if (GetModuleFileName(hModule, path, MAX_PATH) != 0)
            processPath.set(path);
#elif defined (__APPLE__)
        char path[PATH_MAX]; 
        uint32_t size = sizeof(path); 
        ssize_t len = _NSGetExecutablePath(path, &size);
        switch(len)
        {
        case -1:
            {
                char * biggerPath = new char[size]; 
                if (_NSGetExecutablePath(biggerPath, &size) == 0)
                    processPath.set(biggerPath);
                delete [] biggerPath;
            }
            break;
        case 0:
            processPath.set(path);
            break;
        default:
            break;
        }
#else
        char path[PATH_MAX + 1];   
        ssize_t len = readlink("/proc/self/exe", path, PATH_MAX);
        if (len != -1)
        {
            path[len] = 0;
            processPath.set(path);
        }
#endif
    }
    if (processPath.isEmpty())
        return NULL;
    return processPath.str();
}

inline bool isOctChar(char c) 
{ 
    return (c>='0' && c<'8'); 
}

inline int octValue(char c)
{
    return c-'0';
}

int parseCommandLine(const char * cmdline, MemoryBuffer &mb, const char** &argvout)
{ 
    mb.append((char)0);
    size32_t arg[256];
    int argc = 0;
    arg[0] = 0;
    char quotechar = 0;
    loop {
        char c = *(cmdline++);
        switch(c) {
        case ' ':
        case '\t':
            if (quotechar)
                break;
            // fall through
        case 0: {
                if (arg[argc]) {
                    while (mb.length()>arg[argc]) {
                        size32_t l = mb.length()-1;
                        const byte * b = ((const byte *)mb.bufferBase())+l;
                        if ((*b!=' ')&&(*b!='\t'))
                            break;
                        mb.setLength(l);
                    }
                    if (mb.length()>arg[argc]) {
                        mb.append((char)0);
                        argc++;
                    }
                    if (c) {
                        if (argc==256) 
                            throw makeStringException(-1, "parseCommandLine: too many arguments");
                        arg[argc] = 0;
                    }
                }
                if (c) 
                    continue;
                argvout = (const char **)mb.reserve(argc*sizeof(const char *));
                for (int i=0;i<argc;i++) 
                    argvout[i] = arg[i]+(const char *)mb.bufferBase();
                return argc;
            }
            break;
        case '\'':
        case '"':
            if (c==quotechar) {
                quotechar = 0;
                continue;
            }
            if (quotechar)
                break;
            quotechar = c;
            continue;
        case '\\': {
                if (*cmdline&&!quotechar) {
                    c = *(cmdline++);
                    switch (c) {
                    case 'a': c = '\a'; break;
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    case 'v': c = '\v'; break;
                    case 'x': case 'X': {
                            c = 0;
                            if (isHexChar(*cmdline)) {
                                c = hexValue(*(cmdline++));
                                if (isHexChar(*cmdline)) 
                                    c = ((byte)c*16)+hexValue(*(cmdline++));
                            }
                        }
                        break;
                    case '0': case '1': case '2': case '3':
                    case '4': case '5': case '6': case '7': {
                            c = octValue(c);
                            if (isOctChar(*cmdline)) {
                                c = ((byte)c*8)+octValue(*(cmdline++));
                                if (isOctChar(*cmdline)) 
                                    c = ((byte)c*8)+octValue(*(cmdline++));
                            }
                        }
                        break;
                    }
                }
            }
            break;
        }
        if (!arg[argc]) 
            arg[argc] = mb.length();
        mb.append(c);
    }
    return 0;
}

jlib_decl StringBuffer &getTempFilePath(StringBuffer & target, const char * component, IPropertyTree * pTree)
{
    StringBuffer dir;
    if (pTree)
        getConfigurationDirectory(pTree->queryPropTree("Directories"),"temp",component,pTree->queryProp("@name"),dir);
    if (!dir.length())
    {
#ifdef _WIN32
        char path[_MAX_PATH+1];
        if(GetTempPath(sizeof(path),path))
            dir.append(path).append("HPCCSystems\\hpcc-data");
        else
            dir.append("c:\\HPCCSystems\\hpcc-data\\temp");
#else
        dir.append(getenv("TMPDIR"));
        if (!dir.length())
            dir.append("/var/lib");
        dir.append("/HPCCSystems/hpcc-data/temp");
#endif
    }
    dir.append(PATHSEPCHAR).append(component);
    recursiveCreateDirectory(dir.str());
    return target.set(dir);
}

jlib_decl const char *getEnumText(int value, const mapEnums *map)
{
    const char *defval = map->str;
    while (map->str)
    {
        if (value==map->val)
            return map->str;
        map++;
    }
    assertex(!"Unexpected value in getEnumText");
    return defval;
}

//#define TESTURL
#ifdef TESTURL

static int doTests()
{
    const char* ps[] = {
        "ABCD", "@BCD", "%BCD","&BCD","A CD","A/CD", "A@@%%A","A&%/@"
    };
    
    const int N = sizeof(ps)/sizeof(char*);
    for (int i=0; i<N; i++)
    {
        StringBuffer raw, encoded;  
        encodeUrlUseridPassword(encoded,ps[i]);
        printf("Encoded: %s\n", encoded.str());
        decodeUrlUseridPassword(raw,encoded);
        if (strcmp(raw.str(),ps[i])!=0)
            assert(!"decoding error");
    }

    return 0;
}

int gDummy = doTests();
#endif
