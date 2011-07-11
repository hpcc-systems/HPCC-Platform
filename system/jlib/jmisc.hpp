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



#ifndef __JMISC__
#define __JMISC__

#include "platform.h"
#include <stdio.h>
#include "jexpdef.hpp"
#include "jiface.hpp"
#include "jcrc.hpp"
#include "jlog.hpp"

#ifdef _WIN32
#include <windows.h>
#endif


// ISO-8859-1 (aka ISO-Latin-1 and IBM-819) defines 0x00--0x7F and 0xA0--0xFF
// ISO-8859-1 (aka ISO-Latin-1 and IBM-810) is identical to US-ASCII (aka ANSI_X3.4 and ISO-646) in the range 0x00--0x7F
// ISO-8859-1 (aka ISO-Latin-1 and IBM-819) is identical to ISO-8859-15 (aka ISO-Latin-9 and IBM-923) except 0xA4 is Currency rather than Euro
// ISO-8859-1 (aka ISO-Latin-1 and IBM-819) is identical to Windows-1252 (aka IBM-5348) and to IBM-1252 except the latter define 0x80--0x9F
#define ASCII_LIKE_CODEPAGE "iso-8859-1"
#define ASCII_LIKE_SUBS_CHAR 0x1a

inline bool isValidAsciiLikeCharacter(unsigned char c) { return ((c < 0x80) || (c >= 0xA0)); }

#define _LOG_TIME       MSGFIELD_timeDate
#define _LOG_TID        MSGFIELD_thread
#define _LOG_PID        MSGFIELD_process //gcc 3.0.4 on Linux defines LOG_PID as 0x1 already in sys/syslog.h
#define _LOG_CLOCK      MSGFIELD_milliTime
#define _LOG_HIRESCLOCK MSGFIELD_nanoTime

class StringBuffer;
class ILogIntercept 
{
public:
    virtual void print(const char *fmt)=0;
    virtual void close()=0;
    virtual unsigned setLogDetail(unsigned)=0;
};
jlib_decl ILogIntercept* interceptLog(ILogIntercept *intercept); // for custom tracing

jlib_decl void openLogFile(const char *filename, unsigned detail = 0, bool enterQueueMode = true);
jlib_decl void appendLogFile(const char *filename, unsigned detail = 0, bool enterQueueMode = true);
jlib_decl ILogMsgHandler * queryLegacyLogMsgHandler();

#ifndef DISABLE_PRINTLOG
jlib_decl void PrintLogDirect(const char *msg);
jlib_decl int  PrintLog(const char *fmt, ...);
jlib_decl void SPrintLog(const char *fmt,...); // not tertminated by LF
#define PrintExceptionLog(_e,_txt) EXCLOG(_e, _txt)

#ifdef _DEBUG
#define PrintLogDebug       PrintLog
#else
#define PrintLogDebug       1?0:PrintLog
#endif

#endif

jlib_decl void PrintMemoryStatusLog();
extern jlib_decl StringBuffer & hexdump2string(byte const * in, size32_t inSize, StringBuffer & out);

jlib_decl StringBuffer &addFileTimestamp(StringBuffer &fname, bool daily=false);


jlib_decl FILE *xfopen(const char *path, const char *mode);
jlib_decl const char * queryCcLogName();
jlib_decl StringBuffer& queryCcLogName(const char* wuid, StringBuffer& logname);
jlib_decl char* readarg(char*& curptr);
jlib_decl bool invoke_program(const char *command_line, DWORD &runcode, bool wait=true, const char *outfile=NULL, HANDLE *rethandle=NULL, bool throwException = false);
jlib_decl bool wait_program(HANDLE handle,DWORD &runcode,bool block=true);
jlib_decl bool interrupt_program(HANDLE handle,int signum=-9);

#ifndef _WIN32
jlib_decl bool CopyFile(const char *file, const char *newfile, bool fail);
#endif


//Endian support


inline  void _rev2(char *b) { char t=b[0]; b[0]=b[1]; b[1]=t; }
inline  void _rev4(char *b) { char t=b[0]; b[0]=b[3]; b[3]=t; t=b[1]; b[1]=b[2]; b[2]=t; }
inline  void _rev8(char *b) { char t=b[0]; b[0]=b[7]; b[7]=t; t=b[1]; b[1]=b[6]; b[6]=t; t=b[2]; b[2]=b[5]; b[5]=t; t=b[3]; b[3]=b[4]; b[4]=t; }
inline  void _rev(short &v)          { _rev2((char *)&v); }
inline  void _rev(unsigned short &v) { _rev2((char *)&v); }
inline  void _rev(int &v)            { _rev4((char *)&v); }
#if defined (_MSC_VER) && _MSC_VER < 1300
inline  void _rev(__int32 &v)        { _rev4((char *)&v); }
#endif
inline  void _rev(unsigned int &v)   { _rev4((char *)&v); }
inline  void _rev(long &v)           { _rev4((char *)&v); }
inline  void _rev(unsigned long &v)  { _rev4((char *)&v); }
inline  void _rev(__int64 &v)        { _rev8((char *)&v); }
inline  void _rev(unsigned __int64 &v)       { _rev8((char *)&v); }
extern jlib_decl void _rev(size32_t len, void * ptr);

#define _REV2(p) _rev2((char *)&p);
#define _REV4(p) _rev4((char *)&p);
#define _REV8(p) _rev8((char *)&p);

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define _WINREV(x) _rev(x)
#define _WINREV2(p) _REV2(p)
#define _WINREV4(p) _REV4(p)
#define _WINREV8(p) _REV8(p)
#define _WINREVN(l,p) _rev(l, p)
#else
#define _WINREV(x)
#define _WINREV2(p)
#define _WINREV4(p)
#define _WINREV8(p)
#define _WINREVN(l,p)
#endif

inline  void _cpyrev2(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[1]=src[0]; tgt[0] = src[1];
}
inline  void _cpyrev3(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[2] = src[0]; tgt[1]=src[1]; tgt[0] = src[2];
}
inline  void _cpyrev4(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[3]=src[0]; tgt[2] = src[1]; tgt[1]=src[2]; tgt[0] = src[3];
}
inline  void _cpyrev5(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[4]=src[0]; 
    tgt[3]=src[1]; tgt[2] = src[2]; tgt[1]=src[3]; tgt[0] = src[4];
}
inline  void _cpyrev6(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[5]=src[0]; tgt[4] = src[1]; 
    tgt[3]=src[2]; tgt[2] = src[3]; tgt[1]=src[4]; tgt[0] = src[5];
}
inline  void _cpyrev7(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[6] = src[0]; tgt[5]=src[1]; tgt[4]=src[2];
    tgt[3] = src[3]; tgt[2]=src[4]; tgt[1]=src[5]; tgt[0]=src[6];
}
inline  void _cpyrev8(void * _tgt, const void * _src) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src; 
    tgt[7]=src[0]; tgt[6] = src[1]; tgt[5]=src[2]; tgt[4] = src[3];
    tgt[3]=src[4]; tgt[2] = src[5]; tgt[1]=src[6]; tgt[0] = src[7];
}
inline  void _cpyrevn(void * _tgt, const void * _src, unsigned len) { 
    char * tgt = (char *)_tgt; const char * src = (const char *)_src+len; 
    for (;len;len--) {
        *tgt++ = *--src;
    }
}

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define _WINCPYREV(x, y, len)           _cpyrevn(x, y, len)
#define _WINCPYREV2(x, y)               _cpyrev2(x, y)
#define _WINCPYREV4(x, y)               _cpyrev4(x, y)
#define _WINCPYREV8(x, y)               _cpyrev8(x, y)
#else
#define _WINCPYREV(x, y, len)           memcpy(x, y, len)
#define _WINCPYREV2(x, y)               memcpy(x, y, 2)
#define _WINCPYREV4(x, y)               memcpy(x, y, 4)
#define _WINCPYREV8(x, y)               memcpy(x, y, 8)
#endif



#ifdef _WIN32
class jlib_decl C64bitComposite         // used to efficiently manage an int64 as two consecutive DWORDS and vice versa
{

private:
    LARGE_INTEGER value;

public:
    C64bitComposite() {value.QuadPart = 0; }
    C64bitComposite(__int64 v) {value.QuadPart = v; }
    C64bitComposite(DWORD lo, DWORD hi) {value.LowPart = lo; value.HighPart = hi; }

    DWORD getLo() {return value.LowPart; }
    LONG getHi() {return value.HighPart; }
    __int64 get() {return value.QuadPart; }
    void get(DWORD &lo, DWORD &hi) {lo = value.LowPart, hi = value.HighPart; }
    C64bitComposite & set(__int64 v) {value.QuadPart = v; return *this; }
    C64bitComposite & set(DWORD lo, DWORD hi) {value.LowPart = lo; value.HighPart = hi; return *this; }

    C64bitComposite & operator =  (__int64 v) {return set(v); }
    C64bitComposite & operator += (__int64 v) {value.QuadPart += v; return *this; }
    C64bitComposite & operator -= (__int64 v) {value.QuadPart -= v; return *this; }
    C64bitComposite & operator ++ () {++value.QuadPart; return * this; }
    C64bitComposite & operator -- () {--value.QuadPart; return * this; }

    int operator == (const C64bitComposite & s) {return value.QuadPart == s.value.QuadPart; }
    int operator != (const C64bitComposite & s) {return value.QuadPart != s.value.QuadPart; }
    int operator <  (const C64bitComposite & s) {return value.QuadPart < s.value.QuadPart; }
    int operator >  (const C64bitComposite & s) {return value.QuadPart > s.value.QuadPart; }
    int operator <= (const C64bitComposite & s) {return value.QuadPart <= s.value.QuadPart; }
    int operator >= (const C64bitComposite & s) {return value.QuadPart >= s.value.QuadPart; }

    int operator == (__int64 s) {return value.QuadPart == s; }
    int operator != (__int64 s) {return value.QuadPart != s; }
    int operator <  (__int64 s) {return value.QuadPart < s; }
    int operator >  (__int64 s) {return value.QuadPart > s; }
    int operator <= (__int64 s) {return value.QuadPart <= s; }
    int operator >= (__int64 s) {return value.QuadPart >= s; }
};

#endif

// Functions to build and split 64bit integers. Do not depend on the byte order

template<typename T1,typename T2> inline __int64 makeint64(T1 high, T2 low)
{
    return (__int64)high<<32 | (__int64)low & (__int64)0xffffffff;
}

inline __int32 high(__int64 n)
{
    return (__int32)(n>>32);
}

inline unsigned __int32 low(__int64 n)
{
    return (unsigned __int32)(n & (__int64)0xffffffff);
}


//MORE - We really should restructure this file.  Also would this be better with a class interface?
//Handle ^C/break from a console program.
typedef bool (*AbortHandler)();                                                 // return true to exit program

interface IAbortHandler : public IInterface
{
    virtual bool onAbort() = 0;
};

#define JLIBERR_UserAbort       0xffffff00

extern jlib_decl void addAbortHandler(AbortHandler handler=NULL);               // no parameter means just set the flag for later testing.
extern jlib_decl void addAbortHandler(IAbortHandler & handler);
extern jlib_decl void removeAbortHandler(AbortHandler handler);
extern jlib_decl void removeAbortHandler(IAbortHandler & handler);
extern jlib_decl bool isAborting();
extern jlib_decl void throwAbortException();
extern jlib_decl void throwExceptionIfAborting();

interface IAbortRequestCallback
{
    virtual bool abortRequested() = 0;
};


class LocalAbortHandler
{
public:
    LocalAbortHandler(AbortHandler _handler=NULL)   { handler = _handler; addAbortHandler(handler); }
    ~LocalAbortHandler()                            { removeAbortHandler(handler); }
private:
    AbortHandler            handler;
};

class LocalIAbortHandler
{
public:
    LocalIAbortHandler(IAbortHandler & _handler) : handler(_handler)    { addAbortHandler(handler); }
    ~LocalIAbortHandler()                                               { removeAbortHandler(handler); }
private:
    IAbortHandler &         handler;
};

// These are undocumented by Microsoft
#define ENABLE_QUICK_EDIT     0x0040
#define ENABLE_INSERT_MODE    0x0020

class NoQuickEditSection
{
    DWORD savedmode;
    bool saved;
public:
    NoQuickEditSection()
    {
#ifdef _WIN32
        saved = false;
        HANDLE hStdIn = ::GetStdHandle(STD_INPUT_HANDLE);
        if (hStdIn)
        {
            if (::GetConsoleMode(hStdIn, &savedmode))
                saved = true;
            // Set new console mode w/ QuickEdit disabled
            DWORD mode = savedmode & ~(ENABLE_QUICK_EDIT);
            if (!::SetConsoleMode(hStdIn, mode))
                DBGLOG("SetConsoleMode failed code %d", GetLastError());
            ::CloseHandle(hStdIn);
        }
#endif
    }
    ~NoQuickEditSection()
    {
#ifdef _WIN32
        if (saved)
        {
            HANDLE hStdIn = ::GetStdHandle(STD_INPUT_HANDLE);
            if (hStdIn)
            {
                ::SetConsoleMode(hStdIn, savedmode);;
                ::CloseHandle(hStdIn);
            }
        }
#endif
    }
};

#endif
