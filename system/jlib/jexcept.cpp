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


#include "platform.h"
#include <algorithm>

#include "jexcept.hpp"
#include <assert.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include "jptree.hpp"

#ifdef _WIN32
#include "psapi.h"
#include <eh.h>
#elif defined (__linux__) || defined(__FreeBSD__)  || defined(__APPLE__)
#include <sys/wait.h>
#include <sys/types.h>
#include <stddef.h>
#include <errno.h>
#ifdef __linux__
#include <execinfo.h> // comment out if not present
#endif
#ifdef __APPLE__
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>
#endif
#endif

//#define NOSEH

#define NO_LINUX_SEH

#define EXTENDED_EXCEPTION_TRACE

#ifdef EXTENDED_EXCEPTION_TRACE
#include "jmisc.hpp"
#define LINUX_SIGNAL_EXCEPTION
#endif

class jlib_thrown_decl StringException: public IException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    StringException(int code, const char *str, MessageAudience aud = MSGAUD_user) : errcode(code), msg(str), audience(aud) {};
    
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const { str.append(msg); return str;}   
    MessageAudience errorAudience() const { return audience; }
protected:
    int     errcode;
    StringAttr msg;
    MessageAudience audience;
};  


IException *makeStringExceptionVA(int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new StringException(code, eStr.str());
}

IException *makeStringExceptionV(int code,const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *ret = makeStringExceptionVA(code, format, args);
    va_end(args);
    return ret;
}

IException jlib_decl *makeStringException(int code,const char *why)
{
    return new StringException(code,why);
}

IException *makeStringExceptionVA(MessageAudience aud, int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new StringException(code, eStr.str(), aud);
}

IException *makeStringExceptionV(MessageAudience aud, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *ret = makeStringExceptionVA(aud, code, format, args);
    va_end(args);
    return ret;
}

IException jlib_decl *makeStringException(MessageAudience aud,int code,const char *why)
{
    return new StringException(code,why,aud);
}

class jlib_thrown_decl WrappedException: public StringException
{
    typedef StringException PARENT;
public:
    WrappedException(IException *_exception, int code, const char *str, MessageAudience aud = MSGAUD_user) : StringException(code, str, aud), exception(_exception) { }

    virtual StringBuffer &  errorMessage(StringBuffer &str) const override
    {
        PARENT::errorMessage(str);
        if (exception)
        {
            str.appendf("[ %u, ", exception->errorCode());
            exception->errorMessage(str).append(" ]");
        }
        return str;
    }
protected:
    Linked<IException> exception;
};

IException *makeWrappedExceptionVA(IException *e, int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new WrappedException(e, code, eStr.str());
}

IException *makeWrappedExceptionV(IException *e, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *ret = makeWrappedExceptionVA(e, code, format, args);
    va_end(args);
    return ret;
}

void jlib_decl throwStringExceptionV(int code,const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *ret = makeStringExceptionVA(code, format, args);
    va_end(args);
    throw ret;
}

class jlib_thrown_decl OsException: public IOSException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    OsException(int code) : errcode(code) {};
    OsException(int code, const char *_msg) : msg(_msg), errcode(code) {};
    ~OsException() {}
    
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        if (msg)
            str.append(msg).append(", ");
        formatSystemError(str, errcode);
        return str;
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
protected:
    StringAttr msg;
    int     errcode;
};  


IOSException *makeOsException(int code)
{
    return new OsException(code);
}

IOSException *makeOsException(int code, const char *msg)
{
    return new OsException(code, msg);
}

IOSException *makeOsExceptionV(int code, const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new OsException(code, eStr.str());
}

class jlib_thrown_decl ErrnoException: public IErrnoException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ErrnoException(int errn) : audience(MSGAUD_user) { errcode = errn==-1?errno:errn; }
    ErrnoException(int errn, const char *_msg, MessageAudience aud = MSGAUD_user) : msg(_msg), audience(aud) { errcode = errn==-1?errno:errn; }
    ~ErrnoException() { }
    
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        if (msg)
            str.append(msg).append(", ");
        if (errcode==DISK_FULL_EXCEPTION_CODE)
            str.append("Disk full");
        else
            str.append(strerror(errcode));
        return str;
    }
    MessageAudience errorAudience() const { return audience; }
protected:
    StringAttr msg;
    int     errcode;
    MessageAudience audience;
};  


IErrnoException *makeErrnoException(int errn, const char *msg)
{
    return new ErrnoException(errn, msg);
}

IErrnoException *makeErrnoException(const char *msg)
{
    return new ErrnoException(-1, msg);
}

IErrnoException *makeErrnoExceptionV(int errn, const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new ErrnoException(errn, eStr.str());
}

IErrnoException *makeErrnoExceptionV(const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new ErrnoException(-1, eStr.str());
}

IErrnoException *makeErrnoException(MessageAudience aud, int errn, const char *msg)
{
    return new ErrnoException(errn, msg, aud);
}

IErrnoException *makeErrnoExceptionV(MessageAudience aud, int errn, const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new ErrnoException(errn, eStr.str(), aud);
}

IErrnoException *makeErrnoExceptionV(MessageAudience aud, const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new ErrnoException(-1, eStr.str(), aud);
}

const char* serializeMessageAudience(MessageAudience ma)
{
    const char* ret;
    switch(ma)
    {
    case MSGAUD_operator:    ret = "operator";      break;
    case MSGAUD_user:        ret = "user";          break;
    case MSGAUD_programmer:  ret = "programmer";    break;
    case MSGAUD_legacy:      ret = "legacy";        break;
    case MSGAUD_all:         ret = "all";           break;
    default:                 ret = "unknown";       break;
    }
    return ret;
}

MessageAudience deserializeMessageAudience(const char* text)
{
    MessageAudience ma = MSGAUD_programmer;
    if (text && *text)
    {
        if (!strcmp(text, "operator"))
            ma = MSGAUD_operator;
        else if (!strcmp(text, "user"))
            ma = MSGAUD_user;
        else if (!strcmp(text, "programmer"))
            ma = MSGAUD_programmer;
        else if (!strcmp(text, "legacy"))
            ma = MSGAUD_legacy;
        else if (!strcmp(text, "all"))
            ma = MSGAUD_all;
    }
    return ma;
}

class jlib_thrown_decl CMultiException : implements IMultiException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE
        
    CMultiException(const char* source=NULL) 
    { 
        if (source)
            source_.append(source);
    }
    
    //convenience methods for handling this as an array
    virtual aindex_t ordinality() const          
    { 
        synchronized block(m_mutex);
        return array_.ordinality(); 
    }
    virtual IException& item(aindex_t pos) const 
    { 
        synchronized block(m_mutex);
        return array_.item(pos);    
    }
    virtual const char* source() const              
    { 
        synchronized block(m_mutex);
        return source_.str();           
    }
    
    //for complete control...caller is responsible for thread safety!
    virtual IArrayOf<IException>& getArray()     { return array_;              }
    
    //add another exception
    virtual void append(IException& e)              
    { 
        synchronized block(m_mutex);
        array_.append(e); 
    }
    virtual void append(IMultiException& me)
    {
        synchronized block(m_mutex);
        
        IArrayOf<IException>& exceptions = me.getArray();
        const char* source = me.source();
        ForEachItemIn(i, exceptions)
        {
            IException& e = exceptions.item(i);
            if (source && *source)
            {
                StringBuffer msg;
                msg.appendf("[%s] ",source);
                e.errorMessage(msg);
                array_.append(*makeStringExceptionV(e.errorAudience(), e.errorCode(), "%s", msg.str()));
            }
            else
                array_.append(*LINK(&e));
        }
    }
    
    
    StringBuffer& serialize(StringBuffer& buffer, unsigned indent = 0, bool simplified=false, bool root=true) const
    {
        synchronized block(m_mutex);
        
        if (root)   
            buffer.append("<Exceptions>");
        
        if (!simplified)
        {
            if (indent) buffer.append("\n\t");
            buffer.appendf("<Source>%s</Source>", source_.str());
        }
        
        ForEachItemIn(i, array_)
        {
            IException& exception = array_.item(i);
            
            if (indent) buffer.append("\n\t");
            buffer.append("<Exception>");
            
            //tag order is important for some soap clients (i.e. Java Axis)
            
            if (indent) buffer.append("\n\t\t");
            buffer.appendf("<Code>%d</Code>", exception.errorCode());
            
            if (indent) buffer.append("\n\t\t");
            buffer.appendf("<Audience>%s</Audience>", serializeMessageAudience( exception.errorAudience() ));
            
            if (simplified)
            {
                if (indent) buffer.append("\n\t\t");
                StringBuffer msg;
                buffer.appendf("<Source>%s</Source>", source_.str());
            }
            
            if (indent) buffer.append("\n\t\t");
            
            StringBuffer msg;
            StringBuffer encoded;
            encodeXML(exception.errorMessage(msg).str(), encoded);
            buffer.appendf("<Message>%s</Message>", encoded.str());
            
            if (indent) buffer.append("\n\t");
            buffer.append("</Exception>");
        }
        
        if (root)
            buffer.append("</Exceptions>");
        return buffer;
    }

    StringBuffer& serializeJSON(StringBuffer& buffer, unsigned indent = 0, bool simplified=false, bool root=true, bool enclose=false) const
    {
        synchronized block(m_mutex);
        if (enclose)
        {
            buffer.append("{");
            if (indent) buffer.append("\n");
        }

        if (root)
            buffer.append("\"Exceptions\": {");

        if (!simplified)
        {
            if (indent) buffer.append("\n\t");
            buffer.appendf("\"Source\": \"%s\"", source_.str());
        }
        ForEachItemIn(i, array_)
        {
            if (i == 0 && !simplified)
                buffer.appendf(", ");
            else if (i > 0)
                buffer.append(", ");
            IException& exception = array_.item(i);

            if (indent) buffer.append("\n\t");
            if (i == 0)
                buffer.append("\"Exception\": [");
            if (indent) buffer.append("\n\t");
            buffer.append("{");

            if (indent) buffer.append("\n\t\t");
            buffer.appendf("\"Code\": %d,", exception.errorCode());

            if (indent) buffer.append("\n\t\t");
            buffer.appendf("\"Audience\": \"%s\",", serializeMessageAudience( exception.errorAudience() ));

            if (!simplified)
            {
                if (indent) buffer.append("\n\t\t");
                buffer.appendf("\"Source\": \"%s\",", source_.str());
            }

            if (indent) buffer.append("\n\t\t");
            StringBuffer msg;
            StringBuffer encoded;
            encodeJSON(encoded, exception.errorMessage(msg).str());
            buffer.appendf("\"Message\": \"%s\"", encoded.str());

            if (indent) buffer.append("\n\t");
            buffer.append("}");
        }

        if (indent) buffer.append("\n\t");
        buffer.append("]");
        if (root)
        {
            if (indent) buffer.append("\n");
            buffer.append("}");
        }
        if (enclose)
        {
            if (indent) buffer.append("\n");
            buffer.append("}");
        }
        return buffer;
    }

    virtual void deserialize(const char* xml)
    {
        synchronized block(m_mutex);
        StringBuffer wrapper;
        
        if (strncmp(xml, "<Exceptions>", 12))
            xml = wrapper.appendf("<Exceptions>%s</Exceptions>", xml).str();
        Owned<IPropertyTree> pTree = createPTreeFromXMLString(xml);
        if (!pTree)
            throw makeStringException(-1, "Failed to deserialize IMultiException!");
        Owned<IPropertyTreeIterator> i = pTree->getElements("Exception");
        
        if (pTree->hasProp("Source"))
            source_.clear().append( pTree->queryProp("Source"));
        else
        {
            if (i->first())
            {
                IPropertyTree* pNode = &i->query();
                source_.clear().append( pNode->queryProp("Source"));
            }
        }
        
        array_.kill();
        ForEach(*i)
        {
            IPropertyTree* pNode = &i->query();
            IException* pException = 
                makeStringException(
                   deserializeMessageAudience(pNode->queryProp("Audience")),
                   pNode->getPropInt("Code", -1),
                   pNode->queryProp("Message"));
            array_.append(*pException);
        }
    }   
    
    //the following methods override those in IIException
    // 
    virtual int errorCode() const
    { 
        synchronized block(m_mutex);
        return ordinality() == 1 ? item(0).errorCode() : -1; 
    }
    virtual StringBuffer& errorMessage(StringBuffer &msg) const
    { 
        synchronized block(m_mutex);
        ForEachItemIn(i, array_)
        {
            IException& e = item(i);
            
            StringBuffer buf;
            msg.appendf("[%3d: %s] ", e.errorCode(), e.errorMessage(buf).str());
        }
        return msg;
    }
    virtual MessageAudience errorAudience() const
    { 
        synchronized block(m_mutex);
        return ordinality() == 1 ? item(0).errorAudience() : MSGAUD_programmer;
    }
    
private:
    CMultiException( const CMultiException& );
    IArrayOf<IException> array_;
    StringBuffer         source_;
    mutable Mutex        m_mutex;
};

IMultiException *makeMultiException(const char* source/*=NULL*/)
{
    return new CMultiException(source);
}


void pexception(const char *msg,IException *e)
{ // like perror except for exceptions
    StringBuffer s;
    fprintf(stderr,"%s : %s\n",msg,e?e->errorMessage(s).str():"NULL Exception!");
}

void userBreakpoint()
{
#ifdef _WIN32
    try
    {
        DebugBreak();
    }
    catch (...)
    {
        //if not debugging don't give an unhandled exception.
    }
#endif
}

const char *sanitizeSourceFile(const char *file)
{
    if (file)
    {
        const char *tail = strrchr(file, '/');
        if (tail)
            return tail+1;
#ifdef _WIN32
        tail = strrchr(file, '\\');
        if (tail)
            return tail+1;
#endif
    }
    return file;
}

void throwUnexpectedException(const char * file, unsigned line)
{
    printStackReport();
    throw makeStringExceptionV(9999, "Internal Error at %s(%d)", sanitizeSourceFile(file), line);
}

void throwUnexpectedException(const char * where, const char * file, unsigned line)
{
    printStackReport();
    throw makeStringExceptionV(9999, "Internal Error '%s' at %s(%d)", where, sanitizeSourceFile(file), line);
}

void raiseAssertException(const char *assertion, const char *file, unsigned line)
{
    StringBuffer s;
    s.append("assert(");
    s.append(assertion);
    s.append(") failed - file: ");
    s.append(sanitizeSourceFile(file));
    s.append(", line ");
    s.append(line);

    if (queryLogMsgManager())
    {
        printStackReport();
        IERRLOG("%s",s.str());       // make sure doesn't get lost!
        queryLogMsgManager()->flushQueue(10*1000);
#ifdef _DEBUG
        // cause a breakpoint in the debugger if we are debugging.
        //userBreakpoint();
#endif
    }
    
#if 0
#ifndef USING_MPATROL
#ifdef _WIN32
    // disable memory leak dump since it is meaningless in this case
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag &= ~_CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif
#endif
#endif

    throw makeStringException(3000, s.str()); // 3000: internal error
}

void raiseAssertCore(const char *assertion, const char *file, unsigned line)
{
    printStackReport();
    StringBuffer s;
    s.append("assert(");
    s.append(assertion);
    s.append(") failed - file: ");
    s.append(sanitizeSourceFile(file));
    s.append(", line ");
    s.append(line);
    ERRLOG("%s",s.str());       // make sure doesn't get lost!
    queryLogMsgManager()->flushQueue(10*1000);
#ifdef _WIN32
    userBreakpoint();
    ExitProcess(255);
#else
    raise(SIGABRT);
    _exit(255);
#endif
    
}
static int SEHnested = 0;
static IExceptionHandler *SEHHandler = NULL;
static bool SEHtermOnSystemDLLs = false;
static bool SEHtermAlways = false;

#ifdef _WIN32
static void *SEHrestore;

#ifdef EXTENDED_EXCEPTION_TRACE


static LPTSTR GetExceptionString( DWORD dwCode )
{
#define EXCEPTION( x ) case EXCEPTION_##x: return #x;
    
    switch ( dwCode )
    {   
    EXCEPTION( ACCESS_VIOLATION )
    EXCEPTION( DATATYPE_MISALIGNMENT )
    EXCEPTION( BREAKPOINT )
    EXCEPTION( SINGLE_STEP )
    EXCEPTION( ARRAY_BOUNDS_EXCEEDED )
    EXCEPTION( FLT_DENORMAL_OPERAND )
    EXCEPTION( FLT_DIVIDE_BY_ZERO )
    EXCEPTION( FLT_INEXACT_RESULT )
    EXCEPTION( FLT_INVALID_OPERATION )
    EXCEPTION( FLT_OVERFLOW )
    EXCEPTION( FLT_STACK_CHECK )
    EXCEPTION( FLT_UNDERFLOW )
    EXCEPTION( INT_DIVIDE_BY_ZERO )
    EXCEPTION( INT_OVERFLOW )
    EXCEPTION( PRIV_INSTRUCTION )
    EXCEPTION( IN_PAGE_ERROR )
    EXCEPTION( ILLEGAL_INSTRUCTION )
    EXCEPTION( NONCONTINUABLE_EXCEPTION )
    EXCEPTION( STACK_OVERFLOW )
    EXCEPTION( INVALID_DISPOSITION )
    EXCEPTION( GUARD_PAGE )
    EXCEPTION( INVALID_HANDLE )
    }
    
    static CHAR szBuffer[512] = { 0 };
    
    FormatMessage(  FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_FROM_HMODULE,
        GetModuleHandle( "NTDLL.DLL" ),
        dwCode, 0, szBuffer, sizeof( szBuffer ), 0 );
    
    return szBuffer;
}

static BOOL GetLogicalAddress( PVOID addr, PTSTR szModule, DWORD len, DWORD& section, DWORD& offset )
{
    szModule[0] = 0;
    section = 0;
    offset = 0;
    if ((unsigned)(memsize_t)addr<0x10000)
        return FALSE;
    
    
    MEMORY_BASIC_INFORMATION mbi;
    
    if ( !VirtualQuery( addr, &mbi, sizeof(mbi) ) )
        return FALSE;
    
    memsize_t hMod = (memsize_t)mbi.AllocationBase;
    
    if ( !GetModuleFileName( (HMODULE)hMod, szModule, len ) )
        return FALSE;
    
    PIMAGE_DOS_HEADER pDosHdr = (PIMAGE_DOS_HEADER)hMod;
    
    PIMAGE_NT_HEADERS pNtHdr = (PIMAGE_NT_HEADERS)(hMod + pDosHdr->e_lfanew);
    
    PIMAGE_SECTION_HEADER pSection = IMAGE_FIRST_SECTION( pNtHdr );
    
    memsize_t rva = (memsize_t)addr - hMod;
    
    for (unsigned i = 0; i < pNtHdr->FileHeader.NumberOfSections; i++, pSection++ )
    {
        DWORD sectionStart = pSection->VirtualAddress;
        DWORD sectionEnd = sectionStart
            + std::max(pSection->SizeOfRawData, pSection->Misc.VirtualSize);
        
        if ( (rva >= sectionStart) && (rva <= sectionEnd) )
        {
            section = i+1;
            offset = (DWORD)(rva - sectionStart);
            return TRUE;
        }
    }
    
    return FALSE;   
}


#if defined(_WIN32)
#ifdef __cplusplus
extern "C" {
    typedef BOOL (CALLBACK* LPENUMPROCESSMODULES)(HANDLE,HMODULE *,DWORD,LPDWORD);
    typedef BOOL (CALLBACK* LPGETMODULEINFORMATION)(HANDLE,HMODULE,LPMODULEINFO,DWORD);
}
#endif
#endif


static void ModuleWalk()
{ 
    HMODULE hmSystem = LoadLibrary("psapi.dll");
    if (!hmSystem) 
        return;
    LPENUMPROCESSMODULES enumProcessModules = (LPENUMPROCESSMODULES)GetProcAddress(hmSystem,"EnumProcessModules");
    if (!enumProcessModules) 
        return;
    LPGETMODULEINFORMATION getModuleInformation = (LPGETMODULEINFORMATION)GetProcAddress(hmSystem,"GetModuleInformation");
    if (!getModuleInformation) 
        return;
    DWORD processID = GetCurrentProcessId();
    PrintLog( "Process ID: %u", processID );
    
    // Get a list of all the modules in this process.
    
    HANDLE hProcess = OpenProcess(  PROCESS_QUERY_INFORMATION |
        PROCESS_VM_READ,
        FALSE, processID );
    if (NULL == hProcess)
        return;
    
    HMODULE hMods[1024];
    DWORD cbNeeded;
    if (enumProcessModules(hProcess, hMods, sizeof(hMods), &cbNeeded)) {
        for (unsigned i = 0; i < (cbNeeded / sizeof(HMODULE)); i++ ) {
            char szModName[MAX_PATH];
            szModName[0] = 0;
            // Get the full path to the module's file.
            
            MODULEINFO modinfo;
            memset(&modinfo,0,sizeof(modinfo));
            getModuleInformation(hProcess, hMods[i], &modinfo, sizeof(modinfo));
            GetModuleFileName( hMods[i], szModName, sizeof(szModName));
            PrintLog("%8X %8X %8X  %s",(unsigned)modinfo.lpBaseOfDll,(unsigned)modinfo.SizeOfImage,(unsigned)modinfo.EntryPoint,szModName);
        }
    }
    
    CloseHandle( hProcess );
    FreeLibrary(hmSystem);
}


static void StackWalk( size_t pc, size_t bp )
{
    PrintLog( "Call stack:" );
    
    PrintLog( "Address   Frame     Logical addr  Module" );
    
    size_t * pFrame;
    size_t * pPrevFrame=NULL;
    
    pFrame = (size_t*)bp;
    
    do
    {
        TCHAR szModule[MAX_PATH] = "";
        DWORD section = 0, offset = 0;
        
        if (pc>0x10000)
            GetLogicalAddress((PVOID)pc, szModule,sizeof(szModule),section,offset );
        else
            strcpy(szModule,"NULL");
        
        PrintLog( "%08X  %08X  %04X:%08X %s",
            pc, pFrame, section, offset, szModule );
        
        if ( (size_t)pFrame & 0x80000003 )    
            break;                  
        
        if ( (size_t)pFrame <= (size_t)pPrevFrame )
            break;
        
        if ( IsBadWritePtr(pFrame, sizeof(PVOID)*2) )
            break;
        
        pc = pFrame[1];
        
        if ( IsBadCodePtr((FARPROC) pc) )
            break;
        
        pPrevFrame = pFrame;
        
        pFrame = (size_t *)pFrame[0]; 
        
    } while ( 1 );
}


static void doPrintStackReport( size_t ip, size_t _bp, size_t sp )
{
    if (_bp==0) {
#ifdef _ARCH_X86_
        __asm { 
            mov eax,ebp
            mov _bp,eax
        }
#else
        PrintLog("inline assembler is only supported for x86_32; StackReport incomplete bp tend to not be used");
#endif
    }
    
    for (unsigned i=0;i<8;i++) {
        StringBuffer s;
#ifdef __64BIT__
        s.appendf("Stack[%016X]:",sp);
#else
        s.appendf("Stack[%08X]:",sp);
#endif
        for (unsigned j=0;j<8;j++) {
            if ( IsBadReadPtr((const void *)sp, sizeof(unsigned)) )
                break;
            size_t v = *(size_t *)sp;
            sp += sizeof(unsigned);
#ifdef __64BIT__
            s.appendf(" %016X",v);
#else
            s.appendf(" %08X",v);
#endif
        }
        PrintLog( "%s",s.str());
    }
    
    
    StackWalk( ip , _bp);
    ModuleWalk();
    StringBuffer threadlist;
    PrintLog( "ThreadList:\n%s",getThreadList(threadlist).str());
    
}



static void PrintExceptionReport( PEXCEPTION_POINTERS pExceptionInfo)
{
    PrintLog("=====================================================");
    
    
    PrintMemoryStatusLog();
    PEXCEPTION_RECORD pExceptionRecord = pExceptionInfo->ExceptionRecord;
    
    PrintLog(   "Exception code: %08X %s",
        pExceptionRecord->ExceptionCode,
        GetExceptionString(pExceptionRecord->ExceptionCode) );
    
    PrintLog( "Fault address:  %08X", pExceptionRecord->ExceptionAddress);
    
    TCHAR szFaultingModule[MAX_PATH];
    DWORD section, offset;
    GetLogicalAddress(  pExceptionRecord->ExceptionAddress,
        szFaultingModule,
        sizeof( szFaultingModule ),
        section, offset );
    
    PrintLog("Fault module:  %02X:%08X %s", section, offset, szFaultingModule);
    
    PCONTEXT pCtx = pExceptionInfo->ContextRecord;
    
    PrintLog( "\nRegisters:" );
    
#ifdef _ARCH_X86_64_
    PrintLog("RAX:%016" I64F "X  RBX:%016" I64F "X  RCX:%016" I64F "X  RDX:%016" I64F "X  RSI:%016" I64F "X  RDI:%016" I64F "X",
        pCtx->Rax, pCtx->Rbx, pCtx->Rcx, pCtx->Rdx, pCtx->Rsi, pCtx->Rdi );
    PrintLog("R8: %016" I64F "X  R9: %016" I64F "X  R10:%016" I64F "X  R11:%016" I64F "X  R12:%016" I64F "X  R13:%016" I64F "X",
        pCtx->R8, pCtx->R9, pCtx->R10, pCtx->R11, pCtx->R12, pCtx->R13);
    PrintLog("R14:%016" I64F "X  R15:%016" I64F "X",
        pCtx->R14, pCtx->R15);

    PrintLog( "CS:RIP:%04X:%016" I64F "X", pCtx->SegCs, pCtx->Rip );
    PrintLog( "SS:PSP:%04X:%016" I64F "X  PBP:%016" I64F "X",
        pCtx->SegSs, pCtx->Rsp, pCtx->Rbp );
#elif defined(_ARCH_X86_)
    PrintLog("EAX:%08X  EBX:%08X  ECX:%08X  EDX:%08X  ESI:%08X  EDI:%08X",
        pCtx->Eax, pCtx->Ebx, pCtx->Ecx, pCtx->Edx, pCtx->Esi, pCtx->Edi );
    
    PrintLog( "CS:EIP:%04X:%08X", pCtx->SegCs, pCtx->Eip );
    PrintLog( "SS:ESP:%04X:%08X  EBP:%08X",
        pCtx->SegSs, pCtx->Esp, pCtx->Ebp );
#else
    // ARMFIX: Implement register bank dump for ARM on _WIN32.
    PrintLog("Register bank not implemented for your platform");
#endif
    
    PrintLog( "DS:%04X  ES:%04X  FS:%04X  GS:%04X",
        pCtx->SegDs, pCtx->SegEs, pCtx->SegFs, pCtx->SegGs );
    PrintLog( "Flags:%08X", pCtx->EFlags );
#ifdef _ARCH_X86_64_
    doPrintStackReport(pCtx->Rip, pCtx->Rbp,pCtx->Rsp);
#elif defined(_ARCH_X86_)
    doPrintStackReport(pCtx->Eip, pCtx->Ebp,pCtx->Esp);
#else
    // ARMFIX: Implement stack dump for ARM on _WIN32.
    PrintLog("Stack report not implemented for your platform");
#endif
    if (SEHtermOnSystemDLLs || SEHtermAlways) {
        char *s = szFaultingModule;
        while (*s) {
            char *sep = strchr(s,'\\');
            if (!sep) {
                sep = strchr(s,'.');
                if (sep)
                    *sep = 0;
                break;
            }
            s = sep+1;
        }
        if (SEHtermAlways || (stricmp(s,"ntdll")==0)||(stricmp(s,"kernel32")==0)||(stricmp(s,"msvcrt")==0)||(stricmp(s,"msvcrtd")==0)) {
            TerminateProcess(GetCurrentProcess(), 1);
        }
    }
}



class jlib_thrown_decl CSEHException: public ISEH_Exception, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSEHException(unsigned int u, _EXCEPTION_POINTERS* pExp) : errcode((int)u) 
    {
#ifdef EXTENDED_EXCEPTION_TRACE
        PrintExceptionReport(pExp);
#endif
#ifdef ALLREGS
        char s[256]; // not too good on stack faults!
        sprintf(s,"SEH Exception(%x)\n"
            "EAX=%08X  EBX=%08X  ECX=%08X  EDX=%08X  ESI=%08X\n"
            "EDI=%08X  EBP=%08X  ESP=%08X  EIP=%08X  FLG=%08X\n"
            "CS=%04X   DS=%04X  SS=%04X  ES=%04X  FS=%04X  GS=%04X",
            u,
            pExp->ContextRecord->Eax, pExp->ContextRecord->Ebx,
            pExp->ContextRecord->Ecx, pExp->ContextRecord->Edx,
            pExp->ContextRecord->Esi, pExp->ContextRecord->Edi,
            pExp->ContextRecord->Ebp, pExp->ContextRecord->Esp,
            pExp->ContextRecord->Eip, pExp->ContextRecord->EFlags,
            pExp->ContextRecord->SegCs, pExp->ContextRecord->SegDs,
            pExp->ContextRecord->SegSs, pExp->ContextRecord->SegEs,
            pExp->ContextRecord->SegFs, pExp->ContextRecord->SegGs);
#else
        char s[80];
#ifdef _ARCH_X86_64_
        sprintf(s,"SEH Exception(%08X) at %04X:%016" I64F "X\n",u,pExp->ContextRecord->SegCs,pExp->ContextRecord->Rip);
#elif defined(_ARCH_X86_)
        sprintf(s,"SEH Exception(%08X) at %04X:%08X\n",u,pExp->ContextRecord->SegCs,pExp->ContextRecord->Eip);
#else
        // ARMFIX: Implement exception dump for ARM on _WIN32.
        sprintf(s,"SEH Exception");
#endif
#endif
        msg.set(s);
    };
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const { str.append(msg); return str;}   
    MessageAudience errorAudience() const { return MSGAUD_user; }
    static void Translate(unsigned int u, _EXCEPTION_POINTERS* pExp) 
    { 
#ifdef _DEBUG
        if (u == 0x80000003) return; // int 3 breakpoints
        static CriticalSection crit;
        {
            CriticalBlock b(crit);
            PrintExceptionReport(pExp);
        }
#endif
        ISEH_Exception *e = new CSEHException(u,pExp);
        if (SEHHandler && SEHHandler->fireException(e)) return;
        throw(e);
    }
protected:
    int     errcode;
    StringAttr msg;
};  
#endif
#else
#ifdef LINUX_SIGNAL_EXCEPTION
#ifndef NO_LINUX_SEH
static IException *sigsegv_exc;
#endif
static int excsignal;

class jlib_thrown_decl CSEHException: public ISEH_Exception, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSEHException(int signum, const char *s)
    {
        errcode = signum;
        msg.set(s);
    };
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const { str.append(msg); return str;}   
    MessageAudience errorAudience() const { return MSGAUD_user; }
protected:
    int     errcode;
    StringAttr msg;
};  

#ifndef NO_LINUX_SEH
static void throwSigSegV()
{
    int childpid = fork();
    if (childpid <= 0) {    // the child
        // generate a coredump on different process
        signal(excsignal, SIG_DFL);
        raise(excsignal);
        return;
    }
    PROGLOG("Dumping core using child process %d",childpid);
    waitpid(childpid, NULL, 0);
    if (SEHHandler && SEHHandler->fireException(sigsegv_exc)) 
        return;
    throw sigsegv_exc;
}
#endif

void excsighandler(int signum, siginfo_t *info, void *extra) 
{
    static byte nested=0;
    if (nested++)
        return;

    excsignal = 0;
#if defined(NO_LINUX_SEH)
    // ensure other signals from now on cause exit
    sigset_t blockset;
    sigemptyset(&blockset);
    struct sigaction act;
    act.sa_mask = blockset;
    act.sa_flags = 0;
    act.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
#endif
    StringBuffer s;

#ifdef _ARCH_X86_64_
#define I64X "%016" I64F "X"
    ucontext_t *uc = (ucontext_t *)extra;
#ifdef __APPLE__
    __int64 ip = uc->uc_mcontext->__ss.__rip;
    __int64 sp = uc->uc_mcontext->__ss.__rsp;
#else
    __int64 ip = uc->uc_mcontext.gregs[REG_RIP];
    __int64 sp = uc->uc_mcontext.gregs[REG_RSP];
#endif
    excsignal = signum;
    s.appendf("SIG: %s(%d), accessing " I64X ", IP=" I64X, strsignal(signum),signum, (__int64)info->si_addr, ip);
    
    StringBuffer networkIp;
    PROGLOG("================================================");
    PROGLOG("Program:   %s:%s", queryHostIP().getIpText(networkIp).str(),queryCurrentProcessPath());
    PROGLOG("Signal:    %d %s",signum,strsignal(signum));
    PROGLOG("Fault IP:  " I64X "", ip);
    PROGLOG("Accessing: " I64X "", (unsigned __int64) info->si_addr);
#ifdef _EXECINFO_H
    printStackReport(ip);
#endif

    PROGLOG("Registers:" );
#ifdef __APPLE__
    PROGLOG("EAX:" I64X "  EBX:" I64X "  ECX:" I64X "  EDX:" I64X "  ESI:" I64X "  EDI:" I64X "",
        (unsigned __int64) uc->uc_mcontext->__ss.__rax, (unsigned __int64)uc->uc_mcontext->__ss.__rbx, 
        (unsigned __int64) uc->uc_mcontext->__ss.__rcx, (unsigned __int64)uc->uc_mcontext->__ss.__rdx, 
        (unsigned __int64) uc->uc_mcontext->__ss.__rsi, (unsigned __int64)uc->uc_mcontext->__ss.__rdi);
    PROGLOG("R8 :" I64X "  R9 :" I64X "  R10:" I64X "  R11:" I64X "",
        (unsigned __int64) uc->uc_mcontext->__ss.__r8, (unsigned __int64)uc->uc_mcontext->__ss.__r9,
        (unsigned __int64) uc->uc_mcontext->__ss.__r10, (unsigned __int64) uc->uc_mcontext->__ss.__r11 );
    PROGLOG("R12:" I64X "  R13:" I64X "  R14:" I64X "  R15:" I64X "",
        (unsigned __int64) uc->uc_mcontext->__ss.__r12, (unsigned __int64)uc->uc_mcontext->__ss.__r13,
        (unsigned __int64) uc->uc_mcontext->__ss.__r14, (unsigned __int64) uc->uc_mcontext->__ss.__r15 );
    PROGLOG( "CS:EIP:%04X:" I64X "", ((unsigned) uc->uc_mcontext->__ss.__cs)&0xffff, ip );
    PROGLOG( "   ESP:" I64X "  EBP:" I64X "", sp, (unsigned __int64) uc->uc_mcontext->__ss.__rbp );
#else
    PROGLOG("EAX:" I64X "  EBX:" I64X "  ECX:" I64X "  EDX:" I64X "  ESI:" I64X "  EDI:" I64X "",
        (unsigned __int64) uc->uc_mcontext.gregs[REG_RAX], (unsigned __int64)uc->uc_mcontext.gregs[REG_RBX], 
        (unsigned __int64) uc->uc_mcontext.gregs[REG_RCX], (unsigned __int64) uc->uc_mcontext.gregs[REG_RDX], 
        (unsigned __int64) uc->uc_mcontext.gregs[REG_RSI], (unsigned __int64) uc->uc_mcontext.gregs[REG_RDI] );
    PROGLOG("R8 :" I64X "  R9 :" I64X "  R10:" I64X "  R11:" I64X "",
        (unsigned __int64) uc->uc_mcontext.gregs[REG_R8], (unsigned __int64)uc->uc_mcontext.gregs[REG_R9],
        (unsigned __int64) uc->uc_mcontext.gregs[REG_R10], (unsigned __int64) uc->uc_mcontext.gregs[REG_R11] );
    PROGLOG("R12:" I64X "  R13:" I64X "  R14:" I64X "  R15:" I64X "",
        (unsigned __int64) uc->uc_mcontext.gregs[REG_R12], (unsigned __int64)uc->uc_mcontext.gregs[REG_R13],
        (unsigned __int64) uc->uc_mcontext.gregs[REG_R14], (unsigned __int64) uc->uc_mcontext.gregs[REG_R15] );
    PROGLOG( "CS:EIP:%04X:" I64X "", ((unsigned) uc->uc_mcontext.gregs[REG_CSGSFS])&0xffff, ip );
    PROGLOG( "   ESP:" I64X "  EBP:" I64X "", sp, (unsigned __int64) uc->uc_mcontext.gregs[REG_RBP] );
#endif
    
    for (unsigned i=0;i<8;i++) {
        StringBuffer s;
        s.appendf("Stack[" I64X "]:",sp);
        for (unsigned j=0;j<8;j++) {
            __int64 v = *(size_t *)sp;
            sp += sizeof(unsigned);
            s.appendf(" " I64X "",v);
        }
        PROGLOG( "%s",s.str());
    }

#elif defined (__linux__) && defined (_ARCH_X86_)
    ucontext_t *uc = (ucontext_t *)extra;
    unsigned ip = uc->uc_mcontext.gregs[REG_EIP];
    unsigned sp = uc->uc_mcontext.gregs[REG_ESP];
    
    excsignal = signum;
    s.appendf("SIG: %s(%d), accessing %p, IP=%x", strsignal(signum),signum, info->si_addr, ip);
    
    StringBuffer networkIp;
    PROGLOG("================================================");
    PROGLOG("Program:   %s:%s", queryHostIP().getIpText(networkIp).str(),queryCurrentProcessPath());
    PROGLOG("Signal:    %d %s",signum,strsignal(signum));
    PROGLOG("Fault IP:  %08X", ip);
    PROGLOG("Accessing: %08X", (unsigned) info->si_addr);
#ifdef _EXECINFO_H
    printStackReport(ip);
#endif

    PROGLOG("Registers:" );
    PROGLOG("EAX:%08X  EBX:%08X  ECX:%08X  EDX:%08X  ESI:%08X  EDI:%08X",
        uc->uc_mcontext.gregs[REG_EAX], uc->uc_mcontext.gregs[REG_EBX], 
        uc->uc_mcontext.gregs[REG_ECX], uc->uc_mcontext.gregs[REG_EDX], 
        uc->uc_mcontext.gregs[REG_ESI], uc->uc_mcontext.gregs[REG_EDI] );
    
    PROGLOG( "CS:EIP:%04X:%08X", uc->uc_mcontext.gregs[REG_CS], ip );
    PROGLOG( "SS:ESP:%04X:%08X  EBP:%08X",
        uc->uc_mcontext.gregs[REG_SS], sp, uc->uc_mcontext.gregs[REG_EBP] );    
    
    for (unsigned i=0;i<8;i++) {
        StringBuffer s;
        s.appendf("Stack[%08X]:",sp);
        for (unsigned j=0;j<8;j++) {
            size_t v = *(size_t *)sp;
            sp += sizeof(unsigned);
            s.appendf(" %08X",v);
        }
        PROGLOG( "%s",s.str());
    }
    PROGLOG("Frame:");
    unsigned* bp = (unsigned*) (uc->uc_mcontext.gregs[REG_EBP]);
    for (unsigned n=0; n<64; n++) {
        unsigned * nextbp = (unsigned *) *bp++;
        unsigned fip = *bp;
        if ((fip < 0x08000000) || (fip > 0x7fffffff) || (nextbp < bp)) 
            break;
        PROGLOG("%2d  %08X  %08X",n+1,fip,(unsigned) bp);
        bp = nextbp;
    }
#elif defined (__linux__) && defined (__ARM_ARCH_7A__)
#pragma message "Implement signal dump for ARMv7-A."

    ucontext_t *uc = (ucontext_t *) extra;

    PROGLOG("================================================");
    PROGLOG("Signal:    %d %s",signum,strsignal(signum));
    PROGLOG("Registers:" );
    PROGLOG("r0 :%08lX  r1 :%08lX  r2 :%08lX  r3 :%08lX  r4 :%08lX  r5 :%08lX",
        uc->uc_mcontext.arm_r0, uc->uc_mcontext.arm_r1, uc->uc_mcontext.arm_r2,
        uc->uc_mcontext.arm_r3, uc->uc_mcontext.arm_r4, uc->uc_mcontext.arm_r5);
    PROGLOG("r6 :%08lX  r7 :%08lX  r8 :%08lX  r9 :%08lX  r10:%08lX  fp :%08lX",
        uc->uc_mcontext.arm_r6, uc->uc_mcontext.arm_r7, uc->uc_mcontext.arm_r8,
        uc->uc_mcontext.arm_r9, uc->uc_mcontext.arm_r10, uc->uc_mcontext.arm_fp);
    PROGLOG("ip :%08lX  sp :%08lX  lr :%08lX  pc :%08lX",
        uc->uc_mcontext.arm_ip, uc->uc_mcontext.arm_sp, uc->uc_mcontext.arm_lr,
        uc->uc_mcontext.arm_pc);
    PROGLOG("CPSR:%08lX\n", uc->uc_mcontext.arm_cpsr);

    struct flags
    {
        unsigned int Mode:4;    // bit 0 - 3
        unsigned int M:1;       // bit 4
        unsigned int T:1;       // bit 5
        unsigned int F:1;       // bit 6
        unsigned int I:1;       // bit 7
        unsigned int A:1;       // bit 8
        unsigned int E:1;       // bit 9
        unsigned int IT1:6;     // bit 10 - 15
        unsigned int GE:4;      // bit 16 - 19
        unsigned int DNM:4;     // bit 20 - 23
        unsigned int J:1;       // bit 24
        unsigned int IT2:2;     // bit 25 - 26
        unsigned int Q:1;       // bit 27
        unsigned int V:1;       // bit 28
        unsigned int C:1;       // bit 29
        unsigned int Z:1;       // bit 30
        unsigned int N:1;       // bit 31
    } *flags_p;

    const char *ArmCpuModes[] = {
                            // M M[3:0]
            "User",         // 1 0000
            "FIQ",          // 1 0001
            "IRQ",          // 1 0010
            "Supervisor",   // 1 0011
            "N/A",          // 1 0100
            "N/A",          // 1 0101
            "Monitor",      // 1 0110
            "Abort",        // 1 0111
            "N/A",          // 1 1000
            "N/A",          // 1 1001
            "Hyp"           // 1 1010
            "Undefined",    // 1 1011
            "N/A",          // 1 1100
            "N/A",          // 1 1101
            "N/A",          // 1 1110
            "System"        // 1 1111
    };

    flags_p = (struct flags *)&uc->uc_mcontext.arm_cpsr;
    PROGLOG("Flags N:%d Z:%d C:%d V:%d Q:%d IT:0x%X J:%d GE:0x%X E:%d A:%d I:%d F:%d T:%d M:0x%X [%s]"
            , flags_p->N, flags_p->Z, flags_p->C, flags_p->V, flags_p->Q
            , (flags_p->IT2 << 6 | flags_p->IT1)
            , flags_p->J, flags_p->GE
            , flags_p->E, flags_p->A, flags_p->I, flags_p->F, flags_p->T, (flags_p->M << 4 | flags_p->Mode)
            , ArmCpuModes[flags_p->Mode]
            );

    PROGLOG("Fault address: %08lX", uc->uc_mcontext.fault_address);
    PROGLOG("Trap no      : %08lX", uc->uc_mcontext.trap_no);
    PROGLOG("Error code   : %08lX", uc->uc_mcontext.error_code);
    PROGLOG("Old mask     : %08lX", uc->uc_mcontext.oldmask);

    unsigned sp = uc->uc_mcontext.arm_sp;
    for (unsigned i=0;i<8;i++)
    {
        StringBuffer s;
        s.appendf("Stack[%08X]:",sp);
        for (unsigned j=0;j<8;j++)
        {
            size_t v = *(size_t *)sp;
            sp += sizeof(unsigned);
            s.appendf(" %08X",v);
        }
    }
    PROGLOG( "%s",s.str());

#elif defined (__linux__) && defined (__arm__)
#pragma message "Unknown ARM architecture!"

    PROGLOG("================================================");
    PROGLOG("Signal:    %d %s",signum,strsignal(signum));
    PROGLOG("More information unavailable on your platform");

#else
    // Placeholder for any new HW-SW platform

#endif

    StringBuffer threadlist;
    PROGLOG( "ThreadList:\n%s",getThreadList(threadlist).str());
    queryLogMsgManager()->flushQueue(10*1000);

    // MCK - really should not return after recv'ing any of these signals

#ifndef NO_LINUX_SEH
    void (* _P)() = throwSigSegV;
    uc->uc_mcontext.gregs[REG_ESP]-=4;
    uc->uc_mcontext.gregs[REG_EIP] = (unsigned)_P;
    unsigned *spp = (unsigned *)sp;
    *spp = ip;
    sigsegv_exc = new CSEHException(signum,s.str());
#else
    if (SEHHandler)
    {
        if ( SEHHandler->fireException(new CSEHException(signum,s.str())) )
            return;
    }
    raise(signum);
#endif
    nested--;
}


#endif
#endif

void jlib_decl setTerminateOnSEHInSystemDLLs(bool set)
{
    SEHtermOnSystemDLLs = set;
}

void jlib_decl setTerminateOnSEH(bool set)
{
    SEHtermAlways = set;
}

void *EnableSEHtranslation()
{
#ifdef NOSEH
    return NULL;
#else
#ifdef _WIN32
    return _set_se_translator( CSEHException::Translate );
#else
    UNIMPLEMENTED;
#endif
#endif
}

void jlib_decl *setSEHtoExceptionHandler(IExceptionHandler *handler)
{
#ifdef NOSEH
    return NULL;
#endif
    void *ret = SEHHandler;
    SEHHandler = handler;
    return ret;
}

void jlib_decl enableSEHtoExceptionMapping()
{
#ifdef NOSEH
    return;
#endif
    if (SEHnested++)
        return; // already done
#ifdef _WIN32
    enableThreadSEH();
    SEHrestore = EnableSEHtranslation();
#else
    struct sigaction act;
    sigset_t blockset;
    sigemptyset(&blockset);
    // defer these for thread in handler
    sigaddset(&blockset, SIGINT);
    sigaddset(&blockset, SIGQUIT);
    sigaddset(&blockset, SIGTERM);
    act.sa_mask = blockset;
    act.sa_flags = SA_SIGINFO;
#if defined(SA_RESETHAND)
    act.sa_flags |= SA_RESETHAND;
#endif
    act.sa_sigaction = &excsighandler; 
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
#endif
}


void  jlib_decl disableSEHtoExceptionMapping()
{
#ifdef NOSEH
    return;
#endif
    if (--SEHnested)
        return;
#ifdef _WIN32
    if (SEHrestore) {
        void *restore = SEHrestore;
        SEHrestore = NULL;
        _set_se_translator( (_se_translator_function)restore );
    }
#else
    sigset_t blockset;
    sigemptyset(&blockset);
    struct sigaction act;
    act.sa_mask = blockset;
    act.sa_flags = 0;
    act.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
#endif
}


StringBuffer & formatSystemError(StringBuffer & out, unsigned errcode)
{
#ifdef _WIN32       
    const char * lpMessageBuffer=NULL;
    FormatMessage( FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
        NULL,
        errcode,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), //The user default
        (LPTSTR) &lpMessageBuffer,
        0,
        NULL );
    if (lpMessageBuffer) {
        out.append(lpMessageBuffer);
        LocalFree( (void *)lpMessageBuffer );
    }
    else {
        out.append(errcode);
    }
#else
    int saverr = errno;
    errno = 0;
    const char *errstr = strerror(errcode);
    if (errno==0) {
        out.append(errstr);
    }
    else {
        out.append(errcode);
    }
    errno = saverr;
#endif
    return out;
}


IException * deserializeException(MemoryBuffer & in)
{
    byte nulle;
    in.read(nulle);
    if (nulle) return NULL;
    int code;
    StringAttr text;
    in.read(code);
    in.read(text);
    return makeStringExceptionV(code, "%s", text.get());
}

void jlib_decl serializeException(IException * e, MemoryBuffer & out)
{
    if (!e)
        out.append((byte)1);
    else
    {
        out.append((byte)0);
        StringBuffer text;
        out.append(e->errorCode());
        out.append(e->errorMessage(text).str());
    }
}


void printStackReport(__int64 startIP)
{
    if (!queryLogMsgManager())
        return;
#ifdef _WIN32
    unsigned onstack=1234;
    doPrintStackReport(0, 0,(unsigned)&onstack);
#elif defined(__linux__)
    DBGLOG("Backtrace:");
    void *btarray[100];
    unsigned btn = backtrace (btarray, 100);
    char **strings = backtrace_symbols (btarray, btn);
    unsigned i;
    unsigned firstReal = 0;
    if (startIP)
    {
        VStringBuffer iptext("[%p]", (void *) startIP);
        for (i=0; i<btn; i++)
        {
            if (strstr(strings[i], iptext) != nullptr)
            {
                firstReal = i;
                break;
            }
        }
    }
    for (i=firstReal; i<btn; i++)
        DBGLOG("  %s", strings[i]);
    free (strings);
#endif
    queryLogMsgManager()->flushQueue(10*1000);
}

//---------------------------------------------------------------------------------------------------------------------

unsigned getCommandOutput(StringBuffer &output, const char *cmd, const char *cmdTitle, const char *allowedPrograms)
{
    Owned<IPipeProcess> pipe = createPipeProcess(allowedPrograms);
    if (pipe->run(cmdTitle, cmd, nullptr, false, true, false))
    {
        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
        readSimpleStream(output, *pipeReader);
    }
    return pipe->wait();
}

//---------------------------------------------------------------------------------------------------------------------

bool getDebuggerGetStacksCmd(StringBuffer &output)
{
#ifndef __linux__
    return false; // unsupported
#endif

    const char *exePath = queryCurrentProcessPath();
    if (!exePath)
    {
        output.append("Unable to capture stacks");
        return false;
    }
    return output.appendf("gdb --batch -n -ex 'thread apply all bt' %s %u", exePath, GetCurrentProcessId());
}

bool getAllStacks(StringBuffer &output)
{
    StringBuffer cmd;
    if (!getDebuggerGetStacksCmd(cmd))
        return false;
    return 0 == getCommandOutput(output, cmd, "get stacks");
}

//---------------------------------------------------------------------------------------------------------------------

class jlib_decl CError : public CInterfaceOf<IError>
{
public:
    CError(WarnErrorCategory _category,ErrorSeverity _severity, int _no, const char* _msg, const char* _filename, int _lineno, int _column, int _position, unsigned _activity, const char * _scope);

    virtual int             errorCode() const override { return no; }
    virtual StringBuffer &  errorMessage(StringBuffer & ret) const override { return ret.append(msg); }
    virtual MessageAudience errorAudience() const override { return MSGAUD_user; }
    virtual const char* getFilename() const override { return filename; }
    virtual WarnErrorCategory getCategory() const override { return category; }
    virtual int getLine() const override { return lineno; }
    virtual int getColumn() const override { return column; }
    virtual int getPosition() const override { return position; }
    virtual StringBuffer& toString(StringBuffer&) const override;
    virtual ErrorSeverity getSeverity() const override { return severity; }
    virtual IError * cloneSetSeverity(ErrorSeverity _severity) const override;
    virtual unsigned getActivity() const override { return activity; }
    virtual const char * queryScope() const override { return scope; }
    virtual IPropertyTree * toTree() const override;

protected:
    ErrorSeverity severity;
    WarnErrorCategory category;
    int no;
    StringAttr msg;
    StringAttr filename;
    StringAttr scope;
    int lineno;
    int column;
    int position;
    unsigned activity;
};

CError::CError(WarnErrorCategory _category, ErrorSeverity _severity, int _no, const char* _msg, const char* _filename, int _lineno, int _column, int _position, unsigned _activity, const char * _scope)
    : severity(_severity), category(_category), msg(_msg), filename(_filename), scope(_scope), activity(_activity)
{
    no = _no;
    lineno = _lineno;
    column = _column;
    position = _position;
}


StringBuffer& CError::toString(StringBuffer& buf) const
{
    buf.append(filename);

    if(lineno && column)
        buf.append('(').append(lineno).append(',').append(column).append(')');
    buf.append(" : ");

    buf.append(no).append(": ").append(msg);
    return buf;
}

IPropertyTree * CError::toTree() const
{
    Owned<IPropertyTree> xml = createPTree("exception", ipt_fast);
    xml->setPropInt("@severity", severity);
    xml->setPropInt("@category", category);
    xml->setPropInt("@code", no);
    xml->setProp("@msg", msg);
    xml->setProp("@filename", filename);
    xml->setProp("@scope", scope);
    if (lineno)
        xml->setPropInt("@line", lineno);
    if (column)
        xml->setPropInt("@column", column);
    if (position)
        xml->setPropInt("@pos", position);
    if (activity)
        xml->setPropInt("@activity", activity);
    return xml.getClear();
}

IError * CError::cloneSetSeverity(ErrorSeverity newSeverity) const
{
    return new CError(category, newSeverity,
                         errorCode(), msg, filename,
                         getLine(), getColumn(), getPosition(), getActivity(), queryScope());
}

ErrorSeverity queryDefaultSeverity(WarnErrorCategory category)
{
    if (category == CategoryError)
        return SeverityFatal;
    if (category == CategoryInformation)
        return SeverityInformation;
    if (category == CategoryMistake)
        return SeverityError;
    return SeverityWarning;
}


IError *createError(WarnErrorCategory category, ErrorSeverity severity, int errNo, const char *msg, const char * filename, int lineno, int column, int pos, unsigned activity, const char * scope)
{
    return new CError(category,severity,errNo,msg,filename,lineno,column,pos, activity, scope);
}

IError *createError(WarnErrorCategory category, ErrorSeverity severity, int errNo, const char *msg, unsigned activity, const char * scope)
{
    return new CError(category,severity,errNo,msg,nullptr, 0, 0, 0, activity, scope);
}

IError *createError(IPropertyTree * tree)
{
    return new CError((WarnErrorCategory)tree->getPropInt("@category"),
                      (ErrorSeverity)tree->getPropInt("@severity"),
                      tree->getPropInt("@code"),
                      tree->queryProp("@msg"),
                      tree->queryProp("@filename"),
                      tree->getPropInt("@line"),
                      tree->getPropInt("@column"),
                      tree->getPropInt("@pos"),
                      tree->getPropInt("@activity"),
                      tree->queryProp("@scope"));
}

//---------------------------------------------------------------------------------------------------------------------

void IErrorReceiver::ThrowStringException(int code,const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    IException *ret = MakeStringExceptionVA(code, format, args);
    va_end(args);
    throw ret;
}


void IErrorReceiver::reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int position)
{
    Owned<IError> err = createError(errNo,msg,filename,lineno,column,position);
    report(err);
}

void IErrorReceiver::reportWarning(WarnErrorCategory category, int warnNo, const char *msg, const char *filename, int lineno, int column, int position)
{
    ErrorSeverity severity = queryDefaultSeverity(category);
    Owned<IError> warn = createError(category, severity,warnNo,msg,filename,lineno,column,position);
    report(warn);
}

//---------------------------------------------------------------------------------------------------------------------

