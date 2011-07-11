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



#ifndef __JEXCEPT__
#define __JEXCEPT__

#include "jexpdef.hpp"
#include "jiface.hpp"
#include "jlib.hpp"
#include "errno.h"

// When changing this enum, be sure to update (a) the string functions, and (b) NUM value in jlog.hpp

typedef enum
{
    MSGAUD_unknown     = 0x00,
    MSGAUD_operator    = 0x01,
    MSGAUD_user        = 0x02,
    MSGAUD_monitor     = 0x04,
    MSGAUD_performance = 0x08,
    MSGAUD_internal    = 0x10,
    MSGAUD_programmer  = 0x20,
    MSGAUD_legacy      = 0x40,
    MSGAUD_audit       = 0x80,
    MSGAUD_all         = 0xFF
} MessageAudience;

jlib_decl const char* SerializeMessageAudience(MessageAudience ma);
jlib_decl MessageAudience DeserializeMessageAudience(const char* text);


interface jlib_thrown_decl IException : public IInterface
{
    virtual int             errorCode() const = 0;
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const = 0;
    virtual MessageAudience errorAudience() const = 0;
};

//the following interface to be thrown when a user command explicitly calls for a failure

interface jlib_thrown_decl IUserException : public IException
{
};

//the following interface defines a collection of exceptions
//
interface jlib_thrown_decl IMultiException : extends IException
{
   //convenience methods for handling this as an array
   virtual aindex_t ordinality() const          = 0;
   virtual IException& item(aindex_t pos) const = 0;
    virtual const char* source() const              = 0;

   //for complete control...
   virtual IArrayOf<IException>& getArray()= 0;

   //add another exception
   virtual void append(IException& e) = 0;
   virtual void append(IMultiException& e) = 0;

   virtual StringBuffer& serialize(StringBuffer& ret, unsigned indent = 0, bool simplified=false, bool root=true) const = 0;
   virtual void deserialize(const char* xml) = 0; //throws IException on failure!

   //the following methods override those in IIException
   // 
    virtual int errorCode() const = 0;
    virtual StringBuffer&   errorMessage(StringBuffer &msg) const = 0;
    virtual MessageAudience errorAudience() const = 0;
};

IMultiException jlib_decl *MakeMultiException(const char* source = NULL);

interface IExceptionHandler
{
   virtual bool fireException(IException *e) = 0;
};

IException jlib_decl *MakeStringException(int code,const char *why, ...);
IException jlib_decl *MakeStringExceptionVA(int code,const char *why, va_list args);
IException jlib_decl *MakeStringExceptionDirect(int code,const char *why);
IException jlib_decl *MakeStringException(MessageAudience aud,int code,const char *why, ...);
IException jlib_decl *MakeStringExceptionVA(MessageAudience aud,int code,const char *why, va_list args);
IException jlib_decl *MakeStringExceptionDirect(MessageAudience aud,int code,const char *why);
void jlib_decl ThrowStringException(int code,const char *format, ...);

interface jlib_thrown_decl IOSException: extends IException{};
IOSException jlib_decl *MakeOsException(int code);
IOSException jlib_decl *MakeOsException(int code, const char *msg, ...);

#define DISK_FULL_EXCEPTION_CODE ENOSPC

interface jlib_thrown_decl IErrnoException: extends IException{};
IErrnoException jlib_decl *MakeErrnoException(int errn=-1);
IErrnoException jlib_decl *MakeErrnoException(int errn, const char *why, ...);
IErrnoException jlib_decl *MakeErrnoException(const char *why, ...);
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,int errn=-1);
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,int errn, const char *why, ...);
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,const char *why, ...);

void jlib_decl pexception(const char *msg,IException *e); // like perror except for exceptions
jlib_decl StringBuffer & formatSystemError(StringBuffer & out, unsigned errcode);

void userBreakpoint();

interface jlib_thrown_decl ISEH_Exception : extends IException
{
};

interface jlib_thrown_decl IOutOfMemException: extends IException
{
};


void  jlib_decl EnableSEHtoExceptionMapping(); // return value can be used to disable
void  jlib_decl DisableSEHtoExceptionMapping();         
// NB only enables for current thread or threads started after call
// requres /EHa option to be set in VC++ options (after /GX)

void jlib_decl *setSEHtoExceptionHandler(IExceptionHandler *handler); // sets handler and return old value


void  jlib_decl setTerminateOnSEHInSystemDLLs(bool set=true);
void jlib_decl setTerminateOnSEH(bool set=true);


#define throwUnexpected()          throw MakeStringException(9999, "Internal Error at %s(%d)", __FILE__, __LINE__)
#define assertThrow(x)             assertex(x)

#define UNIMPLEMENTED throw MakeStringException(-1, "UNIMPLEMENTED feature at %s(%d)", __FILE__, __LINE__)
#define UNIMPLEMENTED_X(reason) throw MakeStringException(-1, "UNIMPLEMENTED '" reason "' at %s(%d)", __FILE__, __LINE__)
#define UNIMPLEMENTED_XY(a,b) throw MakeStringException(-1, "UNIMPLEMENTED " a " %s at %s(%d)", b, __FILE__, __LINE__)

IException jlib_decl * deserializeException(MemoryBuffer & in); 
void jlib_decl serializeException(IException * e, MemoryBuffer & out); 

void  jlib_decl PrintStackReport();

#ifdef _DEBUG
#define RELEASE_CATCH_ALL       int*********
#else
#define RELEASE_CATCH_ALL       ...
#endif

//These are used in several places to wrap error reporting, to keep error numbers+text together.  E.g.,
//#define XYZfail 99    #define XXZfail_Text "Failed"     throwError(XYZfail)
#define throwError(x)                           ThrowStringException(x, (x ## _Text))
#define throwError1(x,a)                        ThrowStringException(x, (x ## _Text), a)
#define throwError2(x,a,b)                      ThrowStringException(x, (x ## _Text), a, b)
#define throwError3(x,a,b,c)                    ThrowStringException(x, (x ## _Text), a, b, c)
#define throwError4(x,a,b,c,d)                  ThrowStringException(x, (x ## _Text), a, b, c, d)

#ifndef _WIN32
#define SIGNAL_TO_EXCEPTION
#endif

#ifdef SIGNAL_TO_EXCEPTION
#include <setjmp.h>
/******************************************************************************
 * !! NOTE: the following only works for single threaded programs at present !!
 *
 * Visual C++ intercepts hardware traps like segmentation violation, 
 * floating point exception as C++ exceptions in catch(...).
 * Unfortunately, this does not work in Linux and causes program to 
 * trap.  This class attempts to implement Visual C++ functionality
 * using signal handlers, setjmp/longjmp and works in conjunction 
 * with the TRY, CATCH, CATCH_ALL, END_CATCH macros as defined below.
 *
 * The first invocation of constructor sets up a signal handler for 
 * SIGSEGV, SIGILL, SIGFPE, SIGPIPE and SIGSYS. TRY macro instantiates
 * an object of this class on stack, which saves the current value 
 * of static class member jmp_buf, s_jmpbuf, in data member m_old_jmpbuf
 * with the intention to restore it upon destruction.  The TRY macro
 * also invokes setjmp to save the CPU state in the static member. If
 * an exception occurs within the TRY block, the signal handler is invoked
 * and simply invokes longjmp on the static jmpbuf oblivious to the 
 * current context.  It so happens that this static jmpbuf member always 
 * points to the current TRY block.  The program control resumes at the 
 * setjmp call, that now returns signal number due to the longjmp call
 * from the signal handler.  This results in a C++ exception to be thrown
 * which gets intercepted in the corresponding catch block.
 */
class jlib_thrown_decl SignalToException
{
public:
   SignalToException();
    virtual ~SignalToException();

    static void processSetJmpResult(int res);
    static jmp_buf s_jmpbuf;

private:
   static void UnixTrapHandler(int sig);
   static void setUnixTrapHandler();

    static bool s_bUnixTrapHandlerSet;
   jmp_buf m_old_jmpbuf;
};

#define TRY \
try \
{\
   SignalToException _signalToException;\
   /* note that setjmp needs to be invoked in this context \
     * or else it traps on longjmp from signal handler */\
    SignalToException::processSetJmpResult( setjmp(SignalToException::s_jmpbuf) );

#define CATCH   } catch
#define AND_CATCH catch
#define END_CATCH /*optional*/

/* use these macros as follows:
 *
 * TRY { 
 * } 
 * CATCH(exception type) {
 * } 
 * AND_CATCH(exception type) {
 * }
 * ...
 * END_CATCH
 *
 */

#else //SIGNAL_TO_EXCEPTION defined

#ifndef __AFX_H__

#define TRY       try
#define CATCH     catch
#define AND_CATCH catch
#define END_CATCH /*optional*/

#endif

#endif //SIGNAL_TO_EXCEPTION not defined

#endif

