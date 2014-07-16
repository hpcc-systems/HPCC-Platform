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



#ifndef __JEXCEPT__
#define __JEXCEPT__

#include "jiface.hpp"
#include "jlib.hpp"
#include "errno.h"

jlib_decl const char* SerializeMessageAudience(MessageAudience ma);
jlib_decl MessageAudience DeserializeMessageAudience(const char* text);

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

IException jlib_decl *MakeStringException(int code,const char *why, ...) __attribute__((format(printf, 2, 3)));
IException jlib_decl *MakeStringExceptionVA(int code,const char *why, va_list args);
IException jlib_decl *MakeStringExceptionDirect(int code,const char *why);
IException jlib_decl *MakeStringException(MessageAudience aud,int code,const char *why, ...) __attribute__((format(printf, 3, 4)));
IException jlib_decl *MakeStringExceptionVA(MessageAudience aud,int code,const char *why, va_list args);
IException jlib_decl *MakeStringExceptionDirect(MessageAudience aud,int code,const char *why);
void jlib_decl ThrowStringException(int code,const char *format, ...) __attribute__((format(printf, 2, 3), noreturn));

interface jlib_thrown_decl IOSException: extends IException{};
IOSException jlib_decl *MakeOsException(int code);
IOSException jlib_decl *MakeOsException(int code, const char *msg, ...) __attribute__((format(printf, 2, 3)));

#define DISK_FULL_EXCEPTION_CODE ENOSPC

interface jlib_thrown_decl IErrnoException: extends IException{};
IErrnoException jlib_decl *MakeErrnoException(int errn=-1);
IErrnoException jlib_decl *MakeErrnoException(int errn, const char *why, ...) __attribute__((format(printf, 2, 3)));
IErrnoException jlib_decl *MakeErrnoException(const char *why, ...) __attribute__((format(printf, 1, 2)));
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,int errn=-1);
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,int errn, const char *why, ...) __attribute__((format(printf, 3, 4)));
IErrnoException jlib_decl *MakeErrnoException(MessageAudience aud,const char *why, ...) __attribute__((format(printf, 2, 3)));

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
// requires /EHa option to be set in VC++ options (after /GX)

void jlib_decl *setSEHtoExceptionHandler(IExceptionHandler *handler); // sets handler and return old value


void  jlib_decl setTerminateOnSEHInSystemDLLs(bool set=true);
void jlib_decl setTerminateOnSEH(bool set=true);


#define makeUnexpectedException()  MakeStringException(9999, "Internal Error at %s(%d)", __FILE__, __LINE__)
#define throwUnexpected()          throw MakeStringException(9999, "Internal Error at %s(%d)", __FILE__, __LINE__)
#define throwUnexpectedX(x)        throw MakeStringException(9999, "Internal Error '" x "' at %s(%d)", __FILE__, __LINE__)
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

#endif

