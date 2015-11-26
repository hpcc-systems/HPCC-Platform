/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef __WSEXCEPT__
#define __WSEXCEPT__

#include "jexcept.hpp"

#ifdef _WIN32
 #ifdef ESDLLIB_EXPORTS
  #define esdl_decl __declspec(dllexport)
 #else
  #define esdl_decl
 #endif
#else
 #define esdl_decl
#endif

typedef enum
{
    WSERR_NOERR=-1,WSERR_CLIENT, WSERR_SERVER, WSERR_VERSION, WSERR_MUSTUNDERSTAND
} WsErrorType;

interface IWsException : extends IException
{
   //convenience methods for handling this as an array
   virtual aindex_t ordinality() const = 0;
   virtual IException& item(aindex_t pos) const = 0;
   virtual const char* source() const = 0;

   //for complete control...
   virtual IArrayOf<IException>& getArray()= 0;

   //add another exception
   virtual void append(IException& e) = 0;
   virtual void append(IMultiException& e) = 0;

   virtual StringBuffer& serialize(StringBuffer& ret, unsigned indent = 0, bool simplified=false, bool root=true) const = 0;

   //the following methods override those in IIException
   //
    virtual int errorCode() const = 0;
    virtual StringBuffer&     errorMessage(StringBuffer &msg) const = 0;
    virtual MessageAudience errorAudience() const = 0;
    virtual WsErrorType     errorType() const = 0;
};

IWsException esdl_decl *makeWsException(IMultiException& me, WsErrorType errorType);
IWsException esdl_decl *makeWsException(const char *source, WsErrorType errorType);
IWsException esdl_decl *makeWsException(IException& e, WsErrorType errorType, const char* source = NULL );
IWsException esdl_decl *makeWsException(int errorCode, WsErrorType errorType, const char* source, const char *format, ...) __attribute__((format(printf,4,5)));

#endif
