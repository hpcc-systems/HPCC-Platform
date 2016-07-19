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

#ifndef __THORSOAPCALL_HPP_
#define __THORSOAPCALL_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include "jlog.hpp"
#include "eclhelper.hpp"

//Web Service Call Row Provider
interface IWSCRowProvider : extends IInterface
{
    virtual IHThorWebServiceCallActionArg * queryActionHelper() = 0;
    virtual IHThorWebServiceCallArg * queryCallHelper() = 0;
    virtual const void * getNextRow() = 0;
    virtual void releaseRow(const void * row) = 0;
};
typedef IWSCRowProvider ISoapCallRowProvider;//DEPRECATED

//Web Service Call Helper
interface IWSCHelper : extends IInterface
{
    virtual void start() = 0;
    virtual void abort() = 0;
    virtual void waitUntilDone() = 0;
    virtual IException * getError() = 0;
    virtual const void * getRow() = 0;

    virtual IXmlToRowTransformer * getRowTransformer() = 0;
    virtual size32_t transformRow(ARowBuilder & rowBuilder, IColumnProvider * row) = 0;
    virtual void putRow(const void * row) = 0;
};
typedef IWSCHelper ISoapCallHelper ;//DEPRECATED

enum WSCMode { SCrow, SCdataset };  //Web Service Call Mode
typedef WSCMode SoapCallMode;//DEPRECATED

class ClientCertificate : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    StringAttr certificate;
    StringAttr privateKey;
    StringAttr passphrase;
};


interface IRoxieAbortMonitor
{
    virtual void checkForAbort() = 0;//needed for Roxie abort polling mechanism
};


extern THORHELPER_API unsigned soapTraceLevel;
extern THORHELPER_API IWSCHelper * createSoapCallHelper(IWSCRowProvider *, IEngineRowAllocator * outputAllocator, const char *authToken, SoapCallMode scMode, ClientCertificate *clientCert, const IContextLogger &logctx, IRoxieAbortMonitor * roxieAbortMonitor);
extern THORHELPER_API IWSCHelper * createHttpCallHelper(IWSCRowProvider *, IEngineRowAllocator * outputAllocator, const char *authToken, SoapCallMode scMode, ClientCertificate *clientCert, const IContextLogger &logctx, IRoxieAbortMonitor * roxieAbortMonitor);

#endif /* __THORSOAPCALL_HPP_ */
