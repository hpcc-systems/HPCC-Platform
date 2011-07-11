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

#ifndef __THORSOAPCALL_HPP_
#define __THORSOAPCALL_HPP_

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
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
    virtual bool rowAvailable() = 0;
    virtual size32_t __deprecated__getRow(void * buffer) = 0;
    virtual bool queryDone() = 0;
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
