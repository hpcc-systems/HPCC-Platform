/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef _CCDPROTOCOL_INCL
#define _CCDPROTOCOL_INCL

#include "hpccprotocol.hpp"

#define HPCC_PROTOCOL_NATIVE           0x10000
#define HPCC_PROTOCOL_NATIVE_RAW       0x20000
#define HPCC_PROTOCOL_NATIVE_XML       0x40000
#define HPCC_PROTOCOL_NATIVE_ASCII     0x80000

interface IHpccNativeProtocolMsgSink : extends IHpccProtocolMsgSink
{
    virtual void onControlMsg(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol) = 0;
    virtual void onDebugMsg(IHpccProtocolMsgContext *msgctx, const char *uid, IPropertyTree *msg, IXmlWriter &out) = 0;
};

interface IHpccNativeProtocolResultsWriter : extends IHpccProtocolResultsWriter
{
    virtual void appendRawRow(unsigned sequence, unsigned len, const char *data) = 0;
    virtual void appendSimpleRow(unsigned sequence, const char *str) = 0;
    virtual void appendRaw(unsigned sequence, unsigned len, const char *data) = 0;
};

interface IHpccNativeProtocolResponse : extends IHpccProtocolResponse
{
    virtual SafeSocket *querySafeSocket() = 0; //still passed to debug context, and row streaming, for now.. tbd get rid of this
};

#endif
