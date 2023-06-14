/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#pragma once

#include "esp.hpp"

//ESP Bindings
#include "http/platform/mime.hpp"

#include "xpp/XmlPullParser.h"
using namespace xpp;

//Jlib
#include "jstring.hpp"

interface ISoapMessage : extends IInterface
{
    virtual const char * getMessageType() = 0;
    virtual StringBuffer & toString(StringBuffer & str) = 0;
    virtual IEspContext * queryContext() = 0;
};


interface IRpcMessage : extends IInterface
{
    virtual IEspContext * queryContext() = 0;
    virtual void setContext(IEspContext * value) = 0;
    virtual const char * get_name() = 0;
    virtual void set_name(const char * name) = 0;
    virtual void set_ns(const char * ns) = 0;
    virtual void set_nsuri(const char * nsuri) = 0;
    virtual StringBuffer & get_nsuri(StringBuffer & nsuri) = 0;
    virtual const char * get_text() = 0;
    virtual void set_text(const char * text) = 0;
    virtual void append_text(const char * text) = 0;
    virtual bool get_value(const char * path, StringAttr & value) = 0;
    virtual bool get_value(const char * path, StringBuffer & value) = 0;
    virtual bool get_value(const char * path, StringBuffer & value, bool bSimpleXml) = 0;
    virtual bool get_value(const char * path, int & value) = 0;
    virtual bool get_value(const char * path, long & value) = 0;
    virtual bool get_value(const char * path, __int64 & value) = 0;
    virtual bool get_value(const char * path, unsigned long & value) = 0;
    virtual bool get_value(const char * path, unsigned & value) = 0;
    virtual bool get_value(const char * path, unsigned short & value) = 0;
    virtual bool get_value(const char * path, unsigned char & value) = 0;
    virtual bool get_value(const char * path, short & value) = 0;
    virtual bool get_value(const char * path, bool & value) = 0;
    virtual bool get_value(const char * path, double & value) = 0;
    virtual bool get_value(const char * path, float & value) = 0;
    virtual bool get_value(const char * path, StringArray & value) = 0;
    virtual bool get_value(const char * path, StringArray & value, bool simpleXml) = 0;
    virtual bool get_value(const char * path, IntArray & value) = 0;
    virtual bool get_value(const char * path, ShortArray & value) = 0;
    virtual bool get_value(const char * path, Int64Array & value) = 0;
    virtual bool get_value(const char * path, BoolArray & value) = 0;
    virtual bool get_value(const char * path, FloatArray & value) = 0;
    virtual bool get_value(const char * path, DoubleArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, const char * value, bool encodeXml) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, const char * value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, StringBuffer & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, long value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, __int64 value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, unsigned long value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, int value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, unsigned value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, unsigned short value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, bool value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, double value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, StringArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, ShortArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, IntArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, Int64Array & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, BoolArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, FloatArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * childns, const char * childname, const char * childtype, DoubleArray & value) = 0;
    virtual void add_value(const char * path, const char * ns, const char * name, const char * type, const char * value, IProperties & attrs) = 0;
    virtual void add_value(const char * path, const char * name, const char * value, IProperties & attrs) = 0;
    virtual void add_attr(const char * path, const char * name, const char * value, IProperties & attrs) = 0;
    virtual void unmarshall(XJXPullParser * xpp) = 0;
    virtual void marshall(StringBuffer & outbuf, CMimeMultiPart * multipart) = 0;
    virtual void simple_marshall(StringBuffer & outbuf) = 0;
    virtual void setEncodeXml(bool encode) = 0;
    virtual bool getEncodeXml() = 0;
    virtual void setSerializedContent(const char * c) = 0;
};


interface IRpcSerializable : extends IInterface
{
    virtual void serialize(IEspContext * ctx, StringBuffer & buffer, const char * rootname) = 0;
    virtual bool unserialize(IRpcMessage & rpc, const char * tagname, const char * basepath) = 0;
};


interface ISoapService : extends IEspService
{
    virtual int processRequest(ISoapMessage & request, ISoapMessage & response) = 0;
};


typedef IArrayOf<IRpcMessage> IRpcMessageArray;

interface ISoapClient : extends IInterface
{
    virtual int setUsernameToken(const char * username, const char * password, const char * realm) = 0;
    virtual void disableKeepAlive() = 0;
    virtual int postRequest(IRpcMessage & rpccall, IRpcMessage & response) = 0;
    virtual int postRequest(const char * soapaction, IRpcMessage & rpccall, IRpcMessage & response) = 0;
    virtual int postRequest(const char * soapaction, IRpcMessage & rpccall, StringBuffer & responsebuf) = 0;
    virtual int postRequest(const char * contenttype, const char * soapaction, IRpcMessage & rpccall, IRpcMessage & response) = 0;
    virtual int postRequest(const char * contenttype, const char * soapaction, IRpcMessage & rpccall, StringBuffer & responsebuf) = 0;
    virtual int postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf) = 0;
    virtual int postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf, IRpcMessageArray * headers) = 0;
    virtual int postRequest(const char * contenttype, const char * soapaction, IRpcMessage & rpccall, StringBuffer & responsebuf, IRpcMessageArray * headers) = 0;
    virtual int postRequest(const char * soapaction, IRpcMessage & rpccall, StringBuffer & responsebuf, IRpcMessageArray * headers) = 0;
};


interface IHttpServerService : extends IInterface
{
    virtual int processRequest() = 0;
    virtual int onPost() = 0;
    virtual int onGet() = 0;
};


interface ITransportClient : extends IInterface
{
    virtual int postRequest(ISoapMessage & request, ISoapMessage & response) = 0;
};


interface IEspSoapBinding : extends IEspRpcBinding
{
    virtual int processRequest(IRpcMessage * rpc_call, IRpcMessage * rpc_response) = 0;
};


enum RpcMessageState
{
    RPC_MESSAGE_OK = 0,
    RPC_MESSAGE_CONNECTION_ERROR = 1,
    RPC_MESSAGE_ERROR = 2
};


interface IRpcMessageBinding : extends IInterface
{
    virtual void setClientValue(unsigned val) = 0;
    virtual unsigned getClientValue() = 0;
    virtual void setReqId(unsigned val) = 0;
    virtual unsigned getReqId() = 0;
    virtual void setEventSink(void * val) = 0;
    virtual void * getEventSink() = 0;
    virtual void setState(IInterface * state) = 0;
    virtual IInterface * queryState() = 0;
    virtual void setThunkHandle(void * val) = 0;
    virtual void * getThunkHandle() = 0;
    virtual void setMethod(const char * method) = 0;
    virtual const char * getMethod() = 0;
    virtual void lock() = 0;
    virtual void unlock() = 0;
};


interface IRpcResponseBinding : extends IRpcMessageBinding
{
    virtual void setRpcState(RpcMessageState state) = 0;
    virtual RpcMessageState getRpcState() = 0;
    virtual bool unserialize(IRpcMessage & rpc_response, const char * tag, const char * path) = 0;
};


interface IRpcRequestBinding : extends IRpcMessageBinding
{
    virtual const char * getUrl() = 0;
    virtual void setUrl(const char * val) = 0;
    virtual void serialize(IRpcMessage & rpc_response) = 0;
    virtual void post(IRpcResponseBinding & response) = 0;
};
