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

#ifndef _CCDPROTOCOL_INCL
#define _CCDPROTOCOL_INCL

#include <jlib.hpp>
#include "roxiehelper.hpp"

#define HPCC_PROTOCOL_NATIVE           0x0001
#define HPCC_PROTOCOL_NATIVE_RAW       0x0002
#define HPCC_PROTOCOL_NATIVE_XML       0x0004
#define HPCC_PROTOCOL_NATIVE_ASCII     0x0008
#define HPCC_PROTOCOL_BLOCKED          0x0010
#define HPCC_PROTOCOL_TRIM             0x0020

interface IRoxieListener : extends IInterface
{
    virtual CriticalSection &getActiveCrit() = 0;
    virtual bool getIsSuspended() = 0;
    virtual unsigned getActiveThreadCount() = 0;  //ADF todo - consider moving all of these counters into the roxie side.. don't make protocols implement this
    virtual unsigned getPoolSize() = 0;
    virtual unsigned getMaxActiveThreads() = 0;
    virtual void setMaxActiveThreads(unsigned val) = 0;
    virtual void incActiveThreadCount() = 0;
    virtual void decActiveThreadCount() = 0;

    virtual void start() = 0;
    virtual bool stop(unsigned timeout) = 0;
    virtual void stopListening() = 0;
    virtual void disconnectQueue() = 0;
    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errMsg, int errCode) = 0;
    virtual unsigned queryPort() const = 0;
    virtual const SocketEndpoint &queryEndpoint() const = 0;
    virtual bool suspend(bool suspendIt) = 0;

    virtual void runOnce(const char *query) = 0;
};

interface IHpccProtocolMsgContext : extends IInterface
{
    virtual bool initQuery(StringBuffer &target, const char *queryname) = 0;
    virtual void noteQueryActive() = 0;
    virtual unsigned getQueryPriority() = 0;
    virtual IContextLogger *queryLogContext() = 0;
    virtual bool checkSetBlind(bool blind) = 0;
    virtual void verifyAllowDebug() = 0;
    virtual bool logFullQueries() = 0;
    virtual bool trapTooManyActiveQueries() = 0;
    virtual bool getStripWhitespace() = 0;
    virtual int getBindCores() = 0;
    virtual void setTraceLevel(unsigned val) = 0;
    virtual void setIntercept(bool val) = 0;
    virtual bool getIntercept() = 0;
    virtual void outputLogXML(IXmlStreamFlusher &out) = 0;
};

interface IHpccProtocolScalarResult : extends IInterface
{
};

interface IHpccProtocolResultsWriter : extends IInterface
{
    virtual IXmlWriter *addDataset(const char *name, unsigned sequence, const char *elementName, bool &appendRawData, unsigned xmlFlags, bool _extend, const IProperties *xmlns) = 0;
    virtual void finalizeXmlRow(unsigned sequence) = 0;

    virtual void appendRawRow(unsigned sequence, unsigned len, const char *data) = 0;
    virtual void appendSimpleRow(unsigned sequence, const char *str) = 0;
    virtual void appendRaw(unsigned sequence, unsigned len, const char *data) = 0;

    virtual void setResultBool(const char *name, unsigned sequence, bool value) = 0;
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) = 0;
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) = 0;
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) = 0;
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) = 0;
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) = 0;
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) = 0;
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) = 0;
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) = 0;
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) = 0;
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) = 0;
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) = 0;
};

interface IHpccProtocolResponse : extends IInterface
{
    virtual unsigned getFlags() = 0;
    virtual IHpccProtocolResultsWriter *queryHpccResultsSection() = 0;

    virtual void appendContent(TextMarkupFormat mlFmt, const char *content, const char *name=NULL) = 0; //will be transformed
    virtual IXmlWriter *writeAppendContent(const char *name = NULL) = 0;

    virtual void finalize(unsigned seqNo) = 0; //adf not native (raw or xml)

    virtual bool checkConnection() = 0;
    virtual void sendHeartBeat() = 0;

    virtual SafeSocket *querySafeSocket() = 0; //still passed to debug context, and row streaming, for now.. best to get rid of this

    //native protocol "post processing":
    virtual void flush() = 0;
    virtual void appendProbeGraph(const char *xml) = 0;
};


interface IHpccProtocolMsgSink : extends IInterface
{
    virtual IHpccProtocolMsgContext *createMsgContext(time_t startTime) = 0;
    virtual StringArray &getTargetNames(StringArray &targets) = 0;
    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errorMsg, int errorCode) = 0;
    virtual void checkAccess(IpAddress &peer, const char *queryName, const char *queryText, bool isBlind) = 0;
    virtual void queryAccessInfo(StringBuffer &info) = 0;

    virtual void query(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol, unsigned flags, PTreeReaderOptions xmlReadFlags, const char *querySetName, unsigned idx, unsigned &memused, unsigned &slaveReplyLen) = 0;
    virtual void control(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, const char *logtext, IHpccProtocolResponse *protocol) = 0;
    virtual void debug(IHpccProtocolMsgContext *msgctx, const char *uid, IPropertyTree *msg, const char *logtext, IXmlWriter &out) = 0;

    virtual void noteQuery(IHpccProtocolMsgContext *msgctx, const char *peer, bool failed, unsigned bytesOut, unsigned elapsed, unsigned priority, unsigned memused, unsigned slavesReplyLen, bool continuationNeeded) = 0;
};


extern IRoxieListener *createProtocolSocketListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned poolSize, unsigned listenQueue, bool suspended);

interface IActiveQueryLimiter : extends IInterface
{
    virtual bool isAccepted() = 0;
};

extern IActiveQueryLimiter *createActiveQueryLimiter(IRoxieListener *listener);


#endif
