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

#ifndef _HPCCPROTOCOL_INCL
#define _HPCCPROTOCOL_INCL

#include <jlib.hpp>
#include "roxiehelper.hpp"

#define HPCC_PROTOCOL_BLOCKED          0x0001
#define HPCC_PROTOCOL_TRIM             0x0002

interface IHpccProtocolMsgContext : extends IInterface
{
    virtual void initQuery(StringBuffer &target, const char *queryname) = 0;
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
    virtual void writeLogXML(IXmlWriter &writer) = 0;
    virtual void setTransactionId(const char *id, const char *caller, bool global) = 0;
    virtual void setCallerId(const char *id) = 0;
};

interface IHpccProtocolResultsWriter : extends IInterface
{
    virtual IXmlWriter *addDataset(const char *name, unsigned sequence, const char *elementName, bool &appendRawData, unsigned xmlFlags, bool _extend, const IProperties *xmlns) = 0;
    virtual void finalizeXmlRow(unsigned sequence) = 0;

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
    virtual void appendProbeGraph(const char *xml) = 0;

    virtual void finalize(unsigned seqNo) = 0;

    virtual bool checkConnection() = 0;
    virtual void sendHeartBeat() = 0;
    virtual void flush() = 0;
};

interface IHpccProtocolMsgSink : extends IInterface
{
    virtual CriticalSection &getActiveCrit() = 0;
    virtual bool getIsSuspended() = 0;
    virtual unsigned getActiveThreadCount() = 0;
    virtual unsigned getPoolSize() = 0;
    virtual unsigned getMaxActiveThreads() = 0;
    virtual void setMaxActiveThreads(unsigned val) = 0;
    virtual void incActiveThreadCount() = 0;
    virtual void decActiveThreadCount() = 0;

    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errorMsg, int errorCode) = 0;
    virtual void checkAccess(IpAddress &peer, const char *queryName, const char *queryText, bool isBlind) = 0;
    virtual void queryAccessInfo(StringBuffer &info) = 0;

    virtual IHpccProtocolMsgContext *createMsgContext(time_t startTime) = 0;
    virtual StringArray &getTargetNames(StringArray &targets) = 0;

    virtual void noteQuery(IHpccProtocolMsgContext *msgctx, const char *peer, bool failed, unsigned bytesOut, unsigned elapsed, unsigned memused, unsigned agentsReplyLen, bool continuationNeeded) = 0;
    virtual void onQueryMsg(IHpccProtocolMsgContext *msgctx, IPropertyTree *msg, IHpccProtocolResponse *protocol, unsigned flags, PTreeReaderOptions readFlags, const char *target, unsigned idx, unsigned &memused, unsigned &agentReplyLen) = 0;
};

interface IHpccProtocolListener : extends IInterface
{
    virtual IHpccProtocolMsgSink *queryMsgSink() = 0;

    virtual unsigned queryPort() const = 0;
    virtual const SocketEndpoint &queryEndpoint() const = 0;

    virtual void start() = 0;
    virtual bool stop(unsigned timeout) = 0;
    virtual void stopListening() = 0;
    virtual void disconnectQueue() = 0;

    virtual void runOnce(const char *query) = 0;
};

interface IHpccProtocolPluginContext : extends IInterface
{
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const = 0;
    virtual bool ctxGetPropBool(const char *propName, bool defaultValue) const = 0;
    virtual const char *ctxQueryProp(const char *propName) const = 0;
};

interface IActiveQueryLimiter : extends IInterface
{
    virtual bool isAccepted() = 0;
};

interface IActiveQueryLimiterFactory : extends IInterface
{
    virtual IActiveQueryLimiter *create(IHpccProtocolListener *listener) = 0;
};

interface IHpccProtocolPlugin : extends IInterface
{
    virtual IHpccProtocolListener *createListener(const char *protocol, IHpccProtocolMsgSink *sink, unsigned port, unsigned listenQueue, const char *config, const char *certFile=nullptr, const char *keyFile=nullptr, const char *passPhrase=nullptr)=0;
};

extern IHpccProtocolPlugin *loadHpccProtocolPlugin(IHpccProtocolPluginContext *ctx, IActiveQueryLimiterFactory *limiterFactory);
extern void unloadHpccProtocolPlugin();
typedef IHpccProtocolPlugin *(HpccProtocolInstallFunction)(IHpccProtocolPluginContext *ctx, IActiveQueryLimiterFactory *limiterFactory);


#endif
