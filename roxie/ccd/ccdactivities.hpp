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

#ifndef _CCDACTIVITIES_INCL
#define _CCDACTIVITIES_INCL

#include <stddef.h>
#include <eclhelper.hpp>
#include <jhtree.hpp>
#include <thorcommon.ipp>
#include <roxiemem.hpp>
#include "ccdkey.hpp"

class ActivityArray ;
typedef IHThorArg * (HelperFactory)() ;

interface IRoxiePackage;
interface IQueryFactory;
interface IIndexReadActivityInfo;

// That which is common between Activity factory on slave and Roxie server....

interface IActivityFactory : extends IInterface
{
    virtual void addChildQuery(unsigned id, ActivityArray *childQuery) = 0;
    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const = 0;
    virtual void getNodeProgressInfo(IPropertyTree &node) const = 0;
    virtual ActivityArray *queryChildQuery(unsigned idx, unsigned &id) = 0;
    virtual unsigned queryId() const = 0;
    virtual void resetNodeProgressInfo() = 0;
    virtual IQueryFactory &queryQueryFactory() const = 0;
    virtual ThorActivityKind getKind() const = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;
    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const = 0;
    virtual void mergeStats(const CRuntimeStatisticCollection &from) const = 0;
};

interface IRoxieSlaveActivity : extends IInterface
{
    virtual IMessagePacker *process() = 0;
    virtual bool check() = 0;
    virtual void abort() = 0;
    virtual IRoxieQueryPacket *queryPacket() const = 0;
    virtual StringBuffer &toString(StringBuffer &) const = 0;
    virtual unsigned queryId() = 0;
    virtual IIndexReadActivityInfo *queryIndexReadActivity() = 0;
};

interface ISlaveActivityFactory : extends IActivityFactory
{
    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const = 0;
    virtual StringBuffer &toString(StringBuffer &ret) const = 0;
    virtual const char *queryQueryName() const = 0;
    virtual void addChildQuery(unsigned id, ActivityArray *childQuery) = 0;
};

typedef const void * cvp;

class CJoinGroup;
struct KeyedJoinHeader
{
    offset_t fpos;
    CJoinGroup *thisGroup;
    unsigned short partNo;
    char rhsdata[1]; // n actually
}; 

static inline int KEYEDJOIN_RECORD_SIZE(int dataSize)
{
    // As long as KeyedJoinHeader remains a POD, this is OK
    return offsetof(KeyedJoinHeader, rhsdata) + dataSize;
}

ISlaveActivityFactory *createRoxieCsvReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieXmlReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieDiskReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieDiskNormalizeActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieDiskCountActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieDiskAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieDiskGroupAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieIndexReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieIndexNormalizeActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieIndexCountActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieIndexAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieIndexGroupAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory, ThorActivityKind _kind);
ISlaveActivityFactory *createRoxieFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieXMLFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieCSVFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieKeyedJoinIndexActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieKeyedJoinFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory);
ISlaveActivityFactory *createRoxieRemoteActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &queryFactory, HelperFactory *_factory, unsigned _remoteId);

ISlaveActivityFactory *createRoxieDummyActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, bool isLoadDataOnly);


// Code for processing in-memory data quickly, shared between Roxie server and slave implementations

interface IInMemoryFileProcessor : extends IInterface
{
    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter) = 0;
    virtual void abort() = 0;
};

IInMemoryFileProcessor *createGroupAggregateRecordProcessor(RowAggregator &results, IHThorDiskGroupAggregateArg &helper, IDirectReader *reader);

#endif
