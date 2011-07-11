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
};

interface IRoxieSlaveActivity : extends IInterface
{
    virtual bool process() = 0;
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
    virtual void noteStatistics(const StatsCollector &stats) = 0;
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

IInMemoryFileProcessor *createKeyedGroupAggregateRecordProcessor(IInMemoryIndexCursor *cursor, RowAggregator &results, IHThorDiskGroupAggregateArg &helper);
IInMemoryFileProcessor *createUnkeyedGroupAggregateRecordProcessor(IInMemoryIndexCursor *cursor, RowAggregator &results, IHThorDiskGroupAggregateArg &helper, IDirectReader *reader, ICodeContext *ctx, unsigned activityId);

#endif
