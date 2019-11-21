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

#include "thsoapcallslave.ipp"
#include "thactivityutil.ipp"
#include "dasess.hpp"

//---------------------------------------------------------------------------


static StringBuffer &buildAuthToken(IUserDescriptor *userDesc, StringBuffer &authToken)
{
    StringBuffer uidpair;
    userDesc->getUserName(uidpair);
    uidpair.append(":");
    userDesc->getPassword(uidpair);
    JBASE64_Encode(uidpair.str(), uidpair.length(), authToken, false);
    return authToken;
}

class CWscRowCallSlaveActivity : public CSlaveActivity, implements IWSCRowProvider
{
    typedef CSlaveActivity PARENT;

    bool eof;
    Owned<IWSCHelper> wscHelper;
    StringBuffer authToken;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CWscRowCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
        setRequireInitData(false);
        appendOutputLinked(this);
    }

    // IThorDataLink methods
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = false;
        if (container.queryLocalOrGrouped() || firstNode())
        {
            switch (container.getKind())
            {
                case TAKsoap_rowdataset:
                    wscHelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken.str(), SCrow, NULL, container.queryJob().queryContextLogger(), NULL));
                    break;
                case TAKhttp_rowdataset:
                    wscHelper.setown(createHttpCallHelper(this, queryRowAllocator(), authToken.str(), SCrow, NULL, container.queryJob().queryContextLogger(), NULL));
                    break;
                default:
                    throwUnexpected();
            }
            wscHelper->start();
        }
    }
    virtual void stop() override
    {
        if (wscHelper)
        {
            wscHelper->waitUntilDone();
            wscHelper.clear();
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!eof && wscHelper.get())
        {
            OwnedConstThorRow row = wscHelper->getRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        eof = true;
        return NULL;
    }
    virtual bool isGrouped() const override { return false; }
    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper() override
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() override
    {
        return (static_cast <IHThorWebServiceCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow() override { return NULL; }
    virtual void releaseRow(const void *r) override { }
};

//---------------------------------------------------------------------------

class SoapDatasetCallSlaveActivity : public CSlaveActivity, implements IWSCRowProvider
{
    typedef CSlaveActivity PARENT;

    bool eof;
    StringBuffer authToken;
    Owned<IWSCHelper> wscHelper;
    CriticalSection crit;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    SoapDatasetCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        setRequireInitData(false);
        appendOutputLinked(this);
    }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
    }
    // IThorDataLink methods
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = false;
        wscHelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken.str(), SCdataset, NULL, container.queryJob().queryContextLogger(), NULL));
        wscHelper->start();
    }
    virtual void stop() override
    {
        if (wscHelper)
        {
            wscHelper->waitUntilDone();
            wscHelper.clear();
        }
        eof = true;
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow row = wscHelper->getRow();
        if (row)
        {
            dataLinkIncrement();
            return row.getClear();
        }
        eof = true;
        return NULL;
    }
    virtual bool isGrouped() const override
    {
        return queryInput(0)->isGrouped();
    }
    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper() override
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() override
    {
        return (static_cast <IHThorWebServiceCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow() override
    {
        CriticalBlock b(crit);
        if (eof)
            return NULL;
        return inputStream->nextRow();
    }
    virtual void releaseRow(const void *r) override
    {
        ReleaseThorRow(r);
    }
};

//---------------------------------------------------------------------------

class SoapRowActionSlaveActivity : public ProcessSlaveActivity, implements IWSCRowProvider
{
    typedef ProcessSlaveActivity PARENT;
    StringBuffer authToken;
    Owned<IWSCHelper> wscHelper;

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    SoapRowActionSlaveActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        setRequireInitData(false);
    }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
    }

    // IThorSlaveProcess overloaded methods
    virtual void process() override
    {
        if (container.queryLocalOrGrouped() || firstNode())
        {
            wscHelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCrow, NULL, container.queryJob().queryContextLogger(),NULL));
            wscHelper->start();
            wscHelper->waitUntilDone();
            IException *e = wscHelper->getError();
            if (e)
                throw e;
        }
    }
    virtual void endProcess() override { }
    virtual void abort() override
    {
        ProcessSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper() override
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() override { return NULL; }
    virtual const void * getNextRow() override { return NULL; }
    virtual void releaseRow(const void *r) override { }
};

//---------------------------------------------------------------------------

class SoapDatasetActionSlaveActivity : public ProcessSlaveActivity, implements IWSCRowProvider
{
    typedef ProcessSlaveActivity PARENT;

    Owned<IWSCHelper> wscHelper;
    StringBuffer authToken;
    CriticalSection crit;

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    SoapDatasetActionSlaveActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        setRequireInitData(false);
    }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
    }

    // IThorSlaveProcess overloaded methods
    virtual void process() override
    {
        start();
        processed = 0;

        processed = THORDATALINK_STARTED;

        wscHelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCdataset, NULL, container.queryJob().queryContextLogger(),NULL));
        wscHelper->start();
        wscHelper->waitUntilDone();
        IException *e = wscHelper->getError();
        if (e)
            throw e;
    }
    virtual void endProcess() override
    {
        if (processed & THORDATALINK_STARTED)
        {
            stop();
            processed |= THORDATALINK_STOPPED;
        }
    }
    virtual void abort() override
    {
        ProcessSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper() override
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() override { return NULL; }
    virtual const void * getNextRow() override
    {
        CriticalBlock b(crit);

        if (abortSoon)
            return NULL;

        const void *row = inputStream->nextRow();
        if (!row) return NULL;
        processed++;
        return row;
    }
    virtual void releaseRow(const void *r) override
    {
        ReleaseThorRow(r);
    }
};

//---------------------------------------------------------------------------

CActivityBase *createSoapRowCallSlave(CGraphElementBase *container)
{ 
    return new CWscRowCallSlaveActivity(container);
}

CActivityBase *createHttpRowCallSlave(CGraphElementBase *container)
{
    return new CWscRowCallSlaveActivity(container);
}

CActivityBase *createSoapDatasetCallSlave(CGraphElementBase *container)
{
    return new SoapDatasetCallSlaveActivity(container);
}

CActivityBase *createSoapRowActionSlave(CGraphElementBase *container)
{
    return new SoapRowActionSlaveActivity(container);
}

CActivityBase *createSoapDatasetActionSlave(CGraphElementBase *container)
{
    return new SoapDatasetActionSlaveActivity(container);
}
