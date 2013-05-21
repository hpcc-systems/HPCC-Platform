/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    JBASE64_Encode(uidpair.str(), uidpair.length(), authToken);
    return authToken;
}

class CWscRowCallSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IWSCRowProvider
{
    bool eof;
    Owned<IWSCHelper> wscHelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWscRowCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer authToken;
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
        appendOutputLinked(this);
        if (container.queryLocalOrGrouped() || firstNode())
        {
            switch (container.getKind())
            {
                case TAKsoap_rowdataset:
                    wscHelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
                    break;
                case TAKhttp_rowdataset:
                    wscHelper.setown(createHttpCallHelper(this, queryRowAllocator(), authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
                    break;
                default:
                    throwUnexpected();
            }
        }
    }
    // IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        dataLinkStart(container.getKind(), container.queryId());
        if (wscHelper)
            wscHelper->start();
    }
    virtual void stop()
    {
        if (wscHelper)
            wscHelper->waitUntilDone();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
    virtual bool isGrouped() { return false; }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper()
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper()
    {
        return (static_cast <IHThorWebServiceCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow() { return NULL; }
    virtual void releaseRow(const void *r) { }
};

//---------------------------------------------------------------------------

class SoapDatasetCallSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IWSCRowProvider
{
    bool eof;
    Owned<IWSCHelper> wscHelper;
    CriticalSection crit;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapDatasetCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer authToken;
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
        appendOutputLinked(this);
        wscHelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken.str(), SCdataset, NULL, queryDummyContextLogger(),NULL));
    }
    // IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart(container.getKind(), container.queryId());
        wscHelper->start();
    }
    virtual void stop()
    {
        eof = true;
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row = wscHelper->getRow();
        if (row)
        {
            dataLinkIncrement();
            return row.getClear();
        }
        eof = true;
        return NULL;
    }
    virtual bool isGrouped()
    {
        return inputs.item(0)->isGrouped();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        wscHelper->abort();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper()
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper()
    {
        return (static_cast <IHThorWebServiceCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow()
    {
        CriticalBlock b(crit);
        if (eof)
            return NULL;
        return input->nextRow();
    }
    virtual void releaseRow(const void *r)
    {
        ReleaseThorRow(r);
    }
};

//---------------------------------------------------------------------------

class SoapRowActionSlaveActivity : public ProcessSlaveActivity, implements IWSCRowProvider
{
    Owned<IWSCHelper> wscHelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapRowActionSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer authToken;
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
        if (container.queryLocalOrGrouped() || firstNode())
            wscHelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
    }

    // IThorSlaveProcess overloaded methods
    virtual void process()
    {
        if (wscHelper)
        {
            wscHelper->start();
            wscHelper->waitUntilDone();
            IException *e = wscHelper->getError();
            if (e)
                throw e;
        }
    }
    virtual void endProcess() { }
    virtual void abort()
    {
        ProcessSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper()
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() { return NULL; }
    virtual const void * getNextRow() { return NULL; }
    virtual void releaseRow(const void *r) { }
};

//---------------------------------------------------------------------------

class SoapDatasetActionSlaveActivity : public ProcessSlaveActivity, implements IWSCRowProvider
{
    Owned<IWSCHelper> wscHelper;
    CriticalSection crit;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapDatasetActionSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer authToken;
        buildAuthToken(queryJob().queryUserDescriptor(), authToken);
        wscHelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCdataset, NULL, queryDummyContextLogger(),NULL));
    }

    // IThorSlaveProcess overloaded methods
    virtual void process()
    {
        processed = 0;

        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;

        wscHelper->start();
        wscHelper->waitUntilDone();
        IException *e = wscHelper->getError();
        if (e)
            throw e;
    }
    virtual void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }
    virtual void abort()
    {
        ProcessSlaveActivity::abort();
        if (wscHelper)
            wscHelper->abort();
    }

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper()
    {
        return (static_cast <IHThorWebServiceCallActionArg *> (queryHelper()));
    }
    virtual IHThorWebServiceCallArg * queryCallHelper() { return NULL; }
    virtual const void * getNextRow()
    {
        CriticalBlock b(crit);

        if (abortSoon)
            return NULL;

        const void *row = input->nextRow();
        if (!row) return NULL;
        processed++;
        return row;
    }
    virtual void releaseRow(const void *r) 
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
