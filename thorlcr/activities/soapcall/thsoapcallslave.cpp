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

#include "thsoapcallslave.ipp"
#include "thactivityutil.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/soapcall/thsoapcallslave.cpp $ $Id: thsoapcallslave.cpp 62376 2011-02-04 21:59:58Z sort $");

//---------------------------------------------------------------------------

class SoapRowCallSlaveActivity : public CSlaveActivity, public CThorDataLink, implements ISoapCallRowProvider
{
    bool eof;
    Owned<ISoapCallHelper> soaphelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapRowCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        const char *authToken;
        data.read(authToken);
        appendOutputLinked(this);
        if (container.queryLocalOrGrouped() || (1 == container.queryJob().queryMyRank()))
            soaphelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken, SCrow, NULL, queryDummyContextLogger(),NULL));
    }
    // IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        ActPrintLog("SOAPCALL");
        dataLinkStart("SOAPCALL", container.queryId());
        if (soaphelper)
            soaphelper->start();
    }
    virtual void stop()
    {
        abortSoon = true;
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!eof && soaphelper.get())
        {
            OwnedConstThorRow row = soaphelper->getRow();
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
        if (soaphelper)
            soaphelper->abort();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // ISoapCallRowProvider
    virtual IHThorSoapActionArg * queryActionHelper()
    {
        return (static_cast <IHThorSoapActionArg *> (queryHelper()));
    }
    virtual IHThorSoapCallArg * queryCallHelper()
    {
        return (static_cast <IHThorSoapCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow() { return NULL; }
    virtual void releaseRow(const void *r) { }
};

//---------------------------------------------------------------------------

class SoapDatasetCallSlaveActivity : public CSlaveActivity, public CThorDataLink, implements ISoapCallRowProvider
{
    bool eof;
    Owned<ISoapCallHelper> soaphelper;
    CriticalSection crit;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapDatasetCallSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        const char *authToken;
        data.read(authToken);
        appendOutputLinked(this);
        soaphelper.setown(createSoapCallHelper(this, queryRowAllocator(), authToken, SCdataset, NULL, queryDummyContextLogger(),NULL));
    }
    // IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ActPrintLog("SOAPCALL");
        eof = false;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("SOAPCALL", container.queryId());
        soaphelper->start();
    }
    virtual void stop()
    {
        abortSoon = true;
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row = soaphelper->getRow();
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
        soaphelper->abort();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }

    // ISoapCallRowProvider
    virtual IHThorSoapActionArg * queryActionHelper()
    {
        return (static_cast <IHThorSoapActionArg *> (queryHelper()));
    }
    virtual IHThorSoapCallArg * queryCallHelper()
    {
        return (static_cast <IHThorSoapCallArg *> (queryHelper()));
    }
    virtual const void * getNextRow()
    {
        CriticalBlock b(crit);
        if (abortSoon)
            return NULL;
        return input->nextRow();
    }
    virtual void releaseRow(const void *r)
    {
        ReleaseThorRow(r);
    }
};

//---------------------------------------------------------------------------

class SoapRowActionSlaveActivity : public ProcessSlaveActivity, implements ISoapCallRowProvider
{
    Owned<ISoapCallHelper> soaphelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapRowActionSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        const char *authToken;
        data.read(authToken);
        if (container.queryLocalOrGrouped() || (1 == container.queryJob().queryMyRank()))
            soaphelper.setown(createSoapCallHelper(this, NULL, authToken, SCrow, NULL, queryDummyContextLogger(),NULL));
    }

    // IThorSlaveProcess overloaded methods
    virtual void process()
    {
        if (soaphelper)
        {
            soaphelper->start();
            soaphelper->waitUntilDone();
            IException *e = soaphelper->getError();
            if (e)
                throw e;
        }
    }
    virtual void endProcess() { }
    virtual void abort()
    {
        ProcessSlaveActivity::abort();
        if (soaphelper)
            soaphelper->abort();
    }

    // ISoapCallRowProvider
    virtual IHThorSoapActionArg * queryActionHelper()
    {
        return (static_cast <IHThorSoapActionArg *> (queryHelper()));
    }
    virtual IHThorSoapCallArg * queryCallHelper() { return NULL; }
    virtual const void * getNextRow() { return NULL; }
    virtual void releaseRow(const void *r) { }
};

//---------------------------------------------------------------------------

class SoapDatasetActionSlaveActivity : public ProcessSlaveActivity, implements ISoapCallRowProvider
{
    Owned<ISoapCallHelper> soaphelper;
    CriticalSection crit;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SoapDatasetActionSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container) { }

    // IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        const char *authToken;
        data.read(authToken);

        soaphelper.setown(createSoapCallHelper(this, NULL, authToken, SCdataset, NULL, queryDummyContextLogger(),NULL));
    }

    // IThorSlaveProcess overloaded methods
    virtual void process()
    {
        processed = 0;

        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;

        soaphelper->start();
        soaphelper->waitUntilDone();
        IException *e = soaphelper->getError();
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
        if (soaphelper)
            soaphelper->abort();
    }

    // ISoapCallRowProvider
    virtual IHThorSoapActionArg * queryActionHelper()
    {
        return (static_cast <IHThorSoapActionArg *> (queryHelper()));
    }
    virtual IHThorSoapCallArg * queryCallHelper() { return NULL; }
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
    return new SoapRowCallSlaveActivity(container);
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
