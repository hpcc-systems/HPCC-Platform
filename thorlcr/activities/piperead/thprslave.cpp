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

#include "jio.hpp"

#include "thorpipe.hpp"
#include "thbufdef.hpp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thprslave.ipp"
#include "thcrc.hpp"
#include "thexception.hpp"

/////////////////////////

class CPipeSlaveBase : public CSlaveActivity
{
protected:
    MemoryBuffer trailingBuffer;

    Owned<IPipeProcess> pipe;
    Owned<IReadRowStream> readTransformer;
    StringAttr pipeCommand;
    bool eof, pipeFinished;
    unsigned retcode;

protected:

    class CPipeStream : public CSimpleInterface, implements ISimpleReadStream
    {
        CPipeSlaveBase *parent;
        Owned<ISimpleReadStream> stream;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPipeStream(CPipeSlaveBase *_parent) : parent(_parent) { }
        virtual size32_t read(size32_t max_len, void *ptr)
        {
            byte * target = static_cast<byte *>(ptr);
            size32_t sizeRead = 0;
            while ((sizeRead<max_len)&&!parent->pipeFinished) {
                size32_t toread = RECORD_BUFFER_SIZE;
                if (sizeRead+toread>max_len)
                    toread=max_len-sizeRead;
                size32_t numRead = stream->read(toread, target+sizeRead);
                if (numRead==(size32_t)-1) {
                    ::ActPrintLog(parent, "read aborted");
                    numRead = 0;
                    sizeRead = 0;
                }
                sizeRead += numRead;
                if (numRead==0) {
                    parent->verifyPipe();
                    break;
                }
            }
            return sizeRead;
        }
        void setStream(ISimpleReadStream *_stream) { stream.setown(_stream); }
    } *pipeStream;

    void openPipe(char const *cmd, const char *pipeTrace)
    {
        pipeCommand.setown(cmd);
        ActPrintLog("open: %s", cmd);
        if (!pipe->run(pipeTrace, cmd, globals->queryProp("@externalProgDir"), true, true, true, 0x10000)) // 64K error buffer
            throw MakeActivityException(this, TE_FailedToCreateProcess, "Failed to create process in %s for : %s", globals->queryProp("@externalProgDir"), cmd);
        pipeFinished = false;
        registerSelfDestructChildProcess(pipe->getProcessHandle());
        pipeStream->setStream(pipe->getOutputStream());
        readTransformer->setStream(pipeStream);
    }
    void closePipe()
    {
        pipe->closeInput();
    }
    void verifyPipe()
    {
        if (!pipeFinished)
        {
            pipeFinished = true;
            HANDLE pipeProcess = pipe->getProcessHandle();
            retcode = pipe->wait();
            unregisterSelfDestructChildProcess(pipeProcess);
            if (retcode)
            {
                StringBuffer stdError;
                if (pipe->hasError())
                {
                    try
                    {
                        char error[512];
                        size32_t sz = pipe->readError(sizeof(error), error);
                        if (sz && sz!=(size32_t)-1)
                            stdError.append(", stderr: '").append(sz, error).append("'");
                    }
                    catch (IException *e)
                    {
                        ActPrintLog(e, "Error reading pipe stderr");
                        e->Release();
                    }
                }
                throw MakeActivityException(this, TE_PipeReturnedFailure, "Process returned %d:%s - PIPE(%s)", retcode, stdError.str(), pipeCommand.get());
            }
        }
    }
    void readTrailing()
    {
        if (!pipeFinished&&pipe->hasOutput())
        {
            ActPrintLog("reading trailing");
            try
            {
                void *buf = trailingBuffer.clear().reserve(RECORD_BUFFER_SIZE);
                size32_t read;
                do
                {
                    read = pipe->read(RECORD_BUFFER_SIZE, buf);
                } while ((read!=0)&&(read!=(size32_t)-1));
            }
            catch (IException *e)
            {
                e->Release();
            }
        }
    }
    bool queryGetContinue(size32_t & sizeGot, bool & eogNext)
    {
        if(eogNext || eof || abortSoon)
        {
            sizeGot = 0;
            eogNext = false;
            return false;
        }
        return true;
    }
    void abortPipe()
    {
        unregisterSelfDestructChildProcess(pipe->getProcessHandle());
        pipe->abort();
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CPipeSlaveBase(CGraphElementBase *_container)
        : CSlaveActivity(_container)
    {
        pipeFinished = true;
        retcode = 0;
        pipe.setown(createPipeProcess(globals->queryProp("@allowedPipePrograms")));
        pipeStream = new CPipeStream(this);
    }
    ~CPipeSlaveBase()
    {
        ::Release(pipeStream);
    }
};


//---------------------------------------------------------------------------

class CPipeReadSlaveActivity : public CPipeSlaveBase, public CThorDataLink
{
protected:
    IHThorPipeReadArg *helper;
    Owned<IRowInterfaces> inrowif;
    bool needTransform;

    bool eof;

public:
    IMPLEMENT_IINTERFACE_USING(CPipeSlaveBase);

    CPipeReadSlaveActivity(CGraphElementBase *_container) 
        : CPipeSlaveBase(_container), CThorDataLink(this)
    {
    }
    CATCH_NEXTROW()
    {   
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof || abortSoon)
            return NULL;
        try
        {
            loop
            {
                if (readTransformer->eos())
                    break;
                OwnedConstThorRow ret = readTransformer->next();
                if (ret.get())
                {
                    dataLinkIncrement();
                    return ret.getClear();
                }
            }
        }
        catch(IException *e) // trying to catch OsException here should we not have a IOSException?
        {       
#ifdef _WIN32
            if (e->errorCode() == ERROR_INVALID_HANDLE || e->errorCode() == ERROR_BROKEN_PIPE) // JCSMORE - this this occurs when pipeProgram closes output when finished with input.
#else
            if (e->errorCode() == EBADF || e->errorCode() == EPIPE) // JCSMORE - this this occurs when pipeProgram closes output when finished with input.
#endif
            {
                StringBuffer s;
                ActPrintLog("WARNING: input terminated for pipe with : %s", e->errorMessage(s).str());
            }
            else
                throw;      
        }
        eof = true;
        return NULL;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorPipeReadArg *> (queryHelper());
        needTransform = false;

        if (needTransform)
            inrowif.setown(createRowInterfaces(helper->queryDiskRecordSize(),queryActivityId(),queryCodeContext()));
        else
            inrowif.set(this);
        readTransformer.setown(createReadRowStream(inrowif->queryRowAllocator(), inrowif->queryRowDeserializer(), helper->queryXmlTransformer(), helper->queryCsvTransformer(), helper->queryXmlIteratorPath(), helper->getPipeFlags()));
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        openPipe(helper->getPipeProgram(), "PIPEREAD");
        dataLinkStart("PIPEREAD", container.queryId());
    }
    virtual void stop()
    {
        readTrailing();
        verifyPipe();
        dataLinkStop();
    }
    virtual void abort()
    {
        CPipeSlaveBase::abort();
        abortPipe();
    }
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.unknownRowsOutput = true;
        // MORE TODO fill in numbers of rows
    }
};

CActivityBase *createPipeReadSlave(CGraphElementBase *container)
{
    return new CPipeReadSlaveActivity(container);
}


//////////////////////////////////



class CPipeThroughSlaveActivity;
class PipeWriterThread : public Thread
{
public:
    PipeWriterThread(CPipeThroughSlaveActivity & _activity);

    virtual int run();

    IException * joinExc()
    {
        Thread::join();
        return exc.getClear();
    }

protected:
    CPipeThroughSlaveActivity &     activity;
    IThorDataLink *                 input;
    Owned<IException>               exc;
};


//---------------------------------------------------------------------------

class CPipeThroughSlaveActivity : public CPipeSlaveBase, public CThorDataLink
{
    friend class PipeWriterThread;

    IHThorPipeThroughArg *helper;
    bool recreate, anyThisGroup, inputExhausted, firstRead, grouped;
    PipeWriterThread *pipeWriter;
    Owned<IPipeWriteXformHelper> writeTransformer;
    Semaphore pipeOpened;
    Semaphore pipeVerified;

    void openPipe(char const *cmd, const char *pipeTrace)
    {
        CPipeSlaveBase::openPipe(cmd, pipeTrace);
        writeTransformer->writeHeader(pipe);
    }
    void closePipe()
    {
        writeTransformer->writeFooter(pipe);
        CPipeSlaveBase::closePipe();
    }
    void writeTranslatedText(const void *row)
    {
        writeTransformer->writeTranslatedText(row, pipe);
    }
public:
    IMPLEMENT_IINTERFACE_USING(CPipeSlaveBase);

    CPipeThroughSlaveActivity(CGraphElementBase *_container)
        : CPipeSlaveBase(_container), CThorDataLink(this)
    {
        pipeWriter = NULL;
        grouped = false;
    }
    ~CPipeThroughSlaveActivity()
    {
        ::Release(pipeWriter);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorPipeThroughArg *> (queryHelper());
        recreate = helper->recreateEachRow();
        grouped = 0 != (helper->getPipeFlags() & TPFgroupeachrow);

        readTransformer.setown(createReadRowStream(queryRowAllocator(), queryRowDeserializer(), helper->queryXmlTransformer(), helper->queryCsvTransformer(), helper->queryXmlIteratorPath(), helper->getPipeFlags()));
        readTransformer->setStream(pipeStream); // NB the pipe process stream is provided to pipeStream after pipe->run()

        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = anyThisGroup = inputExhausted = false;
        firstRead = true;

        if (!writeTransformer)
        {
            writeTransformer.setown(createPipeWriteXformHelper(helper->getPipeFlags(), helper->queryXmlOutput(), helper->queryCsvOutput(), ::queryRowInterfaces(inputs.item(0))->queryRowSerializer()));
            writeTransformer->ready();
        }
        if (!recreate)
            openPipe(helper->getPipeProgram(), "PIPETHROUGH");

        startInput(inputs.item(0));
        dataLinkStart("PIPETHROUGH", container.queryId());
        pipeWriter = new PipeWriterThread(*this);
        pipeWriter->start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof || abortSoon)
            return NULL;
        loop
        {
            if (recreate && firstRead)
            {
                pipeOpened.wait();
                if (inputExhausted)
                {
                    eof = true;
                    break;
                }
                else
                {
                    firstRead = false;
                    if (grouped && anyThisGroup)
                    {
                        anyThisGroup = false;
                        break;
                    }
                }
            }
            if (!readTransformer->eos())
            {
                try
                {
                    const void *ret = readTransformer->next();
                    if (ret)
                    {
                        anyThisGroup = true;
                        dataLinkIncrement();
                        return ret;
                    }
                }
                catch(IException *e) // trying to catch OsException here should we not have a IOSException?
                {
#ifdef _WIN32
                    if (e->errorCode() == ERROR_INVALID_HANDLE || e->errorCode() == ERROR_BROKEN_PIPE) // JCS - this occurs when pipeProgram closes output when finished with input.
#else
                    if (e->errorCode() == EBADF || e->errorCode() == EPIPE) // JCS - this occurs when pipeProgram closes output when finished with input.
#endif
                    {
                        StringBuffer s;
                        ActPrintLog("WARNING: input terminated for pipe with : %s", e->errorMessage(s).str());
                        e->Release();
                    }
                    else
                        throw;
                }
            }
            if (!recreate)
            {
                eof = true;
                break;
            }
            pipeVerified.signal();
            firstRead = true;
        }
        return NULL;
    }
    virtual void stop()
    {
        abortSoon = true;
        readTrailing();
        if (recreate)
            pipeVerified.signal();
        Owned<IException> wrexc = pipeWriter->joinExc();
        stopInput(inputs.item(0));
        verifyPipe();
        dataLinkStop();
        if (wrexc)
            throw wrexc.getClear();
        if (retcode!=0)
            throw MakeActivityException(this, TE_PipeReturnedFailure, "Process returned %d", retcode);
    }
    virtual void kill()
    {
        CPipeSlaveBase::kill();
        inputExhausted = true;
        pipeOpened.signal();
        readTrailing();
        ActPrintLog("waiting for pipe writer");
        Owned<IException> wrexc;
        if (pipeWriter)
            pipeWriter->join();
        pipe->wait();
        if (wrexc)
            throw wrexc.getClear();
        ActPrintLog("kill exit");
    }
    virtual void abort()
    {
        CPipeSlaveBase::abort();
        inputExhausted = true;
        pipeOpened.signal();
        abortPipe();
    }
    virtual bool isGrouped() { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = false;
        info.unknownRowsOutput = true;
        // MORE TODO fill in numbers of rows
    }
};


PipeWriterThread::PipeWriterThread(CPipeThroughSlaveActivity & _activity)
   : Thread("PipeWriterThread"), activity(_activity)
{
    input = activity.inputs.item(0);
}

int PipeWriterThread::run()
{
    bool dolog = true;
    MemoryBuffer mb;
    CMemoryRowSerializer mbs(mb);
    bool eos = false;
    int ret = 0;
    try
    {
        loop
        {
            if (eos||activity.abortSoon)
                break;
            OwnedConstThorRow row = input->ungroupedNextRow();
            if (!row.get())
                break;
            if (activity.recreate)
            {
                activity.openPipe(activity.helper->getNameFromRow(row), dolog?"PIPETHROUGH":NULL);
                dolog = false;
                activity.pipeOpened.signal();
            }
            activity.writeTranslatedText(row);
            if (activity.recreate)
            {
                activity.closePipe();
                activity.pipeVerified.wait();
            }
        }
        if (activity.recreate)
        {
            activity.inputExhausted = true;
            activity.pipeOpened.signal();
        }
        else
            activity.closePipe();
    }
    catch (IException *e)
    {
        ActPrintLog(&activity,e,"PipeWriterThread.3");
        if (exc.get())
            e->Release();
        else
            exc.setown(e);
    }
    return ret;
}


CActivityBase *createPipeThroughSlave(CGraphElementBase *container)
{
    return new CPipeThroughSlaveActivity(container);
}

