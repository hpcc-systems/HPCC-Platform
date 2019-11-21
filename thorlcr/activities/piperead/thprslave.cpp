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

#include "jio.hpp"

#include "thorpipe.hpp"
#include "thbufdef.hpp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thprslave.ipp"
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
    unsigned flags;
    Owned<IException> verifyPipeException;

protected:

    class CPipeStream : implements ISimpleReadStream, public CSimpleInterface
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
        pipeFinished = false;
        if (!pipe->run(pipeTrace, cmd, globals->queryProp("@externalProgDir"), true, true, true, 0x10000)) // 64K error buffer
        {
            // NB: pipe->run can't rely on the child process failing fast enough to return false here, failure picked up later with stderr context.
            WARNLOG(TE_FailedToCreateProcess, "Failed to create process in %s for : %s", globals->queryProp("@externalProgDir"), cmd);
        }
        registerSelfDestructChildProcess(pipe->getProcessHandle());
        pipeStream->setStream(pipe->getOutputStream());
        readTransformer->setStream(pipeStream);
    }
    void closePipe()
    {
        pipe->closeInput();
    }
    virtual void verifyPipe()
    {
        if (!pipeFinished)
        {
            pipeFinished = true;
            HANDLE pipeProcess = pipe->getProcessHandle();
            retcode = pipe->wait();
            unregisterSelfDestructChildProcess(pipeProcess);
            if (retcode!=0 && !(flags & TPFnofail))
            {
                StringBuffer stdError;
                if (pipe->hasError())
                {
                    try
                    {
                        char error[512];
                        size32_t sz = pipe->readError(sizeof(error), error);
                        if (sz && sz!=(size32_t)-1)
                            stdError.append(sz, error).append("'");
                    }
                    catch (IException *e)
                    {
                        ActPrintLog(e, "Error reading pipe stderr");
                        e->Release();
                    }
                }
                if (START_FAILURE == retcode) // PIPE process didn't start at all, START_FAILURE is our own error code
                    verifyPipeException.setown(MakeActivityException(this, TE_PipeReturnedFailure, "Process failed to start: %s - PIPE(%s)", stdError.str(), pipeCommand.get()));
                else
                    verifyPipeException.setown(MakeActivityException(this, TE_PipeReturnedFailure, "Process returned %d:%s - PIPE(%s)", retcode, stdError.str(), pipeCommand.get()));
                throw verifyPipeException.getLink();
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
    CPipeSlaveBase(CGraphElementBase *_container)
        : CSlaveActivity(_container)
    {
        pipeFinished = true;
        retcode = 0;
        flags = 0;
        pipe.setown(createPipeProcess(globals->queryProp("@allowedPipePrograms")));
        pipeStream = new CPipeStream(this);
    }
    ~CPipeSlaveBase()
    {
        ::Release(pipeStream);
    }
};


//---------------------------------------------------------------------------

class CPipeReadSlaveActivity : public CPipeSlaveBase
{
    typedef CPipeSlaveBase PARENT;

protected:
    IHThorPipeReadArg *helper;
    Owned<IThorRowInterfaces> inrowif;
    bool needTransform;

    bool eof;

public:
    CPipeReadSlaveActivity(CGraphElementBase *_container) 
        : CPipeSlaveBase(_container)
    {
        helper = static_cast <IHThorPipeReadArg *> (queryHelper());
        flags = helper->getPipeFlags();
        needTransform = false;

        if (needTransform)
            inrowif.setown(createRowInterfaces(helper->queryDiskRecordSize()));
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    CATCH_NEXTROW()
    {   
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eof || abortSoon)
            return NULL;
        try
        {
            for (;;)
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
                e->Release();
            }
            else
                throw;      
        }
        eof = true;
        return NULL;
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        OwnedRoxieString xmlIteratorPath(helper->getXmlIteratorPath());
        IThorRowInterfaces *_inrowif = needTransform ? inrowif.get() : this;
        readTransformer.setown(createReadRowStream(_inrowif->queryRowAllocator(), _inrowif->queryRowDeserializer(), helper->queryXmlTransformer(), helper->queryCsvTransformer(), xmlIteratorPath, flags));
        eof = false;
        OwnedRoxieString pipeProgram(helper->getPipeProgram());
        openPipe(pipeProgram, "PIPEREAD");
    }
    virtual void stop() override
    {
        if (hasStarted())
        {
            readTrailing();
            verifyPipe();
        }
        PARENT::stop();
    }
    virtual void abort()
    {
        CPipeSlaveBase::abort();
        abortPipe();
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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
    IException * checkError()
    {
        return exc.getClear();
    }

protected:
    CPipeThroughSlaveActivity &activity;
    IEngineRowStream *inputStream;
    Owned<IException> exc;
};


//---------------------------------------------------------------------------

class CPipeThroughSlaveActivity : public CPipeSlaveBase
{
    typedef CPipeSlaveBase PARENT;

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
    CPipeThroughSlaveActivity(CGraphElementBase *_container)
        : CPipeSlaveBase(_container)
    {
        helper = static_cast <IHThorPipeThroughArg *> (queryHelper());
        pipeWriter = NULL;
        grouped = false;
        flags = helper->getPipeFlags();
        recreate = helper->recreateEachRow();
        grouped = 0 != (flags & TPFgroupeachrow);
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    ~CPipeThroughSlaveActivity()
    {
        ::Release(pipeWriter);
    }
    virtual void verifyPipe() override
    {
        if (!pipeFinished)
        {
            // If verifyPipe catches exception starting pipe program, clear follow-on errors from pipeWriter thread
            try
            {
                PARENT::verifyPipe();
            }
            catch (IException *e)
            {
                retcode = 0;
                ::Release(pipeWriter->checkError());
                throw;
            }
        }
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        OwnedRoxieString xmlIterator(helper->getXmlIteratorPath());
        readTransformer.setown(createReadRowStream(queryRowAllocator(), queryRowDeserializer(), helper->queryXmlTransformer(), helper->queryCsvTransformer(), xmlIterator, flags));
        readTransformer->setStream(pipeStream); // NB the pipe process stream is provided to pipeStream after pipe->run()

        eof = anyThisGroup = inputExhausted = false;
        firstRead = true;

        if (!writeTransformer)
        {
            writeTransformer.setown(createPipeWriteXformHelper(flags, helper->queryXmlOutput(), helper->queryCsvOutput(), ::queryRowInterfaces(queryInput(0))->queryRowSerializer()));
            writeTransformer->ready();
        }
        if (!recreate)
        {
            OwnedRoxieString pipeProgram(helper->getPipeProgram());
            openPipe(pipeProgram, "PIPETHROUGH");
        }
        pipeWriter = new PipeWriterThread(*this);
        pipeWriter->start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eof || abortSoon)
            return NULL;
        for (;;)
        {
            if (recreate && firstRead)
            {
                pipeOpened.wait();
                Owned<IException> wrexc = pipeWriter->checkError();
                if (wrexc)
                    throw wrexc.getClear();
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
    virtual void stop() override
    {
        if (!hasStarted())
        {
            PARENT::stop();
            return;
        }
        abortSoon = true;
        readTrailing();
        if (recreate)
            pipeVerified.signal();
        Owned<IException> wrexc = pipeWriter->joinExc();
        PARENT::stop();
        verifyPipe();
        Owned<IException> hadVerifyPipeException = verifyPipeException.getClear(); // ensured cleared in case in CQ
        if (wrexc && !hadVerifyPipeException)
            throw wrexc.getClear();
        if (retcode!=0 && !(flags & TPFnofail))
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
    virtual bool isGrouped() const override { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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
    inputStream = activity.inputStream;
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
        for (;;)
        {
            if (eos||activity.abortSoon)
                break;
            OwnedConstThorRow row = inputStream->ungroupedNextRow();
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
        if (activity.recreate)
            activity.pipeOpened.signal();
    }
    return ret;
}


CActivityBase *createPipeThroughSlave(CGraphElementBase *container)
{
    return new CPipeThroughSlaveActivity(container);
}

