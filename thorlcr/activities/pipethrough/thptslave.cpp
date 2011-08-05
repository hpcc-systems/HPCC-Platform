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


#include "platform.h"
#include "jio.hpp"


#include "thorpipe.hpp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thptslave.ipp"
#include "thcrc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"

//---------------------------------------------------------------------------

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
    bool                            writeBlock;
    IOutputRowSerializer *          serializer;
    Owned<IException>               exc;
};





//---------------------------------------------------------------------------

class CPipeThroughSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    friend class PipeWriterThread;
protected:
    IHThorPipeThroughArg * helper;
    bool recreate;
    PipeWriterThread * pipeWriter;
    IRecordSize * outputInclRecordSize;
    IRecordSize * outputExclRecordSize;

    unsigned bufferSize;
    MemoryBuffer readBuffer;

    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    bool eof;
    bool pipeFinished;
    unsigned retcode;

    bool inputExhausted;
    bool firstRead;
    Semaphore pipeOpened;
    Semaphore pipeVerified;
    Owned<IReadRowStream> readTransformer;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    virtual bool isGrouped() { return false; }

    class CPipeStream : public CSimpleInterface, implements ISimpleReadStream
    {
        CPipeThroughSlaveActivity *parent;
        Owned<ISimpleReadStream> stream;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPipeStream(CPipeThroughSlaveActivity *_parent) : parent(_parent) { }
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
                    ::ActPrintLog(parent, "PIPETHROUGH: read aborted");
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

    CPipeThroughSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        pipeWriter = NULL;
        pipeFinished = true;
        retcode = 0;
        pipe.setown(createPipeProcess(globals->queryProp("@allowedPipePrograms")));
        pipeStream = new CPipeStream(this);
    }
    ~CPipeThroughSlaveActivity()
    {
        ::Release(pipeStream);
        ::Release(outputInclRecordSize);
        ::Release(pipeWriter);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop
        {
            if (eof||abortSoon)
                break;
            if (recreate && firstRead)
            {
                pipeOpened.wait();
                if (inputExhausted)
                {
                    eof = true;
                    return NULL;
                }
                firstRead = false;
                if (readTransformer->eos())
                    eof = true;
            }
            if (eof||abortSoon)
                break;
            try {
                const void *ret = readTransformer->next();
                if (ret)
                {
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
                    ActPrintLog("PIPETHROUGH: WARNING: input terminated for pipe with : %s", e->errorMessage(s).str());
                    e->Release();
                }
                else
                    throw;
            }
            if (!recreate) {
                eof = true;
                break;
            }
            pipeVerified.signal();
            firstRead = true;
        }
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = false;
        info.unknownRowsOutput = true;
        // MORE TODO fill in numbers of rows
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorPipeThroughArg *> (queryHelper());
        recreate = helper->recreateEachRow();
        outputExclRecordSize = helper->queryOutputMeta();
        outputInclRecordSize = LINK(outputExclRecordSize);

        readTransformer.setown(createReadRowStream(queryRowAllocator(), queryRowDeserializer(), helper->queryXmlTransformer(), helper->queryCsvTransformer(), helper->queryXmlIteratorPath(), helper->getPipeFlags()));
        readTransformer->setStream(pipeStream); // NB the pipe process stream is provided to pipeStream after pipe->run()

        appendOutputLinked(this);
    }
    void readTrailing()
    {
        if (!pipeFinished&&pipe->hasOutput()) {
            ActPrintLog("PIPETHROUGH: reading trailing");
            try {
                void *buf = readBuffer.clear().reserve(RECORD_BUFFER_SIZE);
                size32_t read;
                do {
                    read = pipe->read(RECORD_BUFFER_SIZE, buf);
                } while ((read!=0)&&(read!=(size32_t)-1));
            }
            catch (IException *e)
            {
                e->Release();
            }
        }
    }
    void kill()
    {
        inputExhausted = true;
        pipeOpened.signal();
        readTrailing();
        ActPrintLog("PIPETHROUGH: waiting for pipe writer");
        Owned<IException> wrexc;
        if (pipeWriter)
            pipeWriter->join();
        pipe->wait();
        if (wrexc)
            throw wrexc.getClear();
        ActPrintLog("PIPETHROUGH: kill exit");
        CSlaveActivity::kill();
    }
    void open()
    {
        ActPrintLog("PIPETHROUGH:");
        assertex(helper);
        if(!recreate)
            openPipe(helper->getPipeProgram(),true);
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
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        inputExhausted = false;
        firstRead = true;
        open();
        ActPrintLog("PIPETHROUGH");
        startInput(inputs.item(0));
        dataLinkStart("PIPETHROUGH", container.queryId());
        pipeWriter = new PipeWriterThread(*this);
        pipeWriter->start();
    }
    void stop()
    {
        abortSoon = true;
        readTrailing();
        if(recreate)
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
    void abort()
    {
        inputExhausted = true;
        pipeOpened.signal();
        unregisterSelfDestructChildProcess(pipe->getProcessHandle());
        pipe->abort();
        CSlaveActivity::abort();
    }
    void openPipe(char const * cmd, bool dolog)
    {
        pipeCommand.setown(cmd);
        if (!pipe->run(dolog?"PIPETHROUGH":NULL, cmd, globals->queryProp("@externalProgDir"), true, true, true, 0x10000)) // 64K error buffer
            throw MakeThorException(TE_FailedToCreateProcess, "PIPETHROUGH: Failed to create process in %s for : %s", globals->queryProp("@externalProgDir"), cmd);
        pipeFinished = false;
        registerSelfDestructChildProcess(pipe->getProcessHandle());
        pipeStream->setStream(pipe->getOutputStream());
    }
    void closePipe()
    {
        pipe->closeInput();
    }
    void verifyPipe()
    {
        if (!pipeFinished) {
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
};


PipeWriterThread::PipeWriterThread(CPipeThroughSlaveActivity & _activity)
   : Thread("PipeWriterThread"), activity(_activity)
{
    input = activity.inputs.item(0);
    serializer = queryRowSerializer(input);
    writeBlock = !activity.recreate;

}


int PipeWriterThread::run()
{
    bool dolog = true;
    MemoryBuffer mb;
    CMemoryRowSerializer mbs(mb);
    bool eos=false;
    int ret = 0;
    while (!eos&&!activity.abortSoon) {
        OwnedConstThorRow row = input->ungroupedNextRow();
        if (row.get())
            serializer->serialize(mbs,(const byte *)row.get());
        else
            eos = true;
        size32_t sizeWritten;
        if (writeBlock) {
            if ((mb.length()>PIPETHROUGH_BUFF_SIZE)||((mb.length()>0)&&eos)) {
                if (activity.pipe->hasInput()) {
                    try {
                        sizeWritten = activity.pipe->write(mb.length(),mb.bufferBase());
                    }
                    catch (IException *e)
                    {
                        ActPrintLog(&activity,e,"PipeWriterThread.1");
                        ret = 1;
                        exc.setown(e);
                        activity.abortSoon = true;
                        break;
                    }
                    if (sizeWritten != mb.length())
                        break;
                }
                mb.clear();
            }
        }
        else if (!eos) {
            const void *cur = mb.bufferBase();
            if (activity.recreate)
            {
                activity.openPipe(activity.helper->getNameFromRow(cur),dolog);
                dolog = false;
                activity.pipeOpened.signal();
            }
            if (activity.pipe->hasInput()) {
                try {
                    sizeWritten = activity.pipe->write(mb.length(), cur);
                }
                catch (IException *e)
                {
                    ActPrintLog(&activity,e,"PipeWriterThread.2");
                    ret = 2;
                    exc.setown(e);
                    activity.abortSoon = true;
                    break;
                }
                if (sizeWritten != mb.length())
                    break;
            }
            if(activity.recreate)
            {
                activity.closePipe();
                activity.pipeVerified.wait();
            }
            mb.clear();
        }
    }
    if(activity.recreate)
    {
        activity.inputExhausted = true;
        activity.pipeOpened.signal();
    }
    try {
        activity.closePipe(); // may have already been done in recreate case but doesn't matter
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

