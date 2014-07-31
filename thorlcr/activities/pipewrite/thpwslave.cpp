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


#include "jio.hpp"
#include "jtime.hpp"
#include "jfile.ipp"

#include "thorpipe.hpp"
#include "thpwslave.ipp"
#include "thexception.hpp"

#define PIPEWRITE_BUFFER_SIZE (0x10000)

class CPipeWriteSlaveActivity : public ProcessSlaveActivity
{
private:
    IHThorPipeWriteArg *helper;
    bool recreate;
    Owned<IPipeProcess> pipe;
    Owned<IPipeWriteXformHelper> writeTransformer;
    StringAttr pipeCommand;
    bool pipeOpen;
    IThorDataLink *input;

public:
    CPipeWriteSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        helper = static_cast <IHThorPipeWriteArg *> (queryHelper());
        pipe.setown(createPipeProcess(globals->queryProp("@allowedPipePrograms")));
        pipeOpen = false;
    }
    ~CPipeWriteSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        recreate = helper->recreateEachRow();
    }
    void open()
    {
        assertex(helper);
        if (!recreate)
        {
            OwnedRoxieString pipeProgram(helper->getPipeProgram());
            openPipe(pipeProgram);
        }
    }
    void abort()
    {
        if (pipeOpen)
        {
            unregisterSelfDestructChildProcess(pipe->getProcessHandle());
            pipe->abort();
        }
        ProcessSlaveActivity::abort();
    }
    void close()
    {
        if (!recreate)
            closePipeAndVerify();
    }
    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        if(!pipe->run("PIPEWRITE", cmd, globals->queryProp("@externalProgDir"), true, false, true, 0x10000 )) // 64K error buffer
            throw MakeActivityException(*this, TE_FailedToCreateProcess, "PIPEWRITE: Failed to create process in %s for : %s", globals->queryProp("@externalProgDir"), cmd);
        pipeOpen = true;
        registerSelfDestructChildProcess(pipe->getProcessHandle());
        writeTransformer->writeHeader(pipe);
    }
    void closePipeAndVerify()
    {
        if (!pipeOpen)
            return;
        pipeOpen = false;
        writeTransformer->writeFooter(pipe);

        pipe->closeInput();
        HANDLE pipeProcess = pipe->getProcessHandle();
        unsigned retcode = pipe->wait();
        unregisterSelfDestructChildProcess(pipeProcess);
        if (retcode!=0 && !(helper->getPipeFlags() & TPFnofail))
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
            throw MakeActivityException(*this, TE_PipeReturnedFailure, "Process returned %d:%s - PIPE(%s)", retcode, stdError.str(), pipeCommand.get());
        }
    }
    void process()
    {
        input = inputs.item(0);
        startInput(input);
        if (!writeTransformer)
        {
            writeTransformer.setown(createPipeWriteXformHelper(helper->getPipeFlags(), helper->queryXmlOutput(), helper->queryCsvOutput(), ::queryRowInterfaces(input)->queryRowSerializer()));
            writeTransformer->ready();
        }
        processed = THORDATALINK_STARTED;
        try
        {
            ActPrintLog("process");

            open(); 
            write();
        }
        catch (IException *e)
        {
            try { close(); } catch (CATCHALL) { ActPrintLog("Error closing file"); }
            ActPrintLog(e, "exception");
            throw;
        }
        close();
        ActPrintLog("Wrote %"RCPF"d records", processed & THORDATALINK_COUNT_MASK);
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }

    }
    void processDone(MemoryBuffer &mb)
    {
        rowcount_t _processed = processed & THORDATALINK_COUNT_MASK;
        mb.append(_processed);
    }
    void write()
    {
        ActPrintLog("write");           
        while(!abortSoon)
        {
            OwnedConstThorRow row = input->ungroupedNextRow();
            if (!row) 
                break;
            if (recreate)
                openPipe(helper->getNameFromRow(row.get()));
            writeTransformer->writeTranslatedText(row, pipe);
            if(recreate)
                closePipeAndVerify();
            processed++;
        }
    }
};

CActivityBase *createPipeWriteSlave(CGraphElementBase *container)
{
    return new CPipeWriteSlaveActivity(container);
}

