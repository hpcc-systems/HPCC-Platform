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
#include "jtime.hpp"
#include "jfile.ipp"

#include "thorpipe.hpp"
#include "thpwslave.ipp"
#include "thexception.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/pipewrite/thpwslave.cpp $ $Id: thpwslave.cpp 65274 2011-06-09 13:42:22Z jsmith $");

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
            openPipe(helper->getPipeProgram());
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
            throw MakeActivityException(this, TE_FailedToCreateProcess, "PIPEWRITE: Failed to create process in %s for : %s", globals->queryProp("@externalProgDir"), cmd);
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
            throw MakeActivityException(this, TE_PipeReturnedFailure, "Process returned %d:%s - PIPE(%s)", container.queryId(), retcode, stdError.str(), pipeCommand.get());    
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

