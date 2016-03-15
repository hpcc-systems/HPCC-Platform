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
#include "jlzw.hpp"
#include "jtime.hpp"
#include "jfile.ipp"

#include "dafdesc.hpp"

#include "thbuf.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "csvsplitter.hpp"
#include "thactivityutil.ipp"
#include "thdiskbaseslave.ipp"
#include "thdwslave.ipp"

class CDiskWriteSlaveActivity : public CDiskWriteSlaveActivityBase
{
protected:
    virtual void write()
    {
        ActPrintLog("%s",grouped?"Grouped":"Ungrouped");            

        while(!abortSoon)
        {       
            OwnedConstThorRow r = inputStream->nextRow();
            if (!r.get()) {
                if (grouped) {
                    if ((processed & THORDATALINK_COUNT_MASK)!=0)
                        out->putRow(NULL);
                }
                r.setown(inputStream->nextRow());
                if (!r.get())
                    break;
            }
            out->putRow(r.getClear());
            processed++;
        }
    }

public:
    CDiskWriteSlaveActivity(CGraphElementBase *container) : CDiskWriteSlaveActivityBase(container)
    {
    }
};


CActivityBase *createDiskWriteSlave(CGraphElementBase *container)
{
    return new CDiskWriteSlaveActivity(container);
}


//---------------------------------------------------------------------------



class CCsvWriteSlaveActivity : public CDiskWriteSlaveActivity
{
protected:
    IHThorCsvWriteArg *helper;
    CSVOutputStream csvOutput;
    bool singleHF;
protected:
    virtual void write()
    {
        if (!singleHF || firstNode())
        {
            OwnedRoxieString header(helper->queryCsvParameters()->getHeader());
            if (header)
            {
                csvOutput.beginLine();
                csvOutput.writeHeaderLn(strlen(header),header);
                const char * outText = csvOutput.str();
                unsigned outLength = csvOutput.length();

                outraw->write(outLength,outText);
                if (calcFileCrc)
                    fileCRC.tally(outLength, outText);
            }
        }
        while(!abortSoon)
        {
            OwnedConstThorRow r(inputStream->ungroupedNextRow());
            if (!r) 
                break;

            csvOutput.beginLine();
            helper->writeRow((const byte *)r.get(), &csvOutput);
            csvOutput.endLine();

            const char * outText = csvOutput.str();
            unsigned outLength = csvOutput.length();

            outraw->write(outLength,outText);
            if (calcFileCrc)
                fileCRC.tally(outLength, outText);

            processed++;
        }
        if (!singleHF || lastNode())
        {
            OwnedRoxieString footer(helper->queryCsvParameters()->getFooter());
            if (footer)
            {
                csvOutput.beginLine();
                csvOutput.writeHeaderLn(strlen(footer),footer);
                const char * outText = csvOutput.str();
                unsigned outLength = csvOutput.length();

                outraw->write(outLength,outText);
                if (calcFileCrc)
                    fileCRC.tally(outLength, outText);
            }
        }
    }

public:
    CCsvWriteSlaveActivity(CGraphElementBase *container) : CDiskWriteSlaveActivity(container) { }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskWriteSlaveActivity::init(data, slaveData);
        helper = static_cast <IHThorCsvWriteArg *> (queryHelper());

        singleHF = 0 != (ICsvParameters::singleHeaderFooter & helper->queryCsvParameters()->getFlags());
        csvOutput.init(helper->queryCsvParameters(), 0 != container.queryJob().getWorkUnitValueInt("oldCSVoutputFormat", 0));
    }
    virtual bool wantRaw() { return true; }
};


CActivityBase *createCsvWriteSlave(CGraphElementBase *container)
{
    return new CCsvWriteSlaveActivity(container);
}

#if 0
void CsvWriteSlaveActivity::setFormat(IFileDescriptor * desc)
{
    desc->queryAttributes().setProp("@format","csv");
}
#endif
