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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/diskwrite/thdwslave.cpp $ $Id: thdwslave.cpp 63406 2011-03-23 17:33:52Z jsmith $");


class CDiskWriteSlaveActivity : public CDiskWriteSlaveActivityBase
{
protected:
    virtual void write()
    {
        ActPrintLog("%s",grouped?"Grouped":"Ungrouped");            

        while(!abortSoon)
        {       
            OwnedConstThorRow r = input->nextRow();
            if (!r.get()) {
                if (grouped) {
                    if ((processed & THORDATALINK_COUNT_MASK)!=0)
                        out->putRow(NULL);
                }
                r.setown(input->nextRow());
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
        if (!singleHF || 1 == container.queryJob().queryMyRank())
        {
            const char * header = helper->queryCsvParameters()->queryHeader();
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
            OwnedConstThorRow r(input->ungroupedNextRow());
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
        if (!singleHF || container.queryJob().querySlaves() == container.queryJob().queryMyRank())
        {
            const char * footer = helper->queryCsvParameters()->queryFooter();
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
    desc->queryProperties().setProp("@format","csv");
}
#endif
