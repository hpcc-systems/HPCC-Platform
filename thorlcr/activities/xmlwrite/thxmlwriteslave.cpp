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

#include "thbuf.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thmfilemanager.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thorxmlwrite.hpp"
#include "thxmlwriteslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/xmlwrite/thxmlwriteslave.cpp $ $Id: thxmlwriteslave.cpp 63466 2011-03-25 11:09:22Z jsmith $");


class CXmlWriteSlaveActivity : public CDiskWriteSlaveActivityBase
{
    IHThorXmlWriteArg *helper;

public:
    CXmlWriteSlaveActivity(CGraphElementBase *container) : CDiskWriteSlaveActivityBase(container)
    {
        helper = static_cast <IHThorXmlWriteArg *> (queryHelper());
    }
    virtual void write()
    {
        StringBuffer rowTag;
        const char * path = helper->queryIteratorPath();
        if (!path)
        {
            rowTag.append("Row");
        }
        else
        {
            if (*path == '/') path++;
            if (strchr(path, '/')) UNIMPLEMENTED;
            rowTag.append(path);
        }

        StringBuffer xmlOutput;
        CommonXmlWriter xmlWriter(helper->getXmlFlags());
        if (!dlfn.isExternal() || container.queryJob().queryMyRank() == 1) // if external, 1 header,footer
        {
            const char * header = helper->queryHeader();
            if (header)
                xmlOutput.clear().append(header);
            else
                xmlOutput.clear().append("<Dataset>").newline();
            outraw->write(xmlOutput.length(), xmlOutput.toCharArray());
            if (calcFileCrc)
                fileCRC.tally(xmlOutput.length(), xmlOutput.toCharArray());
        }
        while(!abortSoon)
        {
            OwnedConstThorRow row = input->ungroupedNextRow();
            if (!row)
                break;
            xmlWriter.clear().outputBeginNested(rowTag, false);
            helper->toXML((const byte *)row.get(), xmlWriter);
            xmlWriter.outputEndNested(rowTag);
            outraw->write(xmlWriter.length(), xmlWriter.str());
            if (calcFileCrc)
                fileCRC.tally(xmlWriter.length(), xmlWriter.str());
            processed++;
        }
        if (!dlfn.isExternal() || container.queryJob().queryMyRank() == container.queryJob().querySlaves()) // if external, 1 header,footer
        {
            const char * footer = helper->queryFooter();
            if (footer)
                xmlOutput.clear().append(footer);
            else
                xmlOutput.clear().append("</Dataset>").newline();
            outraw->write(xmlOutput.length(), xmlOutput.toCharArray());
            if (calcFileCrc)
                fileCRC.tally(xmlOutput.length(), xmlOutput.toCharArray());
        }
    }
    virtual bool wantRaw() { return true; }
};

CActivityBase *createXmlWriteSlave(CGraphElementBase *container)
{
    return new CXmlWriteSlaveActivity(container);
}


