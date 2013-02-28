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
#include "thbuf.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thmfilemanager.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thorxmlwrite.hpp"
#include "thxmlwriteslave.ipp"

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
        OwnedRoxieString xmlpath(helper->getXmlIteratorPath());
        if (!xmlpath)
        {
            rowTag.append("Row");
        }
        else
        {
            const char *path = xmlpath;
            if (*path == '/') path++;
            if (strchr(path, '/')) UNIMPLEMENTED;
            rowTag.append(path);
        }

        StringBuffer xmlOutput;
        CommonXmlWriter xmlWriter(helper->getXmlFlags());
        if (!dlfn.isExternal() || firstNode()) // if external, 1 header,footer
        {
            OwnedRoxieString header(helper->getHeader());
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
        if (!dlfn.isExternal() || lastNode()) // if external, 1 header,footer
        {
            OwnedRoxieString footer(helper->getFooter());
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


