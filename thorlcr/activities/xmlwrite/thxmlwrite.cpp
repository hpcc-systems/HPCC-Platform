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

#include "jiface.hpp"
#include "jtime.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "thxmlwrite.ipp"

class CXmlWriteActivityMaster : public CWriteMasterBase
{
    IHThorXmlWriteArg *helper;

public:
    CXmlWriteActivityMaster(CMasterGraphElement *info) : CWriteMasterBase(info)
    {
        helper = (IHThorXmlWriteArg *)queryHelper();
    }
    void init()
    {
        assertex(!(helper->getFlags() & TDWextend));
        CWriteMasterBase::init();

        IPropertyTree &props = fileDesc->queryProperties();
        StringBuffer rowTag;
        const char * path = helper->queryIteratorPath();
        if (!path)
            rowTag.append("Row");
        else
        {
            if (*path == '/') path++;
            if (strchr(path, '/')) UNIMPLEMENTED;
            rowTag.append(path);
        }
        props.setProp("@rowTag", rowTag.str());
        props.setProp("@format", "utf8n");
    }
};

CActivityBase *createXmlWriteActivityMaster(CMasterGraphElement *container)
{
    return new CXmlWriteActivityMaster(container);
}

