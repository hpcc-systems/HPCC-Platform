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

#include "thexception.hpp"
#include "thmfilemanager.hpp"


#include "eclhelper.hpp"
#include "deftype.hpp"

#include "thdiskwrite.ipp"

class CDiskWriteActivityMaster : public CWriteMasterBase
{
public:
    CDiskWriteActivityMaster(CMasterGraphElement *info) : CWriteMasterBase(info) {}
    void init()
    {
        CWriteMasterBase::init();

        IHThorDiskWriteArg *helper=(IHThorDiskWriteArg *)queryHelper();
        IOutputMetaData *irecsize = helper->queryDiskRecordSize()->querySerializedMeta();
        IPropertyTree &props = fileDesc->queryProperties();
        if (0 != (helper->getFlags() & TDXgrouped))
            props.setPropBool("@grouped", true);
        if (irecsize->isFixedSize()) // fixed size
        {
            size32_t rSz = irecsize->getMinRecordSize();
            if (0 != (helper->getFlags() & TDXgrouped))
                ++rSz;
            props.setPropInt("@recordSize", rSz);
        }
        props.setPropInt("@formatCrc", helper->getFormatCrc());
    }
};

//---------------------------------------------------------------------------

class CsvWriteActivityMaster : public CWriteMasterBase
{
public:
    CsvWriteActivityMaster(CMasterGraphElement *info) : CWriteMasterBase(info) {}
    void init()
    {
        CWriteMasterBase::init();
        IPropertyTree &props = fileDesc->queryProperties();
        props.setPropBool("@csv", true);
    }
    void done()
    {
        fileDesc->queryProperties().setProp("@format", "utf8n");
        CWriteMasterBase::done();
    }
};

CActivityBase *createDiskWriteActivityMaster(CMasterGraphElement *info)
{
    return new CDiskWriteActivityMaster(info);
}

CActivityBase *createCsvWriteActivityMaster(CMasterGraphElement *info)
{
    return new CsvWriteActivityMaster(info);
}

