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
#include "eclhelper.hpp"
#include "thbufdef.hpp"
#include "mptag.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"

#include "../hashdistrib/thhashdistrib.ipp"
#include "thfetch.ipp"

class CFetchActivityMaster : public CMasterActivity
{
    Owned<CSlavePartMapping> mapping;
    MemoryBuffer offsetMapMb;
    SocketEndpoint *endpoints;

protected:
    Owned<IDistributedFile> fetchFile;
public:
    CFetchActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        endpoints = NULL;
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
    }
    ~CFetchActivityMaster()
    {
        if (endpoints) free(endpoints);
    }
    virtual void init()
    {
        IHThorFetchArg *helper = (IHThorFetchArg *)queryHelper();
        fetchFile.setown(queryThorFileManager().lookup(container.queryJob(), helper->getFileName(), false, 0 != (helper->getFetchFlags() & FFdatafileoptional), true));
        if (fetchFile)
        {
            queryThorFileManager().noteFileRead(container.queryJob(), fetchFile);

            Owned<IFileDescriptor> fileDesc = getConfiguredFileDescriptor(*fetchFile);
            void *ekey;
            size32_t ekeylen;
            helper->getFileEncryptKey(ekeylen,ekey);
            bool encrypted = fileDesc->queryProperties().getPropBool("@encrypted");
            if (0 != ekeylen)
            {
                memset(ekey,0,ekeylen);
                free(ekey);
                if (!encrypted)
                {
                    Owned<IException> e = MakeActivityWarning(&container, TE_EncryptionMismatch, "Ignoring encryption key provided as file '%s' was not published as encrypted", helper->getFileName());
                    container.queryJob().fireException(e);
                }
            }
            else if (encrypted)
                throw MakeActivityException(this, 0, "File '%s' was published as encrypted but no encryption key provided", helper->getFileName());
            mapping.setown(getFileSlaveMaps(fetchFile->queryLogicalName(), *fileDesc, container.queryJob().queryUserDescriptor(), container.queryJob().querySlaveGroup(), container.queryLocalOrGrouped(), false, NULL, fetchFile->querySuperFile()));
            mapping->serializeFileOffsetMap(offsetMapMb);
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (fetchFile)
        {
            mapping->serializeMap(slave, dst);
            dst.append(offsetMapMb);
        }
        else
        {
            CSlavePartMapping::serializeNullMap(dst);
            CSlavePartMapping::serializeNullOffsetMap(dst);
        }
        if (!container.queryLocalOrGrouped())
            dst.append((int)mpTag);
    }
};

class CCsvFetchActivityMaster : public CFetchActivityMaster
{
public:
    CCsvFetchActivityMaster(CMasterGraphElement *info) : CFetchActivityMaster(info) { }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        CFetchActivityMaster::serializeSlaveData(dst, slave);

        if (fetchFile)
        {
            IHThorFetchArg *helper = (IHThorFetchArg *)queryHelper();
            fetchFile->queryAttributes().serialize(dst);
        }
    }
};

class CXmlFetchActivityMaster : public CFetchActivityMaster
{
public:
    CXmlFetchActivityMaster(CMasterGraphElement *info) : CFetchActivityMaster(info)
    {
    }
};


CActivityBase *createFetchActivityMaster(CMasterGraphElement *container)
{
    return new CFetchActivityMaster(container);
}

CActivityBase *createCsvFetchActivityMaster(CMasterGraphElement *container)
{
    return new CCsvFetchActivityMaster(container);
}

CActivityBase *createXmlFetchActivityMaster(CMasterGraphElement *container)
{
    return new CXmlFetchActivityMaster(container);
}

