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

#ifndef _THACTIVITYMASTER_HPP
#define _THACTIVITYMASTER_HPP

#ifdef ACTIVITYMASTERS_EXPORTS
 #define actmaster_decl DECL_EXPORT
#else
 #define actmaster_decl DECL_IMPORT
#endif


enum masterEvents { ev_unknown, ev_done };

#include "thmfilemanager.hpp"

interface IGetSlaveData : extends IInterface
{
    virtual bool getData(unsigned slave, unsigned part, MemoryBuffer &mb) = 0;
};

struct CSlaveMap : public IArrayOf<IPartDescriptor>, public CInterface
{
};

class CSlavePartMapping : public CSimpleInterface, implements IInterface
{
    CIArrayOf<CSlaveMap> maps;
    UnsignedArray partToNode;
    unsigned fileWidth;
    bool local;
    StringAttr logicalName;
    Linked<IFileDescriptor> fileDesc;
    Linked<IUserDescriptor> userDesc;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSlavePartMapping(const char *logicalName, IFileDescriptor &fileDesc, IUserDescriptor *userDesc, IGroup &localGroup, bool local, bool index, IHash *hash, IDistributedSuperFile *super);
    unsigned queryNode(unsigned part)
    {
        if (part > partToNode.ordinality()) return NotFound;
        return partToNode.item(part);
    }
    unsigned queryParts() { return partToNode.ordinality(); }
    unsigned queryNumMaps() { return maps.ordinality(); }
    unsigned queryMapWidth(unsigned map)
    {
        if (local)
            map = 0;
        if (!maps.isItem(map))
            return 0;
        return maps.item(map).ordinality();
    }
    void serializeFileOffsetMap(MemoryBuffer &mb);
    void getParts(unsigned i, IArrayOf<IPartDescriptor> &parts);
    void serializeMap(unsigned map, MemoryBuffer &mb, IGetSlaveData *extra=NULL);
    static void serializeNullMap(MemoryBuffer &mb);
    static void serializeNullOffsetMap(MemoryBuffer &mb);
};
CSlavePartMapping *getFileSlaveMaps(const char *logicalName, IFileDescriptor &file, IUserDescriptor *userDesc, IGroup &localGroup, bool local=false, bool index=false, IHash *hash=NULL, IDistributedSuperFile *super=NULL);
void checkSuperFileOwnership(IDistributedFile &file);

actmaster_decl void loadMasters();
#endif
