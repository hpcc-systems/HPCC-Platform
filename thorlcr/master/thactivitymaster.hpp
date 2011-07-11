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

#ifndef _THACTIVITYMASTER_HPP
#define _THACTIVITYMASTER_HPP

#ifdef _WIN32
 #ifdef ACTIVITYMASTERS_EXPORTS
  #define actmaster_decl __declspec(dllexport)
 #else
  #define actmaster_decl __declspec(dllimport)
 #endif
#else
 #define actmaster_decl
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
    unsigned queryMapWidth(unsigned map) { return maps.isItem(map)?maps.item(map).ordinality():0; }
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
