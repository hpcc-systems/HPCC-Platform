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

#ifndef _THDISKBASE_IPP
#define _THDISKBASE_IPP

#include "dautils.hpp"
#include "thactivitymaster.ipp"
#include "eclhelper.hpp"
#include "thorfile.hpp"


class CDiskReadMasterBase : public CMasterActivity
{
protected:
    StringArray subfileLogicalFilenames;
    Owned<IFileDescriptor> fileDesc;
    Owned<CSlavePartMapping> mapping;
    IHash *hash;
    Owned<ProgressInfo> inputProgress;

public:
    CDiskReadMasterBase(CMasterGraphElement *info);
    void init();
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave);
    void done();
    virtual void validateFile(IDistributedFile *file) { }
    void deserializeStats(unsigned node, MemoryBuffer &mb);
    void getXGMML(unsigned idx, IPropertyTree *edge);
};

class CWriteMasterBase : public CMasterActivity
{
    bool publishReplicatedDone;
    Owned<ProgressInfo> replicateProgress;
    __int64 recordsProcessed;
    bool published;
    CDfsLogicalFileName dlfn;
protected:
    StringArray clusters;
    Owned<IFileDescriptor> fileDesc;
    IHThorDiskWriteArg *diskHelperBase;
    unsigned targetOffset;

    void publish();
public:
    CWriteMasterBase(CMasterGraphElement *info);
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb);
    virtual void getXGMML(IWUGraphProgress *progress, IPropertyTree *node);
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void init();
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave);
    virtual void done();
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb);
};


const void *getAggregate(CActivityBase &activity, unsigned partialResults, IRowInterfaces &rowIf, IHThorCompoundAggregateExtra &aggHelper, mptag_t mpTag);
rowcount_t getCount(CActivityBase &activity, unsigned partialResults, rowcount_t limit, mptag_t mpTag);

#endif
