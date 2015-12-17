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
    CThorStatsCollection diskStats;
    StringAttr fileName;

public:
    CDiskReadMasterBase(CMasterGraphElement *info);
    virtual void init();
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave);
    virtual void validateFile(IDistributedFile *file) { }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb);
    virtual void getActivityStats(IStatisticGatherer & stats);
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx);
};

class CWriteMasterBase : public CMasterActivity
{
    bool publishReplicatedDone;
    Owned<ProgressInfo> replicateProgress;
    CThorStatsCollection diskStats;
    __int64 recordsProcessed;
    bool published;
    StringAttr fileName;
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
    virtual void getActivityStats(IStatisticGatherer & stats);
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void init();
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave);
    virtual void done();
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb);
};


const void *getAggregate(CActivityBase &activity, unsigned partialResults, IRowInterfaces &rowIf, IHThorCompoundAggregateExtra &aggHelper, mptag_t mpTag);
rowcount_t getCount(CActivityBase &activity, unsigned partialResults, rowcount_t limit, mptag_t mpTag);

#endif
