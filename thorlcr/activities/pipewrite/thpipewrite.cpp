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

#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "thpipewrite.ipp"

//
// CPipeWriteActivityMaster
//

class CPipeWriteActivityMaster : public CMasterActivity
{
    __int64 recordsProcessed;
public:
    CPipeWriteActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        recordsProcessed = 0;
    }
    virtual void done()
    {
        CMasterActivity::done();
        IHThorPipeWriteArg *helper = (IHThorPipeWriteArg *)queryHelper();
        Owned<IWUResult> r;
        Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
        r.setown(wu->updateResultBySequence(helper->getSequence()));
        r->setResultTotalRowCount(recordsProcessed);    
        r->setResultStatus(ResultStatusCalculated);
        r.clear();
        wu.clear();
    }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        if (mb.length()) // if 0 implies aborted out from this slave.
        {
            rowcount_t rc;
            mb.read(rc);
            recordsProcessed += rc;
        }
    }
};

CActivityBase *createPipeWriteActivityMaster(CMasterGraphElement *container)
{
    return new CPipeWriteActivityMaster(container);
}
