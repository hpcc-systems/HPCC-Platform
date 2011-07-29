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
