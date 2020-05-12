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

#include "thlookupjoin.ipp"

#include "thexception.hpp"
#include "thbufdef.hpp"

#include "thlookupjoincommon.hpp"

class CLookupJoinActivityMaster : public CMasterActivity
{
    mptag_t broadcast2MpTag, broadcast3MpTag, lhsDistributeTag, rhsDistributeTag;
    bool isGlobal = false;

    bool isAll() const
    {
        switch (container.getKind())
        {
            case TAKalljoin:
            case TAKalldenormalize:
            case TAKalldenormalizegroup:
                return true;
            default:
                break;
        }
        return false;
    }
public:
    CLookupJoinActivityMaster(CMasterGraphElement * info) : CMasterActivity(info, lookupJoinActivityStatistics)
    {
        isGlobal = !container.queryLocal() && (queryJob().querySlaves()>1);
        if (isGlobal)
        {
            mpTag = container.queryJob().allocateMPTag(); // NB: base takes ownership and free's
            if (!isAll())
            {
                broadcast2MpTag = container.queryJob().allocateMPTag();
                broadcast3MpTag = container.queryJob().allocateMPTag();
                lhsDistributeTag = container.queryJob().allocateMPTag();
                rhsDistributeTag = container.queryJob().allocateMPTag();
            }
        }
    }
    ~CLookupJoinActivityMaster()
    {
        if (isGlobal && !isAll())
        {
            container.queryJob().freeMPTag(broadcast2MpTag);
            container.queryJob().freeMPTag(broadcast3MpTag);
            container.queryJob().freeMPTag(lhsDistributeTag);
            container.queryJob().freeMPTag(rhsDistributeTag);
            // NB: if mpTag is allocated, the activity base class frees
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        if (!isGlobal)
            return;
        serializeMPtag(dst, mpTag);
        if (!isAll())
        {
            serializeMPtag(dst, broadcast2MpTag);
            serializeMPtag(dst, broadcast3MpTag);
            serializeMPtag(dst, lhsDistributeTag);
            serializeMPtag(dst, rhsDistributeTag);
        }
    }
};

CActivityBase *createLookupJoinActivityMaster(CMasterGraphElement *container)
{
    return new CLookupJoinActivityMaster(container);
}
