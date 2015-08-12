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
#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "wujobq.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "daclient.hpp"
#include "dasds.hpp"

#include "workunit.hpp"





bool switchWorkunitQueue(const char *wuid, const char *cluster)
{
    class cQswitcher: public CInterface, implements IQueueSwitcher
    {
    public:
        IMPLEMENT_IINTERFACE;
        void * getQ(const char * qname, const char * wuid)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            return q->take(wuid);
        }
        void putQ(const char * qname, const char * wuid, void * qitem)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            q->enqueue((IJobQueueItem *)qitem);
        }
        bool isAuto()
        {
            return false;
        }
    } switcher;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if (!wu)
        return false;
    return wu->switchThorQueue(cluster, &switcher);
}

