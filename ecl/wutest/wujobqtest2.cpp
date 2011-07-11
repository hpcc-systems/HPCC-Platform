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

