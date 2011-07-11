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

#ifndef _THFUNNELSLAVE_IPP
#define _THFUNNELSLAVE_IPP

#include "platform.h"

#include "jmutex.hpp"
#include "jthread.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"




activityslaves_decl CActivityBase *createFunnelSlave(CGraphElementBase *container);
activityslaves_decl IRowStream *createParallelFunnel(CActivityBase &activity, IRowStream **instreams, unsigned numstreams, const bool &aborted);
activityslaves_decl IRowStream *createParallelFunnel(CActivityBase &activity, IThorDataLink **instreams, unsigned numstreams, bool startInputs, const bool &aborted);
activityslaves_decl CActivityBase *createCombineSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRegroupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNonEmptySlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWaySelectSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWayInputSlave(CGraphElementBase *container);



#endif
