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

#ifndef _THMERGESLAVE_IPP
#define _THMERGESLAVE_IPP

#include "platform.h"

#include "jmutex.hpp"
#include "jthread.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"

#include "thsortu.hpp"
#include "jsort.hpp"


activityslaves_decl CActivityBase *createLocalMergeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createGlobalMergeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWayMergeActivity(CGraphElementBase *container);

#endif
