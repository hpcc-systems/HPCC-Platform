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

// Handling Ungrouped Rollup and Dedup
// (Grouped code in groupdedup)

#ifndef _throllupslave_ipp
#define _throllupslave_ipp

#include "platform.h"
#include "eclhelper.hpp"
#include "slave.ipp"


activityslaves_decl CActivityBase *createDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createGroupDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createGroupRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRollupGroupSlave(CGraphElementBase *container);

#endif
