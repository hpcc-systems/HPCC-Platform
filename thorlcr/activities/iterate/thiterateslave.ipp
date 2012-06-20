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

#ifndef _thiterateslave_ipp
#define _thiterateslave_ipp

#include "platform.h"
#include "eclhelper.hpp"        // for IHThorIterateArg
#include "slave.ipp"
#include "thactivityutil.ipp"

activityslaves_decl CActivityBase *createIterateSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalIterateSlave(CGraphElementBase *container);

activityslaves_decl CActivityBase *createChildIteratorSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRawIteratorSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLinkedRawIteratorSlave(CGraphElementBase *container);

activityslaves_decl CActivityBase *createProcessSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalProcessSlave(CGraphElementBase *container);

activityslaves_decl CActivityBase *createStreamedIteratorSlave(CGraphElementBase *container);

#endif
