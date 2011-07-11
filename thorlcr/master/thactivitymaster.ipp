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

#ifndef _THACTIVITYMASTER_IPP
#define _THACTIVITYMASTER_IPP

#include "jthread.hpp"
#include "jset.hpp"

#include "dasess.hpp"
#include "deftype.hpp"


#include "thgraphmaster.ipp"
#include "eclhelper.hpp"        // for IHThorArg etc.


#include "thactivitymaster.hpp"

WUFileKind getDiskOutputKind(unsigned flags);

void updateActivityResult(IConstWorkUnit &workunit, unsigned helperFlags, unsigned sequence, const char *logicalFilename, unsigned __int64 recordCount);
void checkFormatCrc(CActivityBase *activity, IDistributedFile *file, unsigned helperCrc, bool index);

#endif
