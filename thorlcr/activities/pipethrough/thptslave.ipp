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

#ifndef _THPTSLAVE_IPP
#define _THPTSLAVE_IPP

#include "slave.ipp"

#include "jsocket.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "thactivityutil.ipp"

#include "eclhelper.hpp" // tmp for IHThor..Arg interfaces.

#define RECORD_BUFFER_SIZE  64 * 1024       // 64k




activityslaves_decl CActivityBase *createPipeThroughSlave(CGraphElementBase *container);



#endif
