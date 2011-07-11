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

#ifndef DAFTDIR_HPP
#define DAFTDIR_HPP

#include "daft.hpp"


void doDirectory(const char * directory, IGroup * machines, IPropertyTree * options, IPropertyTree * result);
void doPhysicalCopy(IPropertyTree * source, const char * target, IPropertyTree * options, IDaftCopyProgress * progress);

extern DALIFT_API bool processDirCommand(ISocket *, MemoryBuffer & cmd, MemoryBuffer & result);
extern DALIFT_API bool processPhysicalCopyCommand(ISocket *, MemoryBuffer & cmd, MemoryBuffer & result);

#endif
