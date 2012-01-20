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

#ifndef _ROXIEROW_INCL
#define _ROXIEROW_INCL

#ifdef _WIN32
 #ifdef ROXIEHELPER_EXPORTS
  #define ROXIEHELPER_API __declspec(dllexport)
 #else
  #define ROXIEHELPER_API __declspec(dllimport)
 #endif
#else
 #define ROXIEHELPER_API
#endif

#include "roxiemem.hpp"
#include "eclhelper.hpp"

extern ROXIEHELPER_API IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool packed);

#endif
