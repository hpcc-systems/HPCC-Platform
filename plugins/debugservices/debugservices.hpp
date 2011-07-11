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

#ifndef _DEBUGSERVICES_INCL
#define _DEBUGSERVICES_INCL

#ifdef _WIN32
#define DEBUGSERVICES_CALL _cdecl
#ifdef DEBUGSERVICES_EXPORTS
#define DEBUGSERVICES_API __declspec(dllexport)
#else
#define DEBUGSERVICES_API __declspec(dllimport)
#endif
#else
#define DEBUGSERVICES_CALL
#define DEBUGSERVICES_API
#endif

#include "hqlplugins.hpp"

extern "C" {
DEBUGSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
DEBUGSERVICES_API char * DEBUGSERVICES_CALL dsGetBuildInfo(void);
DEBUGSERVICES_API void DEBUGSERVICES_CALL dsSleep(unsigned millis);

void DEBUGSERVICES_API __stdcall dsInitDebugServices(const char *_wuid);
}

#endif

