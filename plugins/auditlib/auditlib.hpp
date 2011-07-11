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

#ifndef AUDITLIB_INCL
#define AUDITLIB_INCL

#ifdef _WIN32
#define AUDITLIB_CALL _cdecl
#ifdef AUDITLIB_EXPORTS
#define AUDITLIB_API __declspec(dllexport)
#else
#define AUDITLIB_API __declspec(dllimport)
#endif
#else
#define AUDITLIB_CALL
#define AUDITLIB_API
#endif

#include "hqlplugins.hpp"

extern "C" {
AUDITLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
AUDITLIB_API bool alAudit(unsigned typeLen, char const * type, unsigned msgLen, char const * msg);
AUDITLIB_API bool alAuditData(unsigned typeLen, char const * type, unsigned msgLen, char const * msg, unsigned dataLen, void const * dataBlock);
}

#endif
