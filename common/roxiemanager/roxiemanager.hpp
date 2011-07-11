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

#ifndef ROXIEQUERYMANAGER_INCL
#define ROXIEQUERYMANAGER_INCL

#ifdef _WIN32
    #ifdef ROXIEMANAGER_EXPORTS
        #define ROXIEMANAGER_API __declspec(dllexport)
    #else
        #define ROXIEMANAGER_API __declspec(dllimport)
    #endif
#else
    #define ROXIEMANAGER_API
#endif

#include "roxiemanagerscm.hpp"

#include "errorlist.h"

#define ROXIEMANAGER_IGNORE_EXCEPTION   0   // not a real exception

#define ROXIEMANAGER_UNRESOLVED_FILE    ROXIE_MGR_START
#define ROXIEMANAGER_MISSING_FILE_PARTS ROXIE_MGR_START+1
#define ROXIEMANAGER_MISSING_ID         ROXIE_MGR_START+2
#define ROXIEMANAGER_SOCKET_ERROR       ROXIE_MGR_START+3
#define ROXIEMANAGER_UNEXPECTION_WU_ERROR ROXIE_MGR_START+4
#define ROXIEMANAGER_FILE_MISMATCH      ROXIE_MGR_START+5
#define ROXIEMANAGER_DEPLOY_FAILED      ROXIE_MGR_START+6
#define ROXIEMANAGER_DALI_LOOKUP_ERROR  ROXIE_MGR_START+7
#define ROXIEMANAGER_ROD_NOT_SUPPORTED  ROXIE_MGR_START+8
#define ROXIEMANAGER_FILE_PERMISSION_ERR ROXIE_MGR_START+9
#define ROXIEMANAGER_FILE_SIZE_ERROR    ROXIE_MGR_START+10

#endif

